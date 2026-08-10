/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.log.files.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.KernelVersionProviders.fixed;
import static org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent.NULL;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;

import java.io.IOException;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.entry.LogSegments;
import org.neo4j.kernel.impl.transaction.log.entry.v202608.DetachedCheckpointLogEntrySerializerV2026_08;
import org.neo4j.kernel.impl.transaction.log.entry.v522.DetachedCheckpointLogEntrySerializerV5_22;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.Neo4jLayoutExtension;

@Neo4jLayoutExtension
@ExtendWith(LifeExtension.class)
class EnvelopedCheckpointLogFileTest {
    // Hardcoding things in the test to the version before KernelVersion.VERSION_CHECKPOINT_POWER_OF_2_IN_ENVELOPES to
    // be able to come up with a segment size checkpoints will be split on
    private static final int FORCE_SPLIT_CHECKPOINT_SEGMENT_SIZE = 1024;

    @Inject
    private DatabaseLayout databaseLayout;

    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private LifeSupport life;

    private final long rotationThreshold = ByteUnit.kibiBytes(1);
    private final TransactionIdStore transactionIdStore = new SimpleTransactionIdStore(
            2L, 3L, KernelVersion.V2026_07, 0, BASE_TX_COMMIT_TIMESTAMP, UNKNOWN_CONSENSUS_INDEX, 0, 0);
    private CheckpointFile checkpointFile;

    @BeforeEach
    void setUp() throws IOException {
        LogFiles logFiles = buildLogFiles();
        life.add(logFiles);
        life.start();
        checkpointFile = logFiles.getCheckpointFile();
    }

    @Test
    void lastEnvelopeCheckpointStraddlingFileBoundary() throws IOException {
        var checkpointAppender = checkpointFile.getCheckpointAppender();
        assertThat(checkpointFile.reachableCheckpoints()).isEmpty();
        assertThat(checkpointFile.getCurrentLogVersion()).isZero();
        int i = 0;
        while (checkpointFile.getCurrentLogVersion() == 0L) {
            ++i;
            checkpointAppender.checkPoint(
                    NULL,
                    new TransactionId(i, i, KernelVersion.V2026_07, i, 3, 4),
                    i,
                    KernelVersion.V2026_07,
                    new LogPosition(1, i),
                    new LogPosition(1, i),
                    Instant.now(),
                    "test " + i);
        }
        assertThat(checkpointFile.getCurrentLogVersion()).isEqualTo(1L);
        assertThat(checkpointFile.reachableCheckpoints()).hasSize(i);
        var last = checkpointFile.findLatestCheckpoint();
        assertThat(last).isPresent();
        var lastInfo = last.get();
        assertThat(lastInfo.appendIndex())
                .withFailMessage("Wrong final checkpoint found")
                .isEqualTo(i);
        assertThat(lastInfo.checkpointEntryPosition().getLogVersion())
                .withFailMessage("Checkpoint doesn't start in first file")
                .isEqualTo(0L);
        assertThat(lastInfo.checkpointFilePostReadPosition().getLogVersion())
                .withFailMessage("Checkpoint doesn't end in second file")
                .isEqualTo(1L);
    }

    @Test
    void envelopedCheckpointsShouldNotCrossFileBoundariesWithDefaultSegmentSize() {
        int checkpointRecordSize =
                DetachedCheckpointLogEntrySerializerV5_22.checkPointRecordSizeDependingOnVersion(true);
        assertThat(checkpointRecordSize)
                .withFailMessage(
                        "Enveloped Checkpoints of size %d bytes cannot be larger than the %d segment size",
                        checkpointRecordSize, LogSegments.DEFAULT_LOG_SEGMENT_SIZE)
                .isLessThan(LogSegments.DEFAULT_LOG_SEGMENT_SIZE);
        int residualBytesAtEndOfSegment = LogSegments.DEFAULT_LOG_SEGMENT_SIZE % checkpointRecordSize;
        assertThat(residualBytesAtEndOfSegment)
                .withFailMessage(
                        "Enveloped Checkpoints of size %d bytes written to segments of size %d might split across files and cause issues",
                        checkpointRecordSize, LogSegments.DEFAULT_LOG_SEGMENT_SIZE)
                .isLessThanOrEqualTo(LogEnvelopeHeader.HEADER_SIZE);
        assertThat(FORCE_SPLIT_CHECKPOINT_SEGMENT_SIZE % checkpointRecordSize)
                .isGreaterThanOrEqualTo(LogEnvelopeHeader.HEADER_SIZE);
    }

    @Test
    void envelopedCheckpointsShouldFitPerfectlyInSegmentFrom2026_08() {
        int checkpointRecordSize =
                DetachedCheckpointLogEntrySerializerV2026_08.checkPointRecordSizeDependingOnVersion(true);
        assertThat(checkpointRecordSize)
                .withFailMessage(
                        "Enveloped Checkpoints of size %d bytes cannot be larger than the %d segment size",
                        checkpointRecordSize, LogSegments.DEFAULT_LOG_SEGMENT_SIZE)
                .isLessThan(LogSegments.DEFAULT_LOG_SEGMENT_SIZE);
        int residualBytesAtEndOfSegment = LogSegments.DEFAULT_LOG_SEGMENT_SIZE % checkpointRecordSize;
        assertThat(residualBytesAtEndOfSegment)
                .withFailMessage(
                        "Enveloped Checkpoints of size %d bytes written to segments of size %d will leave padding that we don't want to deal with",
                        checkpointRecordSize, LogSegments.DEFAULT_LOG_SEGMENT_SIZE)
                .isZero();
    }

    private LogFiles buildLogFiles() throws IOException {
        var storeId = new StoreId(1, 2, "engine-1", "format-1", 3, 4);
        final var futureEnabledConf = Config.newBuilder()
                .set(GraphDatabaseInternalSettings.latest_runtime_version, DbmsRuntimeVersion.V2026_07.getVersion())
                .set(GraphDatabaseInternalSettings.latest_kernel_version, KernelVersion.V2026_07.version())
                .set(GraphDatabaseInternalSettings.allow_new_log_format_on_upgrade_or_create, true)
                .build();
        return LogFilesBuilder.writeableBuilder(
                        databaseLayout, fileSystem, fixed(KernelVersion.V2026_07), () -> LogFormat.V10)
                .withConfig(futureEnabledConf)
                .withRotationThreshold(rotationThreshold)
                .withTransactionIdStore(transactionIdStore)
                .withCommandReaderFactory(TestCommandReaderFactory.INSTANCE)
                .withStoreId(storeId)
                .withEnvelopeSegmentBlockSizeBytes(
                        FORCE_SPLIT_CHECKPOINT_SEGMENT_SIZE) // ensure checkpoints don't fit nicely
                .build();
    }
}
