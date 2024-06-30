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
import static org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent.NULL;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.SimpleAppendIndexProvider;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.CheckpointInfo;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.LogTailInformation;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLog;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.HealthEventGenerator;
import org.neo4j.storageengine.AppendIndexProvider;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.Neo4jLayoutExtension;

@Neo4jLayoutExtension
@ExtendWith(LifeExtension.class)
class DetachedCheckpointLogFileTest {
    @Inject
    private DatabaseLayout databaseLayout;

    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private LifeSupport life;

    private final long rotationThreshold = ByteUnit.mebiBytes(1);
    private final DatabaseHealth databaseHealth = new DatabaseHealth(HealthEventGenerator.NO_OP, NullLog.getInstance());
    private final LogVersionRepository logVersionRepository = new SimpleLogVersionRepository(1L);
    private final TransactionIdStore transactionIdStore = new SimpleTransactionIdStore(
            2L, 3L, LATEST_KERNEL_VERSION, 0, BASE_TX_COMMIT_TIMESTAMP, UNKNOWN_CONSENSUS_INDEX, 0, 0);
    private final AppendIndexProvider appendIndexProvider = new SimpleAppendIndexProvider();
    private CheckpointFile checkpointFile;
    private LogFiles logFiles;
    private final FakeKernelVersionProvider versionProvider = new FakeKernelVersionProvider();

    @BeforeEach
    void setUp() throws IOException {
        logFiles = buildLogFiles();
        life.add(logFiles);
        life.start();
        checkpointFile = logFiles.getCheckpointFile();
    }

    @Test
    void findLogTailShouldWorkForDetachedCheckpoints() throws IOException {
        LogPosition logPosition =
                new LogPosition(logVersionRepository.getCurrentLogVersion(), LATEST_LOG_FORMAT.getHeaderSize());
        TransactionId transactionId = new TransactionId(1, 2, LATEST_KERNEL_VERSION, 2, 3, 4);
        checkpointFile
                .getCheckpointAppender()
                .checkPoint(
                        NULL,
                        transactionId,
                        transactionId.id() + 6,
                        LatestVersions.LATEST_KERNEL_VERSION,
                        logPosition,
                        logPosition,
                        Instant.now(),
                        "detached");
        CheckpointInfo lastCheckPoint = ((LogTailInformation) buildLogFiles().getTailMetadata()).lastCheckPoint;
        assertThat(lastCheckPoint.transactionLogPosition()).isEqualTo(logPosition);
    }

    @Test
    void findLatestCheckpointShouldWorkForDetachedCheckpoints() throws IOException {
        // Should find the detached checkpoint first
        LogPosition logPosition2 =
                new LogPosition(logVersionRepository.getCurrentLogVersion(), LATEST_LOG_FORMAT.getHeaderSize());
        TransactionId transactionId = new TransactionId(5, 6, LATEST_KERNEL_VERSION, 6, 7, 8);
        checkpointFile
                .getCheckpointAppender()
                .checkPoint(
                        NULL,
                        transactionId,
                        transactionId.id() + 9,
                        LatestVersions.LATEST_KERNEL_VERSION,
                        logPosition2,
                        logPosition2,
                        Instant.now(),
                        "detached");
        assertThat(checkpointFile.findLatestCheckpoint().orElseThrow().transactionLogPosition())
                .isEqualTo(logPosition2);
    }

    @Test
    void shouldFindReachableCheckpointsForDetachedCheckpoints() throws IOException {
        assertThat(checkpointFile.reachableCheckpoints()).isEmpty();
        assertThat(checkpointFile.getReachableDetachedCheckpoints()).isEmpty();

        // Add detached checkpoints
        LogPosition logPosition = new LogPosition(0, 3);
        LogPosition logPosition1 = new LogPosition(0, 4);
        TransactionId transactionId = new TransactionId(5, 6, LATEST_KERNEL_VERSION, 6, 7, 8);
        TransactionId transactionId1 = new TransactionId(6, 7, LATEST_KERNEL_VERSION, 7, 8, 9);
        checkpointFile
                .getCheckpointAppender()
                .checkPoint(
                        NULL,
                        transactionId,
                        transactionId.id() + 3,
                        LatestVersions.LATEST_KERNEL_VERSION,
                        logPosition,
                        logPosition,
                        Instant.now(),
                        "detached");
        checkpointFile
                .getCheckpointAppender()
                .checkPoint(
                        NULL,
                        transactionId1,
                        transactionId1.id() + 7,
                        LatestVersions.LATEST_KERNEL_VERSION,
                        logPosition1,
                        logPosition1,
                        Instant.now(),
                        "detached");

        List<CheckpointInfo> reachableCheckpoints = checkpointFile.reachableCheckpoints();
        assertThat(reachableCheckpoints.size()).isEqualTo(2);
        assertThat(reachableCheckpoints.get(0).transactionLogPosition()).isEqualTo(logPosition);
        assertThat(reachableCheckpoints.get(1).transactionLogPosition()).isEqualTo(logPosition1);
        List<CheckpointInfo> detachedCheckpoints = checkpointFile.getReachableDetachedCheckpoints();
        assertThat(detachedCheckpoints.size()).isEqualTo(2);
        assertThat(detachedCheckpoints.get(0).transactionLogPosition()).isEqualTo(logPosition);
        assertThat(detachedCheckpoints.get(1).transactionLogPosition()).isEqualTo(logPosition1);
    }

    private LogFiles buildLogFiles() throws IOException {
        var storeId = new StoreId(1, 2, "engine-1", "format-1", 3, 4);
        return LogFilesBuilder.builder(databaseLayout, fileSystem, versionProvider)
                .withRotationThreshold(rotationThreshold)
                .withTransactionIdStore(transactionIdStore)
                .withAppendIndexProvider(appendIndexProvider)
                .withDatabaseHealth(databaseHealth)
                .withLogVersionRepository(logVersionRepository)
                .withCommandReaderFactory(TestCommandReaderFactory.INSTANCE)
                .withStoreId(storeId)
                .build();
    }

    private static class FakeKernelVersionProvider implements KernelVersionProvider {
        volatile KernelVersion version = LatestVersions.LATEST_KERNEL_VERSION;

        @Override
        public KernelVersion kernelVersion() {
            return version;
        }
    }
}
