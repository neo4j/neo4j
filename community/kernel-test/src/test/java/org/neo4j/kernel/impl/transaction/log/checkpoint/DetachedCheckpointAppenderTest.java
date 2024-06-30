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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.impl.transaction.log.LogPosition.UNSPECIFIED;
import static org.neo4j.kernel.impl.transaction.log.rotation.LogRotation.NO_ROTATION;
import static org.neo4j.storageengine.AppendIndexProvider.BASE_APPEND_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TRANSACTION_ID;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;

import java.io.IOException;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.SimpleAppendIndexProvider;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogChannelAllocator;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesContext;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.CheckpointFile;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.DetachedLogTailScanner;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLog;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.HealthEventGenerator;
import org.neo4j.storageengine.AppendIndexProvider;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.Neo4jLayoutExtension;

@Neo4jLayoutExtension
@ExtendWith(LifeExtension.class)
class DetachedCheckpointAppenderTest {
    @Inject
    private DatabaseLayout databaseLayout;

    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private LifeSupport life;

    private final long rotationThreshold = ByteUnit.mebiBytes(1);
    private final DatabaseHealth databaseHealth = new DatabaseHealth(HealthEventGenerator.NO_OP, NullLog.getInstance());
    private final LogVersionRepository logVersionRepository = new SimpleLogVersionRepository(1L);
    private final AppendIndexProvider appendIndexProvider = new SimpleAppendIndexProvider();
    private final TransactionIdStore transactionIdStore = new SimpleTransactionIdStore(
            2L, 3L, LATEST_KERNEL_VERSION, 0, BASE_TX_COMMIT_TIMESTAMP, UNKNOWN_CONSENSUS_INDEX, 0, 0);
    private CheckpointAppender checkpointAppender;
    private LogFiles logFiles;

    @BeforeEach
    void setUp() throws IOException {
        logFiles = buildLogFiles();
        life.add(logFiles);
        life.start();

        checkpointAppender = logFiles.getCheckpointFile().getCheckpointAppender();
    }

    @Test
    void detachedCheckpointAppenderUsedForSeparateCheckpointFiles() {
        assertThat(checkpointAppender).isInstanceOf(DetachedCheckpointAppender.class);
    }

    @Test
    void failToWriteCheckpointOnUnhealthyDatabase() {
        databaseHealth.panic(new RuntimeException("Panic"));

        LogPosition logPosition = new LogPosition(0, 10);
        assertThrows(
                IOException.class,
                () -> checkpointAppender.checkPoint(
                        LogCheckPointEvent.NULL,
                        UNKNOWN_TRANSACTION_ID,
                        BASE_APPEND_INDEX,
                        LATEST_KERNEL_VERSION,
                        logPosition,
                        logPosition,
                        Instant.now(),
                        "test"));
    }

    @Test
    void skipCheckpointOnAttemptToAppendCheckpointWhenNotStarted() {
        DetachedCheckpointAppender appender = new DetachedCheckpointAppender(
                mock(LogFiles.class),
                mock(TransactionLogChannelAllocator.class),
                mock(TransactionLogFilesContext.class, RETURNS_MOCKS),
                logFiles.getCheckpointFile(),
                NO_ROTATION,
                mock(DetachedLogTailScanner.class),
                LatestVersions.BINARY_VERSIONS);
        assertDoesNotThrow(() -> appender.checkPoint(
                LogCheckPointEvent.NULL,
                UNKNOWN_TRANSACTION_ID,
                BASE_APPEND_INDEX,
                LATEST_KERNEL_VERSION,
                UNSPECIFIED,
                UNSPECIFIED,
                Instant.now(),
                "test"));
    }

    @Test
    void appendedCheckpointsCanBeLookedUpFromCheckpointFile() throws IOException {
        CheckpointFile checkpointFile = logFiles.getCheckpointFile();

        var logPosition1 = new LogPosition(0, 10);
        var logPosition2 = new LogPosition(0, 20);
        var logPosition3 = new LogPosition(0, 30);
        assertThat(checkpointFile.reachableCheckpoints()).hasSize(0);
        checkpointAppender.checkPoint(
                LogCheckPointEvent.NULL,
                UNKNOWN_TRANSACTION_ID,
                BASE_APPEND_INDEX,
                LATEST_KERNEL_VERSION,
                logPosition1,
                logPosition1,
                Instant.now(),
                "first");
        checkpointAppender.checkPoint(
                LogCheckPointEvent.NULL,
                UNKNOWN_TRANSACTION_ID,
                BASE_APPEND_INDEX,
                LATEST_KERNEL_VERSION,
                logPosition2,
                logPosition2,
                Instant.now(),
                "second");
        checkpointAppender.checkPoint(
                LogCheckPointEvent.NULL,
                UNKNOWN_TRANSACTION_ID,
                BASE_APPEND_INDEX,
                LATEST_KERNEL_VERSION,
                logPosition3,
                logPosition3,
                Instant.now(),
                "third");

        var checkpoints = checkpointFile.reachableCheckpoints();
        assertThat(checkpoints).hasSize(3);
        assertThat(checkpoints.get(0)).hasFieldOrPropertyWithValue("transactionLogPosition", logPosition1);
        assertThat(checkpoints.get(1)).hasFieldOrPropertyWithValue("transactionLogPosition", logPosition2);
        assertThat(checkpoints.get(2)).hasFieldOrPropertyWithValue("transactionLogPosition", logPosition3);
    }

    private LogFiles buildLogFiles() throws IOException {
        var storeId = new StoreId(1, 2, "engine-1", "format-1", 3, 4);
        return LogFilesBuilder.builder(databaseLayout, fileSystem, LatestVersions.LATEST_KERNEL_VERSION_PROVIDER)
                .withRotationThreshold(rotationThreshold)
                .withTransactionIdStore(transactionIdStore)
                .withAppendIndexProvider(appendIndexProvider)
                .withDatabaseHealth(databaseHealth)
                .withLogVersionRepository(logVersionRepository)
                .withCommandReaderFactory(TestCommandReaderFactory.INSTANCE)
                .withStoreId(storeId)
                .build();
    }
}
