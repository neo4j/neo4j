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
package org.neo4j.wal.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.KernelVersionProviders.fixed;
import static org.neo4j.storageengine.AppendIndexProvider.BASE_APPEND_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TRANSACTION_ID;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;
import static org.neo4j.wal.LogPosition.UNSPECIFIED;
import static org.neo4j.wal.rotation.LogRotation.NO_ROTATION;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLog;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.HealthEventGenerator;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.wal.LogPosition;
import org.neo4j.wal.WALService;
import org.neo4j.wal.entry.LogFormat;
import org.neo4j.wal.files.LogFilesBuilder;
import org.neo4j.wal.files.TransactionLogChannelAllocator;
import org.neo4j.wal.files.TransactionLogFilesContext;
import org.neo4j.wal.files.TransactionLogFilesProviders;
import org.neo4j.wal.files.checkpoint.DetachedLogTailScanner;

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
    private final TransactionIdStore transactionIdStore = new SimpleTransactionIdStore(
            2L, 3L, LATEST_KERNEL_VERSION, 0, BASE_TX_COMMIT_TIMESTAMP, UNKNOWN_CONSENSUS_INDEX, 0, 0);
    private CheckpointAppender checkpointAppender;
    private WALService walService;

    @BeforeEach
    void setUp() throws IOException {
        setUp(LATEST_KERNEL_VERSION);
    }

    void setUp(KernelVersion kernelVersion) throws IOException {
        walService = buildLogFiles(kernelVersion);
        life.add(walService);
        life.start();

        checkpointAppender = walService.getCheckpointFile().getCheckpointAppender();
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
                        UNKNOWN_CONSENSUS_INDEX,
                        LATEST_KERNEL_VERSION,
                        logPosition,
                        logPosition,
                        Instant.now(),
                        "test"));
    }

    @Test
    void skipCheckpointOnAttemptToAppendCheckpointWhenNotStarted() {
        DetachedCheckpointAppender appender = new DetachedCheckpointAppender(
                mock(WALService.class),
                mock(TransactionLogChannelAllocator.class),
                mock(TransactionLogFilesContext.class, RETURNS_MOCKS),
                walService.getCheckpointFile(),
                NO_ROTATION,
                mock(DetachedLogTailScanner.class),
                LatestVersions.BINARY_VERSIONS,
                mock(TransactionLogFilesProviders.class));
        assertDoesNotThrow(() -> appender.checkPoint(
                LogCheckPointEvent.NULL,
                UNKNOWN_TRANSACTION_ID,
                BASE_APPEND_INDEX,
                UNKNOWN_CONSENSUS_INDEX,
                LATEST_KERNEL_VERSION,
                UNSPECIFIED,
                UNSPECIFIED,
                Instant.now(),
                "test"));
    }

    @Test
    void appendedCheckpointsCanBeLookedUpFromCheckpointFile() throws IOException {
        CheckpointFile checkpointFile = walService.getCheckpointFile();

        var logPosition1 = new LogPosition(0, 10);
        var logPosition2 = new LogPosition(0, 20);
        var logPosition3 = new LogPosition(0, 30);
        assertThat(checkpointFile.reachableCheckpoints()).hasSize(0);
        checkpointAppender.checkPoint(
                LogCheckPointEvent.NULL,
                UNKNOWN_TRANSACTION_ID,
                BASE_APPEND_INDEX,
                UNKNOWN_CONSENSUS_INDEX,
                LATEST_KERNEL_VERSION,
                logPosition1,
                logPosition1,
                Instant.now(),
                "first");
        checkpointAppender.checkPoint(
                LogCheckPointEvent.NULL,
                UNKNOWN_TRANSACTION_ID,
                BASE_APPEND_INDEX,
                UNKNOWN_CONSENSUS_INDEX,
                LATEST_KERNEL_VERSION,
                logPosition2,
                logPosition2,
                Instant.now(),
                "second");
        checkpointAppender.checkPoint(
                LogCheckPointEvent.NULL,
                UNKNOWN_TRANSACTION_ID,
                BASE_APPEND_INDEX,
                UNKNOWN_CONSENSUS_INDEX,
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

    @Test
    void shouldFindAllCheckpointsAfterRotatingBeforeRotationThreshold() throws IOException {
        life.clear();
        // cleanup any latest version logs, so that we can force V5_22
        fileSystem.deleteRecursively(databaseLayout.getTransactionLogsDirectory());
        setUp(KernelVersion.V5_22);

        CheckpointFile checkpointFile = walService.getCheckpointFile();

        var logPosition1 = new LogPosition(0, 10);
        var logPosition2 = new LogPosition(0, 20);
        assertThat(checkpointFile.reachableCheckpoints()).hasSize(0);
        checkpointAppender.checkPoint(
                LogCheckPointEvent.NULL,
                UNKNOWN_TRANSACTION_ID,
                BASE_APPEND_INDEX,
                UNKNOWN_CONSENSUS_INDEX,
                KernelVersion.V5_22,
                logPosition1,
                logPosition1,
                Instant.now(),
                "first");
        // Trigger rotation on kernel version = rotate before reaching threshold
        checkpointAppender.checkPoint(
                LogCheckPointEvent.NULL,
                UNKNOWN_TRANSACTION_ID,
                BASE_APPEND_INDEX,
                UNKNOWN_CONSENSUS_INDEX,
                LATEST_KERNEL_VERSION,
                logPosition2,
                logPosition2,
                Instant.now(),
                "second");

        assertThat(checkpointFile.getMatchedFiles()).hasSize(2);
        var checkpoints = checkpointFile.reachableCheckpoints();
        // Should find the checkpoints in both of the files
        assertThat(checkpoints).hasSize(2);
        assertThat(checkpoints.get(0)).hasFieldOrPropertyWithValue("transactionLogPosition", logPosition1);
        assertThat(checkpoints.get(1)).hasFieldOrPropertyWithValue("transactionLogPosition", logPosition2);
    }

    @Test
    void shouldTruncatePrevFileOnRotation() throws IOException {
        life.clear();
        // cleanup any latest version logs, so that we can force V5_22
        fileSystem.deleteRecursively(databaseLayout.getTransactionLogsDirectory());
        setUp(KernelVersion.V5_22);

        CheckpointFile checkpointFile = walService.getCheckpointFile();

        var logPosition1 = new LogPosition(0, 10);
        var logPosition2 = new LogPosition(0, 20);
        assertThat(checkpointFile.reachableCheckpoints()).hasSize(0);
        checkpointAppender.checkPoint(
                LogCheckPointEvent.NULL,
                UNKNOWN_TRANSACTION_ID,
                BASE_APPEND_INDEX,
                UNKNOWN_CONSENSUS_INDEX,
                KernelVersion.V5_22,
                logPosition1,
                logPosition1,
                Instant.now(),
                "first");
        long positionAfterCheckpoint = ((DetachedCheckpointAppender) checkpointAppender).getCurrentPosition();
        assertThat(Files.size(checkpointFile.getLogFileForVersion(
                        checkpointFile.getLogRangeInfo().lowestVersion())))
                .isGreaterThanOrEqualTo(positionAfterCheckpoint);
        // Trigger rotation on kernel version = rotate before reaching threshold
        checkpointAppender.checkPoint(
                LogCheckPointEvent.NULL,
                UNKNOWN_TRANSACTION_ID,
                BASE_APPEND_INDEX,
                UNKNOWN_CONSENSUS_INDEX,
                LATEST_KERNEL_VERSION,
                logPosition2,
                logPosition2,
                Instant.now(),
                "second");

        assertThat(checkpointFile.getMatchedFiles()).hasSize(2);
        assertThat(Files.size(checkpointFile.getLogFileForVersion(
                        checkpointFile.getLogRangeInfo().lowestVersion())))
                .isEqualTo(positionAfterCheckpoint);
    }

    private WALService buildLogFiles(KernelVersion initialKernelVersion) throws IOException {
        var storeId = new StoreId(1, 2, "engine-1", "format-1", 3, 4);
        return LogFilesBuilder.writeableBuilder(
                        databaseLayout,
                        fileSystem,
                        fixed(initialKernelVersion),
                        () -> LogFormat.fromKernelVersion(initialKernelVersion))
                .withRotationThreshold(rotationThreshold)
                .withTransactionIdStore(transactionIdStore)
                .withDatabaseHealth(databaseHealth)
                .withLogVersionRepository(logVersionRepository)
                .withCommandReaderFactory(TestCommandReaderFactory.INSTANCE)
                .withStoreId(storeId)
                .build();
    }
}
