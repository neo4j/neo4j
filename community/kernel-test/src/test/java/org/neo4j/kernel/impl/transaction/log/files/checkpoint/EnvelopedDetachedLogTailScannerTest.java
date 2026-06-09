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
import static org.neo4j.common.Subject.ANONYMOUS;
import static org.neo4j.kernel.impl.api.LeaseService.NO_LEASE;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TX_SEQUENCE_NUMBER;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.log.CompleteCommandBatch;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.storageengine.api.Leases;
import org.neo4j.storageengine.api.LogMetadataProvider;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

@Neo4jLayoutExtension
public class EnvelopedDetachedLogTailScannerTest {
    private static final KernelVersion kernelVersion = KernelVersion.VERSION_ENVELOPED_TRANSACTION_LOGS_GUARANTEED;

    @Inject
    protected FileSystemAbstraction fs;

    @Inject
    private DatabaseLayout databaseLayout;

    @BeforeEach
    void setUp() throws IOException {
        setupLogFiles();
    }

    private LogFiles setupLogFiles() throws IOException {
        var storeId = new StoreId(1, 2, "engine-1", "format-1", 3, 4);
        var config = Config.newBuilder()
                .set(
                        GraphDatabaseInternalSettings.latest_runtime_version,
                        DbmsRuntimeVersion.GLORIOUS_FUTURE.getVersion())
                .set(GraphDatabaseInternalSettings.latest_kernel_version, kernelVersion.version())
                .build();
        return LogFilesBuilder.writeableBuilder(
                        databaseLayout, fs, () -> kernelVersion, () -> LogFormat.fromKernelVersion(kernelVersion))
                .withCommandReaderFactory(TestCommandReaderFactory.INSTANCE)
                .withStoreId(storeId)
                .withRotationThreshold(4096L)
                .withEnvelopeSegmentBlockSizeBytes(1024)
                .withConfig(config)
                .build();
    }

    @Test
    void checkpointTxStraddlingLogFilesShouldBeReturnedByDetachedTailScanner() throws IOException {
        var lastTxPositions = createStraddledLogs(true, false);
        var logFiles = setupLogFiles();
        var logTailInformation = logFiles.getTailMetadata();
        var lastCheckpoint = logTailInformation.getLastCheckPoint().orElseThrow();
        assertThat(lastCheckpoint.transactionId().id()).isEqualTo(lastTxPositions.straddledTxId);
        assertThat(lastCheckpoint.appendIndex()).isEqualTo(lastTxPositions.straddledAppendIndex);
        assertThat(lastCheckpoint.transactionLogPosition()).isEqualTo(lastTxPositions.afterStraddlePosition);
        assertThat(lastCheckpoint.oldestNotVisibleTransactionLogPosition())
                .isEqualTo(lastTxPositions.afterStraddlePosition);
        assertThat(logTailInformation.isRecoveryRequired()).isFalse();
    }

    @Test
    void checkpointTxStraddlingAndMissingTxDetachedTailScanner() throws IOException {
        var lastTxPositions = createStraddledLogs(true, true);
        var logFiles = setupLogFiles();
        var logTailInformation = logFiles.getTailMetadata();
        var lastCheckpoint = logTailInformation.getLastCheckPoint().orElseThrow();
        assertThat(lastCheckpoint.transactionId().id()).isEqualTo(lastTxPositions.straddledTxId);
        assertThat(lastCheckpoint.appendIndex()).isEqualTo(lastTxPositions.straddledAppendIndex);
        assertThat(lastCheckpoint.transactionLogPosition()).isEqualTo(lastTxPositions.afterStraddlePosition);
        assertThat(lastCheckpoint.oldestNotVisibleTransactionLogPosition())
                .isEqualTo(lastTxPositions.afterStraddlePosition);
        assertThat(logTailInformation.isRecoveryRequired()).isTrue();
    }

    @Test
    void noCheckpointTxStraddlingLogFilesShouldBeReturnedByDetachedTailScanner() throws IOException {
        createStraddledLogs(false, false);
        var logFiles = setupLogFiles();
        var logTailInformation = logFiles.getTailMetadata();
        assertThat(logTailInformation.getLastCheckPoint()).isEmpty();
        assertThat(logTailInformation.isRecoveryRequired()).isTrue();
    }

    private record LastTxInfo(long straddledTxId, long straddledAppendIndex, LogPosition afterStraddlePosition) {}

    private static int writeTxEntries(LogEntryWriter<?> entryWriter, long txId, long appendIndex, int previousChecksum)
            throws IOException {
        byte[] emptyArray = new byte[0];
        entryWriter.writeStartEntry(
                kernelVersion, 0, txId, appendIndex, UNKNOWN_TX_SEQUENCE_NUMBER, previousChecksum, emptyArray);
        CompleteCommandBatch commands = new CompleteCommandBatch(
                List.of(new TestCommand(kernelVersion)),
                UNKNOWN_CONSENSUS_INDEX,
                0,
                txId - 1L,
                0,
                NO_LEASE,
                Leases.NO_LEASES,
                kernelVersion,
                ANONYMOUS);
        entryWriter.serialize(commands);
        previousChecksum = entryWriter.writeCommitEntry(kernelVersion, txId, 0);
        return previousChecksum;
    }

    private LastTxInfo createStraddledLogs(boolean checkpoint, boolean addPostCommitTx) throws IOException {
        var lastTxId = new AtomicLong();
        var logLifeCycle = new LifeSupport();
        var logFiles = setupLogFiles();
        try {
            logLifeCycle.start();
            logLifeCycle.add(logFiles);
            var logFile = logFiles.getLogFile();
            var checkpointFile = logFiles.getCheckpointFile();
            LogMetadataProvider logMetadataProvider = logFiles.logMetadataProvider();
            var logWriter = logFile.getTransactionLogWriter();
            LogEntryWriter<?> entryWriter = logWriter.getWriter();
            int previousChecksum = BASE_TX_CHECKSUM;
            long txId;
            long appendIndex;
            LogPosition preStartPosition;
            LogPosition postCommitPosition;
            do {
                preStartPosition = logWriter.getCurrentPosition();
                appendIndex = logMetadataProvider.nextAppendIndex();
                txId = lastTxId.incrementAndGet();
                previousChecksum = writeTxEntries(entryWriter, txId, appendIndex, previousChecksum);
                postCommitPosition = logWriter.getCurrentPosition();
            } while (preStartPosition.getLogVersion() == postCommitPosition.getLogVersion());
            long straddledTxId = txId;
            long straddledAppendIndex = appendIndex;
            if (checkpoint) {
                checkpointFile
                        .getCheckpointAppender()
                        .checkPoint(
                                LogCheckPointEvent.NULL,
                                new TransactionId(
                                        txId, appendIndex, kernelVersion, previousChecksum, 0, UNKNOWN_CONSENSUS_INDEX),
                                appendIndex,
                                kernelVersion,
                                postCommitPosition,
                                postCommitPosition,
                                Instant.now(),
                                "test");
                if (addPostCommitTx) {
                    appendIndex = logMetadataProvider.nextAppendIndex();
                    txId = lastTxId.incrementAndGet();
                    writeTxEntries(entryWriter, txId, appendIndex, previousChecksum);
                }
            }
            return new LastTxInfo(straddledTxId, straddledAppendIndex, postCommitPosition);
        } finally {
            logLifeCycle.shutdown();
        }
    }
}
