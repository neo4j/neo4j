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
package org.neo4j.wal.files.checkpoint;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.common.Subject.ANONYMOUS;
import static org.neo4j.kernel.impl.api.LeaseService.NO_LEASE;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TX_SEQUENCE_NUMBER;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.storageengine.api.Leases;
import org.neo4j.storageengine.api.LogMetadataProvider;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.wal.CompleteCommandBatch;
import org.neo4j.wal.LogPosition;
import org.neo4j.wal.TransactionLogWriter;
import org.neo4j.wal.WALService;
import org.neo4j.wal.checkpoint.LogCheckPointEvent;
import org.neo4j.wal.entry.LogEntryWriter;
import org.neo4j.wal.entry.LogEnvelopeHeader;
import org.neo4j.wal.entry.LogFormat;
import org.neo4j.wal.files.LogFilesBuilder;
import org.neo4j.wal.files.LogTailInformation;

@Neo4jLayoutExtension
public class EnvelopedDetachedLogTailScannerTest {
    private static final KernelVersion kernelVersion = KernelVersion.VERSION_ENVELOPED_TRANSACTION_LOGS_GUARANTEED;
    private static final int SEGMENT_BLOCK_SIZE_BYTES = 1024;

    @Inject
    protected FileSystemAbstraction fs;

    @Inject
    private DatabaseLayout databaseLayout;

    @BeforeEach
    void setUp() throws IOException {
        setupLogFiles();
    }

    private WALService setupLogFiles() throws IOException {
        return setupLogFiles(4096L);
    }

    private WALService setupLogFiles(long rotationThreshold) throws IOException {
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
                .withRotationThreshold(rotationThreshold)
                .withEnvelopeSegmentBlockSizeBytes(SEGMENT_BLOCK_SIZE_BYTES)
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

    @Test
    void checkpointAtSegmentBoundaryOfTruncatedLogShouldFindPostCheckpointEntries() throws IOException {
        // The file shape a raw log pull (backup/catchup) produces: the pulled log is not pre-allocated
        // and simply ends where the stream was cut, mid-segment, and the pull start position that gets
        // checkpointed can land exactly on a segment boundary.
        int segmentSize = SEGMENT_BLOCK_SIZE_BYTES;
        LogPosition boundary = null;
        LogPosition truncationPoint = null;
        TransactionId checkpointedTx = null;
        Path logFilePath;

        var logLifeCycle = new LifeSupport();
        var logFiles = setupLogFiles(ByteUnit.mebiBytes(1));
        try {
            logLifeCycle.start();
            logLifeCycle.add(logFiles);
            var logFile = logFiles.getLogFile();
            var checkpointFile = logFiles.getCheckpointFile();
            var logMetadataProvider = logFiles.logMetadataProvider();
            TransactionLogWriter logWriter = (TransactionLogWriter) logFile.getTransactionLogWriter();
            LogEntryWriter<?> entryWriter = logWriter.getWriter();
            int previousChecksum = BASE_TX_CHECKSUM;
            long txId = 0;

            // Vary the command size until a transaction ends so close to a segment boundary that the
            // writer has to zero-pad, which makes the next entry start exactly on the boundary.
            while (boundary == null) {
                assertThat(++txId)
                        .as("varying command sizes should hit the zero-padding zone of some segment")
                        .isLessThan(2000);
                long appendIndex = logMetadataProvider.nextAppendIndex();
                previousChecksum = writeTxEntries(entryWriter, txId, appendIndex, previousChecksum, (int) (txId % 97));
                var position = logWriter.getCurrentPosition();
                long spaceLeftInSegment = segmentSize - position.getByteOffset() % segmentSize;
                if (spaceLeftInSegment <= LogEnvelopeHeader.HEADER_SIZE) {
                    boundary = new LogPosition(position.getLogVersion(), position.getByteOffset() + spaceLeftInSegment);
                    checkpointedTx = new TransactionId(
                            txId, appendIndex, kernelVersion, previousChecksum, 0, UNKNOWN_CONSENSUS_INDEX);
                }
            }

            // Entries after the boundary, with the last one ending mid-segment (a pull cuts at batch ends).
            for (int i = 0; i < 3 || truncationPoint == null; i++) {
                long appendIndex = logMetadataProvider.nextAppendIndex();
                previousChecksum = writeTxEntries(entryWriter, ++txId, appendIndex, previousChecksum, 40);
                var position = logWriter.getCurrentPosition();
                if (position.getByteOffset() % segmentSize != 0) {
                    truncationPoint = position;
                }
            }
            assertThat(truncationPoint.getLogVersion()).isEqualTo(boundary.getLogVersion());

            checkpointFile
                    .getCheckpointAppender()
                    .checkPoint(
                            LogCheckPointEvent.NULL,
                            checkpointedTx,
                            checkpointedTx.appendIndex(),
                            UNKNOWN_CONSENSUS_INDEX,
                            kernelVersion,
                            boundary,
                            boundary,
                            Instant.now(),
                            "test");

            var txLogPaths = Stream.of(logFiles.logFiles())
                    .filter(path -> path.getFileName().toString().startsWith("neostore.transaction.db"))
                    .toList();
            assertThat(txLogPaths).hasSize(1);
            logFilePath = txLogPaths.get(0);
        } finally {
            logLifeCycle.shutdown();
        }

        fs.truncate(logFilePath, truncationPoint.getByteOffset());

        var logTailInformation =
                (LogTailInformation) setupLogFiles(ByteUnit.mebiBytes(1)).getTailMetadata();
        var lastCheckpoint = logTailInformation.getLastCheckPoint().orElseThrow();
        assertThat(lastCheckpoint.transactionLogPosition()).isEqualTo(boundary);
        assertThat(logTailInformation.hasRecordsToRecover()).isTrue();
        assertThat(logTailInformation.isRecoveryRequired()).isTrue();
    }

    private record LastTxInfo(long straddledTxId, long straddledAppendIndex, LogPosition afterStraddlePosition) {}

    private static int writeTxEntries(LogEntryWriter<?> entryWriter, long txId, long appendIndex, int previousChecksum)
            throws IOException {
        return writeTxEntries(entryWriter, txId, appendIndex, previousChecksum, 50);
    }

    private static int writeTxEntries(
            LogEntryWriter<?> entryWriter, long txId, long appendIndex, int previousChecksum, int commandSize)
            throws IOException {
        byte[] emptyArray = new byte[0];
        entryWriter.writeStartEntry(
                kernelVersion, 0, txId, appendIndex, UNKNOWN_TX_SEQUENCE_NUMBER, previousChecksum, emptyArray);
        CompleteCommandBatch commands = new CompleteCommandBatch(
                List.of(new TestCommand(commandSize, kernelVersion)),
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
            var logWriter = (TransactionLogWriter) logFile.getTransactionLogWriter();
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
                                UNKNOWN_CONSENSUS_INDEX,
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
