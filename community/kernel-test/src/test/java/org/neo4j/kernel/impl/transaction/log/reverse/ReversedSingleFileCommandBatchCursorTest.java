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
package org.neo4j.kernel.impl.transaction.log.reverse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.common.Subject.ANONYMOUS;
import static org.neo4j.kernel.KernelVersionProviders.fixed;
import static org.neo4j.kernel.impl.transaction.log.GivenCommandBatchCursor.exhaust;
import static org.neo4j.kernel.impl.transaction.log.TestLogEntryReader.logEntryReader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.TX_START;
import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CHUNK_ID;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION_WITHOUT_ENVELOPES;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatchRepresentation;
import org.neo4j.kernel.impl.transaction.log.CompleteCommandBatch;
import org.neo4j.kernel.impl.transaction.log.FlushableLogPositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReaderLogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.storageengine.api.Leases;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RandomExtension;

@Neo4jLayoutExtension
@ExtendWith({RandomExtension.class, LifeExtension.class})
class ReversedSingleFileCommandBatchCursorTest {
    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private DatabaseLayout databaseLayout;

    @Inject
    private LifeSupport life;

    @Inject
    private RandomSupport random;

    private long txId = TransactionIdStore.BASE_TX_ID;
    private final InternalLogProvider logProvider = new AssertableLogProvider(true);
    private final ReverseTransactionCursorLoggingMonitor monitor =
            new ReverseTransactionCursorLoggingMonitor(logProvider.getLog(ReversedSingleFileCommandBatchCursor.class));
    private LogFile logFile;
    private LogFiles logFiles;

    @BeforeEach
    void setUp() throws IOException {
        var storeId = new StoreId(1, 2, "engine-1", "format-1", 3, 4);
        logFiles = LogFilesBuilder.writeableBuilder(
                        databaseLayout,
                        fs,
                        fixed(LATEST_KERNEL_VERSION_WITHOUT_ENVELOPES),
                        () -> LogFormat.fromKernelVersion(LATEST_KERNEL_VERSION_WITHOUT_ENVELOPES))
                .withRotationThreshold(ByteUnit.mebiBytes(10))
                .withCommandReaderFactory(TestCommandReaderFactory.INSTANCE)
                .withStoreId(storeId)
                .build();
        life.add(logFiles);
        logFile = logFiles.getLogFile();
    }

    @Test
    void reverseCursorBatchStartPositions() throws IOException {
        List<LogPosition> startPositions = writeTransactions(10, 10, 15);
        Collections.reverse(startPositions);

        int transaction = 0;
        long expectedTxId = txId;
        try (var cursor = txCursor(false)) {
            while (cursor.next()) {
                var committedCommandBatch = cursor.get();
                assertEquals(expectedTxId--, committedCommandBatch.txId());
                assertEquals(startPositions.get(transaction++), cursor.position());
            }
        }
    }

    @Test
    void shouldHandleVerySmallTransactions() throws Exception {
        // given
        writeTransactions(10, 1, 1);

        // when
        CommittedCommandBatchRepresentation[] readTransactions = readAllFromReversedCursor();

        // then
        assertTransactionRange(readTransactions, txId, TransactionIdStore.BASE_TX_ID);
    }

    @Test
    void shouldHandleManyVerySmallTransactions() throws Exception {
        // given
        writeTransactions(20_000, 1, 1);

        // when
        CommittedCommandBatchRepresentation[] readTransactions = readAllFromReversedCursor();

        // then
        assertTransactionRange(readTransactions, txId, TransactionIdStore.BASE_TX_ID);
    }

    @Test
    void shouldHandleLargeTransactions() throws Exception {
        // given
        writeTransactions(10, 1000, 1000);

        // when
        CommittedCommandBatchRepresentation[] readTransactions = readAllFromReversedCursor();

        // then
        assertTransactionRange(readTransactions, txId, TransactionIdStore.BASE_TX_ID);
    }

    @Test
    void shouldHandleEmptyLog() throws Exception {
        // given

        // when
        CommittedCommandBatchRepresentation[] readTransactions = readAllFromReversedCursor();

        // then
        assertEquals(0, readTransactions.length);
    }

    @Test
    void shouldDetectAndPreventChannelReadingMultipleLogVersions() throws Exception {
        // given
        writeTransactions(1, 1, 1);
        logFile.rotate();
        writeTransactions(1, 1, 1);

        // when
        PhysicalLogVersionedStoreChannel logChannel = logFile.openForVersion(0);
        logChannel.position(
                logFiles.getLogFile().extractHeader(0).getStartPosition().getByteOffset());
        try (ReadAheadLogChannel channel = new ReadAheadLogChannel(
                logChannel, ReaderLogVersionBridge.forFile(logFile), EmptyMemoryTracker.INSTANCE)) {
            assertThatThrownBy(() -> ReversedSingleFileCommandBatchCursor.create(
                            channel, logEntryReader(), false, monitor, LogPosition.UNSPECIFIED))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("multiple log versions");
        }
    }

    @Test
    void readCorruptedTransactionLog() throws IOException {
        int readableTransactions = 10;
        writeTransactions(readableTransactions, 1, 1);
        appendCorruptedTransaction();
        writeTransactions(readableTransactions, 1, 1);
        CommittedCommandBatchRepresentation[] committedTransactionRepresentations = readAllFromReversedCursor();
        assertTransactionRange(
                committedTransactionRepresentations,
                readableTransactions + TransactionIdStore.BASE_TX_ID,
                TransactionIdStore.BASE_TX_ID);
    }

    @Test
    void failToReadCorruptedTransactionLogWhenConfigured() throws IOException {
        int readableTransactions = 10;
        writeTransactions(readableTransactions, 1, 1);
        appendCorruptedTransaction();
        writeTransactions(readableTransactions, 1, 1);

        assertThrows(IllegalStateException.class, this::readAllFromReversedCursorFailOnCorrupted);
    }

    @Test
    void respectMaxPosition() throws IOException {
        TransactionLogWriter writer = logFile.getTransactionLogWriter();
        int firstBatch = 99;
        writeTransactions(firstBatch, 1, 1);
        LogPosition offsetAfterFirstBatch = writer.getCurrentPosition();

        int secondBatch = 1;
        writeTransactions(1, 2000, 2000);
        LogPosition offsetAfterSecondBatch = writer.getCurrentPosition();

        int thirdBatch = 20;
        writeTransactions(thirdBatch, 1, 1);

        // We should skip the third batch
        txId -= thirdBatch;

        CommittedCommandBatchRepresentation[] committedTransactionRepresentations =
                readAllFromReversedCursorFailOnCorrupted(offsetAfterSecondBatch);
        assertTransactionRange(
                committedTransactionRepresentations,
                firstBatch + secondBatch + TransactionIdStore.BASE_TX_ID,
                TransactionIdStore.BASE_TX_ID);

        // We should skip the second and the third batch
        txId -= secondBatch;
        committedTransactionRepresentations = readAllFromReversedCursorFailOnCorrupted(offsetAfterFirstBatch);
        assertTransactionRange(
                committedTransactionRepresentations,
                firstBatch + TransactionIdStore.BASE_TX_ID,
                TransactionIdStore.BASE_TX_ID);
    }

    @Test
    void handleMaxPositionWithoutAnythingToReturnInLastFile() throws IOException {
        TransactionLogWriter writer = logFile.getTransactionLogWriter();
        int firstBatch = 1;
        writeTransactions(firstBatch, 1, 1);
        logFile.getLogRotation().rotateLogFile(LogAppendEvent.NULL);
        LogPosition offsetBeginningOfFile = writer.getCurrentPosition();

        writeTransactions(1, 1, 1);

        // First try the first file which should work even with the position being in another file
        CommittedCommandBatchRepresentation[] committedTransactionRepresentations =
                readAllFromReversedCursorFailOnCorrupted(offsetBeginningOfFile);
        assertTransactionRange(
                committedTransactionRepresentations,
                firstBatch + TransactionIdStore.BASE_TX_ID,
                TransactionIdStore.BASE_TX_ID);

        // The second that should return nothing
        try (ReversedSingleFileCommandBatchCursor cursor =
                txCursor(true, logFile.extractHeader(1).getStartPosition(), offsetBeginningOfFile)) {
            CommittedCommandBatchRepresentation[] exhaust = exhaust(cursor);
            assertThat(exhaust.length).isZero();
        }
    }

    @Test
    void validateMaxPositionBounds() {
        // Recover to position before start
        assertThatThrownBy(() -> readAllFromReversedCursorFailOnCorrupted(new LogPosition(0, 10)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("read past max position");

        // Recover to position outside of file size
        assertThatThrownBy(() -> readAllFromReversedCursorFailOnCorrupted(new LogPosition(0, Integer.MAX_VALUE)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("before requested maxOffset " + Integer.MAX_VALUE);
    }

    private CommittedCommandBatchRepresentation[] readAllFromReversedCursor() throws IOException {
        try (ReversedSingleFileCommandBatchCursor cursor = txCursor(false)) {
            return exhaust(cursor);
        }
    }

    private CommittedCommandBatchRepresentation[] readAllFromReversedCursorFailOnCorrupted() throws IOException {
        try (ReversedSingleFileCommandBatchCursor cursor = txCursor(true)) {
            return exhaust(cursor);
        }
    }

    private CommittedCommandBatchRepresentation[] readAllFromReversedCursorFailOnCorrupted(LogPosition maxPosition)
            throws IOException {
        try (ReversedSingleFileCommandBatchCursor cursor = txCursor(true, maxPosition)) {
            return exhaust(cursor);
        }
    }

    private static void assertTransactionRange(
            CommittedCommandBatchRepresentation[] readTransactions, long highTxId, long lowTxId) {
        long expectedTxId = highTxId;
        for (CommittedCommandBatchRepresentation commandBatch : readTransactions) {
            assertEquals(expectedTxId, commandBatch.txId());
            expectedTxId--;
        }
        assertEquals(expectedTxId, lowTxId);
    }

    private ReversedSingleFileCommandBatchCursor txCursor(boolean failOnCorruptedLogFiles) throws IOException {
        return txCursor(failOnCorruptedLogFiles, LogPosition.UNSPECIFIED);
    }

    private ReversedSingleFileCommandBatchCursor txCursor(boolean failOnCorruptedLogFiles, LogPosition maxPosition)
            throws IOException {
        return txCursor(
                failOnCorruptedLogFiles, logFiles.getLogFile().extractHeader(0).getStartPosition(), maxPosition);
    }

    private ReversedSingleFileCommandBatchCursor txCursor(
            boolean failOnCorruptedLogFiles, LogPosition startPosition, LogPosition maxPosition) throws IOException {
        ReadAheadLogChannel fileReader =
                (ReadAheadLogChannel) logFile.getReader(startPosition, LogVersionBridge.NO_MORE_CHANNELS);
        try {
            return ReversedSingleFileCommandBatchCursor.create(
                    fileReader, logEntryReader(), failOnCorruptedLogFiles, monitor, maxPosition);
        } catch (Exception e) {
            fileReader.close();
            throw e;
        }
    }

    private List<LogPosition> writeTransactions(int transactionCount, int minTransactionSize, int maxTransactionSize)
            throws IOException {
        FlushableLogPositionAwareChannel channel =
                logFile.getTransactionLogWriter().getChannel();
        TransactionLogWriter writer = logFile.getTransactionLogWriter();
        int previousChecksum = BASE_TX_CHECKSUM;
        List<LogPosition> startPositions = new ArrayList<>(transactionCount);
        for (int i = 0; i < transactionCount; i++) {
            long txId = ++this.txId;
            startPositions.add(writer.getCurrentPosition());
            previousChecksum = writer.append(
                    tx(random.intBetween(minTransactionSize, maxTransactionSize)),
                    txId,
                    txId,
                    UNKNOWN_CHUNK_ID,
                    previousChecksum,
                    UNKNOWN_APPEND_INDEX,
                    LogAppendEvent.NULL);
        }
        channel.prepareForFlush().flush();
        // Don't close the channel, LogFile owns it

        return startPositions;
    }

    private void appendCorruptedTransaction() throws IOException {
        var channel = logFile.getTransactionLogWriter().getChannel();
        TransactionLogWriter writer = new TransactionLogWriter(
                channel,
                new CorruptedLogEntryWriter<>(channel),
                fixed(LATEST_KERNEL_VERSION_WITHOUT_ENVELOPES),
                LogRotation.NO_ROTATION);
        long txId = ++this.txId;
        writer.append(
                tx(random.intBetween(100, 1000)),
                txId,
                txId,
                UNKNOWN_CHUNK_ID,
                BASE_TX_CHECKSUM,
                UNKNOWN_APPEND_INDEX,
                LogAppendEvent.NULL);
    }

    private static CommandBatch tx(int size) {
        List<StorageCommand> commands = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            // The type of command doesn't matter here
            commands.add(new TestCommand(LATEST_KERNEL_VERSION_WITHOUT_ENVELOPES));
        }
        return new CompleteCommandBatch(
                commands,
                UNKNOWN_CONSENSUS_INDEX,
                0,
                0,
                0,
                0,
                Leases.NO_LEASES,
                LATEST_KERNEL_VERSION_WITHOUT_ENVELOPES,
                ANONYMOUS);
    }

    private static class CorruptedLogEntryWriter<T extends WritableChannel> extends LogEntryWriter<T> {
        CorruptedLogEntryWriter(T channel) {
            super(channel, LatestVersions.BINARY_VERSIONS);
        }

        @Override
        public void writeStartEntry(
                KernelVersion kernelVersion,
                long timeWritten,
                long latestCommittedTxWhenStarted,
                long appendIndex,
                long transactionSequenceNumber,
                int previousChecksum,
                byte[] additionalHeaderData)
                throws IOException {
            channel.put(kernelVersion.version()).put(TX_START);
            for (int i = 0; i < 100; i++) {
                channel.put((byte) -1);
            }
        }
    }
}
