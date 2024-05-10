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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.common.Subject.ANONYMOUS;
import static org.neo4j.kernel.impl.api.TransactionToApply.NOT_SPECIFIED_CHUNK_ID;
import static org.neo4j.kernel.impl.transaction.log.GivenCommandBatchCursor.exhaust;
import static org.neo4j.kernel.impl.transaction.log.LogPosition.UNSPECIFIED;
import static org.neo4j.kernel.impl.transaction.log.TestLogEntryReader.logEntryReader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.TX_START;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION_PROVIDER;

import java.io.IOException;
import java.util.ArrayList;
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
import org.neo4j.kernel.impl.transaction.CommittedCommandBatch;
import org.neo4j.kernel.impl.transaction.SimpleAppendIndexProvider;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.CompleteTransaction;
import org.neo4j.kernel.impl.transaction.log.FlushableLogPositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.storageengine.api.LogVersionRepository;
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
        LogVersionRepository logVersionRepository = new SimpleLogVersionRepository();
        SimpleTransactionIdStore transactionIdStore = new SimpleTransactionIdStore();
        SimpleAppendIndexProvider appendIndexProvider = new SimpleAppendIndexProvider();
        var storeId = new StoreId(1, 2, "engine-1", "format-1", 3, 4);
        logFiles = LogFilesBuilder.builder(databaseLayout, fs, LATEST_KERNEL_VERSION_PROVIDER)
                .withRotationThreshold(ByteUnit.mebiBytes(10))
                .withLogVersionRepository(logVersionRepository)
                .withTransactionIdStore(transactionIdStore)
                .withAppendIndexProvider(appendIndexProvider)
                .withCommandReaderFactory(TestCommandReaderFactory.INSTANCE)
                .withStoreId(storeId)
                .build();
        life.add(logFiles);
        logFile = logFiles.getLogFile();
    }

    @Test
    void shouldHandleVerySmallTransactions() throws Exception {
        // given
        writeTransactions(10, 1, 1);

        // when
        CommittedCommandBatch[] readTransactions = readAllFromReversedCursor();

        // then
        assertTransactionRange(readTransactions, txId, TransactionIdStore.BASE_TX_ID);
    }

    @Test
    void shouldHandleManyVerySmallTransactions() throws Exception {
        // given
        writeTransactions(20_000, 1, 1);

        // when
        CommittedCommandBatch[] readTransactions = readAllFromReversedCursor();

        // then
        assertTransactionRange(readTransactions, txId, TransactionIdStore.BASE_TX_ID);
    }

    @Test
    void shouldHandleLargeTransactions() throws Exception {
        // given
        writeTransactions(10, 1000, 1000);

        // when
        CommittedCommandBatch[] readTransactions = readAllFromReversedCursor();

        // then
        assertTransactionRange(readTransactions, txId, TransactionIdStore.BASE_TX_ID);
    }

    @Test
    void shouldHandleEmptyLog() throws Exception {
        // given

        // when
        CommittedCommandBatch[] readTransactions = readAllFromReversedCursor();

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
        try (ReadAheadLogChannel channel = (ReadAheadLogChannel)
                logFile.getReader(logFiles.getLogFile().extractHeader(0).getStartPosition())) {
            new ReversedSingleFileCommandBatchCursor(channel, logEntryReader(), false, monitor, false);
            fail("Should've failed");
        } catch (IllegalArgumentException e) {
            // then good
            assertThat(e.getMessage()).contains("multiple log versions");
        }
    }

    @Test
    void readCorruptedTransactionLog() throws IOException {
        int readableTransactions = 10;
        writeTransactions(readableTransactions, 1, 1);
        appendCorruptedTransaction();
        writeTransactions(readableTransactions, 1, 1);
        CommittedCommandBatch[] committedTransactionRepresentations = readAllFromReversedCursor();
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

    private CommittedCommandBatch[] readAllFromReversedCursor() throws IOException {
        try (ReversedSingleFileCommandBatchCursor cursor = txCursor(false)) {
            return exhaust(cursor);
        }
    }

    private CommittedCommandBatch[] readAllFromReversedCursorFailOnCorrupted() throws IOException {
        try (ReversedSingleFileCommandBatchCursor cursor = txCursor(true)) {
            return exhaust(cursor);
        }
    }

    private static void assertTransactionRange(CommittedCommandBatch[] readTransactions, long highTxId, long lowTxId) {
        long expectedTxId = highTxId;
        for (CommittedCommandBatch commandBatch : readTransactions) {
            assertEquals(expectedTxId, commandBatch.txId());
            expectedTxId--;
        }
        assertEquals(expectedTxId, lowTxId);
    }

    private ReversedSingleFileCommandBatchCursor txCursor(boolean failOnCorruptedLogFiles) throws IOException {
        ReadAheadLogChannel fileReader = (ReadAheadLogChannel)
                logFile.getReader(logFiles.getLogFile().extractHeader(0).getStartPosition());
        try {
            return new ReversedSingleFileCommandBatchCursor(
                    fileReader, logEntryReader(), failOnCorruptedLogFiles, monitor, false);
        } catch (Exception e) {
            fileReader.close();
            throw e;
        }
    }

    private void writeTransactions(int transactionCount, int minTransactionSize, int maxTransactionSize)
            throws IOException {
        FlushableLogPositionAwareChannel channel =
                logFile.getTransactionLogWriter().getChannel();
        TransactionLogWriter writer = logFile.getTransactionLogWriter();
        int previousChecksum = BASE_TX_CHECKSUM;
        for (int i = 0; i < transactionCount; i++) {
            long txId = ++this.txId;
            previousChecksum = writer.append(
                    tx(random.intBetween(minTransactionSize, maxTransactionSize)),
                    txId,
                    txId,
                    NOT_SPECIFIED_CHUNK_ID,
                    previousChecksum,
                    UNSPECIFIED);
        }
        channel.prepareForFlush().flush();
        // Don't close the channel, LogFile owns it
    }

    private void appendCorruptedTransaction() throws IOException {
        var channel = logFile.getTransactionLogWriter().getChannel();
        TransactionLogWriter writer = new TransactionLogWriter(
                channel, new CorruptedLogEntryWriter<>(channel), LATEST_KERNEL_VERSION_PROVIDER);
        long txId = ++this.txId;
        writer.append(
                tx(random.intBetween(100, 1000)), txId, txId, NOT_SPECIFIED_CHUNK_ID, BASE_TX_CHECKSUM, UNSPECIFIED);
    }

    private static CommandBatch tx(int size) {
        List<StorageCommand> commands = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            // The type of command doesn't matter here
            commands.add(new TestCommand());
        }
        return new CompleteTransaction(
                commands, UNKNOWN_CONSENSUS_INDEX, 0, 0, 0, 0, LatestVersions.LATEST_KERNEL_VERSION, ANONYMOUS);
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
