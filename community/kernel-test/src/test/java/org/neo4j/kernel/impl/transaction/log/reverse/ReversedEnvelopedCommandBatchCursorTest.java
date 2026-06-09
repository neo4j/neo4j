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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.common.Subject.ANONYMOUS;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.latest_kernel_version;
import static org.neo4j.kernel.KernelVersionProviders.fixed;
import static org.neo4j.kernel.impl.transaction.log.GivenCommandBatchCursor.exhaust;
import static org.neo4j.kernel.impl.transaction.log.TestLogEntryReader.logEntryReader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes.TX_START;
import static org.neo4j.kernel.impl.transaction.log.entry.LogSegments.DEFAULT_LOG_SEGMENT_SIZE;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CHUNK_ID;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TX_CHECKSUM;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.ArrayUtil;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FlushableChannel;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatchRepresentation;
import org.neo4j.kernel.impl.transaction.EmptyBatchRepresentation;
import org.neo4j.kernel.impl.transaction.log.CompleteCommandBatch;
import org.neo4j.kernel.impl.transaction.log.FlushableLogPositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReaderLogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.enveloped.EnvelopeReadChannel;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.LogRangeInfo;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.storageengine.api.CommandBatch;
import org.neo4j.storageengine.api.Leases;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.RandomExtension;

@Neo4jLayoutExtension
@ExtendWith({RandomExtension.class, LifeExtension.class})
class ReversedEnvelopedCommandBatchCursorTest {
    private static final KernelVersion kernelVersion = KernelVersion.VERSION_ENVELOPED_TRANSACTION_LOGS_GUARANTEED;
    public static final long ROTATION_THRESHOLD = ByteUnit.mebiBytes(10);
    private final InternalLogProvider logProvider = new AssertableLogProvider(true);
    private final ReverseTransactionCursorLoggingMonitor monitor =
            new ReverseTransactionCursorLoggingMonitor(logProvider.getLog(ReversedEnvelopedCommandBatchCursor.class));

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private DatabaseLayout databaseLayout;

    @Inject
    private LifeSupport life;

    @Inject
    private RandomSupport random;

    private long txId = TransactionIdStore.BASE_TX_ID;
    private LogFile logFile;
    private Config config;

    private static void assertTransactionRange(CommittedCommandBatchRepresentation[] readTransactions, long highTxId) {
        assertTransactionRange(readTransactions, highTxId, TransactionIdStore.BASE_TX_ID);
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

    private static CommandBatch tx(int size) {
        List<StorageCommand> commands = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            // The type of command doesn't matter here
            commands.add(new TestCommand(kernelVersion));
        }
        return new CompleteCommandBatch(
                commands, UNKNOWN_CONSENSUS_INDEX, 0, 0, 0, 0, Leases.NO_LEASES, kernelVersion, ANONYMOUS);
    }

    @BeforeEach
    void setUp() throws IOException {
        var storeId = new StoreId(1, 2, "engine-1", "format-1", 3, 4);
        config = Config.defaults(latest_kernel_version, kernelVersion.version());
        LogFiles logFiles = LogFilesBuilder.writeableBuilder(
                        databaseLayout, fs, fixed(kernelVersion), () -> LogFormat.fromKernelVersion(kernelVersion))
                .withRotationThreshold(ROTATION_THRESHOLD)
                .withCommandReaderFactory(TestCommandReaderFactory.INSTANCE)
                .withStoreId(storeId)
                .withConfig(config)
                .build();
        life.add(logFiles);
        logFile = logFiles.getLogFile();
    }

    @Test
    void positionShouldBeStartOfCurrentBatch() throws Exception {
        TransactionLogWriter writer = logFile.getTransactionLogWriter();
        LogPosition firstTxStart = writer.getCurrentPosition();
        writeTransactions(1, 1, 1);
        LogPosition secondTxStart = writer.getCurrentPosition();
        writeTransactions(1, 1, 1);

        try (ReversedEnvelopedCommandBatchCursor cursor = txCursor(true, LogPosition.UNSPECIFIED)) {
            cursor.next();
            assertThat(cursor.position()).isEqualTo(secondTxStart);
            cursor.next();
            assertThat(cursor.position()).isEqualTo(firstTxStart);
        }
    }

    @Test
    void shouldHandleVerySmallTransactions() throws Exception {
        // given
        writeTransactions(10, 1, 1);

        // when
        CommittedCommandBatchRepresentation[] readTransactions = readAllFromReversedCursor();

        // then
        assertTransactionRange(readTransactions, txId);
    }

    @Test
    void shouldHandleManyVerySmallTransactions() throws Exception {
        // given
        writeTransactions(20_000, 1, 1);

        // when
        CommittedCommandBatchRepresentation[] readTransactions = readAllFromReversedCursor();

        // then
        assertTransactionRange(readTransactions, txId);
    }

    @Test
    void shouldHandleLargeTransactions() throws Exception {
        // given
        writeTransactions(5, 1000, 1000);

        // when
        CommittedCommandBatchRepresentation[] readTransactions = readAllFromReversedCursor();

        // then
        assertTransactionRange(readTransactions, txId);
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
        LogPosition startPosition = logFile.extractHeader(0).getStartPosition();
        assertThatThrownBy(() -> {
                    try (ReadableLogChannel channel = logFile.getReader(startPosition);
                            ReadableLogChannel bridgedChannel =
                                    logFile.getReader(startPosition, ReaderLogVersionBridge.forFile(logFile))) {
                        new ReversedEnvelopedCommandBatchCursor(
                                (EnvelopeReadChannel) channel,
                                logEntryReader(),
                                false,
                                monitor,
                                (EnvelopeReadChannel) bridgedChannel,
                                LogPosition.UNSPECIFIED);
                    }
                })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("multiple log versions");
    }

    @Test
    void readCorruptedTransactionLog() throws IOException {
        int readableTransactions = 10;
        writeTransactions(readableTransactions, 1, 1);
        appendCorruptedTransaction();
        writeTransactions(readableTransactions, 1, 1);
        CommittedCommandBatchRepresentation[] committedTransactionRepresentations = readAllFromReversedCursor();
        CommittedCommandBatchRepresentation[] afterCorruption =
                Arrays.copyOfRange(committedTransactionRepresentations, 0, readableTransactions);
        CommittedCommandBatchRepresentation[] beforeCorruption = Arrays.copyOfRange(
                committedTransactionRepresentations, readableTransactions, committedTransactionRepresentations.length);
        long corruptedTxId = readableTransactions + TransactionIdStore.BASE_TX_ID + 1;
        assertTransactionRange(afterCorruption, corruptedTxId + readableTransactions, corruptedTxId);
        assertTransactionRange(beforeCorruption, corruptedTxId - 1);
    }

    @Test
    void failToReadCorruptedTransactionLogWhenConfigured() throws IOException {
        int readableTransactions = 10;
        writeTransactions(readableTransactions, 1, 1);
        appendCorruptedTransaction();
        writeTransactions(readableTransactions, 1, 1);

        assertThatThrownBy(this::readAllFromReversedCursorFailOnCorrupted)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Unreadable bytes are encountered after last readable position");
    }

    private void validateReversedRead(
            int totalTransactions, boolean expectLastTxToBeReadable, boolean isCorrupted, LogPosition maxPosition)
            throws IOException {
        CommittedCommandBatchRepresentation[] committedTransactionRepresentations;
        if (!isCorrupted) {
            // We expect no corrupted data encountered
            committedTransactionRepresentations = readAllFromReversedCursorFailOnCorrupted(maxPosition);
        } else {
            assertThrows(Exception.class, this::readAllFromReversedCursorFailOnCorrupted);
            // We know we can't read the final transaction for some reason in subsequent file
            // so don't throw to fully exercise the code paths in the next method
            committedTransactionRepresentations = readAllFromReversedCursor(maxPosition);
        }
        validateState(totalTransactions, expectLastTxToBeReadable, committedTransactionRepresentations);
    }

    private void validateState(
            int totalTransactions,
            boolean expectLastTxToBeReadable,
            CommittedCommandBatchRepresentation[] committedTransactionRepresentations) {
        // Confirm we have the reverse ordered list possibly including the final transaction
        int expectedTransactionCount = expectLastTxToBeReadable ? totalTransactions : totalTransactions - 1;
        assertEquals(expectedTransactionCount, committedTransactionRepresentations.length);
        List<Long> txIds = Arrays.stream(committedTransactionRepresentations)
                .map(CommittedCommandBatchRepresentation::txId)
                .toList();
        long expectedHighestTxId = expectLastTxToBeReadable ? txId : txId - 1L;
        assertEquals(expectedHighestTxId, txIds.getFirst());
        assertEquals(TransactionIdStore.BASE_TX_ID + 1L, txIds.getLast());
    }

    @Test
    void readWhenPreAllocatedFile() throws IOException {
        int readableTransactions = 100;
        try (PhysicalLogVersionedStoreChannel channel = logFile.createLogChannelForVersion(
                0L, () -> 1L, fixed(KernelVersion.GLORIOUS_FUTURE), BASE_TX_CHECKSUM, () -> LogFormat.V10)) {
            var zeros = ByteBuffer.allocate(DEFAULT_LOG_SEGMENT_SIZE);
            for (int i = 0; i < ROTATION_THRESHOLD / DEFAULT_LOG_SEGMENT_SIZE; i++) {
                channel.writeAll(zeros);
                zeros.position(0);
            }
            channel.flush();
        }
        writeTransactions(readableTransactions, 1, 1);
        // confirm all in one file
        assertEquals(0L, logFile.getLogRangeInfo().lowestVersion());
        // read back reversed
        validateReversedRead(readableTransactions, true, false, LogPosition.UNSPECIFIED);
    }

    @Test
    void readWhenLastTransactionIsRolled() throws IOException {
        int readableTransactions = 100;
        writeTransactions(readableTransactions, 1, 1);
        // Write large enough to bridge into next file
        writeTransactions(1, 200000, 200000);
        validateRolled();
        // read back reversed
        validateReversedRead(readableTransactions + 1, true, false, LogPosition.UNSPECIFIED);
    }

    @Test
    void readWhenLastTransactionIsRolledButNextFileMissing() throws IOException {
        int readableTransactions = 100;
        writeTransactions(readableTransactions, 1, 1);
        // Write large enough to bridge into next file
        writeTransactions(1, 200000, 200000);
        LogRangeInfo logRangeInfo = validateRolled();
        // remove all except first file
        for (long version = logRangeInfo.highestVersion(); version > logRangeInfo.lowestVersion(); version--) {
            logFile.delete(version);
        }
        // read back reversed
        validateReversedRead(readableTransactions + 1, false, false, LogPosition.UNSPECIFIED);
    }

    @Test
    void readWhenLastTransactionIsRolledButNextFileCorrupted() throws IOException {
        int readableTransactions = 100;
        writeTransactions(readableTransactions, 1, 1);
        // Write large enough to bridge into next file
        writeTransactions(1, 200000, 200000);
        LogRangeInfo logRangeInfo = validateRolled();
        // corrupt all except first file
        for (long version = logRangeInfo.highestVersion(); version > logRangeInfo.lowestVersion(); version--) {
            try (var channel = logFile.createLogChannelForExistingVersion(version)) {
                // Skip header
                channel.position(DEFAULT_LOG_SEGMENT_SIZE);
                // trash first real segment
                byte[] data = new byte[DEFAULT_LOG_SEGMENT_SIZE];
                Arrays.fill(data, (byte) 0xFF);
                channel.writeAll(ByteBuffer.wrap(data));
                channel.flush();
            }
        }
        // read back reversed
        validateReversedRead(readableTransactions + 1, false, true, LogPosition.UNSPECIFIED);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void readWhenLastTransactionIsRolledAndCutShortInNextFile(boolean truncateToSegmentBoundary) throws IOException {
        int readableTransactions = 100;
        writeTransactions(readableTransactions, 1, 1);
        // Write large enough to bridge into next file
        writeTransactions(1, 200000, 200000);
        LogRangeInfo logRangeInfo = validateRolled();
        // truncate the rollover file
        try (var channel = logFile.createLogChannelForExistingVersion(logRangeInfo.lowestVersion() + 1L)) {
            if (truncateToSegmentBoundary) {
                // truncate to header plus one segment
                channel.truncate(2L * DEFAULT_LOG_SEGMENT_SIZE);
            } else {
                // truncate to header plus an unaligned amount
                channel.truncate(DEFAULT_LOG_SEGMENT_SIZE + (DEFAULT_LOG_SEGMENT_SIZE / 3L));
            }
            channel.flush();
        }
        // read back reversed
        validateReversedRead(readableTransactions + 1, false, false, LogPosition.UNSPECIFIED);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void readWhenLastTransactionIsRolledAndNextFileTruncatedAfterIt(boolean truncateExactlyToTxEnd) throws IOException {
        int readableTransactions = 100;
        writeTransactions(readableTransactions, 1, 1);
        // Write large enough to bridge into next file
        writeTransactions(1, 200000, 200000);
        validateRolled();
        // Locate end of final transaction
        TransactionLogWriter writer = logFile.getTransactionLogWriter();
        LogPosition txEnd = writer.getCurrentPosition();
        // truncate the rollover file
        try (var channel = logFile.createLogChannelForExistingVersion(txEnd.getLogVersion())) {
            if (truncateExactlyToTxEnd) {
                // clip neatly to end of transaction
                channel.truncate(txEnd.getByteOffset());
            } else {
                // leave some ragged space after transaction
                channel.truncate(txEnd.getByteOffset() + DEFAULT_LOG_SEGMENT_SIZE);
            }
            channel.flush();
        }
        // read back reversed
        validateReversedRead(readableTransactions + 1, true, false, LogPosition.UNSPECIFIED);
    }

    @Test
    void respectMaxPosition() throws IOException {
        TransactionLogWriter writer = logFile.getTransactionLogWriter();
        int firstBatch = 99;
        writeTransactions(firstBatch, 1, 1);
        LogPosition offsetAfterFirstBatch = writer.getCurrentPosition();

        // Write large enough to bridge into next file
        int secondBatch = 1;
        writeTransactions(secondBatch, 200000, 200000);
        LogPosition offsetAfterSecondBatch = writer.getCurrentPosition();
        validateRolled();

        int thirdBatch = 20;
        writeTransactions(thirdBatch, 1, 1);

        // We should skip the third batch
        txId -= thirdBatch;

        CommittedCommandBatchRepresentation[] committedCommandBatchRepresentations =
                readAllFromReversedCursor(true, offsetAfterSecondBatch);
        LogPosition startPosition = logFile.extractHeader(1).getStartPosition();
        CommittedCommandBatchRepresentation[] committedCommandBatchRepresentationsFile1 =
                readAllFromReversedCursor(true, startPosition, offsetAfterSecondBatch);
        validateState(
                firstBatch + secondBatch,
                true,
                ArrayUtil.concat(committedCommandBatchRepresentationsFile1, committedCommandBatchRepresentations));

        // We should skip the second and the third batch
        txId -= secondBatch;
        committedCommandBatchRepresentations = readAllFromReversedCursor(true, offsetAfterFirstBatch);
        validateState(firstBatch, true, committedCommandBatchRepresentations);
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

    @Test
    void treatsNonKernelContentAsEmptyTx() throws IOException {
        TransactionLogWriter writer = logFile.getTransactionLogWriter();

        FlushableLogPositionAwareChannel channel =
                logFile.getTransactionLogWriter().getChannel();

        // Add tx content
        txId++;
        writer.append(
                tx(5),
                txId,
                UNKNOWN_CHUNK_ID,
                txId,
                UNKNOWN_TX_CHECKSUM,
                LogVersionRepository.UNKNOWN_LOG_OFFSET,
                LogAppendEvent.NULL);

        // Add non-tx content
        txId++;
        channel.beginChecksumForWriting();
        channel.putVersion(kernelVersion.version());
        channel.putContentType((byte) 5); // Non-kernel type
        channel.put(new byte[] {1, 1, 1, 1, 1, 1, 1, 1}, 8); // Put some data
        channel.putChecksum();

        // Add tx content
        txId++;
        writer.append(
                tx(5),
                txId,
                UNKNOWN_CHUNK_ID,
                txId,
                UNKNOWN_TX_CHECKSUM,
                LogVersionRepository.UNKNOWN_LOG_OFFSET,
                LogAppendEvent.NULL);

        channel.prepareForFlush().flush();

        // See that all three were processed and the middle one was considered an empty tx
        CommittedCommandBatchRepresentation[] committedCommandBatchRepresentations = readAllFromReversedCursor();
        validateState(3, true, committedCommandBatchRepresentations);

        assertThat(committedCommandBatchRepresentations[1]).isInstanceOf(EmptyBatchRepresentation.class);
    }

    private LogRangeInfo validateRolled() {
        LogRangeInfo logRangeInfo = logFile.getLogRangeInfo();
        assertNotEquals(logRangeInfo.lowestVersion(), logRangeInfo.highestVersion());
        return logRangeInfo;
    }

    private CommittedCommandBatchRepresentation[] readAllFromReversedCursor() throws IOException {
        return readAllFromReversedCursor(LogPosition.UNSPECIFIED);
    }

    private CommittedCommandBatchRepresentation[] readAllFromReversedCursor(LogPosition maxPosition)
            throws IOException {
        return readAllFromReversedCursor(false, maxPosition);
    }

    private CommittedCommandBatchRepresentation[] readAllFromReversedCursor(
            boolean failOnCorruptedLogFiles, LogPosition maxPosition) throws IOException {
        try (ReversedEnvelopedCommandBatchCursor cursor = txCursor(failOnCorruptedLogFiles, maxPosition)) {
            return exhaust(cursor);
        }
    }

    private CommittedCommandBatchRepresentation[] readAllFromReversedCursor(
            boolean failOnCorruptedLogFiles, LogPosition startPosition, LogPosition maxPosition) throws IOException {
        try (ReversedEnvelopedCommandBatchCursor cursor =
                txCursor(failOnCorruptedLogFiles, startPosition, maxPosition)) {
            return exhaust(cursor);
        }
    }

    private CommittedCommandBatchRepresentation[] readAllFromReversedCursorFailOnCorrupted() throws IOException {
        return readAllFromReversedCursorFailOnCorrupted(LogPosition.UNSPECIFIED);
    }

    private CommittedCommandBatchRepresentation[] readAllFromReversedCursorFailOnCorrupted(LogPosition maxPosition)
            throws IOException {
        try (ReversedEnvelopedCommandBatchCursor cursor = txCursor(true, maxPosition)) {
            return exhaust(cursor);
        }
    }

    private ReversedEnvelopedCommandBatchCursor txCursor(boolean failOnCorruptedLogFiles, LogPosition maxPosition)
            throws IOException {
        LogPosition startPosition = logFile.extractHeader(0).getStartPosition();
        return txCursor(failOnCorruptedLogFiles, startPosition, maxPosition);
    }

    private ReversedEnvelopedCommandBatchCursor txCursor(
            boolean failOnCorruptedLogFiles, LogPosition startPosition, LogPosition maxPosition) throws IOException {
        // get unbridged channel
        ReadableLogChannel fileReader = logFile.getReader(startPosition, LogVersionBridge.NO_MORE_CHANNELS);
        ReadableLogChannel fileReaderBridged =
                logFile.getReader(startPosition, ReaderLogVersionBridge.forFile(logFile));
        try {
            return new ReversedEnvelopedCommandBatchCursor(
                    (EnvelopeReadChannel) fileReader,
                    logEntryReader(new BinarySupportedKernelVersions(config)),
                    failOnCorruptedLogFiles,
                    monitor,
                    (EnvelopeReadChannel) fileReaderBridged,
                    maxPosition);
        } catch (Exception e) {
            fileReader.close();
            fileReaderBridged.close();
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
            long transactionId = ++txId;
            previousChecksum = writer.append(
                    tx(random.intBetween(minTransactionSize, maxTransactionSize)),
                    transactionId,
                    UNKNOWN_CHUNK_ID,
                    transactionId,
                    previousChecksum,
                    LogVersionRepository.UNKNOWN_LOG_OFFSET,
                    LogAppendEvent.NULL);
        }
        channel.prepareForFlush().flush();
        // Don't close the channel, LogFile owns it
    }

    private void appendCorruptedTransaction() throws IOException {
        var channel = logFile.getTransactionLogWriter().getChannel();
        TransactionLogWriter writer = new TransactionLogWriter(
                channel, new CorruptedLogEntryWriter<>(channel), fixed(kernelVersion), LogRotation.NO_ROTATION);
        long transactionId = ++txId;
        writer.append(
                tx(random.intBetween(100, 1000)),
                transactionId,
                UNKNOWN_CHUNK_ID,
                transactionId,
                BASE_TX_CHECKSUM,
                LogVersionRepository.UNKNOWN_LOG_OFFSET,
                LogAppendEvent.NULL);
    }

    private static class CorruptedLogEntryWriter<T extends FlushableChannel> extends LogEntryWriter<T> {
        CorruptedLogEntryWriter(T channel) {
            super(
                    channel,
                    new BinarySupportedKernelVersions(Config.defaults(latest_kernel_version, kernelVersion.version())));
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
            channel.putVersion(kernelVersion.version()).put(TX_START);
            for (int i = 0; i < 100; i++) {
                channel.put((byte) -1);
            }
        }
    }
}
