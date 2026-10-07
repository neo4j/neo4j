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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.common.Subject.ANONYMOUS;
import static org.neo4j.kernel.impl.api.LeaseService.NO_LEASE;
import static org.neo4j.storageengine.AppendIndexProvider.BASE_APPEND_INDEX;
import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TX_SEQUENCE_NUMBER;
import static org.neo4j.wal.LogIndexEncoding.encodeLogIndex;
import static org.neo4j.wal.entry.LogEnvelopeHeader.HEADER_SIZE;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.database.DbmsRuntimeVersion;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.api.TestCommand;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.SimpleAppendIndexProvider;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.Leases;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.wal.AppendBatchInfo;
import org.neo4j.wal.CompleteCommandBatch;
import org.neo4j.wal.LogAppendEvent;
import org.neo4j.wal.LogFile;
import org.neo4j.wal.LogPosition;
import org.neo4j.wal.TransactionLogWriter;
import org.neo4j.wal.WALService;
import org.neo4j.wal.entry.LogFormat;
import org.neo4j.wal.entry.UnsupportedLogVersionException;
import org.neo4j.wal.files.LogFilesBuilder;
import org.neo4j.wal.files.LogRangeInfo;

@Neo4jLayoutExtension
class DetachedLogTailAppendIndexProviderTest {
    private static final long LAST_KNOWN_APPEND_INDEX = 5;
    private static final LogPosition LAST_KNOWN_POSITION = new LogPosition(0, 128);

    private static final int SEGMENT_BLOCK_SIZE_BYTES = 1024;
    private static final long ROTATION_THRESHOLD = 4L * SEGMENT_BLOCK_SIZE_BYTES;
    private static final int DEFAULT_COMMAND_SIZE = 50;
    // any content type other than kernel, replicated tx or version upgrade reads back as a LogEntryEmpty
    private static final byte RAFT_CONTENT_TYPE = 1;
    // raft logs start from index -1, below UNKNOWN_APPEND_INDEX
    private static final long MERGED_LOG_HEADER_APPEND_INDEX = -1;
    private static final StoreId STORE_ID = new StoreId(1, 2, "engine-1", "format-1", 3, 4);

    private final Config config = Config.newBuilder()
            .set(GraphDatabaseInternalSettings.latest_runtime_version, DbmsRuntimeVersion.GLORIOUS_FUTURE.getVersion())
            .set(GraphDatabaseInternalSettings.latest_kernel_version, KernelVersion.GLORIOUS_FUTURE.version())
            .build();
    private final BinarySupportedKernelVersions binarySupportedKernelVersions =
            new BinarySupportedKernelVersions(config);
    private final LifeSupport life = new LifeSupport();
    private final SimpleAppendIndexProvider appendIndexProvider = new SimpleAppendIndexProvider();

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private DatabaseLayout databaseLayout;

    private LogFormat logFormat;
    private KernelVersion kernelVersion;
    private LogFile logFile;
    private TransactionLogWriter logWriter;
    private int previousChecksum = BASE_TX_CHECKSUM;
    private long txId;

    @AfterEach
    void tearDown() {
        life.shutdown();
    }

    @Test
    void unsupportedLogVersionFallsBackToLastKnownAppendIndexByDefault() throws IOException {
        LogFile mockedLogFile = logFileFailingWithUnsupportedVersion();

        AppendBatchInfo lastBatch = mockedProvider(mockedLogFile, false).get();

        assertThat(lastBatch.appendIndex()).isEqualTo(LAST_KNOWN_APPEND_INDEX);
        assertThat(lastBatch.logPositionAfter()).isEqualTo(LAST_KNOWN_POSITION);
        verify(mockedLogFile).getReader(any(), any());
    }

    @Test
    void unsupportedLogVersionPropagatesUnwrappedWhenRequested() throws IOException {
        LogFile mockedLogFile = logFileFailingWithUnsupportedVersion();

        assertThatThrownBy(() -> mockedProvider(mockedLogFile, true).get())
                .isInstanceOf(UnsupportedLogVersionException.class);
    }

    @ParameterizedTest
    @EnumSource(
            value = LogFormat.class,
            names = {"V9", "V10", "V11"})
    void startOfEmptyFileWithoutCheckpointInfoUsesHeader(LogFormat format) throws IOException {
        startLogFiles(format);
        LogPosition start = startOfFile(0);

        var info = read(start, UNKNOWN_APPEND_INDEX, UNKNOWN_CONSENSUS_INDEX);

        assertThat(logFile.extractHeader(0).getLastAppendIndex()).isEqualTo(BASE_APPEND_INDEX);
        assertInfo(info, BASE_APPEND_INDEX, start, consensusWhenOnlyAppendIndexKnown(BASE_APPEND_INDEX));
    }

    @ParameterizedTest
    @EnumSource(
            value = LogFormat.class,
            names = {"V9", "V10", "V11"})
    void startOfEmptyFileWithMergedLogHeaderIsNeverBelowUnknownAppendIndex(LogFormat format) throws IOException {
        startLogFiles(format, MERGED_LOG_HEADER_APPEND_INDEX);
        LogPosition start = startOfFile(0);

        var info = read(start, UNKNOWN_APPEND_INDEX, UNKNOWN_CONSENSUS_INDEX);

        assertThat(logFile.extractHeader(0).getLastAppendIndex()).isEqualTo(MERGED_LOG_HEADER_APPEND_INDEX);
        assertInfo(info, UNKNOWN_APPEND_INDEX, start, consensusWhenOnlyAppendIndexKnown(UNKNOWN_APPEND_INDEX));
    }

    @ParameterizedTest
    @EnumSource(
            value = LogFormat.class,
            names = {"V9", "V10", "V11"})
    void startOfEmptyFileWithCheckpointInfoReturnsCheckpointInfo(LogFormat format) throws IOException {
        startLogFiles(format);
        LogPosition start = startOfFile(0);

        var info = read(start, LAST_KNOWN_APPEND_INDEX, LAST_KNOWN_APPEND_INDEX + 1);

        assertInfo(info, LAST_KNOWN_APPEND_INDEX, start, LAST_KNOWN_APPEND_INDEX + 1);
    }

    @ParameterizedTest
    @EnumSource(
            value = LogFormat.class,
            names = {"V9", "V10", "V11"})
    void startOfFileFindsLastEntry(LogFormat format) throws IOException {
        startLogFiles(format);
        LogPosition start = startOfFile(0);
        writeTx();
        writeTx();
        var last = writeTx();

        var info = read(start, UNKNOWN_APPEND_INDEX, UNKNOWN_CONSENSUS_INDEX);

        assertInfo(info, last);
    }

    @ParameterizedTest
    @EnumSource(
            value = LogFormat.class,
            names = {"V9", "V10", "V11"})
    void middleOfFileFindsLastEntry(LogFormat format) throws IOException {
        startLogFiles(format);
        var checkpointed = writeTx();
        writeTx();
        var last = writeTx();

        assertInfo(read(checkpointed.positionAfter(), checkpointed.appendIndex(), checkpointed.consensusIndex()), last);
        assertInfo(read(checkpointed.positionAfter(), UNKNOWN_APPEND_INDEX, UNKNOWN_CONSENSUS_INDEX), last);
    }

    @ParameterizedTest
    @EnumSource(
            value = LogFormat.class,
            names = {"V9", "V10", "V11"})
    void maxPositionStopsReadingAtEntryEndingOnIt(LogFormat format) throws IOException {
        startLogFiles(format);
        LogPosition start = startOfFile(0);
        writeTx();
        var lastStable = writeTx();
        writeTx();

        var info = read(start, UNKNOWN_APPEND_INDEX, UNKNOWN_CONSENSUS_INDEX, lastStable.positionAfter());

        assertInfo(info, lastStable);
    }

    @ParameterizedTest
    @EnumSource(
            value = LogFormat.class,
            names = {"V9", "V10", "V11"})
    void endOfFileWithCheckpointInfoReturnsCheckpointInfo(LogFormat format) throws IOException {
        startLogFiles(format);
        writeTx();
        var last = writeTx();

        var info = read(last.positionAfter(), last.appendIndex(), last.consensusIndex());

        assertInfo(info, last);
    }

    @ParameterizedTest
    @MethodSource("envelopedFormatsAndHeaderIndexes")
    void endOfEnvelopedFileWithoutCheckpointInfoLoadsAppendIndexBeforePosition(
            LogFormat format, long firstHeaderAppendIndex) throws IOException {
        startLogFiles(format, firstHeaderAppendIndex);
        writeTx();
        writeTx();
        var last = writeTx();

        var info = read(last.positionAfter(), UNKNOWN_APPEND_INDEX, UNKNOWN_CONSENSUS_INDEX);

        assertInfo(
                info, last.appendIndex(), last.positionAfter(), consensusWhenOnlyAppendIndexKnown(last.appendIndex()));
    }

    @ParameterizedTest
    @EnumSource(
            value = LogFormat.class,
            names = {"V10", "V11"})
    void middleOfRotatedEnvelopedFileWithoutCheckpointInfoLoadsAppendIndexBeforePosition(LogFormat format)
            throws IOException {
        startLogFiles(format);
        writeTx();
        rotate();
        writeTx();
        var last = writeTx();

        var info = read(last.positionAfter(), UNKNOWN_APPEND_INDEX, UNKNOWN_CONSENSUS_INDEX);

        assertThat(logFile.extractHeader(1).getLastAppendIndex()).isLessThan(last.appendIndex());
        assertInfo(
                info, last.appendIndex(), last.positionAfter(), consensusWhenOnlyAppendIndexKnown(last.appendIndex()));
    }

    @ParameterizedTest
    @ValueSource(longs = {BASE_APPEND_INDEX, MERGED_LOG_HEADER_APPEND_INDEX})
    void endOfNonEnvelopedFileWithoutCheckpointInfoCanOnlyUseHeader(long firstHeaderAppendIndex) throws IOException {
        startLogFiles(LogFormat.V9, firstHeaderAppendIndex);
        writeTx();
        var last = writeTx();

        var info = read(last.positionAfter(), UNKNOWN_APPEND_INDEX, UNKNOWN_CONSENSUS_INDEX);

        assertInfo(
                info,
                Math.max(firstHeaderAppendIndex, UNKNOWN_APPEND_INDEX),
                last.positionAfter(),
                UNKNOWN_CONSENSUS_INDEX);
    }

    @ParameterizedTest
    @EnumSource(
            value = LogFormat.class,
            names = {"V9", "V10", "V11"})
    void startOfEmptyRotatedFileUsesHeaderOfThatFile(LogFormat format) throws IOException {
        startLogFiles(format);
        writeTx();
        var last = writeTx();
        rotate();
        LogPosition start = startOfFile(1);

        var info = read(start, UNKNOWN_APPEND_INDEX, UNKNOWN_CONSENSUS_INDEX);

        assertThat(logFile.extractHeader(1).getLastAppendIndex()).isEqualTo(last.appendIndex());
        assertInfo(info, last.appendIndex(), start, consensusWhenOnlyAppendIndexKnown(last.appendIndex()));
    }

    @ParameterizedTest
    @EnumSource(
            value = LogFormat.class,
            names = {"V9", "V10", "V11"})
    void endOfFileFollowedByEmptyRotatedFileUsesHeaderOfLaterFile(LogFormat format) throws IOException {
        startLogFiles(format);
        writeTx();
        var last = writeTx();
        rotate();

        var info = read(last.positionAfter(), UNKNOWN_APPEND_INDEX, UNKNOWN_CONSENSUS_INDEX);

        assertInfo(
                info, last.appendIndex(), last.positionAfter(), consensusWhenOnlyAppendIndexKnown(last.appendIndex()));
    }

    @ParameterizedTest
    @EnumSource(
            value = LogFormat.class,
            names = {"V9", "V10", "V11"})
    void endOfFileFindsEntryInRotatedFile(LogFormat format) throws IOException {
        startLogFiles(format);
        var checkpointed = writeTx();
        rotate();
        var last = writeTx();

        assertThat(last.positionAfter().getLogVersion()).isEqualTo(1);
        assertInfo(read(checkpointed.positionAfter(), checkpointed.appendIndex(), checkpointed.consensusIndex()), last);
    }

    @Test
    void mergedLogEmptyEntriesSetConsensusIndexToAppendIndex() throws IOException {
        startLogFiles(LogFormat.V11);
        var checkpointed = writeTx();
        writeRaftEntry();
        var last = writeRaftEntry();

        assertInfo(read(checkpointed.positionAfter(), checkpointed.appendIndex(), checkpointed.consensusIndex()), last);
        assertInfo(read(startOfFile(0), UNKNOWN_APPEND_INDEX, UNKNOWN_CONSENSUS_INDEX), last);
    }

    @Test
    void mergedLogEndOfEmptyEntriesWithoutCheckpointInfoLoadsAppendIndexBeforePosition() throws IOException {
        startLogFiles(LogFormat.V11, MERGED_LOG_HEADER_APPEND_INDEX);
        writeRaftEntry();
        writeRaftEntry();
        writeRaftEntry();
        var last = writeRaftEntry();

        var info = read(last.positionAfter(), UNKNOWN_APPEND_INDEX, UNKNOWN_CONSENSUS_INDEX);

        assertInfo(info, last);
    }

    @ParameterizedTest
    @MethodSource("paddedEndOfFileWithHeaderIndexCases")
    void paddedEndOfDataOnEdgeOfRotationWithNothingAfter(
            LogFormat format, int bytesLeftInFile, long firstHeaderAppendIndex) throws IOException {
        startLogFiles(format, firstHeaderAppendIndex);
        var last = writeTxEndingAtBytesLeftInFile(bytesLeftInFile);

        assertThat(logFile.versionExists(1)).isFalse();
        assertInfo(read(last.positionAfter(), last.appendIndex(), last.consensusIndex()), last);
        assertInfo(
                read(last.positionAfter(), UNKNOWN_APPEND_INDEX, UNKNOWN_CONSENSUS_INDEX),
                last.appendIndex(),
                last.positionAfter(),
                consensusWhenOnlyAppendIndexKnown(last.appendIndex()));
    }

    @ParameterizedTest
    @MethodSource("paddedEndOfFileCases")
    void paddedEndOfDataOnEdgeOfRotationFindsEntryInNextFile(LogFormat format, int bytesLeftInFile) throws IOException {
        startLogFiles(format);
        var checkpointed = writeTxEndingAtBytesLeftInFile(bytesLeftInFile);
        var last = writeTx();

        assertThat(last.positionAfter().getLogVersion())
                .as("next entry should have been padded into a rotated file")
                .isEqualTo(1);
        assertInfo(read(checkpointed.positionAfter(), checkpointed.appendIndex(), checkpointed.consensusIndex()), last);
        assertInfo(read(checkpointed.positionAfter(), UNKNOWN_APPEND_INDEX, UNKNOWN_CONSENSUS_INDEX), last);
    }

    private static Stream<Arguments> paddedEndOfFileCases() {
        return Stream.of(LogFormat.V10, LogFormat.V11)
                .flatMap(format -> Stream.of(0, 1, HEADER_SIZE).map(bytesLeft -> Arguments.of(format, bytesLeft)));
    }

    private static Stream<Arguments> paddedEndOfFileWithHeaderIndexCases() {
        return paddedEndOfFileCases()
                .flatMap(args -> Stream.of(BASE_APPEND_INDEX, MERGED_LOG_HEADER_APPEND_INDEX)
                        .map(headerIndex -> Arguments.of(args.get()[0], args.get()[1], headerIndex)));
    }

    private static Stream<Arguments> envelopedFormatsAndHeaderIndexes() {
        return Stream.of(LogFormat.V10, LogFormat.V11)
                .flatMap(format -> Stream.of(BASE_APPEND_INDEX, MERGED_LOG_HEADER_APPEND_INDEX)
                        .map(headerIndex -> Arguments.of(format, headerIndex)));
    }

    private record Entry(long appendIndex, LogPosition positionAfter, long consensusIndex) {}

    private void startLogFiles(LogFormat format) throws IOException {
        startLogFiles(format, BASE_APPEND_INDEX);
    }

    private void startLogFiles(LogFormat format, long firstHeaderAppendIndex) throws IOException {
        logFormat = format;
        appendIndexProvider.setAppendIndex(firstHeaderAppendIndex);
        kernelVersion = switch (format) {
            case V9 -> KernelVersion.V5_25;
            case V10 -> KernelVersion.getLatestVersion(config);
            default -> KernelVersion.GLORIOUS_FUTURE;
        };
        WALService walService = LogFilesBuilder.writeableBuilder(databaseLayout, fs, () -> kernelVersion, () -> format)
                .withCommandReaderFactory(TestCommandReaderFactory.INSTANCE)
                .withStoreId(STORE_ID)
                .withAppendIndexProvider(appendIndexProvider)
                .withRotationThreshold(ROTATION_THRESHOLD)
                .withEnvelopeSegmentBlockSizeBytes(SEGMENT_BLOCK_SIZE_BYTES)
                .withConfig(config)
                .build();
        life.add(walService);
        life.start();
        logFile = walService.getLogFile();
        logWriter = (TransactionLogWriter) logFile.getTransactionLogWriter();
        assertThat(logFile.extractHeader(0).getLogFormatVersion()).isEqualTo(format);
    }

    private Entry writeTx() throws IOException {
        return writeTx(DEFAULT_COMMAND_SIZE);
    }

    private Entry writeTx(int commandSize) throws IOException {
        long appendIndex = appendIndexProvider.nextAppendIndex();
        long consensusIndex = logFormat == LogFormat.V11 ? appendIndex : appendIndex + 100;
        var entryWriter = logWriter.getWriter();
        entryWriter.writeStartEntry(
                kernelVersion,
                0,
                txId,
                appendIndex,
                UNKNOWN_TX_SEQUENCE_NUMBER,
                previousChecksum,
                encodeLogIndex(consensusIndex));
        entryWriter.serialize(new CompleteCommandBatch(
                List.of(new TestCommand(commandSize, kernelVersion)),
                consensusIndex,
                0,
                txId,
                0,
                NO_LEASE,
                Leases.NO_LEASES,
                kernelVersion,
                ANONYMOUS));
        previousChecksum = entryWriter.writeCommitEntry(kernelVersion, ++txId, 0);
        return new Entry(appendIndex, logWriter.getCurrentPosition(), consensusIndex);
    }

    private Entry writeRaftEntry() throws IOException {
        long appendIndex = appendIndexProvider.nextAppendIndex();
        var channel = logWriter.getChannel();
        channel.beginChecksumForWriting();
        channel.putVersion(kernelVersion.version());
        channel.putContentType(RAFT_CONTENT_TYPE);
        channel.putAppendIndex(appendIndex);
        channel.putLong(appendIndex);
        previousChecksum = channel.putChecksum();
        return new Entry(appendIndex, logWriter.getCurrentPosition(), appendIndex);
    }

    /**
     * Writes transactions so that the last one ends in the final segment of the first file with exactly
     * {@code bytesLeftInFile} bytes after it. Anything up to {@link org.neo4j.wal.entry.LogEnvelopeHeader#HEADER_SIZE}
     * is too small for another envelope, so the next write pads the segment and rotates.
     */
    private Entry writeTxEndingAtBytesLeftInFile(int bytesLeftInFile) throws IOException {
        long before = logWriter.getCurrentPosition().getByteOffset();
        long txSizeWithDefaultCommand = writeTx().positionAfter().getByteOffset() - before;
        long bytesLeft;
        while ((bytesLeft = ROTATION_THRESHOLD - logWriter.getCurrentPosition().getByteOffset())
                > 2 * txSizeWithDefaultCommand + HEADER_SIZE) {
            writeTx();
        }
        long targetTxSize = bytesLeft - bytesLeftInFile;
        var last = writeTx((int) (DEFAULT_COMMAND_SIZE + targetTxSize - txSizeWithDefaultCommand));

        assertThat(last.positionAfter().getLogVersion()).isEqualTo(0);
        assertThat(ROTATION_THRESHOLD - last.positionAfter().getByteOffset()).isEqualTo(bytesLeftInFile);
        return last;
    }

    private void rotate() throws IOException {
        logFile.getLogRotation().rotateLogFile(LogAppendEvent.NULL);
    }

    private LogPosition startOfFile(long version) throws IOException {
        return logFile.extractHeader(version).getStartPosition();
    }

    private long consensusWhenOnlyAppendIndexKnown(long appendIndex) {
        return logFormat == LogFormat.V11 ? appendIndex : UNKNOWN_CONSENSUS_INDEX;
    }

    private AppendBatchInfo read(LogPosition position, long startingAppendIndex, long startingConsensusIndex)
            throws IOException {
        return read(position, startingAppendIndex, startingConsensusIndex, LogPosition.UNSPECIFIED);
    }

    private AppendBatchInfo read(
            LogPosition position, long startingAppendIndex, long startingConsensusIndex, LogPosition maxPosition)
            throws IOException {
        logFile.flush();
        return new DetachedLogTailAppendIndexProvider(
                        TestCommandReaderFactory.INSTANCE,
                        binarySupportedKernelVersions,
                        logFile,
                        kernelVersion,
                        startingAppendIndex,
                        startingConsensusIndex,
                        position,
                        EmptyMemoryTracker.INSTANCE,
                        maxPosition,
                        true)
                .get();
    }

    private static void assertInfo(AppendBatchInfo info, Entry expected) {
        assertInfo(info, expected.appendIndex(), expected.positionAfter(), expected.consensusIndex());
    }

    private static void assertInfo(
            AppendBatchInfo info, long appendIndex, LogPosition positionAfter, long consensusIndex) {
        assertThat(info.appendIndex()).as("appendIndex").isEqualTo(appendIndex);
        assertThat(info.logPositionAfter()).as("logPositionAfter").isEqualTo(positionAfter);
        assertThat(info.consensusIndex()).as("consensusIndex").isEqualTo(consensusIndex);
    }

    private LogFile logFileFailingWithUnsupportedVersion() throws IOException {
        Path file = Path.of("neostore.transaction.db.0");
        LogFile mockedLogFile = mock(LogFile.class);
        when(mockedLogFile.versionExists(anyLong())).thenReturn(true);
        when(mockedLogFile.getLogRangeInfo()).thenReturn(new LogRangeInfo(0, file, 0, file));
        when(mockedLogFile.getReader(any(), any()))
                .thenThrow(UnsupportedLogVersionException.unsupported(
                        new BinarySupportedKernelVersions(config), KernelVersion.GLORIOUS_FUTURE.version()));
        return mockedLogFile;
    }

    private DetachedLogTailAppendIndexProvider mockedProvider(
            LogFile mockedLogFile, boolean failOnUnsupportedLogVersion) {
        return new DetachedLogTailAppendIndexProvider(
                mock(CommandReaderFactory.class),
                new BinarySupportedKernelVersions(config),
                mockedLogFile,
                KernelVersion.getLatestVersion(config),
                LAST_KNOWN_APPEND_INDEX,
                UNKNOWN_CONSENSUS_INDEX,
                LAST_KNOWN_POSITION,
                EmptyMemoryTracker.INSTANCE,
                LogPosition.UNSPECIFIED,
                failOnUnsupportedLogVersion);
    }
}
