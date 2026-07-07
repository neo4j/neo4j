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
package org.neo4j.kernel.impl.transaction.log.enveloped;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.setMaxStackTraceElementsDisplayed;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.UNSPECIFIED_CREATION_TIME;
import static org.neo4j.kernel.impl.transaction.log.enveloped.LogsRepository.BASE_VERSION;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.nativeimpl.NativeAccessProvider;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.io.fs.filename.SequentialFileNameHelper;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.AppendTransactionEvent;
import org.neo4j.kernel.impl.transaction.log.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.log.LogFileCreateEvent;
import org.neo4j.kernel.impl.transaction.log.LogFileFlushEvent;
import org.neo4j.kernel.impl.transaction.log.LogForceEvent;
import org.neo4j.kernel.impl.transaction.log.LogForceWaitEvent;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.LogTracers;
import org.neo4j.kernel.impl.transaction.log.StoreChannelNativeAccessor;
import org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.EnvelopeType;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.pruning.ThresholdFactory;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotateEvent;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreIdentifier;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.time.FakeClock;

@TestDirectoryExtension
@RandomSupportExtension
class EnvelopedLogFilesTest {

    @Inject
    RandomSupport randomSupport;

    private static final String EIGHT_BYTES_MESSAGE = "message!";
    public static final int writeBufferedBlocks = 2;
    private final int segmentBlockSize = 256;
    private final int totalSegments = 3;
    private final int totalFileDataSize = segmentBlockSize * (totalSegments - 1);
    private final FakeClock clock = new FakeClock();

    @Inject
    TestDirectory testDirectory;

    @Inject
    FileSystemAbstraction fs;

    private EnvelopedLogFiles envelopedLogFiles;

    private LogsRepository mirroringRepository;

    private static void writeData(EnvelopeWriteChannel writeChannel, byte[] data) throws IOException {
        writeData(writeChannel, data, -1);
    }

    private static void writeData(EnvelopeWriteChannel writeChannel, byte[] data, long term) throws IOException {
        writeChannel.beginChecksumForWriting();
        if (term >= 0) {
            writeChannel.putTerm(term);
        }
        writeChannel.putVersion(KernelVersion.V2026_01.version());
        writeChannel.putContentType(LogEnvelopeHeader.KERNEL_CONTENT_TYPE);
        writeChannel.put(data, data.length);
        writeChannel.endCurrentEntry();
    }

    @BeforeEach
    void setUp() {
        recreateEnvelopedLogFiles(LatestVersions.LATEST_KERNEL_VERSION);
    }

    private void recreateEnvelopedLogFiles(KernelVersion kernelVersion) {
        recreateEnvelopedLogFiles(kernelVersion, LogTracers.NULL);
    }

    private void recreateEnvelopedLogFiles(KernelVersion kernelVersion, LogTracers logTracers) {
        var baseFileName = "raft.log";
        var baseFolder = testDirectory.directory("logsFolder");
        var filesHelper = new SequentialFileNameHelper(baseFolder, baseFileName);
        mirroringRepository = new LogsRepository(fs, filesHelper);
        var logHeaderFactory =
                new BaseLogHeaderFactory(kernelVersion, StoreIdentifier.newStoreIdentifier(12345), clock);
        envelopedLogFiles = new EnvelopedLogFiles(
                mirroringRepository,
                logHeaderFactory,
                segmentBlockSize,
                writeBufferedBlocks,
                totalSegments,
                EmptyMemoryTracker.INSTANCE,
                ThresholdFactory.PRUNE_ALL,
                new StoreChannelNativeAccessor(
                        fs, NativeAccessProvider.getNativeAccess(), NullLogProvider.getInstance(), s -> {}),
                NullLogProvider.getInstance(),
                logTracers);
    }

    @AfterEach
    void tearDown() throws Exception {
        envelopedLogFiles.close();
    }

    @Test
    void shouldFailOnGettingChannelBeforeInitialise() {
        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(() -> envelopedLogFiles.currentWriteChannel());
    }

    @Test
    void shouldCreateNewFileWhenInitialising() throws IOException {
        assertThat(mirroringRepository.isEmpty()).isTrue();

        envelopedLogFiles.initialise();

        assertThat(mirroringRepository.isEmpty()).isFalse();
        assertThat(mirroringRepository.logVersions(false)).containsExactly(BASE_VERSION);

        try (var reader = envelopedLogFiles.openReadChannel()) {
            assertThat(reader.getLogVersion()).isEqualTo(0);
            assertThat(reader.logHeader().getLogVersion()).isEqualTo(0);
        }
    }

    @Test
    void shouldReInitializeCorrectlyAfterRestart() throws IOException {
        assertThat(mirroringRepository.isEmpty()).isTrue();
        envelopedLogFiles.initialise();

        var data = EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8);
        var writeChannel = envelopedLogFiles.currentWriteChannel();
        writeData(writeChannel, data);
        writeData(writeChannel, data);
        writeChannel.prepareForFlush().flush();

        // when
        recreateEnvelopedLogFiles(LatestVersions.LATEST_KERNEL_VERSION);
        var latestLogIndex = envelopedLogFiles.initialise();

        // then
        assertThat(latestLogIndex).isOne();
    }

    @Test
    void shouldReInitializeCorrectlyAfterLogRotation() throws IOException {
        assertThat(mirroringRepository.isEmpty()).isTrue();
        envelopedLogFiles.initialise();

        var data = EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8);
        var writeChannel = envelopedLogFiles.currentWriteChannel();
        writeData(writeChannel, data); // index 0
        writeData(writeChannel, data); // index 1
        writeChannel.prepareForFlush().flush();

        envelopedLogFiles.forceRotate();
        writeData(writeChannel, data); // index 2
        writeData(writeChannel, data); // index 3
        writeChannel.prepareForFlush().flush();

        // when
        recreateEnvelopedLogFiles(LatestVersions.LATEST_KERNEL_VERSION);
        var latestLogIndex = envelopedLogFiles.initialise();

        // then
        assertThat(latestLogIndex).isEqualTo(3);
    }

    @Test
    void shouldReInitializeCorrectlyIfAtLogBoundary() throws IOException {
        assertThat(mirroringRepository.isEmpty()).isTrue();
        envelopedLogFiles.initialise();

        var data = EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8);
        var writeChannel = envelopedLogFiles.currentWriteChannel();
        writeData(writeChannel, data); // index 0
        writeData(writeChannel, data); // index 1
        writeChannel.prepareForFlush().flush();

        envelopedLogFiles.forceRotate();
        writeChannel.prepareForFlush().flush();

        // when
        recreateEnvelopedLogFiles(LatestVersions.LATEST_KERNEL_VERSION);
        var latestLogIndex = envelopedLogFiles.initialise();

        // then
        assertThat(latestLogIndex).isOne();
    }

    @Test
    void shouldReInitializeCorrectlyWhenLastEntrySpansMultipleFiles() throws IOException {
        var smallData = EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8);
        var largeData = new byte[(int) (segmentBlockSize * ((2 * totalSegments) - 0.5))];

        envelopedLogFiles.initialise();

        var writeChannel = envelopedLogFiles.currentWriteChannel();

        writeData(writeChannel, smallData); // index 0
        writeData(writeChannel, largeData); // index 1
        writeData(writeChannel, largeData); // index 2
        writeData(writeChannel, smallData); // index 3
        writeData(writeChannel, largeData); // index 4
        writeChannel.prepareForFlush().flush();

        // when
        recreateEnvelopedLogFiles(LatestVersions.LATEST_KERNEL_VERSION);
        var latestLogIndex = envelopedLogFiles.initialise();

        // then
        assertThat(latestLogIndex).isEqualTo(4);
    }

    @Test
    void shouldReInitializeCorrectlyWithPreallocatedFiles() throws IOException {
        // given - preallocated file
        writePseudoPreallocatedFile(0);

        envelopedLogFiles.initialise();
        var data = EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8);
        var writeChannel = envelopedLogFiles.currentWriteChannel();
        writeData(writeChannel, data); // index 0
        writeData(writeChannel, data); // index 1
        writeData(writeChannel, data); // index 2
        writeChannel.prepareForFlush().flush();
        envelopedLogFiles.close();

        // Plus one more preallocated file following first
        writePseudoPreallocatedFile(1);

        // when
        recreateEnvelopedLogFiles(LatestVersions.LATEST_KERNEL_VERSION);
        var latestLogIndex = envelopedLogFiles.initialise();

        // then
        assertThat(latestLogIndex).isEqualTo(2);
    }

    @Test
    void shouldWriteAndReadData() throws IOException {
        envelopedLogFiles.initialise();

        var data = EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8);
        var writeChannel = envelopedLogFiles.currentWriteChannel();
        writeData(writeChannel, data);
        writeChannel.prepareForFlush().flush();

        var readData = new byte[data.length];
        try (var envelopeReadChannel = envelopedLogFiles.openReadChannel()) {
            envelopeReadChannel.reReadSegment();
            envelopeReadChannel.get(readData, readData.length);
        }
        assertThat(data).isEqualTo(readData);
    }

    @Test
    void shouldReadCorrectlyFromFileStartingWithNotANewEnvelope() throws IOException {
        var smallData = EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8);
        var largeData = new byte[(int) (segmentBlockSize * (totalSegments - 0.5))];

        envelopedLogFiles.initialise();

        var writeChannel = envelopedLogFiles.currentWriteChannel();

        writeData(writeChannel, largeData); // will cause rotation
        writeData(writeChannel, smallData); // first entry in new file
        writeChannel.prepareForFlush().flush();

        var readData = new byte[smallData.length];
        try (var envelopeReadChannel = envelopedLogFiles.openReadChannel(1)) {
            envelopeReadChannel.alignWithStartEntry();
            envelopeReadChannel.get(readData, readData.length);
        }
        assertThat(smallData).isEqualTo(readData);
    }

    @Test
    void shouldReadCorrectlyFromFileContainingEntrySpanningMultipleFiles() throws IOException {
        var smallData = EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8);
        var largeData = new byte[(int) (segmentBlockSize * ((2 * totalSegments) - 0.5))];

        envelopedLogFiles.initialise();

        var writeChannel = envelopedLogFiles.currentWriteChannel();

        writeData(writeChannel, largeData);
        writeData(writeChannel, smallData);
        writeChannel.prepareForFlush().flush();

        var readData = new byte[smallData.length];
        try (var envelopeReadChannel = envelopedLogFiles.openReadChannel(1)) {
            envelopeReadChannel.alignWithStartEntry();
            envelopeReadChannel.get(readData, readData.length);
        }
        assertThat(smallData).isEqualTo(readData);
    }

    @Test
    void shouldHandlePreAllocatedFileWhenOpeningReadChannel() throws IOException {

        var baseData = new byte[segmentBlockSize / 3];

        envelopedLogFiles.initialise();

        var writeChannel = envelopedLogFiles.currentWriteChannel();

        int maxIndex = 0;
        while (mirroringRepository.logVersionsRange().to() < 10) {
            baseData[0] = (byte) maxIndex++; // set unique data index
            writeData(writeChannel, baseData);
        }
        writeChannel.prepareForFlush().flush();
        // rollback by one as new logic won't create a new file until it has to
        // Subsequent logic will overwrite that last envelope/file
        --maxIndex;
        // create pre-allocated files
        for (int i = 10; i < 20; i++) {
            try (var channel = mirroringRepository.createWriteChannel(i).channel()) {
                var zeros = ByteBuffer.wrap(new byte[segmentBlockSize]);
                for (int j = 0; j < totalSegments; ++j) {
                    channel.writeAll(zeros);
                    zeros.position(0);
                }
                channel.flush();
            }
        }

        for (int i = 0; i < maxIndex; i++) {
            try (var readChannel = envelopedLogFiles.openReadChannel(i)) {
                var currentLogHeader = readChannel.logHeader();
                var readData = new byte[baseData.length];
                while (readChannel.entryIndex() != i) {
                    readChannel.goToNextEntry();
                }
                readChannel.get(readData, readData.length);
                // assert it did not switch file
                assertThat(currentLogHeader).isEqualTo(readChannel.logHeader());
                // and that we found the expected entry
                assertThat(readData[0]).isEqualTo((byte) i);
            }
        }
    }

    @Test
    void shouldHandleOnlyPreAllocatedFilesWithLogHeaderMetadata() throws IOException {
        // Start on a preallocated file
        writePseudoPreallocatedFile(0);

        envelopedLogFiles.initialise();

        // append an extra preallocated file now that first file has a header
        writePseudoPreallocatedFile(1);

        var itr = envelopedLogFiles.logFilesMetadata();
        // envelopedLogFiles.initialise() will write a header to the first file
        assertThat(itr.next()).isTrue();
        var logHeaderMetadata = itr.get();
        assertThat(logHeaderMetadata.logHeader().getLastAppendIndex()).isEqualTo(-1);
        assertThat(logHeaderMetadata.version()).isZero();

        // the next file exits but is an empty pre-allocatd file and should be ignore
        assertThat(itr.next()).isFalse();
    }

    @Test
    void shouldHandleOnlyPreAllocatedFile() throws IOException {
        writePseudoPreallocatedFile(0);

        envelopedLogFiles.initialise();
        assertThat(envelopedLogFiles.currentWriteChannel().currentIndex()).isEqualTo(-1);
        byte[] data = {1, 2, 3};
        writeData(envelopedLogFiles.currentWriteChannel(), data);
        envelopedLogFiles.currentWriteChannel().prepareForFlush().flush();
        try (EnvelopeReadChannel channel = envelopedLogFiles.openReadChannel()) {
            readData(data, channel, 0);
        }
    }

    @Test
    void shouldRotateWhenWritingMoreThanFileSize() throws IOException {
        envelopedLogFiles.initialise();

        var data = EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8);
        var totalMessages = writeManyMessages(data);

        assertThat(mirroringRepository.logVersions(false)).hasSizeGreaterThan(2);

        try (var envelopeReadChannel = envelopedLogFiles.openReadChannel()) {
            for (int i = 0; i < totalMessages; i++) {
                var readData = new byte[data.length];
                envelopeReadChannel.get(readData, readData.length);
                assertThat(data).isEqualTo(readData);
            }
        }
    }

    @Test
    void shouldRotateWithCorrectHeaderState() throws IOException {
        envelopedLogFiles.initialise();

        int dataSize = segmentBlockSize - HEADER_SIZE - 4;
        var data = new byte[dataSize];
        writeData(envelopedLogFiles.currentWriteChannel(), data, 0); // file 1
        writeData(envelopedLogFiles.currentWriteChannel(), data, 1); // file 1
        writeData(envelopedLogFiles.currentWriteChannel(), data, 2); // file 2
        envelopedLogFiles.currentWriteChannel().prepareForFlush().flush();

        try (var envelopeReadChannel = envelopedLogFiles.openReadChannel()) {
            envelopeReadChannel.alignWithStartEntry();
            var firstHeader = envelopeReadChannel.logHeader();
            envelopeReadChannel.goToNextEntry();
            assertThat(firstHeader.getLogVersion()).isEqualTo(envelopeReadChannel.getLogVersion());
            assertThat(envelopeReadChannel.entryIndex()).isOne();

            envelopeReadChannel.goToNextEntry();
            var secondHeader = envelopeReadChannel.logHeader();
            assertThat(envelopeReadChannel.entryIndex()).isEqualTo(2);
            assertThat(secondHeader.getLogVersion()).isEqualTo(firstHeader.getLogVersion() + 1);

            assertThat(secondHeader.getLastAppendIndex()).isOne();
            assertThat(secondHeader.getLastTerm()).isOne();
        }
    }

    @Test
    void shouldReturnNullIfNoFileMatches() throws IOException {
        assertThat(mirroringRepository.isEmpty()).isTrue();

        envelopedLogFiles.initialise();

        assertThat(mirroringRepository.isEmpty()).isFalse();
        assertThat(mirroringRepository.logVersions(false)).containsExactly(BASE_VERSION);

        try (var reader = envelopedLogFiles.openReadChannel(-5)) {
            assertThat(reader).isNull();
        }
    }

    @Test
    void shouldSetCorrectLogIndexInHeader() throws IOException {
        assertThat(mirroringRepository.isEmpty()).isTrue();
        envelopedLogFiles.initialise();

        var largeMessage = new byte[segmentBlockSize];
        var writeChannel = envelopedLogFiles.currentWriteChannel();
        writeData(writeChannel, largeMessage); // 0, start in v0, end in v1
        writeData(writeChannel, largeMessage); // 1, start in v1, end in v2
        writeData(writeChannel, largeMessage); // 2, start in v2, end in v3
        writeData(writeChannel, largeMessage); // 3  start in v3, end in v4
        writeChannel.prepareForFlush().flush();

        try (var reader = envelopedLogFiles.openReadChannel()) {
            reader.alignWithStartEntry();
            assertThat(reader.entryIndex()).isZero(); // points to first entry
            LogHeader currentLogHeader = null;
            for (int i = 0; i < 3; i++) {
                assertThat(reader.entryIndex()).isEqualTo(i);
                var logHeader = reader.logHeader();
                if (logHeader != currentLogHeader) {
                    currentLogHeader = logHeader;
                    int expectedPrevIndex = i - 1;
                    assertThat(logHeader).matches(metadata -> metadata.getLastAppendIndex() == expectedPrevIndex);
                }
                reader.goToNextEntry();
            }
        }
    }

    @Test
    void shouldOpenLowestExistingFile() throws IOException {
        envelopedLogFiles.initialise();

        var data = EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8);
        writeManyMessages(data);

        assertThat(mirroringRepository.logVersions(false)).hasSizeGreaterThan(2);

        try (var envelopeReadChannel = envelopedLogFiles.openReadChannel()) {
            assertThat(envelopeReadChannel.getLogVersion()).isZero();
        }

        envelopedLogFiles.deleteLogFilesTo(2);

        try (var envelopeReadChannel = envelopedLogFiles.openReadChannel()) {
            assertThat(envelopeReadChannel.getLogVersion()).isEqualTo(3);
        }
    }

    @Test
    void shouldNotPruneLogFilesContainingEntriesNotToPrune() throws IOException {
        assertThat(mirroringRepository.isEmpty()).isTrue();

        envelopedLogFiles.initialise();

        var data = EIGHT_BYTES_MESSAGE.getBytes();
        var largeData = new byte[totalFileDataSize / 2];
        writeData(envelopedLogFiles.currentWriteChannel(), largeData);
        writeData(envelopedLogFiles.currentWriteChannel(), largeData); // spills over to next file
        writeData(envelopedLogFiles.currentWriteChannel(), data);
        writeData(envelopedLogFiles.currentWriteChannel(), data);
        envelopedLogFiles.currentWriteChannel().prepareForFlush().flush();

        assertThat(mirroringRepository.isEmpty()).isFalse();
        assertThat(mirroringRepository.logVersions(false)).containsExactly(BASE_VERSION, BASE_VERSION + 1);

        // 1 is completed in first file, so we expect it to be removed
        assertThat(envelopedLogFiles.prune(2)).isOne();

        assertThat(mirroringRepository.logVersions(false)).containsExactly(BASE_VERSION + 1);

        try (var reader = envelopedLogFiles.openReadChannel()) {
            reader.alignWithStartEntry();
            for (int i = 2; i < 4; i++) {
                var readData = new byte[data.length];
                reader.read(ByteBuffer.wrap(readData));
                assertThat(readData).containsExactly(data);
                assertThat(reader.entryIndex()).isEqualTo(i);
            }
        }
    }

    @Test
    void shouldNotPruneCurrentFileIfIndexIsSamesAsAhead() throws IOException {
        assertThat(mirroringRepository.isEmpty()).isTrue();

        envelopedLogFiles.initialise();

        var data = EIGHT_BYTES_MESSAGE.getBytes();
        writeData(envelopedLogFiles.currentWriteChannel(), data);
        writeData(envelopedLogFiles.currentWriteChannel(), data);
        writeData(envelopedLogFiles.currentWriteChannel(), data);
        envelopedLogFiles.currentWriteChannel().prepareForFlush().flush();

        assertThat(mirroringRepository.isEmpty()).isFalse();
        assertThat(mirroringRepository.logVersions(false)).containsExactly(BASE_VERSION);

        assertThat(envelopedLogFiles.prune(
                        envelopedLogFiles.currentWriteChannel().currentIndex()))
                .isEqualTo(-1);

        try (var reader = envelopedLogFiles.openReadChannel()) {
            for (int i = 0; i < 3; i++) {
                var readData = new byte[data.length];
                reader.read(ByteBuffer.wrap(readData));
                assertThat(readData).containsExactly(data);
                assertThat(reader.entryIndex()).isEqualTo(i);
            }
        }
    }

    @Test
    void shouldNotPruneFilesIfStrategyDoesNotAllow() throws IOException {
        envelopedLogFiles.setPruneThreshold(ThresholdFactory.KEEP_ALL);
        assertThat(mirroringRepository.isEmpty()).isTrue();

        envelopedLogFiles.initialise();

        var data = EIGHT_BYTES_MESSAGE.getBytes();
        var largeData = new byte[totalFileDataSize / 2];
        writeData(envelopedLogFiles.currentWriteChannel(), largeData);
        writeData(envelopedLogFiles.currentWriteChannel(), largeData); // spills over to next file
        writeData(envelopedLogFiles.currentWriteChannel(), data);
        writeData(envelopedLogFiles.currentWriteChannel(), data);
        envelopedLogFiles.currentWriteChannel().prepareForFlush().flush();

        assertThat(mirroringRepository.isEmpty()).isFalse();
        assertThat(mirroringRepository.logVersions(false)).containsExactly(BASE_VERSION, BASE_VERSION + 1);

        assertThat(envelopedLogFiles.prune(3)).isEqualTo(-1);
        assertThat(mirroringRepository.logVersions(false)).containsExactly(BASE_VERSION, BASE_VERSION + 1);
    }

    @Test
    void shouldPruneFileIfStrategyDoesAllow() throws IOException {
        envelopedLogFiles.setPruneThreshold(ThresholdFactory.PRUNE_ALL);
        assertThat(mirroringRepository.isEmpty()).isTrue();

        envelopedLogFiles.initialise();

        var data = EIGHT_BYTES_MESSAGE.getBytes();
        var largeData = new byte[totalFileDataSize / 2];
        writeData(envelopedLogFiles.currentWriteChannel(), largeData);
        writeData(envelopedLogFiles.currentWriteChannel(), largeData); // spills over to next file
        writeData(envelopedLogFiles.currentWriteChannel(), data);
        writeData(envelopedLogFiles.currentWriteChannel(), data);
        envelopedLogFiles.currentWriteChannel().prepareForFlush().flush();

        assertThat(mirroringRepository.isEmpty()).isFalse();
        assertThat(mirroringRepository.logVersions(false)).containsExactly(BASE_VERSION, BASE_VERSION + 1);

        assertThat(envelopedLogFiles.prune(3)).isOne();
        assertThat(mirroringRepository.logVersions(false)).containsExactly(BASE_VERSION + 1);
    }

    @Test
    void shouldNotPruneLogFiles() throws IOException {
        assertThat(mirroringRepository.isEmpty()).isTrue();

        envelopedLogFiles.initialise();

        var data = EIGHT_BYTES_MESSAGE.getBytes();
        writeData(envelopedLogFiles.currentWriteChannel(), data);
        writeData(envelopedLogFiles.currentWriteChannel(), data);
        writeData(envelopedLogFiles.currentWriteChannel(), data);
        envelopedLogFiles.currentWriteChannel().prepareForFlush().flush();

        assertThat(mirroringRepository.isEmpty()).isFalse();
        assertThat(mirroringRepository.logVersions(false)).containsExactly(BASE_VERSION);

        assertThat(envelopedLogFiles.prune(1)).isEqualTo(-1);

        try (var reader = envelopedLogFiles.openReadChannel()) {
            for (int i = 0; i < 3; i++) {
                var readData = new byte[data.length];
                reader.read(ByteBuffer.wrap(readData));
                assertThat(readData).containsExactly(data);
                assertThat(reader.entryIndex()).isEqualTo(i);
            }
        }
    }

    @Test
    void shouldNotPruneAgain() throws IOException {
        assertThat(mirroringRepository.isEmpty()).isTrue();

        envelopedLogFiles.initialise();

        var data = EIGHT_BYTES_MESSAGE.getBytes();
        var largeData = new byte[totalFileDataSize / 2];
        writeData(envelopedLogFiles.currentWriteChannel(), largeData);
        writeData(envelopedLogFiles.currentWriteChannel(), largeData); // spills over to next file
        writeData(envelopedLogFiles.currentWriteChannel(), data);
        writeData(envelopedLogFiles.currentWriteChannel(), data);
        envelopedLogFiles.currentWriteChannel().prepareForFlush().flush();

        assertThat(mirroringRepository.isEmpty()).isFalse();
        assertThat(mirroringRepository.logVersions(false)).containsExactly(BASE_VERSION, BASE_VERSION + 1);

        // 1 is completed in first file, so we expect it to be removed
        assertThat(envelopedLogFiles.prune(2)).isOne();
        assertThat(mirroringRepository.logVersions(false)).containsExactly(BASE_VERSION + 1);
        assertThat(envelopedLogFiles.prune(2)).isEqualTo(-1);
        assertThat(mirroringRepository.logVersions(false)).containsExactly(BASE_VERSION + 1);
    }

    @Test
    void shouldNotNotThrowOnAlreadyPrunedIndex() throws IOException {
        assertThat(mirroringRepository.isEmpty()).isTrue();

        envelopedLogFiles.initialise();

        var data = EIGHT_BYTES_MESSAGE.getBytes();
        var largeData = new byte[totalFileDataSize / 2];
        writeData(envelopedLogFiles.currentWriteChannel(), largeData);
        writeData(envelopedLogFiles.currentWriteChannel(), largeData); // spills over to next file
        writeData(envelopedLogFiles.currentWriteChannel(), data);
        writeData(envelopedLogFiles.currentWriteChannel(), data);
        envelopedLogFiles.currentWriteChannel().prepareForFlush().flush();

        assertThat(mirroringRepository.isEmpty()).isFalse();
        assertThat(mirroringRepository.logVersions(false)).containsExactly(BASE_VERSION, BASE_VERSION + 1);

        // 1 is completed in first file, so we expect it to be removed
        assertThat(envelopedLogFiles.prune(2)).isOne();
        assertThat(mirroringRepository.logVersions(false)).containsExactly(BASE_VERSION + 1);
        assertThat(envelopedLogFiles.prune(0)).isEqualTo(-1);
        assertThat(mirroringRepository.logVersions(false)).containsExactly(BASE_VERSION + 1);
    }

    @Test
    void shouldTruncateAndSetCorrectChecksum() throws IOException {
        assertThat(mirroringRepository.isEmpty()).isTrue();

        envelopedLogFiles.initialise();

        var data = EIGHT_BYTES_MESSAGE.getBytes();
        var largeData = new byte[totalFileDataSize - 20];
        writeData(envelopedLogFiles.currentWriteChannel(), largeData);
        writeData(envelopedLogFiles.currentWriteChannel(), largeData);
        envelopedLogFiles.currentWriteChannel().prepareForFlush().flush();

        try (var reader = envelopedLogFiles.openReadChannel()) {
            for (int i = 0; i < 2; i++) {
                readData(largeData, reader, i);
            }
        }

        envelopedLogFiles.truncate(1);

        writeData(envelopedLogFiles.currentWriteChannel(), data);
        writeData(envelopedLogFiles.currentWriteChannel(), data);
        writeData(envelopedLogFiles.currentWriteChannel(), data);
        envelopedLogFiles.currentWriteChannel().prepareForFlush().flush();

        try (var reader = envelopedLogFiles.openReadChannel()) {
            readData(largeData, reader, 0);
            for (int i = 1; i < 4; i++) {
                readData(data, reader, i);
            }
        }
    }

    private static void readData(byte[] expectedData, EnvelopeReadChannel reader, int expectedIndex)
            throws IOException {
        var readData = new byte[expectedData.length];
        reader.read(ByteBuffer.wrap(readData));
        assertThat(readData).isEqualTo(expectedData);
        assertThat(reader.entryIndex()).isEqualTo(expectedIndex);
    }

    @Test
    void shouldTruncateEntries() throws IOException {
        assertThat(mirroringRepository.isEmpty()).isTrue();

        envelopedLogFiles.initialise();

        var messagesBefore = new String[] {"beforeTruncate1", "beforeTruncate2", "beforeTruncate3"};
        var messagesAfter = new String[] {"afterTruncate1", "afterTruncate2"};
        var writeChannel = envelopedLogFiles.currentWriteChannel();
        writeData(writeChannel, messagesBefore[0].getBytes(), 0);
        writeData(writeChannel, messagesBefore[1].getBytes(), 1);
        writeData(writeChannel, messagesBefore[2].getBytes(), 2);
        writeChannel.prepareForFlush().flush();

        try (var reader = envelopedLogFiles.openReadChannel()) {
            for (int i = 0; i < 3; i++) {
                var message = messagesBefore[i];
                var readData = new byte[message.length()];
                reader.read(ByteBuffer.wrap(readData));
                assertThat(new String(readData)).isEqualTo(message);
                assertThat(reader.entryIndex()).isEqualTo(i);
            }
        }

        envelopedLogFiles.truncate(2);

        writeData(writeChannel, messagesAfter[0].getBytes(), writeChannel.currentTerm() + 1);
        writeData(writeChannel, messagesAfter[1].getBytes(), writeChannel.currentTerm() + 1);
        writeChannel.prepareForFlush().flush();

        try (var reader = envelopedLogFiles.openReadChannel()) {
            for (int i = 0; i < 2; i++) {
                var currentMessage = messagesBefore[i];
                var readData = new byte[currentMessage.length()];
                reader.read(ByteBuffer.wrap(readData));
                assertThat(new String(readData)).isEqualTo(currentMessage);
                assertThat(reader.entryIndex()).isEqualTo(i);
                assertThat(reader.currentTerm()).isEqualTo(i);
            }
            for (int i = 0; i < 2; i++) {
                var currentMessage = messagesAfter[i];
                var readData = new byte[currentMessage.length()];
                reader.read(ByteBuffer.wrap(readData));
                assertThat(new String(readData)).isEqualTo(currentMessage);
                assertThat(reader.entryIndex()).isEqualTo(i + 2);
                assertThat(reader.currentTerm()).isEqualTo(i + 2);
            }
        }
    }

    @Test
    void shouldTruncateAll() throws IOException {
        assertThat(mirroringRepository.isEmpty()).isTrue();

        envelopedLogFiles.initialise();

        var message1 = "one";
        var message2 = "two";
        var writeChannel = envelopedLogFiles.currentWriteChannel();
        writeData(writeChannel, message1.getBytes());
        writeData(writeChannel, message1.getBytes());
        writeData(writeChannel, message1.getBytes());
        writeChannel.prepareForFlush().flush();

        envelopedLogFiles.truncate(0);
        writeChannel = envelopedLogFiles.currentWriteChannel();

        writeData(writeChannel, message2.getBytes());
        writeChannel.prepareForFlush().flush();

        try (var reader = envelopedLogFiles.openReadChannel()) {
            var readData = new byte[message2.length()];
            reader.read(ByteBuffer.wrap(readData));
            assertThat(new String(readData)).isEqualTo(message2);
            assertThat(reader.entryIndex()).isZero();
        }
    }

    @Test
    void shouldFailToTruncateEntriesThatHasBeenPruned() throws IOException {
        assertThat(mirroringRepository.isEmpty()).isTrue();
        envelopedLogFiles.initialise();

        var largeMessage = new byte[segmentBlockSize / 2];
        var writeChannel = envelopedLogFiles.currentWriteChannel();
        writeData(writeChannel, largeMessage);
        writeData(writeChannel, largeMessage);
        writeData(writeChannel, largeMessage);
        writeChannel.prepareForFlush().flush();

        envelopedLogFiles.deleteLogFilesTo(1);

        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> envelopedLogFiles.truncate(1));
    }

    @Test
    void shouldTruncateEntriesSpanningOverMultipleFiles() throws IOException {
        assertThat(mirroringRepository.isEmpty()).isTrue();
        envelopedLogFiles.initialise();

        var largeMessage = new byte[segmentBlockSize / 2];
        var message1 = "beforeTruncate";
        var message2 = "afterTruncate";
        var writeChannel = envelopedLogFiles.currentWriteChannel();
        writeData(writeChannel, message1.getBytes());
        writeData(writeChannel, largeMessage);
        writeData(writeChannel, largeMessage);
        writeData(writeChannel, largeMessage);
        writeData(writeChannel, largeMessage);
        writeData(writeChannel, largeMessage);
        writeChannel.prepareForFlush().flush();

        envelopedLogFiles.truncate(1);

        writeChannel = envelopedLogFiles.currentWriteChannel();
        writeData(writeChannel, message2.getBytes());
        writeChannel.prepareForFlush().flush();

        try (var reader = envelopedLogFiles.openReadChannel()) {
            var readData = new byte[message1.length()];
            reader.read(ByteBuffer.wrap(readData));
            assertThat(new String(readData)).isEqualTo(message1);
            assertThat(reader.entryIndex()).isZero();

            readData = new byte[message2.length()];
            reader.read(ByteBuffer.wrap(readData));
            assertThat(new String(readData)).isEqualTo(message2);
            assertThat(reader.entryIndex()).isOne();
        }
    }

    // Generate a wide range of message sizes to exercise corner cases
    private int shuffledMessageSize(int index) {
        return 1 + ((97 * index) % (3 * segmentBlockSize));
    }

    @Test
    void positionsShouldMatchWhenReadingAndWritingManyPacketSizes() throws IOException {
        assertThat(mirroringRepository.isEmpty()).isTrue();
        envelopedLogFiles.initialise();

        final var N = 1000;
        var positions = new ArrayList<Pair<Long, Long>>();
        try (var writeChannel = envelopedLogFiles.currentWriteChannel()) {
            for (int count = 0; count < N; ++count) {
                var messageSize = shuffledMessageSize(count);
                var data = new byte[messageSize];
                data[0] = (byte) (count & 0xFF);
                data[messageSize - 1] = data[0];
                writeData(writeChannel, data);
                writeChannel.prepareForFlush().flush();
                positions.add(Pair.of(mirroringRepository.logVersionsRange().to(), writeChannel.position()));
            }
        }

        try (var reader = envelopedLogFiles.openReadChannel()) {
            for (int count = 0; count < N; ++count) {
                var messageSize = shuffledMessageSize(count);
                var readData = new byte[messageSize];
                reader.read(ByteBuffer.wrap(readData));
                assertThat(readData[0]).isEqualTo((byte) (count & 0xFF));
                assertThat(readData[messageSize - 1]).isEqualTo(readData[0]);
                var pos = Pair.of(reader.getLogVersion(), reader.position());
                assertThat(pos).isEqualTo(positions.get(count));
            }
        }
    }

    @Test
    void readPositionsShouldBeValidSeekPoints() throws IOException {
        assertThat(mirroringRepository.isEmpty()).isTrue();
        envelopedLogFiles.initialise();
        final var N = 1000;
        try (var writeChannel = envelopedLogFiles.currentWriteChannel()) {
            for (int count = 0; count < N; ++count) {
                var messageSize = shuffledMessageSize(count);
                var data = new byte[messageSize];
                data[0] = (byte) (count & 0xFF);
                data[messageSize - 1] = data[0];
                writeData(writeChannel, data);
                writeChannel.prepareForFlush().flush();
            }
        }

        var positions = new ArrayList<Pair<Long, Long>>();
        positions.add(Pair.of(0L, (long) segmentBlockSize));
        try (var reader = envelopedLogFiles.openReadChannel()) {
            for (int count = 0; count < N; ++count) {
                var messageSize = shuffledMessageSize(count);
                var readData = new byte[messageSize];
                reader.read(ByteBuffer.wrap(readData));
                assertThat(readData[0]).isEqualTo((byte) (count & 0xFF));
                assertThat(readData[messageSize - 1]).isEqualTo(readData[0]);
                positions.add(Pair.of(reader.getLogVersion(), reader.position()));
            }
        }
        positions.removeLast();

        verifyReadingAtPositions(positions);
    }

    @Test
    void writePositionsShouldBeValidSeekPoints() throws IOException {
        assertThat(mirroringRepository.isEmpty()).isTrue();
        envelopedLogFiles.initialise();

        var positions = new ArrayList<Pair<Long, Long>>();
        positions.add(Pair.of(0L, (long) segmentBlockSize));
        final var N = 1000;
        try (var writeChannel = envelopedLogFiles.currentWriteChannel()) {
            for (int count = 0; count < N; ++count) {
                var messageSize = shuffledMessageSize(count);
                var data = new byte[messageSize];
                data[0] = (byte) (count & 0xFF);
                data[messageSize - 1] = data[0];
                writeData(writeChannel, data);
                writeChannel.prepareForFlush().flush();
                positions.add(Pair.of(mirroringRepository.logVersionsRange().to(), writeChannel.position()));
            }
        }
        positions.removeLast();

        verifyReadingAtPositions(positions);
    }

    @Test
    void shouldRemoveAllFiles() throws IOException {
        envelopedLogFiles.initialise();
        var envelopeWriteChannel = envelopedLogFiles.currentWriteChannel();

        for (int i = 0; i < totalSegments * 4; i++) {
            writeData(envelopeWriteChannel, new byte[segmentBlockSize]);
        }
        envelopeWriteChannel.prepareForFlush().flush();

        assertThat(mirroringRepository.logVersionsRange()).isEqualTo(LongRange.range(0, 7));

        envelopedLogFiles.remove();

        assertThat(mirroringRepository.isEmpty()).isTrue();
    }

    @Test
    void shouldSetCorrectChecksumOnSkip() throws Exception {
        envelopedLogFiles.initialise();
        var messageOne = new byte[randomSupport.nextInt(1, segmentBlockSize)];
        var messageTwo = new byte[randomSupport.nextInt(1, segmentBlockSize)];
        var messageThree = new byte[randomSupport.nextInt(1, segmentBlockSize)];

        // create the log
        var writeChannel = envelopedLogFiles.currentWriteChannel();
        writeData(writeChannel, messageOne);
        writeData(writeChannel, messageTwo);
        writeData(writeChannel, messageThree);
        writeChannel.prepareForFlush().flush();

        var entryChecksums = new int[3];
        int offset;
        // read checksums and get the offset
        try (var envelopeReadChannel = envelopedLogFiles.openReadChannel()) {
            gotToEndOfNextEntry(envelopeReadChannel);
            entryChecksums[0] = envelopeReadChannel.getChecksum();

            gotToEndOfNextEntry(envelopeReadChannel);
            entryChecksums[1] = envelopeReadChannel.getChecksum();
            long position = envelopeReadChannel.goToNextEnvelope();
            offset = envelopeReadChannel.getSegmentOffset(position);

            if (!(envelopeReadChannel.payloadType == EnvelopeType.FULL
                    || envelopeReadChannel.payloadType == EnvelopeType.END)) {
                gotToEndOfNextEntry(envelopeReadChannel);
            }
            entryChecksums[2] = envelopeReadChannel.getChecksum();
        }

        // recreate the log
        envelopedLogFiles.remove();
        setUp();
        envelopedLogFiles.initialise();

        // simulated a joining member by skipping and writing the same entry
        envelopedLogFiles.skip(1, entryChecksums[1], offset, -1);
        writeChannel = envelopedLogFiles.currentWriteChannel();
        writeData(writeChannel, messageThree);
        writeChannel.prepareForFlush().flush();

        // validate log tails are the same
        try (var envelopeReadChannel = envelopedLogFiles.openReadChannel()) {
            var logHeader = envelopeReadChannel.logHeader();
            var logHeaderChecksum = logHeader.getPreviousLogFileChecksum();
            assertThat(logHeaderChecksum).isEqualTo(entryChecksums[1]);
            gotToEndOfNextEntry(envelopeReadChannel);
            assertThat(envelopeReadChannel.getChecksum()).isEqualTo(entryChecksums[2]);
        }
    }

    @Test
    void shouldSetCorrectChecksumOnTruncate() throws Exception {
        envelopedLogFiles.initialise();
        var messageOne = new byte[randomSupport.nextInt(1, segmentBlockSize)];
        var messageTwo = new byte[randomSupport.nextInt(1, segmentBlockSize)];
        var messageThree = new byte[randomSupport.nextInt(1, segmentBlockSize)];
        var messageFour = new byte[randomSupport.nextInt(segmentBlockSize, segmentBlockSize * 3)];

        var messages = shuffledMessages(messageOne, messageTwo, messageThree, messageFour);

        // create the log
        var writeChannel = envelopedLogFiles.currentWriteChannel();
        writeData(writeChannel, messages[0], 0);
        writeData(writeChannel, messages[1]);
        writeData(writeChannel, messages[2]);
        writeChannel.prepareForFlush().flush();

        var entryChecksums = new ArrayList<Integer>();
        // read checksums
        try (var envelopeReadChannel = envelopedLogFiles.openReadChannel()) {
            while (true) {
                try {
                    envelopeReadChannel.goToNextEnvelope();
                    entryChecksums.add(envelopeReadChannel.getChecksum());
                } catch (ReadPastEndException ignore) {
                    break;
                }
            }
        }

        // truncate log - should not impact the checksum chain
        var truncateAt = randomSupport.nextInt(0, 3);
        envelopedLogFiles.truncate(truncateAt);
        writeChannel = envelopedLogFiles.currentWriteChannel();
        for (var i = truncateAt; i < messages.length; i++) {
            writeData(writeChannel, messages[i], 0);
        }
        writeChannel.prepareForFlush().flush();

        // validate logs are the same
        try (var envelopeReadChannel = envelopedLogFiles.openReadChannel()) {
            for (Integer entryChecksum : entryChecksums) {
                envelopeReadChannel.goToNextEnvelope();
                assertThat(envelopeReadChannel.getChecksum()).isEqualTo(entryChecksum);
            }
        }
    }

    @Test
    void shouldFindRangeForTransferAllEntries() throws IOException {
        var smallData = EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8);
        var largeData = new byte[(int) (segmentBlockSize * ((2 * totalSegments) - 0.5))];

        envelopedLogFiles.initialise();

        var writeChannel = envelopedLogFiles.currentWriteChannel();

        writeData(writeChannel, smallData); // index 0
        writeData(writeChannel, largeData); // index 1
        writeData(writeChannel, largeData); // index 2
        writeData(writeChannel, smallData); // index 3
        writeData(writeChannel, largeData); // index 4
        writeChannel.prepareForFlush().flush();

        var storeChannels = envelopedLogFiles.storeChannels(0, 4);
        try {

            assertThat(storeChannels.fromIndex()).isZero();
            assertThat(storeChannels.toIndex()).isEqualTo(4);
            assertThat(storeChannels.toPosition()).isEqualTo(writeChannel.position());
            assertThat(storeChannels.storeChannels()).hasSize(10);
            for (var storeChannel : storeChannels.storeChannels()) {
                assertThat(storeChannel.position()).isEqualTo(segmentBlockSize);
            }
        } finally {
            IOUtils.closeAllSilently(storeChannels.storeChannels());
        }
    }

    @Test
    void shouldFindRangeForTransferTail() throws IOException {
        var smallData = EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8);
        var largeData = new byte[(int) (segmentBlockSize * ((2 * totalSegments) - 0.5))];

        envelopedLogFiles.initialise();

        var writeChannel = envelopedLogFiles.currentWriteChannel();

        writeData(writeChannel, smallData); // index 0
        writeData(writeChannel, largeData); // index 1
        writeData(writeChannel, largeData); // index 2
        writeData(writeChannel, smallData); // index 3
        writeData(writeChannel, largeData); // index 4
        writeChannel.prepareForFlush().flush();

        var storeChannels = envelopedLogFiles.storeChannels(2, 4);
        try {

            assertThat(storeChannels.fromIndex()).isEqualTo(2);
            assertThat(storeChannels.toIndex()).isEqualTo(4);
            assertThat(storeChannels.toPosition()).isEqualTo(writeChannel.position());
            assertThat(storeChannels.storeChannels()).hasSize(7);
            assertThat(storeChannels.storeChannels().get(0).position()).isEqualTo(384);
            for (var i = 1; i < storeChannels.storeChannels().size(); i++) {
                assertThat(storeChannels.storeChannels().get(i).position()).isEqualTo(segmentBlockSize);
            }
        } finally {
            IOUtils.closeAllSilently(storeChannels.storeChannels());
        }
    }

    @Test
    void shouldFindRangeForTransferHead() throws IOException {
        var smallData = EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8);
        var largeData = new byte[(int) (segmentBlockSize * ((2 * totalSegments) - 0.5))];

        envelopedLogFiles.initialise();

        var writeChannel = envelopedLogFiles.currentWriteChannel();

        writeData(writeChannel, smallData); // index 0
        writeData(writeChannel, largeData); // index 1
        writeData(writeChannel, largeData); // index 2
        writeData(writeChannel, smallData); // index 3
        writeData(writeChannel, largeData); // index 4
        writeChannel.prepareForFlush().flush();

        var storeChannels = envelopedLogFiles.storeChannels(0, 2);
        try {

            assertThat(storeChannels.fromIndex()).isZero();
            assertThat(storeChannels.toIndex()).isEqualTo(2);
            assertThat(storeChannels.toPosition()).isEqualTo(473);
            assertThat(storeChannels.storeChannels()).hasSize(7);
            for (var storeChannel : storeChannels.storeChannels()) {
                assertThat(storeChannel.position()).isEqualTo(segmentBlockSize);
            }
        } finally {
            IOUtils.closeAllSilently(storeChannels.storeChannels());
        }
    }

    @Test
    void shouldFindSingleElementRangeForTransfer() throws IOException {
        var smallData = EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8);
        var largeData = new byte[(int) (segmentBlockSize * ((2 * totalSegments) - 0.5))];

        envelopedLogFiles.initialise();

        var writeChannel = envelopedLogFiles.currentWriteChannel();

        writeData(writeChannel, smallData); // index 0
        writeData(writeChannel, largeData); // index 1
        writeData(writeChannel, largeData); // index 2
        writeData(writeChannel, smallData); // index 3
        writeData(writeChannel, largeData); // index 4
        writeChannel.prepareForFlush().flush();

        var storeChannels = envelopedLogFiles.storeChannels(3, 3);
        try {

            assertThat(storeChannels.fromIndex()).isEqualTo(3);
            assertThat(storeChannels.toIndex()).isEqualTo(3);
            assertThat(storeChannels.toPosition()).isEqualTo(512);
            assertThat(storeChannels.storeChannels()).hasSize(1);
            assertThat(storeChannels.storeChannels().get(0).position()).isEqualTo(473);
        } finally {
            IOUtils.closeAllSilently(storeChannels.storeChannels());
        }
    }

    @Test
    void shouldFailIfFromIsOutsideExistingRange() throws IOException {
        envelopedLogFiles.initialise();

        var writeChannel = envelopedLogFiles.currentWriteChannel();
        writeData(writeChannel, new byte[] {'a'});
        writeData(writeChannel, new byte[] {'b'});
        writeChannel.prepareForFlush().flush();

        assertThatThrownBy(() -> envelopedLogFiles.storeChannels(2, 2)).isInstanceOf(ReadPastEndException.class);
    }

    @Test
    void shouldFailIfToIsOutsideExistingRange() throws IOException {
        envelopedLogFiles.initialise();

        var writeChannel = envelopedLogFiles.currentWriteChannel();
        writeData(writeChannel, new byte[] {'a'});
        writeData(writeChannel, new byte[] {'b'});
        writeChannel.prepareForFlush().flush();
        setMaxStackTraceElementsDisplayed(100);
        assertThatThrownBy(() -> envelopedLogFiles.storeChannels(0, 3)).isInstanceOf(ReadPastEndException.class);
    }

    @Test
    void shouldFailIfToIsBehindFrom() {
        assertThatThrownBy(() -> envelopedLogFiles.storeChannels(4, 3)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldAbortOnMissingFileInSequence() throws IOException {
        // create three initial files
        envelopedLogFiles.initialise();
        var data = EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8);
        var writeChannel = envelopedLogFiles.currentWriteChannel();
        while (mirroringRepository.logVersionsRange().to() < 2L) {
            writeData(writeChannel, data);
        }
        for (int i = 0; i < 3; i++) {
            writeData(writeChannel, data);
        }
        envelopedLogFiles.close();
        // remove a file in the sequence
        fs.deleteFile(mirroringRepository.pathFor(1L));

        recreateEnvelopedLogFiles(LatestVersions.LATEST_KERNEL_VERSION);
        assertThatThrownBy(() -> envelopedLogFiles.initialise())
                .isInstanceOf(InconsistentLogFilesException.class)
                .hasMessageContaining("Missing log file");
    }

    @Test
    void shouldAbortOnRenamedFileInSequence() throws IOException {
        // create three initial files
        envelopedLogFiles.initialise();
        var data = EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8);
        var writeChannel = envelopedLogFiles.currentWriteChannel();
        while (mirroringRepository.logVersionsRange().to() < 2L) {
            writeData(writeChannel, data);
        }
        for (int i = 0; i < 3; i++) {
            writeData(writeChannel, data);
        }
        envelopedLogFiles.close();
        // remove first file in the sequence and rename so header doesn't match
        fs.deleteFile(mirroringRepository.pathFor(0L));
        fs.renameFile(mirroringRepository.pathFor(1L), mirroringRepository.pathFor(0L));
        fs.renameFile(mirroringRepository.pathFor(2L), mirroringRepository.pathFor(1L));

        recreateEnvelopedLogFiles(LatestVersions.LATEST_KERNEL_VERSION);
        assertThatThrownBy(() -> envelopedLogFiles.initialise())
                .isInstanceOf(InconsistentLogFilesException.class)
                .hasMessageContaining("mismatched logVersion");
    }

    @Test
    void shouldAbortOnPreallocatedFileNotAtEnd() throws IOException {
        // create three initial files
        envelopedLogFiles.initialise();
        var data = EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8);
        var writeChannel = envelopedLogFiles.currentWriteChannel();
        while (mirroringRepository.logVersionsRange().to() < 2L) {
            writeData(writeChannel, data);
        }
        for (int i = 0; i < 3; i++) {
            writeData(writeChannel, data);
        }
        envelopedLogFiles.close();

        // zero out the middle file of the three
        try (var channel = mirroringRepository.createWriteChannel(1L).channel()) {
            var zeros = ByteBuffer.wrap(new byte[segmentBlockSize]);
            for (int i = 0; i < totalSegments; i++) {
                zeros.clear();
                channel.writeAll(zeros);
            }
        }

        recreateEnvelopedLogFiles(LatestVersions.LATEST_KERNEL_VERSION);
        assertThatThrownBy(() -> envelopedLogFiles.initialise())
                .isInstanceOf(InconsistentLogFilesException.class)
                .hasMessageContaining(
                        "has incomplete header, or is preallocated, but is not last in the log file sequence");
    }

    @Test
    void shouldAbortOnMultiplePreallocatedFiles() throws IOException {
        // create three initial files
        envelopedLogFiles.initialise();
        var data = EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8);
        var writeChannel = envelopedLogFiles.currentWriteChannel();
        while (mirroringRepository.logVersionsRange().to() < 2L) {
            writeData(writeChannel, data);
        }
        for (int i = 0; i < 3; i++) {
            writeData(writeChannel, data);
        }
        envelopedLogFiles.close();

        // Add two preallocated files following last log file
        for (int logVersion = 3; logVersion < 5; ++logVersion) {
            writePseudoPreallocatedFile(logVersion);
        }

        recreateEnvelopedLogFiles(LatestVersions.LATEST_KERNEL_VERSION);
        assertThatThrownBy(() -> envelopedLogFiles.initialise())
                .isInstanceOf(InconsistentLogFilesException.class)
                .hasMessageContaining(
                        "has incomplete header, or is preallocated, but is not last in the log file sequence");
    }

    private void writePseudoPreallocatedFile(int logVersion) throws IOException {
        try (var channel = mirroringRepository.createWriteChannel(logVersion).channel()) {
            var zeros = ByteBuffer.wrap(new byte[segmentBlockSize]);
            channel.writeAll(zeros);
            zeros.position(0);
            channel.writeAll(zeros);
            zeros.position(0);
            channel.writeAll(zeros);
            channel.flush();
        }
    }

    @Test
    void shouldAbortOnOutOfOrderPreviousAppendIndex() throws IOException {
        // create three initial files
        envelopedLogFiles.initialise();
        var data = EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8);
        var writeChannel = envelopedLogFiles.currentWriteChannel();
        while (mirroringRepository.logVersionsRange().to() < 2L) {
            writeData(writeChannel, data);
        }
        for (int i = 0; i < 3; i++) {
            writeData(writeChannel, data);
        }
        LogHeader headerOfLastFile;
        try (var reader = envelopedLogFiles.openReadChannel(writeChannel.currentIndex())) {
            headerOfLastFile = reader.logHeader();
        }
        envelopedLogFiles.close();

        // mess with the last files header
        try (var channel = mirroringRepository.openWriteChannel(2L).channel()) {
            LogHeader newHeader = headerOfLastFile
                    .getLogFormatVersion()
                    .newHeader(
                            headerOfLastFile.getLogVersion(),
                            2, // out of order as previous file is on appendIndex 11
                            headerOfLastFile.getLastTerm(),
                            headerOfLastFile.getStoreIdentifier(),
                            segmentBlockSize,
                            headerOfLastFile.getPreviousLogFileChecksum(),
                            headerOfLastFile.getKernelVersion(),
                            UNSPECIFIED_CREATION_TIME);
            channel.position(0);
            LogFormat.writeLogHeader(channel, newHeader, EmptyMemoryTracker.INSTANCE);
        }

        recreateEnvelopedLogFiles(LatestVersions.LATEST_KERNEL_VERSION);
        assertThatThrownBy(() -> envelopedLogFiles.initialise())
                .isInstanceOf(InconsistentLogFilesException.class)
                .hasMessageContaining("has lower previousAppendIndex: 2 than previous file");
    }

    @Test
    void shouldAbortOnOutOfOrderPreviousTerm() throws IOException {
        // create three initial files
        envelopedLogFiles.initialise();
        var data = EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8);
        var writeChannel = envelopedLogFiles.currentWriteChannel();
        long term = 1L;
        while (mirroringRepository.logVersionsRange().to() < 2L) {
            writeChannel.putTerm(term++);
            writeData(writeChannel, data);
        }
        for (int i = 0; i < 3; i++) {
            writeChannel.putTerm(term++);
            writeData(writeChannel, data);
        }
        LogHeader headerOfLastFile;
        try (var reader = envelopedLogFiles.openReadChannel(writeChannel.currentIndex())) {
            headerOfLastFile = reader.logHeader();
        }
        envelopedLogFiles.close();

        // mess with the last files header
        try (var channel = mirroringRepository.openWriteChannel(2L).channel()) {
            LogHeader newHeader = headerOfLastFile
                    .getLogFormatVersion()
                    .newHeader(
                            headerOfLastFile.getLogVersion(),
                            headerOfLastFile.getLastTerm(), // out of order as previous file is on appendIndex 11
                            2L,
                            headerOfLastFile.getStoreIdentifier(),
                            segmentBlockSize,
                            headerOfLastFile.getPreviousLogFileChecksum(),
                            headerOfLastFile.getKernelVersion(),
                            UNSPECIFIED_CREATION_TIME);
            channel.position(0);
            LogFormat.writeLogHeader(channel, newHeader, EmptyMemoryTracker.INSTANCE);
        }

        recreateEnvelopedLogFiles(LatestVersions.LATEST_KERNEL_VERSION);
        assertThatThrownBy(() -> envelopedLogFiles.initialise())
                .isInstanceOf(InconsistentLogFilesException.class)
                .hasMessageContaining("has lower previousTerm: 2 than previous file");
    }

    @Test
    void shouldAbortOnNonEnvelopedFiles() throws IOException {
        // create a non enveloped log file
        try (var channel = mirroringRepository.createWriteChannel(0L).channel()) {
            LogHeader newHeader = LogFormat.V9.newHeader(
                    0L,
                    -1,
                    -1,
                    StoreIdentifier.UNKNOWN,
                    segmentBlockSize,
                    100,
                    KernelVersion.GLORIOUS_FUTURE,
                    UNSPECIFIED_CREATION_TIME);
            channel.position(0);
            LogFormat.writeLogHeader(channel, newHeader, EmptyMemoryTracker.INSTANCE);
            var zeros = new byte[segmentBlockSize];
            channel.writeAll(ByteBuffer.wrap(zeros));
            channel.writeAll(ByteBuffer.wrap(zeros));
        }

        assertThatThrownBy(() -> envelopedLogFiles.initialise())
                .isInstanceOf(InconsistentLogFilesException.class)
                .hasMessageContaining("is not using Envelopes as required");
    }

    @Test
    void shouldAbortOnLogFormatDowngrade() throws IOException {
        // create three initial files on V11
        recreateEnvelopedLogFiles(KernelVersion.GLORIOUS_FUTURE);
        envelopedLogFiles.initialise();
        var data = EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8);
        var writeChannel = envelopedLogFiles.currentWriteChannel();
        long term = 1L;
        while (mirroringRepository.logVersionsRange().to() < 2L) {
            writeChannel.putTerm(term++);
            writeData(writeChannel, data);
        }
        for (int i = 0; i < 3; i++) {
            writeChannel.putTerm(term++);
            writeData(writeChannel, data);
        }
        LogHeader headerOfLastFile;
        try (var reader = envelopedLogFiles.openReadChannel(writeChannel.currentIndex())) {
            headerOfLastFile = reader.logHeader();
        }
        assertThat(headerOfLastFile.getLogFormatVersion().getVersionByte())
                .isGreaterThan(LogFormat.V10.getVersionByte());
        envelopedLogFiles.close();

        // mess with the last files header and make it V10
        try (var channel = mirroringRepository.openWriteChannel(2L).channel()) {
            LogHeader newHeader = LogFormat.V10.newHeader(
                    headerOfLastFile.getLogVersion(),
                    headerOfLastFile.getLastAppendIndex(),
                    headerOfLastFile.getLastTerm(),
                    // V10 needs a full storeId in its headers so use what we got in V11 and make a compatible one
                    StoreIdentifier.newStoreIdentifier(new StoreId(
                            0, headerOfLastFile.getStoreIdentifier().randomValueIdentifier(), "", "", 1, 1)),
                    segmentBlockSize,
                    headerOfLastFile.getPreviousLogFileChecksum(),
                    headerOfLastFile.getKernelVersion(),
                    UNSPECIFIED_CREATION_TIME);
            channel.position(0);
            LogFormat.writeLogHeader(channel, newHeader, EmptyMemoryTracker.INSTANCE);
        }

        recreateEnvelopedLogFiles(LatestVersions.LATEST_KERNEL_VERSION);
        assertThatThrownBy(() -> envelopedLogFiles.initialise())
                .isInstanceOf(InconsistentLogFilesException.class)
                .hasMessageContaining("uses LogFormat: V10 but previous file used higher LogFormat");
    }

    @Test
    void shouldAbortOnFileWithInvalidHeaders() throws IOException {
        // create a bad file
        try (var file = mirroringRepository.createWriteChannel(0)) {
            var testBytes = "This is not the log file you are looking for".getBytes(StandardCharsets.UTF_8);
            // Fill a couple of segments worth otherwise we might be regarded as a short preallocated file
            var gibberish = ByteBuffer.allocate(2 * segmentBlockSize);
            while (gibberish.hasRemaining()) {
                gibberish.put(testBytes, 0, Math.min(gibberish.remaining(), testBytes.length));
            }
            gibberish.flip();
            file.channel().writeAll(gibberish);
        }
        assertThatThrownBy(() -> envelopedLogFiles.initialise()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void shouldAbortOnBadPreallocatedFile() throws IOException {
        // create three initial files
        envelopedLogFiles.initialise();
        var data = EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8);
        var writeChannel = envelopedLogFiles.currentWriteChannel();
        while (mirroringRepository.logVersionsRange().to() < 2L) {
            writeData(writeChannel, data);
        }
        for (int i = 0; i < 3; i++) {
            writeData(writeChannel, data);
        }
        envelopedLogFiles.close();

        // write a 'preallocated' file, but modify a later byte so that we regard it as corrupted
        try (var channel = mirroringRepository.createWriteChannel(3L).channel()) {
            var zeros = ByteBuffer.wrap(new byte[256]);
            channel.writeAll(zeros);
            channel.writeAll(zeros);
            channel.writeAll(zeros);
            channel.writeAll(zeros);
            channel.writeAll(zeros);
            channel.writeAll(zeros);
            var one = ByteBuffer.wrap(new byte[1]);
            one.put((byte) 1);
            one.flip();
            channel.writeAll(one, 1300);
            channel.flush();
        }

        recreateEnvelopedLogFiles(LatestVersions.LATEST_KERNEL_VERSION);
        assertThatThrownBy(() -> envelopedLogFiles.initialise()).isInstanceOf(IllegalStateException.class);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4, 8, 12, 16, 20, 24, 32, 38})
    void shouldRecoverOnPartiallyWrittenFinalRecord(int offsetOfWipe) throws IOException {
        // Create initial file with 3 entries
        envelopedLogFiles.initialise();
        var data = EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8);
        var writeChannel = envelopedLogFiles.currentWriteChannel();
        writeData(writeChannel, data); // index 0
        writeData(writeChannel, data); // index 1
        var startOfEnvelope2 = writeChannel.position();
        writeData(writeChannel, data); // index 2
        writeChannel.prepareForFlush().flush();
        envelopedLogFiles.close();

        // Wipe the data at various offsets through the data
        try (var channel = mirroringRepository.createWriteChannel(0).channel()) {
            int wipeAt = (int) (startOfEnvelope2 + offsetOfWipe);
            channel.position(wipeAt);
            var zeros = ByteBuffer.wrap(new byte[2 * segmentBlockSize - wipeAt]);
            channel.writeAll(zeros);
            channel.flush();
        }

        // when
        recreateEnvelopedLogFiles(LatestVersions.LATEST_KERNEL_VERSION);
        // should initialise to prev appendIndex 1
        assertThat(envelopedLogFiles.initialise()).isOne();
        writeChannel = envelopedLogFiles.currentWriteChannel();

        // then we expect it to have truncated and rolled to a new file
        assertThat(mirroringRepository.logVersionsRange().to()).isOne();
        // and should be able to append without issues
        writeData(writeChannel, data); // write index 2 again
        assertThat(writeChannel.currentIndex()).isEqualTo(2);
        writeChannel.prepareForFlush().flush();

        // verify readback
        try (var readChannel = envelopedLogFiles.openReadChannel(0)) {
            var buffer = ByteBuffer.allocate(8);
            for (int i = 0; i < 3; i++) {
                buffer.clear();
                readChannel.beginChecksum();
                assertThat(readChannel.read(buffer)).isEqualTo(8);
                assertThat(buffer.array()).isEqualTo(data);
                readChannel.endChecksumAndValidate();
            }
        }
    }

    @Test
    void shouldRecoverOnPartiallyWrittenMultiEnvelopeRecord() throws IOException {
        // Create initial file with 3 entries
        envelopedLogFiles.initialise();
        var data = EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8);
        var bigData = new byte[segmentBlockSize];
        Arrays.fill(bigData, (byte) 1);
        var writeChannel = envelopedLogFiles.currentWriteChannel();
        writeData(writeChannel, data); // index 0
        writeData(writeChannel, data); // index 1
        writeData(writeChannel, bigData); // index 2 - spans envelopes
        writeChannel.prepareForFlush().flush();
        var endOffset = writeChannel.position();
        envelopedLogFiles.close();

        // Ruin the last envelope in the entry
        try (var channel = mirroringRepository.createWriteChannel(0).channel()) {
            int wipeAt = (int) (endOffset - 1);
            channel.position(wipeAt);
            var zero = ByteBuffer.wrap(new byte[1]);
            channel.writeAll(zero);
            channel.flush();
        }

        // when
        recreateEnvelopedLogFiles(LatestVersions.LATEST_KERNEL_VERSION);
        // should initialise to prev appendIndex 1
        assertThat(envelopedLogFiles.initialise()).isOne();
        writeChannel = envelopedLogFiles.currentWriteChannel();

        // then we expect it to have truncated and rolled to a new file
        assertThat(mirroringRepository.logVersionsRange().to()).isOne();
        // and should be able to append without issues
        writeData(writeChannel, data); // write index 2 again (with small message)
        assertThat(writeChannel.currentIndex()).isEqualTo(2);
        writeChannel.prepareForFlush().flush();

        // verify readback
        try (var readChannel = envelopedLogFiles.openReadChannel(0)) {
            var buffer = ByteBuffer.allocate(8);
            for (int i = 0; i < 3; i++) {
                buffer.clear();
                readChannel.beginChecksum();
                assertThat(readChannel.read(buffer)).isEqualTo(8);
                assertThat(buffer.array()).isEqualTo(data);
                readChannel.endChecksumAndValidate();
            }
        }
    }

    @Test
    void shouldTruncateAcrossFilesWhenLastIncompleteEntryStartsInEarlierFiles() throws IOException {
        // Create initial file with 3 entries
        envelopedLogFiles.initialise();
        var data = EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8);
        var bigData = new byte[2 * totalSegments * segmentBlockSize];
        Arrays.fill(bigData, (byte) 1);
        var writeChannel = envelopedLogFiles.currentWriteChannel();
        writeData(writeChannel, data); // index 0
        writeData(writeChannel, data); // index 1
        writeData(writeChannel, bigData); // index 2 - spans multiple files
        writeChannel.prepareForFlush().flush();
        var endOffset = writeChannel.position();
        var lastLogFile = mirroringRepository.logVersionsRange().to();
        envelopedLogFiles.close();

        // Ruin the last envelope in the entry
        try (var channel = mirroringRepository.createWriteChannel(lastLogFile).channel()) {
            int wipeAt = (int) (endOffset - 1);
            channel.position(wipeAt);
            var zero = ByteBuffer.wrap(new byte[1]);
            channel.writeAll(zero);
            channel.flush();
        }

        // when
        recreateEnvelopedLogFiles(LatestVersions.LATEST_KERNEL_VERSION);
        // should initialise to prev appendIndex 1
        assertThat(envelopedLogFiles.initialise()).isOne();
        writeChannel = envelopedLogFiles.currentWriteChannel();

        // then we expect it to have truncated and rolled to a new file
        assertThat(mirroringRepository.logVersionsRange().to()).isOne();
        // and should be able to append without issues
        writeData(writeChannel, data); // write index 2 again (with small message)
        assertThat(writeChannel.currentIndex()).isEqualTo(2);
        writeChannel.prepareForFlush().flush();

        // verify readback
        try (var readChannel = envelopedLogFiles.openReadChannel(0)) {
            var buffer = ByteBuffer.allocate(8);
            for (int i = 0; i < 3; i++) {
                buffer.clear();
                readChannel.beginChecksum();
                assertThat(readChannel.read(buffer)).isEqualTo(8);
                assertThat(buffer.array()).isEqualTo(data);
                readChannel.endChecksumAndValidate();
            }
        }
    }

    @Test
    void shouldRecoverIfFirstEntryIsPartialAndPreserveHeader() throws IOException {
        // generate three files
        envelopedLogFiles.initialise();
        var data = EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8);
        var writeChannel = envelopedLogFiles.currentWriteChannel();
        while (mirroringRepository.logVersionsRange().to() < 3L) {
            writeData(writeChannel, data);
        }
        writeChannel.prepareForFlush().flush();
        long lastAppendIndex = writeChannel.currentIndex();
        long endOffset = writeChannel.position();
        long lastLogFile = mirroringRepository.logVersionsRange().to();
        // keep only the third file, which should contain just a single entry
        envelopedLogFiles.prune(writeChannel.currentIndex());
        LogHeader originalHeader;
        try (var metadata = envelopedLogFiles.logFilesMetadata()) {
            metadata.next();
            originalHeader = metadata.get().logHeader();
        }
        envelopedLogFiles.close();

        // Ruin the one remaining envelope
        try (var channel = mirroringRepository.createWriteChannel(lastLogFile).channel()) {
            int wipeAt = (int) (endOffset - 1);
            channel.position(wipeAt);
            var zero = ByteBuffer.wrap(new byte[1]);
            channel.writeAll(zero);
            channel.flush();
        }

        recreateEnvelopedLogFiles(LatestVersions.LATEST_KERNEL_VERSION);
        assertThat(envelopedLogFiles.initialise()).isEqualTo(lastAppendIndex - 1);
        writeChannel = envelopedLogFiles.currentWriteChannel();
        assertThat(mirroringRepository.logVersionsRange().to()).isEqualTo(3L);
        try (var metadata = envelopedLogFiles.logFilesMetadata()) {
            metadata.next();
            assertThat(metadata.get().logHeader()).isEqualTo(originalHeader);
        }

        // and should be able to append without issues
        writeData(writeChannel, data); // write again
        assertThat(writeChannel.currentIndex()).isEqualTo(lastAppendIndex);
        writeChannel.prepareForFlush().flush();

        // verify readback
        try (var readChannel = envelopedLogFiles.openReadChannel(lastAppendIndex)) {
            var buffer = ByteBuffer.allocate(8);
            readChannel.beginChecksum();
            assertThat(readChannel.read(buffer)).isEqualTo(8);
            assertThat(buffer.array()).isEqualTo(data);
            readChannel.endChecksumAndValidate();
        }
    }

    @Test
    void shouldRecoverWhenManyFilesPrecedeBrokenLastEntry() throws IOException {
        // Generate 4 files
        envelopedLogFiles.initialise();
        var data = EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8);
        var writeChannel = envelopedLogFiles.currentWriteChannel();
        int appendIndex = 0;
        for (int file = 0; file < 4; file++) {
            writeData(writeChannel, data);
            appendIndex++;
            envelopedLogFiles.forceRotate();
        }
        writeData(writeChannel, data);
        appendIndex++;
        writeChannel.prepareForFlush().flush();
        var endOffset = writeChannel.position();
        envelopedLogFiles.close();

        // Ruin the only envelope in file 4 so brokenLastEntry recovery kicks in.
        try (var channel = mirroringRepository.createWriteChannel(4).channel()) {
            int wipeAt = (int) (endOffset - 1);
            channel.position(wipeAt);
            channel.writeAll(ByteBuffer.wrap(new byte[1]));
            channel.flush();
        }

        recreateEnvelopedLogFiles(LatestVersions.LATEST_KERNEL_VERSION);
        assertThat(envelopedLogFiles.initialise()).isEqualTo(appendIndex - 2L);
        writeChannel = envelopedLogFiles.currentWriteChannel();

        // After recovery file 4 is re-created and the cache covers [0..4].
        assertThat(mirroringRepository.logVersionsRange()).isEqualTo(LongRange.range(0L, 4L));

        // and we can keep appending without issues
        writeData(writeChannel, data);
        assertThat(writeChannel.currentIndex()).isEqualTo(appendIndex - 1L);
        writeChannel.prepareForFlush().flush();
    }

    @Test
    void shouldRecoverWithCorruptFirstEntryInFile() throws IOException {
        // Create initial files one with 2 entries and second with a single entry
        envelopedLogFiles.initialise();
        var data = EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8);
        var writeChannel = envelopedLogFiles.currentWriteChannel();
        writeData(writeChannel, data); // index 0
        writeData(writeChannel, data); // index 1
        envelopedLogFiles.forceRotate();
        writeData(writeChannel, data); // index 2
        writeChannel.prepareForFlush().flush();
        var endOffset = writeChannel.position();
        envelopedLogFiles.close();

        // Ruin the last envelope so that second file has no valid entries at all
        try (var channel = mirroringRepository.createWriteChannel(1).channel()) {
            int wipeAt = (int) (endOffset - 1);
            channel.position(wipeAt);
            var zero = ByteBuffer.wrap(new byte[1]);
            channel.writeAll(zero);
            channel.flush();
        }

        // when
        recreateEnvelopedLogFiles(LatestVersions.LATEST_KERNEL_VERSION);
        // should initialise to last appendIndex 1
        assertThat(envelopedLogFiles.initialise()).isOne();
        writeChannel = envelopedLogFiles.currentWriteChannel();

        // then we expect it to have just carried on with recreating file 1
        assertThat(mirroringRepository.logVersionsRange().to()).isOne();
        // and should be able to re-append without issues
        writeData(writeChannel, data); // write index 2
        assertThat(writeChannel.currentIndex()).isEqualTo(2);
        writeChannel.prepareForFlush().flush();

        // verify readback
        try (var readChannel = envelopedLogFiles.openReadChannel(0)) {
            var buffer = ByteBuffer.allocate(8);
            for (int i = 0; i < 3; i++) {
                buffer.clear();
                readChannel.beginChecksum();
                assertThat(readChannel.read(buffer)).isEqualTo(8);
                assertThat(buffer.array()).isEqualTo(data);
                readChannel.endChecksumAndValidate();
            }
        }
    }

    @Test
    void shouldNotTruncateOnHeaderOnlyFile() throws IOException {
        // Create initial files one with 2 entries one that is empty and third with a single entry
        envelopedLogFiles.initialise();
        var data = EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8);
        var writeChannel = envelopedLogFiles.currentWriteChannel();
        writeData(writeChannel, data); // index 0
        writeData(writeChannel, data); // index 1
        envelopedLogFiles.forceRotate();
        // rotate again to leave a header only file
        envelopedLogFiles.forceRotate();
        writeData(writeChannel, data); // index 2
        writeChannel.prepareForFlush().flush();
        envelopedLogFiles.close();

        // when
        recreateEnvelopedLogFiles(LatestVersions.LATEST_KERNEL_VERSION);
        // should initialise to last appendIndex 2 without any truncation
        assertThat(envelopedLogFiles.initialise()).isEqualTo(2L);
        writeChannel = envelopedLogFiles.currentWriteChannel();

        // then we expect it to have just carried on with recreating file 1
        assertThat(mirroringRepository.logVersionsRange().to()).isEqualTo(2);
        // and should be able to re-append without issues
        writeData(writeChannel, data); // write index 3
        assertThat(writeChannel.currentIndex()).isEqualTo(3);
        writeChannel.prepareForFlush().flush();

        // verify readback
        try (var readChannel = envelopedLogFiles.openReadChannel(0)) {
            var buffer = ByteBuffer.allocate(8);
            for (int i = 0; i < 4; i++) {
                buffer.clear();
                readChannel.beginChecksum();
                assertThat(readChannel.read(buffer)).isEqualTo(8);
                assertThat(buffer.array()).isEqualTo(data);
                readChannel.endChecksumAndValidate();
            }
        }
    }

    @Test
    void shouldBeAbleToInitialiseTwiceWithoutException() throws IOException {
        // Create initial file with 2 entries
        assertThat(envelopedLogFiles.initialise()).isEqualTo(-1L);
        envelopedLogFiles.currentWriteChannel().prepareForFlush().flush();
        // should be fine to call initialise again
        assertThat(envelopedLogFiles.initialise()).isEqualTo(-1L);
        var data = EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8);
        var writeChannel = envelopedLogFiles.currentWriteChannel();
        writeData(writeChannel, data); // index 0
        writeData(writeChannel, data); // index 1
        writeChannel.prepareForFlush().flush();
        long expectedAppendIndex = writeChannel.currentIndex();
        // calling initialise on now non-empty files should also not cause issues
        assertThat(envelopedLogFiles.initialise()).isEqualTo(expectedAppendIndex);
        writeChannel = envelopedLogFiles.currentWriteChannel();
        writeData(writeChannel, data);
        assertThat(writeChannel.currentIndex()).isEqualTo(expectedAppendIndex + 1);
    }

    @Test
    void truncatingFirstItemInFileShouldPreservePrevTerm() throws IOException {
        // Create initial files one with 2 entries one that is empty and third with a single entry
        envelopedLogFiles.initialise();
        var data = EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8);
        var writer = envelopedLogFiles.currentWriteChannel();
        writer.beginChecksumForWriting();
        writer.putContentType(LogEnvelopeHeader.KERNEL_CONTENT_TYPE);
        writer.putVersion(LatestVersions.LATEST_KERNEL_VERSION.version());
        writer.putTerm(80L);
        writer.write(ByteBuffer.wrap(data));
        writer.putChecksum();
        writer.beginChecksumForWriting();
        writer.putContentType(LogEnvelopeHeader.KERNEL_CONTENT_TYPE);
        writer.putVersion(LatestVersions.LATEST_KERNEL_VERSION.version());
        writer.putTerm(81L);
        writer.write(ByteBuffer.wrap(data));
        writer.putChecksum();
        writer.beginChecksumForWriting();
        writer.putContentType(LogEnvelopeHeader.KERNEL_CONTENT_TYPE);
        writer.putVersion(LatestVersions.LATEST_KERNEL_VERSION.version());
        writer.putTerm(82L);
        writer.write(ByteBuffer.wrap(data));
        writer.putChecksum();
        // rotate into new file
        envelopedLogFiles.forceRotate();
        writer = envelopedLogFiles.currentWriteChannel();
        writer.beginChecksumForWriting();
        writer.putContentType(LogEnvelopeHeader.KERNEL_CONTENT_TYPE);
        writer.putVersion(LatestVersions.LATEST_KERNEL_VERSION.version());
        writer.putTerm(83L);
        writer.write(ByteBuffer.wrap(data));
        writer.putChecksum();
        writer.prepareForFlush().flush();
        // verify term information
        try (var reader = envelopedLogFiles.openReadChannel()) {
            // initial file starts on term -1
            assertThat(reader.logHeader().getLastTerm()).isEqualTo(-1L);
            reader.alignWithStartEntry();
            assertThat(reader.currentTerm()).isEqualTo(80L);
            reader.goToNextEntry();
            assertThat(reader.currentTerm()).isEqualTo(81L);
            reader.goToNextEntry();
            assertThat(reader.currentTerm()).isEqualTo(82L);
            // move onto next file
            reader.goToNextEntry();
            assertThat(reader.logHeader().getLogVersion()).isOne();
            assertThat(reader.logHeader().getLastTerm()).isEqualTo(82L);
            assertThat(reader.currentTerm()).isEqualTo(83L);
        }
        // truncate away the first entry in second file
        long truncateIndex = writer.currentIndex();
        envelopedLogFiles.truncate(truncateIndex);
        try (var reader = envelopedLogFiles.openReadChannel(3)) {
            // should be log version 2, since we truncated and rolled the version 1 file
            assertThat(reader.logHeader().getLogVersion()).isEqualTo(2L);
            // the term should be rolled over from remaining file
            assertThat(reader.logHeader().getLastTerm()).isEqualTo(82L);
        }
    }

    @Test
    void shouldRecalibrateState() throws IOException {
        envelopedLogFiles.initialise();

        for (var i = 0; i < randomSupport.nextInt(1, 5); i++) {
            writeData(envelopedLogFiles.currentWriteChannel(), EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8));
        }
        envelopedLogFiles.currentWriteChannel().prepareForFlush().flush();

        var firstIndex = envelopedLogFiles.currentWriteChannel().currentIndex();
        long nextIndex;
        try (var reader = envelopedLogFiles.openReadChannel(firstIndex)) {
            reader.goToEntry(firstIndex);
            reader.goToEndOfEntry();

            try (var scopedBuffer = new HeapScopedBuffer(
                            segmentBlockSize, ByteOrder.LITTLE_ENDIAN, EmptyMemoryTracker.INSTANCE);
                    var otherWriteChannel = new EnvelopeWriteChannel(
                            mirroringRepository
                                    .openWriteChannel(mirroringRepository
                                            .logVersionsRange()
                                            .to())
                                    .channel()
                                    .position(envelopedLogFiles
                                            .currentWriteChannel()
                                            .position()),
                            scopedBuffer,
                            segmentBlockSize,
                            reader.getChecksum(),
                            firstIndex,
                            reader.currentTerm(),
                            LogTracers.NULL,
                            LogRotation.NO_ROTATION)) {
                for (var i = 0; i < randomSupport.nextInt(1, 5); i++) {
                    writeData(otherWriteChannel, EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8));
                }
                otherWriteChannel.prepareForFlush().flush();
                nextIndex = otherWriteChannel.currentIndex();
            }
        }
        assertThat(envelopedLogFiles.currentWriteChannel().currentIndex())
                .isEqualTo(firstIndex)
                .isNotEqualTo(nextIndex);
        envelopedLogFiles.recalibrateWriteChannel();
        assertThat(envelopedLogFiles.currentWriteChannel().currentIndex()).isEqualTo(nextIndex);
    }

    @Test
    void shouldRecalibrateStateShouldFailIfDirtyTail() throws IOException {
        envelopedLogFiles.initialise();

        for (var i = 0; i < randomSupport.nextInt(1, 5); i++) {
            writeData(envelopedLogFiles.currentWriteChannel(), EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8));
        }
        envelopedLogFiles.currentWriteChannel().prepareForFlush().flush();

        var data = randomSupport.nextBytes(16);
        envelopedLogFiles.currentWriteChannel().directPutAll(ByteBuffer.wrap(data), -1);
        assertThatThrownBy(() -> envelopedLogFiles.recalibrateWriteChannel())
                .hasMessageContaining("Incomplete entry found at the end of the log");
    }

    @Test
    @Disabled("Currently flaky due to a change when opening a read channel and seting position. This causes the tail"
            + " checker to not catch the dirty bytes properly. Should be enabled when fixed")
    void shouldTruncateAndRecoveryWriteChannelAfterGarbage() throws IOException {
        envelopedLogFiles.initialise();

        for (var i = 0; i < randomSupport.nextInt(0, 5); i++) {
            writeData(envelopedLogFiles.currentWriteChannel(), EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8));
        }
        envelopedLogFiles.currentWriteChannel().prepareForFlush().flush();
        var lastGoodIndex = envelopedLogFiles.currentWriteChannel().currentIndex();

        var data = randomSupport.nextBytes(16);
        envelopedLogFiles.currentWriteChannel().directPutAll(ByteBuffer.wrap(data), -1);
        envelopedLogFiles.truncateToLastSafeEntry(0);
        assertThat(envelopedLogFiles.currentWriteChannel().currentIndex()).isEqualTo(lastGoodIndex);
        writeData(envelopedLogFiles.currentWriteChannel(), EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8));
        assertThat(envelopedLogFiles.currentWriteChannel().currentIndex()).isEqualTo(lastGoodIndex + 1);
        envelopedLogFiles.currentWriteChannel().prepareForFlush().flush();

        try (var envelopeReadChannel = envelopedLogFiles.openReadChannel()) {
            envelopeReadChannel.alignWithStartEntry();
            while (envelopeReadChannel.entryIndex() != lastGoodIndex + 1) {
                envelopeReadChannel.goToEndOfEntry();
            }
        }
    }

    @Test
    void shouldTruncateAndRecoveryWithNoChangeIfNoGarbage() throws IOException {
        envelopedLogFiles.initialise();

        for (var i = 0; i < randomSupport.nextInt(0, 5); i++) {
            writeData(envelopedLogFiles.currentWriteChannel(), EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8));
        }
        envelopedLogFiles.currentWriteChannel().prepareForFlush().flush();
        var lastGoodIndex = envelopedLogFiles.currentWriteChannel().currentIndex();
        envelopedLogFiles.truncateToLastSafeEntry(0);
        assertThat(envelopedLogFiles.currentWriteChannel().currentIndex()).isEqualTo(lastGoodIndex);
        try (var envelopeReadChannel = envelopedLogFiles.openReadChannel()) {
            envelopeReadChannel.alignWithStartEntry();
            while (envelopeReadChannel.entryIndex() != lastGoodIndex) {
                envelopeReadChannel.goToEndOfEntry();
            }
        }
    }

    private static class RotationInterruptor implements LogTracers {
        public static final String BLOCK_ROTATION = "Block rotation";
        private final LogAppendEvent logAppendEvent = new LogAppendEvent() {
            @Override
            public void appendedBytes(long bytes) {}

            @Override
            public void close() {}

            @Override
            public void setLogRotated(boolean logRotated) {}

            @Override
            public AppendTransactionEvent beginAppendTransaction(int appendItems) {
                return null;
            }

            @Override
            public LogForceWaitEvent beginLogForceWait() {
                return null;
            }

            @Override
            public LogForceEvent beginLogForce() {
                return null;
            }

            @Override
            public LogRotateEvent beginLogRotate() {

                throw new RuntimeException(BLOCK_ROTATION);
            }
        };

        @Override
        public LogFileCreateEvent createLogFile() {
            return LogFileCreateEvent.NULL;
        }

        @Override
        public void openLogFile(Path filePath) {}

        @Override
        public void closeLogFile(Path filePath) {}

        @Override
        public LogAppendEvent logAppend() {
            return logAppendEvent;
        }

        @Override
        public LogFileFlushEvent flushFile() {
            return LogFileFlushEvent.NULL;
        }

        @Override
        public long appendedBytes() {
            return 0;
        }

        @Override
        public long numberOfLogRotations() {
            return 0;
        }

        @Override
        public long logRotationAccumulatedTotalTimeMillis() {
            return 0;
        }

        @Override
        public long lastLogRotationTimeMillis() {
            return 0;
        }

        @Override
        public long numberOfFlushes() {
            return 0;
        }

        @Override
        public long lastTransactionLogAppendBatch() {
            return 0;
        }

        @Override
        public long batchesAppended() {
            return 0;
        }

        @Override
        public long rolledbackBatches() {
            return 0;
        }

        @Override
        public long rolledbackBatchedTransactions() {
            return 0;
        }
    }

    @Test
    void adversarialFileTest() throws Exception {
        envelopedLogFiles.initialise();

        // Create data that overflows into new file
        var largeData = new byte[segmentBlockSize];
        var writeChannel = envelopedLogFiles.currentWriteChannel();
        writeData(writeChannel, EIGHT_BYTES_MESSAGE.getBytes(StandardCharsets.UTF_8));
        while (mirroringRepository.latestVersion() < 1) {
            writeData(writeChannel, largeData);
        }
        envelopedLogFiles.close();
        // clip end to make incomplete entry in file
        try (var truncator = mirroringRepository.createWriteChannel(1L)) {
            truncator.channel().truncate(segmentBlockSize + HEADER_SIZE);
        }
        // restart to truncate and roll due to broken last entry
        recreateEnvelopedLogFiles(LatestVersions.LATEST_KERNEL_VERSION);
        long index = envelopedLogFiles.initialise();
        envelopedLogFiles.close();
        // recreate again to do an interrupted truncate before rotation leaving the final empty file stranded
        recreateEnvelopedLogFiles(LatestVersions.LATEST_KERNEL_VERSION, new RotationInterruptor());
        envelopedLogFiles.initialise();
        assertThatThrownBy(() -> envelopedLogFiles.truncate(index)).hasMessage(RotationInterruptor.BLOCK_ROTATION);
        envelopedLogFiles.close();
        // recreate again
        recreateEnvelopedLogFiles(LatestVersions.LATEST_KERNEL_VERSION);
        envelopedLogFiles.initialise();
    }

    private byte[][] shuffledMessages(byte[] messageOne, byte[] messageTwo, byte[] messageThree, byte[] messageFour) {
        var list = new ArrayList<>(List.of(messageOne, messageTwo, messageThree, messageFour));
        Collections.shuffle(list, randomSupport.random());

        return list.toArray(new byte[0][]);
    }

    private static long gotToEndOfNextEntry(EnvelopeReadChannel envelopeReadChannel) throws IOException {
        long position;
        do {
            position = envelopeReadChannel.goToNextEnvelope();
        } while (envelopeReadChannel.payloadType != EnvelopeType.FULL
                && envelopeReadChannel.payloadType != EnvelopeType.END);
        return position;
    }

    private void verifyReadingAtPositions(ArrayList<Pair<Long, Long>> positions) throws IOException {
        int readCount = 0;
        for (var pos : positions) {
            var channel = mirroringRepository.openReadChannel(pos.first());
            try (var reader = envelopedLogFiles.envelopedReadChannel(channel, false)) {
                var posMarker = new LogPositionMarker();
                posMarker.mark(pos.first(), pos.other());
                reader.setLogPosition(posMarker);
                var messageSize = shuffledMessageSize(readCount);
                var readData = new byte[messageSize];
                reader.get(readData, messageSize);
                assertThat(readData[0]).isEqualTo((byte) (readCount & 0xFF));
                assertThat(readData[messageSize - 1]).isEqualTo(readData[0]);
                readCount++;
            }
        }
    }

    private int writeManyMessages(byte[] data) throws IOException {
        var messagesPerFile = totalFileDataSize / data.length; // does not include envelope overhead
        var minimumFiles = 3;
        var totalMessages = messagesPerFile * minimumFiles;

        for (int i = 0; i < totalMessages; i++) {
            writeData(envelopedLogFiles.currentWriteChannel(), data);
        }
        envelopedLogFiles.currentWriteChannel().prepareForFlush().flush();
        return totalMessages;
    }
}
