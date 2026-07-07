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
import static org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.UNSPECIFIED_CREATION_TIME;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.nativeimpl.NativeAccessProvider;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.io.fs.filename.SequentialFileNameHelper;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.StoreChannelNativeAccessor;
import org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruneThreshold;
import org.neo4j.kernel.impl.transaction.log.pruning.ThresholdFactory;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.StoreIdentifier;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@RandomSupportExtension
class SegmentBinarySearchTest {

    public static final int writeBufferedBlocks = 2;
    private final int segmentBlockSize = 256;
    private final int totalSegments = 14;
    private final LogPruneThreshold pruneStrategy = ThresholdFactory.KEEP_ALL;

    @Inject
    TestDirectory testDirectory;

    @Inject
    FileSystemAbstraction fs;

    private EnvelopedLogFiles envelopedLogFiles;

    private static void writeData(EnvelopeWriteChannel writeChannel, byte[] data, int index) throws IOException {
        writeData(writeChannel, data, -1L);
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
        recreateEnvelopedLogFiles();
    }

    private void recreateEnvelopedLogFiles() {
        var baseFolder = testDirectory.directory("logsFolder");
        envelopedLogFiles = new EnvelopedLogFiles(
                new LogsRepository(fs, new SequentialFileNameHelper(baseFolder, "enveloped-log-file")),
                (fileVersion, preFileIndex, preFileChecksum, segmentSize, lastTerm) -> LogFormat.fromKernelVersion(
                                KernelVersion.GLORIOUS_FUTURE)
                        .newHeader(
                                fileVersion,
                                preFileIndex,
                                lastTerm,
                                StoreIdentifier.newStoreIdentifier(
                                        ThreadLocalRandom.current().nextLong()),
                                segmentSize,
                                preFileChecksum,
                                KernelVersion.GLORIOUS_FUTURE,
                                UNSPECIFIED_CREATION_TIME),
                segmentBlockSize,
                writeBufferedBlocks,
                totalSegments,
                EmptyMemoryTracker.INSTANCE,
                pruneStrategy,
                new StoreChannelNativeAccessor(
                        fs, NativeAccessProvider.getNativeAccess(), NullLogProvider.getInstance(), s -> {}),
                NullLogProvider.getInstance());
    }

    /**
     * Structure of segments - one segment per line below, all in one file
     * | LOG-HEADER[li: -1]
     * | ENVELOPE[i:0 t:FULL l:225]
     * | ENVELOPE[i:1 t:FULL l:225]
     * | ENVELOPE[i:2 t:FULL l:225]
     * | ENVELOPE[i:3 t:FULL l:225]
     */
    @Test
    void shouldFindCorrectSegmentWithFullEnvelopes() throws IOException {
        var baseData = new byte[segmentBlockSize - HEADER_SIZE];

        envelopedLogFiles.initialise();

        var writeChannel = envelopedLogFiles.currentWriteChannel();

        for (int i = 0; i < 4; i++) {
            writeData(writeChannel, baseData, i);
        }
        writeChannel.prepareForFlush().flush();

        try (var readChannel = envelopedLogFiles.openReadChannel()) {
            for (int i = 0; i < 4; i++) {
                var segmentBinarySearch = new SegmentBinarySearch(readChannel, totalSegments, segmentBlockSize);
                long segmentPosition = LogBinarySearch.binarySearch(segmentBinarySearch, (long) i);

                assertCorrectSegmentFound(readChannel, segmentPosition, i);
            }
        }
    }

    /**
     * Structure of segments - one segment per line below, all in one file
     * | LOG-HEADER[li: -1]
     * | ENVELOPE[i:0 t:BEGIN l:225]
     * | ENVELOPE[i:0 t:MIDDLE l:225]
     * | ENVELOPE[i:0 t:MIDDLE l:225]
     * | ENVELOPE[i:0 t:MIDDLE l:225]
     * | ENVELOPE[i:0 t:MIDDLE l:225]
     * | ENVELOPE[i:0 t:END l:155] ENVELOPE[i:1 t:BEGIN l:39]
     * | ENVELOPE[i:1 t:MIDDLE l:225]
     * | ENVELOPE[i:1 t:MIDDLE l:225]
     * | ENVELOPE[i:1 t:MIDDLE l:225]
     * | ENVELOPE[i:1 t:MIDDLE l:225]
     * | ENVELOPE[i:1 t:MIDDLE l:225]
     * | ENVELOPE[i:1 t:END l:116]
     */
    @Test
    void shouldFindCorrectSegmentWhenThereSmallAndMiddleEnvelopes() throws IOException {
        var smallBaseData = new byte[segmentBlockSize / 3];
        var largeBaseData = new byte[segmentBlockSize * 7];

        envelopedLogFiles.initialise();

        var writeChannel = envelopedLogFiles.currentWriteChannel();

        writeData(writeChannel, smallBaseData, 0);
        writeData(writeChannel, largeBaseData, 1);

        writeChannel.prepareForFlush().flush();

        for (int i = 0; i < 2; i++) {
            try (var readChannel = envelopedLogFiles.openReadChannel(i)) {
                var segmentBinarySearch = new SegmentBinarySearch(readChannel, totalSegments, segmentBlockSize);
                long segmentPosition = LogBinarySearch.binarySearch(segmentBinarySearch, i);

                assertCorrectSegmentFound(readChannel, segmentPosition, i);
            }
        }
    }

    /**
     * Structure of segments - one segment per line below, all in one file
     * | LOG-HEADER[li: -1]
     * | ENVELOPE[i:0 t:BEGIN l:225]
     * | ENVELOPE[i:0 t:MIDDLE l:225]
     * | ENVELOPE[i:0 t:MIDDLE l:225]
     * | ENVELOPE[i:0 t:MIDDLE l:225]
     * | ENVELOPE[i:0 t:MIDDLE l:225]
     * | ENVELOPE[i:0 t:MIDDLE l:225]
     * | ENVELOPE[i:0 t:MIDDLE l:225]
     * | ENVELOPE[i:0 t:END l:217]
     * | ENVELOPE[i:1 t:FULL l:85]
     */
    @Test
    void shouldFindCorrectSegmentWhenThereMiddleEnvelopesAndThenSmall() throws IOException {
        var smallBaseData = new byte[segmentBlockSize / 3];
        var largeBaseData = new byte[segmentBlockSize * 7];

        envelopedLogFiles.initialise();

        var writeChannel = envelopedLogFiles.currentWriteChannel();

        writeData(writeChannel, largeBaseData, 0);
        writeData(writeChannel, smallBaseData, 1);

        writeChannel.prepareForFlush().flush();

        for (int i = 0; i < 2; i++) {
            try (var readChannel = envelopedLogFiles.openReadChannel(i)) {
                var segmentBinarySearch = new SegmentBinarySearch(readChannel, totalSegments, segmentBlockSize);
                long segmentPosition = LogBinarySearch.binarySearch(segmentBinarySearch, i);

                assertCorrectSegmentFound(readChannel, segmentPosition, i);
            }
        }
    }

    /**
     * Structure of segments - one segment per line below, all in one file
     * | LOG-HEADER[li: -1]
     * | ENVELOPE[i:0 t:BEGIN l:225]
     * | ENVELOPE[i:0 t:MIDDLE l:225]
     * | ENVELOPE[i:0 t:MIDDLE l:225]
     * | ENVELOPE[i:0 t:MIDDLE l:225]
     * | ENVELOPE[i:0 t:MIDDLE l:225]
     * | ENVELOPE[i:0 t:END l:155] ENVELOPE[i:1 t:BEGIN l:39]
     * | ENVELOPE[i:1 t:MIDDLE l:225]
     * | ENVELOPE[i:1 t:MIDDLE l:225]
     * | ENVELOPE[i:1 t:MIDDLE l:225]
     * | ENVELOPE[i:1 t:MIDDLE l:225]
     * | ENVELOPE[i:1 t:MIDDLE l:225]
     * | ENVELOPE[i:1 t:END l:116]
     */
    @Test
    void shouldFindCorrectFileWhenThereMiddleEnvelopes() throws IOException {
        var largeBaseData = new byte[segmentBlockSize * 5];

        envelopedLogFiles.initialise();

        var writeChannel = envelopedLogFiles.currentWriteChannel();

        writeData(writeChannel, largeBaseData, 0);
        writeData(writeChannel, largeBaseData, 1);

        writeChannel.prepareForFlush().flush();

        for (int i = 0; i < 2; i++) {
            try (var readChannel = envelopedLogFiles.openReadChannel(i)) {
                var segmentBinarySearch = new SegmentBinarySearch(readChannel, totalSegments, segmentBlockSize);
                long segmentPosition = LogBinarySearch.binarySearch(segmentBinarySearch, i);

                assertCorrectSegmentFound(readChannel, segmentPosition, i);
            }
        }
    }

    /**
     * Structure of segments - one segment per line below, all in one file
     * | LOG-HEADER[li: -1]
     * | ENVELOPE[i:0 t:FULL l:85] ENVELOPE[i:1 t:BEGIN l:109]
     * | ENVELOPE[i:1 t:MIDDLE l:225]
     * | ENVELOPE[i:1 t:MIDDLE l:225]
     * | ENVELOPE[i:1 t:MIDDLE l:225]
     * | ENVELOPE[i:1 t:MIDDLE l:225]
     * | ENVELOPE[i:1 t:MIDDLE l:225]
     * | ENVELOPE[i:1 t:END l:46] ENVELOPE[i:2 t:BEGIN l:148]
     * | ENVELOPE[i:2 t:END l:108] ENVELOPE[i:3 t:BEGIN l:86]
     * | ENVELOPE[i:3 t:MIDDLE l:225]
     * | ENVELOPE[i:3 t:MIDDLE l:225]
     * | ENVELOPE[i:3 t:MIDDLE l:225]
     * | ENVELOPE[i:3 t:END l:7] ENVELOPE[i:4 t:FULL l:85]
     */
    @Test
    void shouldFindCorrectFileWhenThereAreMixOfEnvelopeSizes() throws IOException {
        var smallBaseData = new byte[segmentBlockSize / 3];
        var fullBaseData = new byte[segmentBlockSize];
        var largeBaseData = new byte[segmentBlockSize * 3];
        var verLargeBaseData = new byte[segmentBlockSize * 5];

        envelopedLogFiles.initialise();

        var writeChannel = envelopedLogFiles.currentWriteChannel();

        writeData(writeChannel, smallBaseData, 0);
        writeData(writeChannel, verLargeBaseData, 1);
        writeData(writeChannel, fullBaseData, 2);
        writeData(writeChannel, largeBaseData, 3);
        writeData(writeChannel, smallBaseData, 4);

        writeChannel.prepareForFlush().flush();

        for (int i = 0; i < 5; i++) {
            try (var readChannel = envelopedLogFiles.openReadChannel(i)) {
                var segmentBinarySearch = new SegmentBinarySearch(readChannel, totalSegments, segmentBlockSize);
                long segmentPosition = LogBinarySearch.binarySearch(segmentBinarySearch, i);

                assertCorrectSegmentFound(readChannel, segmentPosition, i);
            }
        }
    }

    /**
     * Structure of segments - one segment per line below, all in one file
     * | LOG-HEADER[li: -1]
     * | ENVELOPE[i:0 t:FULL l:225]
     * | ENVELOPE[i:1 t:FULL l:225]
     * | ENVELOPE[i:2 t:FULL l:225]
     * | ENVELOPE[i:3 t:FULL l:225]
     * | ENVELOPE[i:4 t:FULL l:225]
     * | ENVELOPE[i:5 t:FULL l:225]
     * | ENVELOPE[i:6 t:FULL l:225]
     * | ENVELOPE[i:7 t:FULL l:225]
     * | ENVELOPE[i:8 t:FULL l:225]
     * | ENVELOPE[i:9 t:FULL l:225]
     * | ENVELOPE[i:10 t:FULL l:225]
     * | ENVELOPE[i:11 t:FULL l:225]
     * | ENVELOPE[i:12 t:FULL l:225]
     * | ENVELOPE[i:13 t:FULL l:225]
     */
    @Test
    void shouldThrowIfEntryDoesNotExist() throws IOException {
        var baseData = new byte[segmentBlockSize - HEADER_SIZE];

        envelopedLogFiles.initialise();

        var writeChannel = envelopedLogFiles.currentWriteChannel();

        for (int i = 0; i < 14; i++) {
            writeData(writeChannel, baseData, i);
        }
        writeChannel.prepareForFlush().flush();

        try (var readChannel = envelopedLogFiles.openReadChannel()) {
            var entryWhichDoesNotExist = 14;
            assertThatExceptionOfType(ReadPastEndException.class)
                    .isThrownBy(() -> readChannel.goToEntry(entryWhichDoesNotExist));
        }
    }

    @Test
    void shouldHandlePreAllocatedFile() throws IOException {
        var baseData = "this is an entry".getBytes(StandardCharsets.UTF_8);

        envelopedLogFiles.initialise();

        var writeChannel = envelopedLogFiles.currentWriteChannel();

        for (int i = 0; i < 14; i++) {
            writeData(writeChannel, baseData, i);
        }
        writeChannel.prepareForFlush().flush();

        int bytesLeft = Math.toIntExact(totalSegments * segmentBlockSize - writeChannel.position());
        var zeroes = ByteBuffer.wrap(new byte[bytesLeft]);
        writeChannel.directPutAll(zeroes, writeChannel.position());

        for (var i = 4; i < 14; i++) {

            try (var envelopeReadChannel = envelopedLogFiles.openReadChannel()) {
                envelopeReadChannel.goToEntry(i);
                var readEntry = new byte[baseData.length];
                var entry = ByteBuffer.wrap(readEntry);
                envelopeReadChannel.read(entry);
                assertThat(readEntry).isEqualTo(baseData);
            }
        }
    }

    @Test
    void shouldHandleFileWithOffset() throws IOException {
        var baseData = "the entry".getBytes(StandardCharsets.UTF_8);

        envelopedLogFiles.initialise();

        var writeChannel = envelopedLogFiles.currentWriteChannel();

        writeChannel.insertStartOffset(87);

        for (int i = 0; i < 14; i++) {
            writeData(writeChannel, baseData, i);
        }
        writeChannel.prepareForFlush().flush();

        for (var i = 4; i < 14; i++) {

            try (var envelopeReadChannel = envelopedLogFiles.openReadChannel()) {
                envelopeReadChannel.goToEntry(i);
                assertThat(envelopeReadChannel.getAppendIndex()).isEqualTo(i);
                var readEntry = new byte[baseData.length];
                var entry = ByteBuffer.wrap(readEntry);
                envelopeReadChannel.read(entry);
                assertThat(readEntry).isEqualTo(baseData);
            }
        }
    }

    @Test
    void shouldFindEntryThatIsBehind() throws IOException {
        var baseData = "base data".getBytes(StandardCharsets.UTF_8);

        envelopedLogFiles.initialise();

        var writeChannel = envelopedLogFiles.currentWriteChannel();

        writeChannel.insertStartOffset(44);

        for (int i = 0; i < 14; i++) {
            writeData(writeChannel, baseData, i);
        }
        writeChannel.prepareForFlush().flush();

        for (var i = 4; i < 13; i++) {

            try (var envelopeReadChannel = envelopedLogFiles.openReadChannel()) {
                while (envelopeReadChannel.entryIndex() < 13) {
                    envelopeReadChannel.goToNextEntry();
                }
                envelopeReadChannel.goToEntry(i);
                assertThat(envelopeReadChannel.getAppendIndex()).isEqualTo(i);
                var readEntry = new byte[baseData.length];
                var entry = ByteBuffer.wrap(readEntry);
                envelopeReadChannel.read(entry);
                assertThat(readEntry).isEqualTo(baseData);
            }
        }
    }

    @Test
    void shouldThrowIfEntryIsInPreviousFile() throws IOException {
        var baseData = new byte[segmentBlockSize - HEADER_SIZE];

        envelopedLogFiles.initialise();

        var writeChannel = envelopedLogFiles.currentWriteChannel();

        for (int i = 0; i < 18; i++) {
            writeData(writeChannel, baseData, i);
        }
        writeChannel.prepareForFlush().flush();

        try (var readChannel = envelopedLogFiles.openReadChannel(17)) {
            assertThat(readChannel.logHeader().getLastAppendIndex()).isEqualTo(12);
            assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> readChannel.goToEntry(12));
        }
    }

    @Test
    void shouldThrowReadPastEndIfEmpty() throws IOException {
        envelopedLogFiles.initialise();
        try (var readChannel = envelopedLogFiles.openReadChannel()) {
            assertThat(readChannel.logHeader().getLastAppendIndex()).isEqualTo(-1);
            assertThatExceptionOfType(ReadPastEndException.class).isThrownBy(() -> readChannel.goToEntry(0));
        }
    }

    private void assertCorrectSegmentFound(EnvelopeReadChannel readChannel, Long segmentPosition, int i)
            throws IOException {
        readChannel.position(segmentPosition);
        int actualSegment = (int) (segmentPosition / segmentBlockSize);

        while (readChannel.currentIndex < i) {
            readChannel.goToNextEntry();
        }

        var newPosition = readChannel.position();
        int expectedSegment = (int) (newPosition / segmentBlockSize);

        assertThat(actualSegment).isEqualTo(expectedSegment);
    }
}
