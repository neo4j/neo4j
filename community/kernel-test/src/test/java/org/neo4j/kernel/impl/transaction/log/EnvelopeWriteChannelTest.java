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
package org.neo4j.kernel.impl.transaction.log;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Arrays.copyOfRange;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.impl.transaction.log.EnvelopeWriteChannel.START_INDEX;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.rotation.LogRotation.NO_ROTATION;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.io.fs.ChecksumWriter;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.io.memory.ScopedBuffer;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.api.tracer.DefaultTracer;
import org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.EnvelopeType;
import org.neo4j.kernel.impl.transaction.log.files.LogFileChannelNativeAccessor;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.kernel.impl.transaction.tracing.LogRotateEvents;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class EnvelopeWriteChannelTest {
    private static final int SEGMENT_SIZE = 128;
    private static final byte KERNEL_VERSION = 7;
    private static final long ROTATION_PERIOD = 42L;
    private static final byte[] SMALL_BYTES = new byte[] {4, 5, 6, 7};

    // not injected as we need the checksums to be stable across each run of the tests and @Seed is per-method
    private final RandomSupport random = random();

    @Inject
    private DefaultFileSystemAbstraction fileSystem;

    @Inject
    private TestDirectory directory;

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void writeDataThatFitsWithinOneSegment(int segmentSize) throws IOException {
        final var version = (byte) 42;
        final var bValue = (byte) random.nextInt();
        final var iValue = random.nextInt();
        final var lValue = random.nextLong();
        ByteBuffer smallByteBuffer = ByteBuffer.wrap(new byte[] {8, 9, 10, 11});

        final var fileChannel = storeChannel();
        final var buffer = buffer(segmentSize);
        try (var channel = writeChannel(fileChannel, segmentSize, buffer)) {
            assertThat(channel.position())
                    .as("should start writing after header and zeroed first segment")
                    .isEqualTo(segmentSize);

            channel.resetAppendedBytesCounter();

            channel.putVersion(version);
            channel.put(bValue);
            channel.putInt(iValue);
            channel.putLong(lValue);
            channel.put(SMALL_BYTES, SMALL_BYTES.length);
            channel.putAll(smallByteBuffer);

            final var payloadLength =
                    Byte.BYTES + Integer.BYTES + Long.BYTES + SMALL_BYTES.length + smallByteBuffer.capacity();
            assertThat(channel.getAppendedBytes()).isEqualTo(payloadLength);

            // data should still be buffer and the header in an undetermined state
            assertThat(fileChannel.position())
                    .as("should NOT have written the data to the file")
                    .isEqualTo(segmentSize);
            assertZeroHeaderBytes(slice(buffer));

            channel.endCurrentEntry();
            assertThat(channel.position())
                    .as("buffer should be at the start of next envelope payload")
                    .isEqualTo(segmentSize + HEADER_SIZE + payloadLength);
            assertThat(fileChannel.position())
                    .as("should NOT have written the data to the file if segment still has capacity")
                    .isEqualTo(segmentSize);

            final var data = slice(buffer);

            byte[] expected = new byte[payloadLength];
            ByteBuffer.wrap(expected)
                    .order(LITTLE_ENDIAN)
                    .put(bValue)
                    .putInt(iValue)
                    .putLong(lValue)
                    .put(SMALL_BYTES)
                    .put(smallByteBuffer.position(0));

            assertEnvelopeContents(data, envelope(EnvelopeType.FULL, START_INDEX, expected, version, 0xEB087CA8));
        }
    }

    @Test
    void writeCompleteDataThatFitsWithinTheSameSegment() throws IOException {
        int segmentSize = 256;
        final var byteData = bytes(random, segmentSize / 4);
        final var chunkSize = byteData.length + HEADER_SIZE;

        final var fileChannel = storeChannel();
        final var buffer = buffer(segmentSize * 2);
        try (var channel = writeChannel(fileChannel, segmentSize, buffer)) {
            channel.putVersion(KERNEL_VERSION);
            channel.put(byteData, byteData.length);
            channel.endCurrentEntry();
            assertThat(channel.position())
                    .as("should have written the data AND the envelope")
                    .isEqualTo(segmentSize + chunkSize);

            channel.putVersion(KERNEL_VERSION);
            channel.put(byteData, byteData.length);
            channel.endCurrentEntry();
            assertThat(channel.position())
                    .as("should have written the data AND the two envelopes")
                    .isEqualTo(segmentSize + chunkSize * 2L);

            assertThat(fileChannel.position())
                    .as("should NOT have written the data to the file")
                    .isEqualTo(segmentSize);

            assertEnvelopeContents(
                    slice(buffer, segmentSize),
                    envelope(EnvelopeType.FULL, START_INDEX, byteData, 0x5C4F1DC1),
                    envelope(EnvelopeType.FULL, START_INDEX + 1, byteData, 0x46104B29));
        }
    }

    @Test
    void writeAndPutChecksumThatFitsWithinTheSameSegment() throws IOException {
        int segmentSize = 128;
        final var byteData = bytes(random, segmentSize / 6);
        final var chunkSize = byteData.length + HEADER_SIZE;
        final var checksums = new int[] {0x963CF93C, 0x748FC9CC};

        final var fileChannel = storeChannel();
        try (var channel = writeChannel(fileChannel, segmentSize, buffer())) {
            channel.putVersion(KERNEL_VERSION);
            channel.put(byteData, byteData.length);
            assertChecksum(channel.putChecksum(), checksums[0]);

            channel.putVersion(KERNEL_VERSION);
            channel.put(byteData, byteData.length);
            assertChecksum(channel.putChecksum(), checksums[1]);
            channel.prepareForFlush();

            assertThat(fileChannel.position())
                    .as("should have written the data to the file")
                    .isEqualTo(segmentSize + (chunkSize * 2L));

            assertEnvelopeContents(
                    channelData(fileChannel, segmentSize),
                    envelope(EnvelopeType.FULL, START_INDEX, byteData, checksums[0]),
                    envelope(EnvelopeType.FULL, START_INDEX + 1, byteData, checksums[1]));
        }
    }

    @Test
    void writeDataThatFitsExactlyWithinOneSegmentAndSomePartialData() throws IOException {
        int segmentSize = 256;
        final var byteData = bytes(random, segmentSize - HEADER_SIZE);

        final var fileChannel = storeChannel();

        final var buffer = buffer(segmentSize * 2);
        try (var channel = writeChannel(fileChannel, segmentSize, buffer)) {
            channel.putVersion(KERNEL_VERSION);
            channel.put(byteData, byteData.length);
            channel.endCurrentEntry();
            channel.put(SMALL_BYTES, SMALL_BYTES.length);

            assertThatThrownBy(channel::position)
                    .as("should not be able to call position() after a put")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("must be called right after");
            assertThat(fileChannel.position())
                    .as("should have flushed changes since buffer should have overflow")
                    .isEqualTo(segmentSize * 2);

            assertEnvelopeContents(
                    channelData(fileChannel, segmentSize),
                    envelope(EnvelopeType.FULL, START_INDEX, byteData, 0x6C50ADE7));

            final var data = slice(buffer);
            skipHeader(data);
            assertBytesArray(data, SMALL_BYTES);
        }
    }

    @Test
    void writeDataThatDoesntFitWithinOneSegmentAndNoComplete() throws IOException {
        int segmentSize = 128;
        final var byteData = bytes(random, segmentSize);
        final var firstPayloadLength = segmentSize - HEADER_SIZE;
        byte[] firstEnvelope = copyOfRange(byteData, 0, firstPayloadLength);
        byte[] secondEnvelope = copyOfRange(byteData, firstPayloadLength, segmentSize);

        final var fileChannel = storeChannel();
        final var buffer = buffer(segmentSize);
        try (var channel = writeChannel(fileChannel, segmentSize, buffer)) {
            channel.putVersion(KERNEL_VERSION);
            channel.put(byteData, byteData.length);
            channel.endCurrentEntry();

            assertThat(fileChannel.position())
                    .as("should have written the first segment to file")
                    .isEqualTo(segmentSize * 2);
            assertThat(channel.position())
                    .as("should have written the data AND the two envelopes")
                    .isEqualTo(segmentSize * 2 + HEADER_SIZE * 2);

            assertEnvelopeContents(
                    channelData(fileChannel, segmentSize),
                    envelope(EnvelopeType.BEGIN, START_INDEX, firstEnvelope, 0x43E374D4));

            // Second envelope only exists in the buffer
            final var data = buffer.getBuffer().position(0);
            skipHeader(data);
            assertBytesArray(data, secondEnvelope);
        }
    }

    @Test
    void writeDataThatDoesntFitWithinOneSegmentAndComplete() throws IOException {
        int segmentSize = 512;
        final var byteData = bytes(random, segmentSize);
        final var firstPayloadLength = segmentSize - HEADER_SIZE;
        byte[] firstEnvelope = copyOfRange(byteData, 0, firstPayloadLength);
        byte[] secondEnvelope = copyOfRange(byteData, firstPayloadLength, segmentSize);

        final int[] checksums = new int[] {0x47AFB72B, 0x4796C421};

        final var fileChannel = storeChannel();
        final var buffer = buffer(segmentSize * 2);
        try (var channel = writeChannel(fileChannel, segmentSize, buffer)) {
            channel.putVersion(KERNEL_VERSION);
            channel.put(byteData, byteData.length);
            channel.endCurrentEntry();

            assertThat(fileChannel.position())
                    .as("should have written the first segment to file")
                    .isEqualTo(segmentSize * 2);
            assertThat(channel.position())
                    .as("should have written the data AND the two envelopes")
                    .isEqualTo(segmentSize + (HEADER_SIZE * 2) + byteData.length);

            assertEnvelopeContents(
                    channelData(fileChannel, segmentSize),
                    envelope(EnvelopeType.BEGIN, START_INDEX, firstEnvelope, checksums[0]));

            assertEnvelopeContents(
                    slice(buffer), checksums[0], envelope(EnvelopeType.END, START_INDEX, secondEnvelope, checksums[1]));
        }
    }

    @Test
    void writeAndPutChecksumThatDoesntFitWithinOneSegment() throws IOException {
        int segmentSize = 256;
        final var byteData = bytes(random, segmentSize);
        final int[] checksums = new int[] {0x503B55D8, 0xAA05BDEB};
        final var firstPayloadLength = segmentSize - HEADER_SIZE;
        byte[] firstEnvelope = copyOfRange(byteData, 0, firstPayloadLength);
        byte[] secondEnvelope = copyOfRange(byteData, firstPayloadLength, segmentSize);

        final var fileChannel = storeChannel();
        final var buffer = buffer(segmentSize * 2);
        try (var channel = writeChannel(fileChannel, segmentSize, buffer)) {
            channel.putVersion(KERNEL_VERSION);
            channel.put(byteData, byteData.length);
            channel.putChecksum();
            channel.prepareForFlush();

            assertThat(fileChannel.position())
                    .as("should have written the first segment to file")
                    .isEqualTo((segmentSize * 2) + (HEADER_SIZE * 2));

            assertEnvelopeContents(
                    channelData(fileChannel, segmentSize),
                    envelope(EnvelopeType.BEGIN, START_INDEX, firstEnvelope, checksums[0]));

            assertEnvelopeContents(
                    slice(buffer), checksums[0], envelope(EnvelopeType.END, START_INDEX, secondEnvelope, checksums[1]));
        }
    }

    @Test
    void writeDataThatSpansMultipleSegmentsAndComplete() throws IOException {
        int segmentSize = 128;
        final var byteData = bytes(random, segmentSize * 2);
        final var segmentPayloadSize = segmentSize - HEADER_SIZE;
        byte[] firstEnvelope = copyOfRange(byteData, 0, segmentPayloadSize);
        byte[] secondEnvelope = copyOfRange(byteData, segmentPayloadSize, segmentPayloadSize * 2);
        byte[] thirdEnvelope = copyOfRange(byteData, segmentPayloadSize * 2, segmentSize * 2);

        final int[] checksums = new int[] {0x43E374D4, 0x85E9481C, 0x1582AD9F};

        final var fileChannel = storeChannel();
        final var buffer = buffer(segmentSize);
        try (var channel = writeChannel(fileChannel, segmentSize, buffer)) {
            channel.putVersion(KERNEL_VERSION);
            channel.put(byteData, byteData.length);
            channel.endCurrentEntry();

            assertThat(fileChannel.position())
                    .as("should have written the two segments to file")
                    .isEqualTo(segmentSize * 3);
            assertThat(channel.position())
                    .as("should have written the data AND the three envelopes")
                    .isEqualTo((segmentSize * 3) + (HEADER_SIZE * 3));

            assertEnvelopeContents(
                    channelData(fileChannel, segmentSize),
                    envelope(EnvelopeType.BEGIN, START_INDEX, firstEnvelope, checksums[0]),
                    envelope(EnvelopeType.MIDDLE, START_INDEX, secondEnvelope, checksums[1]));

            assertEnvelopeContents(
                    slice(buffer), checksums[1], envelope(EnvelopeType.END, START_INDEX, thirdEnvelope, checksums[2]));
        }
    }

    @Test
    void writeAndPutChecksumThatSpansMultipleSegments() throws IOException {
        int segmentSize = 256;
        final var byteData = bytes(random, segmentSize * 2);
        final var segmentPayloadSize = segmentSize - HEADER_SIZE;
        byte[] firstEnvelope = copyOfRange(byteData, 0, segmentPayloadSize);
        byte[] secondEnvelope = copyOfRange(byteData, segmentPayloadSize, segmentPayloadSize * 2);
        byte[] thirdEnvelope = copyOfRange(byteData, segmentPayloadSize * 2, segmentSize * 2);

        final var fileChannel = storeChannel();
        try (var channel = writeChannel(fileChannel, segmentSize, buffer(segmentSize * 3))) {
            channel.putVersion(KERNEL_VERSION);
            channel.put(byteData, byteData.length);
            channel.putChecksum();
            channel.prepareForFlush();

            assertThat(fileChannel.position())
                    .as("should have written the two segments to file")
                    .isEqualTo((segmentSize * 3) + (HEADER_SIZE * 3));

            assertEnvelopeContents(
                    channelData(fileChannel, segmentSize),
                    envelope(EnvelopeType.BEGIN, START_INDEX, firstEnvelope, 0x503B55D8),
                    envelope(EnvelopeType.MIDDLE, START_INDEX, secondEnvelope, 0x19FCDEF1),
                    envelope(EnvelopeType.END, START_INDEX, thirdEnvelope, 0x05EFB889));
        }
    }

    @Test
    void writeAndPutByteBufferThatSpansMultipleSegments() throws IOException {
        int segmentSize = 256;
        final var byteData = bytes(random, segmentSize * 2);
        final var segmentPayloadSize = segmentSize - HEADER_SIZE;
        byte[] firstEnvelope = copyOfRange(byteData, 0, segmentPayloadSize);
        byte[] secondEnvelope = copyOfRange(byteData, segmentPayloadSize, segmentPayloadSize * 2);
        byte[] thirdEnvelope = copyOfRange(byteData, segmentPayloadSize * 2, segmentSize * 2);

        final var fileChannel = storeChannel();
        try (var channel = writeChannel(fileChannel, segmentSize, buffer(segmentSize * 3))) {
            channel.putVersion(KERNEL_VERSION);
            channel.putAll(ByteBuffer.wrap(byteData));
            channel.putChecksum();
            channel.prepareForFlush();

            assertThat(fileChannel.position())
                    .as("should have written the two segments to file")
                    .isEqualTo((segmentSize * 3) + (HEADER_SIZE * 3));

            assertEnvelopeContents(
                    channelData(fileChannel, segmentSize),
                    envelope(EnvelopeType.BEGIN, START_INDEX, firstEnvelope, 0x503B55D8),
                    envelope(EnvelopeType.MIDDLE, START_INDEX, secondEnvelope, 0x19FCDEF1),
                    envelope(EnvelopeType.END, START_INDEX, thirdEnvelope, 0x05EFB889));
        }
    }

    @Test
    void appendEntryToSegmentWithDataAndSpansAcrossSegments() throws IOException {
        int segmentSize = 128;
        final var byteData = bytes(random, segmentSize);
        final var beginChunkSize = segmentSize - (HEADER_SIZE * 2) - SMALL_BYTES.length;
        byte[] secondEnvelope = copyOfRange(byteData, 0, beginChunkSize);
        byte[] thirdEnvelope = copyOfRange(byteData, beginChunkSize, segmentSize);

        final int[] checksums = new int[] {0x93469D92, 0x1DD002AC, 0xB1871410};

        final var fileChannel = storeChannel();

        final var buffer = buffer(segmentSize * 2);
        try (var channel = writeChannel(fileChannel, segmentSize, buffer)) {
            channel.putVersion(KERNEL_VERSION);
            channel.put(SMALL_BYTES, SMALL_BYTES.length);
            channel.endCurrentEntry();
            channel.putVersion(KERNEL_VERSION);
            channel.put(byteData, byteData.length);
            channel.endCurrentEntry();

            assertThat(fileChannel.position())
                    .as("should have written the segment to file")
                    .isEqualTo(segmentSize * 2);
            assertThat(channel.position())
                    .as("should have written the data AND the three envelopes")
                    .isEqualTo((segmentSize * 2) + SMALL_BYTES.length + (HEADER_SIZE * 3));

            assertEnvelopeContents(
                    channelData(fileChannel, segmentSize),
                    envelope(EnvelopeType.FULL, START_INDEX, SMALL_BYTES, KERNEL_VERSION, checksums[0]),
                    envelope(EnvelopeType.BEGIN, START_INDEX + 1, secondEnvelope, KERNEL_VERSION, checksums[1]));

            assertEnvelopeContents(
                    slice(buffer),
                    checksums[1],
                    envelope(EnvelopeType.END, START_INDEX + 1, thirdEnvelope, KERNEL_VERSION, checksums[2]));
        }
    }

    @Test
    void appendEntryToSegmentWithDataThatEndsCloseToTheSegmentBoundary() throws IOException {
        int segmentSize = 256;
        final var paddingSize = SMALL_BYTES.length - 1;
        final var byteData = bytes(random, segmentSize - HEADER_SIZE - paddingSize);

        final int[] checksums = new int[] {0x823B6BD6, 0x98649051};

        final var fileChannel = storeChannel();
        final var buffer = buffer(segmentSize);
        try (var channel = writeChannel(fileChannel, segmentSize, buffer)) {
            channel.putVersion(KERNEL_VERSION);
            channel.put(byteData, byteData.length);
            channel.endCurrentEntry();
            channel.putVersion(KERNEL_VERSION);
            channel.put(SMALL_BYTES, SMALL_BYTES.length);
            channel.endCurrentEntry();

            assertThat(fileChannel.position())
                    .as("should have written the segment to file")
                    .isEqualTo(segmentSize * 2);
            assertThat(channel.position())
                    .as("should have written the data, the two envelopes AND the zero-padding")
                    .isEqualTo(segmentSize + byteData.length + SMALL_BYTES.length + (HEADER_SIZE * 2) + paddingSize);

            assertEnvelopeContents(
                    channelData(fileChannel, segmentSize),
                    envelope(EnvelopeType.FULL, START_INDEX, byteData, checksums[0]),
                    padding(paddingSize));

            assertEnvelopeContents(
                    slice(buffer),
                    checksums[0],
                    envelope(EnvelopeType.FULL, START_INDEX + 1, SMALL_BYTES, checksums[1]));
        }
    }

    @Test
    void appendEntryToSegmentWithDataThatEndsCloseToTheSegmentBoundaryAndPutChecksum() throws IOException {
        int segmentSize = 128;
        int paddingSize = 3;
        byte[] byteData = bytes(random, SEGMENT_SIZE - HEADER_SIZE - paddingSize);

        final var fileChannel = storeChannel();
        try (var channel = writeChannel(fileChannel, segmentSize, buffer(segmentSize * 2))) {
            channel.putVersion(KERNEL_VERSION);
            channel.put(byteData, byteData.length);
            channel.putChecksum();
            channel.putVersion(KERNEL_VERSION);
            channel.put(SMALL_BYTES, SMALL_BYTES.length);
            channel.putChecksum();
            channel.prepareForFlush();

            assertThat(fileChannel.position())
                    .as("should have written the data, the two envelopes AND the zero-padding")
                    .isEqualTo(segmentSize + byteData.length + SMALL_BYTES.length + (HEADER_SIZE * 2) + paddingSize);

            assertEnvelopeContents(
                    channelData(fileChannel, segmentSize),
                    envelope(EnvelopeType.FULL, START_INDEX, byteData, 0x5EC7972E),
                    padding(paddingSize),
                    envelope(EnvelopeType.FULL, START_INDEX + 1, SMALL_BYTES, 0xCEC86301));
        }
    }

    @Test
    void appendEntryToSegmentWithDataThatForcesMaximumPadding() throws IOException {
        int segmentSize = 128;
        int paddingSize = HEADER_SIZE + Long.BYTES - 1;
        byte[] byteData = bytes(random, SEGMENT_SIZE - HEADER_SIZE - paddingSize);
        long value = random.nextLong();

        final var fileChannel = storeChannel();
        try (var channel = writeChannel(fileChannel, segmentSize, buffer(segmentSize * 2))) {
            channel.putVersion(KERNEL_VERSION);
            channel.put(byteData, byteData.length);
            channel.putChecksum();
            channel.putVersion(KERNEL_VERSION);
            channel.putLong(value);
            channel.putChecksum();
            channel.prepareForFlush();

            assertThat(fileChannel.position())
                    .as("should have written the data, the two envelopes AND the zero-padding")
                    .isEqualTo(segmentSize * 2 + HEADER_SIZE + Long.BYTES);

            byte[] valueBytes =
                    ByteBuffer.allocate(8).order(LITTLE_ENDIAN).putLong(value).array();
            assertEnvelopeContents(
                    channelData(fileChannel, segmentSize),
                    envelope(EnvelopeType.FULL, START_INDEX, byteData, 0x06ADE140),
                    padding(paddingSize),
                    envelope(EnvelopeType.FULL, START_INDEX + 1, valueBytes, 0xDA75189B));
        }
    }

    @Test
    void intermittentFlushingOfData() throws IOException {
        int segmentSize = 128;
        final var fileChannel = storeChannel();

        try (var channel = writeChannel(fileChannel, segmentSize, buffer(segmentSize * 3))) {
            channel.putVersion(KERNEL_VERSION);
            channel.put((byte) 1);
            channel.endCurrentEntry();
            channel.prepareForFlush();

            assertThat(channel.position()).isEqualTo(fileChannel.position());
        }
    }

    @Test
    void writeSingleEntryThatWouldSpanOverLogFileAndNoComplete() throws IOException {
        int segmentSize = 256;
        final var maxLogFileSize = segmentSize * 3;
        final var byteData = bytes(random, segmentSize * 3);
        final var segmentPayloadSize = segmentSize - HEADER_SIZE;
        byte[] firstEnvelope = copyOfRange(byteData, 0, segmentPayloadSize);
        byte[] secondEnvelope = copyOfRange(byteData, segmentPayloadSize, segmentPayloadSize * 2);
        byte[] thirdEnvelope = copyOfRange(byteData, segmentPayloadSize * 2, segmentPayloadSize * 3);
        byte[] forthEnvelope = copyOfRange(byteData, segmentPayloadSize * 3, segmentSize * 3);

        final var initialLogVersion = 1L;

        final int[] checksums = new int[] {0x503B55D8, 0x19FCDEF1, 0x986D6E3E};

        final var fileChannel = storeChannel(initialLogVersion);
        final var buffer = buffer(segmentSize);
        try (var channel = writeChannel(
                fileChannel,
                segmentSize,
                buffer,
                logRotation(fileChannel, header(segmentSize), maxLogFileSize),
                DatabaseTracer.NULL)) {
            channel.putVersion(KERNEL_VERSION);
            channel.put(byteData, byteData.length);

            assertThat(fileChannel.position())
                    .as("should have filled the initial file")
                    .isEqualTo(maxLogFileSize);
            assertThat(fileSystem.fileExists(logPath(initialLogVersion + 1)))
                    .as("should have created the new log file")
                    .isTrue();
            assertThat(fileSystem.getFileSize(logPath(initialLogVersion + 1)))
                    .as("should have written some of the data to the new log file")
                    .isEqualTo(segmentSize * 2);

            assertEnvelopeContents(
                    channelData(fileChannel, segmentSize),
                    envelope(EnvelopeType.BEGIN, START_INDEX, firstEnvelope, checksums[0]),
                    envelope(EnvelopeType.MIDDLE, START_INDEX, secondEnvelope, checksums[1]));

            try (var rotatedFileChannel = storeChannel(initialLogVersion + 1)) {
                assertEnvelopeContents(
                        channelData(rotatedFileChannel, segmentSize * 2, segmentSize),
                        checksums[1],
                        envelope(EnvelopeType.MIDDLE, START_INDEX, thirdEnvelope, checksums[2]));
            }

            final var data = slice(buffer);
            skipHeader(data);
            assertBytesArray(data, forthEnvelope);
        }
    }

    @Test
    void writeSingleEntryThatWouldSpanOverLogFileAndComplete() throws IOException {
        int segmentSize = 256;
        final var maxLogFileSize = segmentSize * 3;
        final var byteData = bytes(random, segmentSize * 3);
        final var segmentPayloadSize = segmentSize - HEADER_SIZE;
        byte[] firstEnvelope = copyOfRange(byteData, 0, segmentPayloadSize);
        byte[] secondEnvelope = copyOfRange(byteData, segmentPayloadSize, segmentPayloadSize * 2);
        byte[] thirdEnvelope = copyOfRange(byteData, segmentPayloadSize * 2, segmentPayloadSize * 3);
        byte[] forthEnvelope = copyOfRange(byteData, segmentPayloadSize * 3, segmentSize * 3);

        final int[] checksums = new int[] {0x503B55D8, 0x19FCDEF1, 0x986D6E3E, 0xDBA7BB53};

        final var initialLogVersion = 1L;
        final var fileChannel = storeChannel(initialLogVersion);
        final var buffer = buffer(segmentSize);
        try (var channel = writeChannel(
                fileChannel,
                segmentSize,
                buffer,
                logRotation(fileChannel, header(segmentSize), maxLogFileSize),
                DatabaseTracer.NULL)) {
            channel.putVersion(KERNEL_VERSION);
            channel.put(byteData, byteData.length);
            channel.endCurrentEntry();

            assertThat(fileSystem.getFileSize(logPath(initialLogVersion)))
                    .as("should have filled the initial file")
                    .isEqualTo(maxLogFileSize);
            assertThat(fileSystem.fileExists(logPath(initialLogVersion + 1)))
                    .as("should have created the new log file")
                    .isTrue();
            assertThat(fileSystem.getFileSize(logPath(initialLogVersion + 1)))
                    .as("should have written the data to the new log file")
                    .isEqualTo(segmentSize * 2);

            assertThat(channel.position())
                    .as("should have written the data and the four envelopes")
                    .isEqualTo((byteData.length + (HEADER_SIZE * 4) + segmentSize) - maxLogFileSize + segmentSize);

            assertEnvelopeContents(
                    channelData(fileChannel, segmentSize),
                    envelope(EnvelopeType.BEGIN, START_INDEX, firstEnvelope, checksums[0]),
                    envelope(EnvelopeType.MIDDLE, START_INDEX, secondEnvelope, checksums[1]));

            try (var rotatedFileChannel = storeChannel(initialLogVersion + 1)) {
                assertEnvelopeContents(
                        channelData(rotatedFileChannel, segmentSize * 2, segmentSize),
                        checksums[1],
                        envelope(EnvelopeType.MIDDLE, START_INDEX, thirdEnvelope, checksums[2]));
            }

            assertEnvelopeContents(
                    slice(buffer), checksums[2], envelope(EnvelopeType.END, START_INDEX, forthEnvelope, checksums[3]));
        }
    }

    @Test
    void writeSingleEntryThatWouldSpanOverLogFileAndPutChecksum() throws IOException {
        int segmentSize = 256;
        final var maxLogFileSize = segmentSize * 3;
        final var byteData = bytes(random, segmentSize * 3);
        final var segmentPayloadSize = segmentSize - HEADER_SIZE;
        byte[] firstEnvelope = copyOfRange(byteData, 0, segmentPayloadSize);
        byte[] secondEnvelope = copyOfRange(byteData, segmentPayloadSize, segmentPayloadSize * 2);
        byte[] thirdEnvelope = copyOfRange(byteData, segmentPayloadSize * 2, segmentPayloadSize * 3);
        byte[] forthEnvelope = copyOfRange(byteData, segmentPayloadSize * 3, segmentSize * 3);

        final var initialLogVersion = 1L;

        final int[] checksums = new int[] {0x503B55D8, 0x19FCDEF1, 0x986D6E3E, 0xDBA7BB53};

        final var fileChannel = storeChannel(initialLogVersion);
        final var rotatedPath = logPath(initialLogVersion + 1);

        try (var channel = writeChannel(
                fileChannel,
                segmentSize,
                buffer(segmentSize),
                logRotation(fileChannel, header(segmentSize), maxLogFileSize),
                DatabaseTracer.NULL)) {
            channel.putVersion(KERNEL_VERSION);
            channel.put(byteData, byteData.length);
            channel.putChecksum();
            channel.prepareForFlush();

            assertThat(fileChannel.position())
                    .as("should have filled the initial file")
                    .isEqualTo(maxLogFileSize);
            assertThat(fileSystem.fileExists(rotatedPath))
                    .as("should have created the new log file")
                    .isTrue();

            long rotatedFileSize = fileSystem.getFileSize(rotatedPath);
            assertThat(rotatedFileSize)
                    .as("should have written the data and the four envelopes")
                    .isEqualTo((byteData.length + (HEADER_SIZE * 4) + segmentSize) - maxLogFileSize + segmentSize);

            assertEnvelopeContents(
                    channelData(fileChannel, segmentSize),
                    envelope(EnvelopeType.BEGIN, START_INDEX, firstEnvelope, checksums[0]),
                    envelope(EnvelopeType.MIDDLE, START_INDEX, secondEnvelope, checksums[1]));

            try (var rotatedFileChannel = storeChannel(initialLogVersion + 1)) {
                assertEnvelopeContents(
                        channelData(rotatedFileChannel, (int) rotatedFileSize, segmentSize),
                        checksums[1],
                        envelope(EnvelopeType.MIDDLE, START_INDEX, thirdEnvelope, checksums[2]),
                        envelope(EnvelopeType.END, START_INDEX, forthEnvelope, checksums[3]));
            }
        }
    }

    @Test
    void writeSingleEntryThatWouldSpanOverMultipleLogFiles() throws IOException {
        int segmentSize = 128;
        final var maxLogFileSize = segmentSize * 3;
        final var chunkSize = segmentSize - HEADER_SIZE;
        final var byteData = bytes(random, chunkSize * 5);
        byte[] firstEnvelope = copyOfRange(byteData, 0, chunkSize);
        byte[] secondEnvelope = copyOfRange(byteData, chunkSize, chunkSize * 2);
        byte[] thirdEnvelope = copyOfRange(byteData, chunkSize * 2, chunkSize * 3);
        byte[] forthEnvelope = copyOfRange(byteData, chunkSize * 3, chunkSize * 4);
        byte[] fifthEnvelope = copyOfRange(byteData, chunkSize * 4, chunkSize * 5);

        final var initialLogVersion = 0L;

        final int[] checksums = new int[] {0x43E374D4, 0x85E9481C, 0x7A0C38AE, 0xD1F3CA61, 0x8AD499D4};

        final var fileChannel = storeChannel(initialLogVersion);

        final var rotatedPath1 = logPath(initialLogVersion + 1);
        final var rotatedPath2 = logPath(initialLogVersion + 2);

        final var buffer = buffer(segmentSize * 3);
        try (var channel = writeChannel(
                fileChannel,
                segmentSize,
                buffer,
                logRotation(fileChannel, header(segmentSize), maxLogFileSize),
                DatabaseTracer.NULL)) {
            channel.putVersion(KERNEL_VERSION);
            channel.put(byteData, byteData.length);
            channel.endCurrentEntry();
            channel.prepareForFlush();

            assertThat(fileChannel.position())
                    .as("should have filled the initial file")
                    .isEqualTo(maxLogFileSize);
            assertThat(fileSystem.fileExists(rotatedPath1))
                    .as("should have created the second log file")
                    .isTrue();
            assertThat(fileSystem.getFileSize(rotatedPath1))
                    .as("should have filled the second file")
                    .isEqualTo(maxLogFileSize);
            assertThat(fileSystem.fileExists(rotatedPath2))
                    .as("should have created the third log file")
                    .isTrue();
            assertThat(fileSystem.getFileSize(rotatedPath2))
                    .as("should have written the data to the new log file")
                    .isEqualTo(segmentSize * 2);

            assertEnvelopeContents(
                    channelData(fileChannel, segmentSize),
                    envelope(EnvelopeType.BEGIN, START_INDEX, firstEnvelope, checksums[0]),
                    envelope(EnvelopeType.MIDDLE, START_INDEX, secondEnvelope, checksums[1]));

            try (var rotatedFileChannel = storeChannel(initialLogVersion + 1)) {
                assertEnvelopeContents(
                        channelData(rotatedFileChannel, maxLogFileSize, segmentSize),
                        checksums[1],
                        envelope(EnvelopeType.MIDDLE, START_INDEX, thirdEnvelope, checksums[2]),
                        envelope(EnvelopeType.MIDDLE, START_INDEX, forthEnvelope, checksums[3]));
            }

            try (var rotatedFileChannel = storeChannel(initialLogVersion + 2)) {
                assertEnvelopeContents(
                        channelData(rotatedFileChannel, segmentSize * 2, segmentSize),
                        checksums[3],
                        envelope(EnvelopeType.END, START_INDEX, fifthEnvelope, checksums[4]));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 512, 1024})
    void spanningOverLogFileIsTraced(int segmentSize) throws IOException {
        final var maxLogFileSize = segmentSize * 4;
        final var byteData = bytes(random, segmentSize * 4);
        final var tracer = new DefaultTracer(new DefaultPageCacheTracer());

        final var fileChannel = storeChannel();
        try (var channel = writeChannel(
                fileChannel,
                segmentSize,
                buffer(segmentSize * 2),
                logRotation(fileChannel, header(segmentSize), maxLogFileSize),
                tracer)) {
            channel.putVersion(KERNEL_VERSION);
            channel.put(byteData, byteData.length);
            channel.endCurrentEntry();

            assertThat(tracer.numberOfLogRotations()).isEqualTo(1);
            assertThat(tracer.lastLogRotationTimeMillis()).isEqualTo(ROTATION_PERIOD);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {10, 100})
    void truncateWillDoRotation(int numberOfTruncatedLongs) throws IOException {
        final var fileChannel = storeChannel();

        int segmentSize = 128;

        try (var channel = writeChannel(
                fileChannel,
                segmentSize,
                buffer(segmentSize * 2),
                logRotation(fileChannel, header(segmentSize), segmentSize * 100),
                DatabaseTracer.NULL)) {
            channel.putVersion(KERNEL_VERSION);
            channel.putLong(100);
            channel.endCurrentEntry();
            long truncatePosition = channel.position();

            for (int i = 0; i < numberOfTruncatedLongs; i++) {
                channel.putVersion(KERNEL_VERSION);
                channel.putLong(i);
                channel.endCurrentEntry();
            }
            channel.prepareForFlush();

            // Truncate to first entry
            channel.truncateToPosition(truncatePosition, 0xCF1AE743, START_INDEX + 1);

            // Channel should be usable after truncate
            channel.putVersion(KERNEL_VERSION);
            channel.putLong(101);
            channel.endCurrentEntry();
            channel.prepareForFlush();
            long secondFilePosition = channel.position();

            assertThat(fileSystem.getFileSize(logPath(1)))
                    .as("file should be truncated")
                    .isEqualTo(truncatePosition);

            ByteBuffer byteBuffer = channelData(fileChannel, (int) truncatePosition, segmentSize);
            assertEnvelopeContents(
                    byteBuffer,
                    envelope(EnvelopeType.FULL, START_INDEX, new byte[] {100, 0, 0, 0, 0, 0, 0, 0}, 0x74DA50D6));

            try (var rotatedFileChannel = storeChannel(2)) {
                assertEnvelopeContents(
                        channelData(rotatedFileChannel, (int) secondFilePosition, segmentSize),
                        0xCF1AE743,
                        envelope(
                                EnvelopeType.FULL, START_INDEX + 1, new byte[] {101, 0, 0, 0, 0, 0, 0, 0}, 0xCB77F190));
            }
        }
    }

    @Test
    void failWhenTryingToCompleteAnEmptyEnvelope() throws IOException {
        final int segmentSize = 256;
        final var fileChannel = storeChannel();
        final var buffer = buffer(segmentSize);
        try (var channel = writeChannel(fileChannel, segmentSize, buffer)) {
            channel.putVersion(KERNEL_VERSION); // Version is for the channel, not for a specific envelope.
            assertThat(channel.position())
                    .as("should start writing after header and zeroed first segment")
                    .isEqualTo(segmentSize);

            // Lets try to complete an empty envelope at the beginning of the segment:
            assertThatThrownBy(channel::endCurrentEntry)
                    .as("trying to manually complete an empty envelope")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Closing empty envelope is not allowed.");

            // Now let's add something, complete it and try to complete another empty one:
            channel.putInt(42);
            channel.endCurrentEntry();
            assertThatThrownBy(channel::endCurrentEntry)
                    .as("trying to manually complete an empty envelope")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Closing empty envelope is not allowed.");
        }
    }

    private static Stream<Arguments> provideStartOffsetParameters() {
        return Stream.of(
                // segmentSize, offsetFullLength (header + length)
                Arguments.of(128, 32),
                Arguments.of(256, 32),
                // Minimum START_OFFSET, containing just the size of the header.
                Arguments.of(128, HEADER_SIZE),
                Arguments.of(256, HEADER_SIZE),
                // Maximum START_OFFSET, spawning the whole segment but enough space for a small (header + 4 bytes)
                // envelope after.
                Arguments.of(128, 128 - HEADER_SIZE - Integer.BYTES),
                Arguments.of(256, 256 - HEADER_SIZE - Integer.BYTES));
    }

    @ParameterizedTest
    @MethodSource("provideStartOffsetParameters")
    void writeStartOffsetIntoTheFirstSegment(int segmentSize, int offsetFullLength) throws IOException {
        final int mainPayloadValue = random.nextInt();
        final int mainPayloadLength = Integer.BYTES;

        final int startOffsetPayloadLength = offsetFullLength - HEADER_SIZE;

        final var fileChannel = storeChannel();
        final var buffer = buffer(segmentSize);
        try (var channel = writeChannel(fileChannel, segmentSize, buffer)) {
            channel.putVersion(KERNEL_VERSION); // Version is for the channel, not for a specific envelope.
            assertThat(channel.position())
                    .as("should start writing after header and zeroed first segment")
                    .isEqualTo(segmentSize);

            channel.insertStartOffset(offsetFullLength);

            assertThat(channel.position())
                    .as("after inserting start offset position should be at the offset in the segment")
                    .isEqualTo(segmentSize + offsetFullLength);

            // And we can keep adding envelopes as expected:
            channel.putInt(mainPayloadValue);
            assertThat(channel.getAppendedBytes()).isEqualTo(mainPayloadLength + startOffsetPayloadLength);
            final int mainPayloadEnvelopeChecksum = channel.putChecksum();
            channel.prepareForFlush();
            assertThat(channel.position())
                    .as("buffer should be at the start of next envelope payload")
                    .isEqualTo(segmentSize + offsetFullLength + HEADER_SIZE + mainPayloadLength);

            final var data = channelData(fileChannel, segmentSize);
            byte[] expected = new byte[mainPayloadLength];
            ByteBuffer.wrap(expected).order(LITTLE_ENDIAN).putInt(mainPayloadValue);
            assertEnvelopeContents(
                    data,
                    startOffset(startOffsetPayloadLength),
                    envelope(EnvelopeType.FULL, START_INDEX, expected, mainPayloadEnvelopeChecksum));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void writeStartOffsetFailsIfNotAtTheBeginningOfASegment(int segmentSize) throws IOException {
        final var fileChannel = storeChannel();
        final var buffer = buffer(segmentSize);
        try (var channel = writeChannel(fileChannel, segmentSize, buffer)) {
            channel.putVersion(KERNEL_VERSION); // Version is for the channel, not for a specific envelope.
            assertThat(channel.position())
                    .as("should start writing after header and zeroed first segment")
                    .isEqualTo(segmentSize);

            // Add a regular envelope first:
            channel.put((byte) random.nextInt());
            channel.putChecksum();

            assertThatThrownBy(() -> channel.insertStartOffset(HEADER_SIZE + 1))
                    .as("trying to insert an offset envelope in the middle of a segment will be rejected")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(EnvelopeWriteChannel.ERROR_MSG_TEMPLATE_OFFSET_MUST_BE_FIRST_IN_THE_FIRST_SEGMENT);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void writeStartOffsetFailsIfNotAtTheFirstSegment(int segmentSize) throws IOException {
        // We're going to add a few FULL envelope that takes the whole first segments,
        // so when we try to write the start offset it goes into a middle segment of the file.
        final int fullPayloadLength = segmentSize - HEADER_SIZE - 10;
        final byte[] fullPayloadValue = bytes(random, fullPayloadLength);

        final var fileChannel = storeChannel();
        final var buffer = buffer(segmentSize);
        try (var channel = writeChannel(fileChannel, segmentSize, buffer)) {
            channel.putVersion(KERNEL_VERSION); // Version is for the channel, not for a specific envelope.
            assertThat(channel.position())
                    .as("should start writing after header and zeroed first segment")
                    .isEqualTo(segmentSize);

            channel.put(fullPayloadValue, fullPayloadLength);
            channel.put(fullPayloadValue, fullPayloadLength);
            channel.put(fullPayloadValue, fullPayloadLength);
            channel.put(fullPayloadValue, fullPayloadLength);
            channel.put(fullPayloadValue, fullPayloadLength);
            assertThatThrownBy(() -> channel.insertStartOffset(HEADER_SIZE + 1))
                    .as("trying to insert an offset envelope in a segment that is not the first one will be rejected")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(EnvelopeWriteChannel.ERROR_MSG_TEMPLATE_OFFSET_MUST_BE_FIRST_IN_THE_FIRST_SEGMENT);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void writeStartOffsetFailsIfTryingToInsertInTheMiddleOfAnotherEnvelope(int segmentSize) throws IOException {
        final var fileChannel = storeChannel();
        final var buffer = buffer(segmentSize);
        try (var channel = writeChannel(fileChannel, segmentSize, buffer)) {
            channel.putVersion(KERNEL_VERSION); // Version is for the channel, not for a specific envelope.
            assertThat(channel.position())
                    .as("should start writing after header and zeroed first segment")
                    .isEqualTo(segmentSize);

            // Start add a regular envelope first, but don't close it...
            channel.put((byte) random.nextInt());

            assertThatThrownBy(() -> channel.insertStartOffset(HEADER_SIZE + 1))
                    .as("trying to insert an offset envelope in the middle of another envelope will be rejected")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(EnvelopeWriteChannel.ERROR_MSG_TEMPLATE_OFFSET_MUST_NOT_BE_INSIDE_ANOTHER_ENVELOPE);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void writeStartOffsetDoesNotAllowInvalidSizes(int segmentSize) throws IOException {
        final var fileChannel = storeChannel();
        final var buffer = buffer(segmentSize);
        try (var channel = writeChannel(fileChannel, segmentSize, buffer)) {
            channel.putVersion(KERNEL_VERSION); // Version is for the channel, not for a specific envelope.
            assertThat(channel.position())
                    .as("should start writing after header and zeroed first segment")
                    .isEqualTo(segmentSize);

            // Lower bounds
            assertThatThrownBy(() -> channel.insertStartOffset(-1))
                    .as("trying to use a negative size for offset will be rejected")
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(EnvelopeWriteChannel.ERROR_MSG_TEMPLATE_OFFSET_SIZE_TOO_SMALL.formatted(HEADER_SIZE));
            assertThatThrownBy(() -> channel.insertStartOffset(0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .as("trying to use a zero size for offset will be rejected")
                    .hasMessage(EnvelopeWriteChannel.ERROR_MSG_TEMPLATE_OFFSET_SIZE_TOO_SMALL.formatted(HEADER_SIZE));
            assertThatThrownBy(() -> channel.insertStartOffset(HEADER_SIZE - 1))
                    .as("trying to use less than the size of an envelope header will be rejected")
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(EnvelopeWriteChannel.ERROR_MSG_TEMPLATE_OFFSET_SIZE_TOO_SMALL.formatted(HEADER_SIZE));

            // Upper bounds
            assertThatThrownBy(() -> channel.insertStartOffset(segmentSize * 3 / 2))
                    .as("trying to use an offset size that is bigger than segment will be rejected")
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(EnvelopeWriteChannel.ERROR_MSG_TEMPLATE_OFFSET_SIZE_TOO_LARGE.formatted(segmentSize));
            assertThatThrownBy(() -> channel.insertStartOffset(segmentSize))
                    .as("trying to offset size the whole segment will be rejected")
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(EnvelopeWriteChannel.ERROR_MSG_TEMPLATE_OFFSET_SIZE_TOO_LARGE.formatted(segmentSize));
            assertThatThrownBy(() -> channel.insertStartOffset(segmentSize - HEADER_SIZE))
                    .as("trying to offset without leaving enough space for another envelope will be rejected")
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(EnvelopeWriteChannel.ERROR_MSG_TEMPLATE_OFFSET_SIZE_TOO_LARGE.formatted(segmentSize));

            channel.prepareForFlush();
            assertThat(channel.position())
                    .as("nothing was written to the channel after failed start offset calls")
                    .isEqualTo(segmentSize);
        }
    }

    private PhysicalLogVersionedStoreChannel storeChannel() throws IOException {
        return storeChannel(1L);
    }

    private PhysicalLogVersionedStoreChannel storeChannel(long version) throws IOException {
        final var logPath = logPath(version);
        return new PhysicalLogVersionedStoreChannel(
                fileSystem.write(logPath),
                version,
                LATEST_LOG_FORMAT,
                logPath,
                mock(LogFileChannelNativeAccessor.class),
                DatabaseTracer.NULL);
    }

    private Path logPath(long version) {
        return directory.homePath().resolve("log." + version);
    }

    private LogRotationForChannel logRotation(
            LogVersionedStoreChannel initialChannel, Supplier<byte[]> logHeader, long maxFileSize) {
        final var currentVersion = new MutableInt(initialChannel.getLogVersion());
        // this is to mimic the behaviour in TransactionLogFile/DetachedCheckpointAppender where the writer
        // manages the updates to the channel on a rotation
        return new LogRotationForChannel() {

            private EnvelopeWriteChannel writeChannel;

            @Override
            public void bindWriteChannel(EnvelopeWriteChannel writeChannel) {
                this.writeChannel = writeChannel;
            }

            @Override
            public void rotateLogFile(LogRotateEvents logRotateEvents) throws IOException {
                try (var event = logRotateEvents.beginLogRotate()) {
                    final var logChannel = storeChannel(currentVersion.incrementAndGet());
                    final var header = logHeader.get();
                    if (header.length > 0) {
                        logChannel.write(ByteBuffer.wrap(header));
                        logChannel.flush();
                    }

                    writeChannel.setChannel(logChannel);
                    event.rotationCompleted(ROTATION_PERIOD);
                }
            }

            @Override
            public void locklessRotateLogFile(
                    LogRotateEvents logRotateEvents,
                    KernelVersion kernelVersion,
                    long lastTransactionId,
                    long lastAppendIndex,
                    int previousChecksum) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long rotationSize() {
                return maxFileSize;
            }

            @Override
            public boolean rotateLogIfNeeded(LogRotateEvents logRotateEvents) {
                throw new UnsupportedOperationException("envelope channel rotation checks are done internally");
            }

            @Override
            public boolean batchedRotateLogIfNeeded(
                    LogRotateEvents logRotateEvents, long lastTransactionId, long appendIndex) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean locklessRotateLogIfNeeded(LogRotateEvents logRotateEvents) {
                return rotateLogIfNeeded(logRotateEvents);
            }
        };
    }

    private EnvelopeWriteChannel writeChannel(StoreChannel channel, int segmentSize, ScopedBuffer scopedBuffer)
            throws IOException {
        return writeChannel(channel, segmentSize, BASE_TX_CHECKSUM, scopedBuffer);
    }

    private EnvelopeWriteChannel writeChannel(
            StoreChannel channel, int segmentSize, int checksum, ScopedBuffer scopedBuffer) throws IOException {
        return writeChannel(channel, segmentSize, checksum, scopedBuffer, NO_ROTATION, DatabaseTracer.NULL);
    }

    private EnvelopeWriteChannel writeChannel(
            StoreChannel channel,
            int segmentSize,
            ScopedBuffer scopedBuffer,
            LogRotation logRotation,
            DatabaseTracer databaseTracer)
            throws IOException {
        return writeChannel(channel, segmentSize, BASE_TX_CHECKSUM, scopedBuffer, logRotation, databaseTracer);
    }

    private EnvelopeWriteChannel writeChannel(
            StoreChannel channel,
            int segmentSize,
            int checksum,
            ScopedBuffer scopedBuffer,
            LogRotation logRotation,
            DatabaseTracer databaseTracer)
            throws IOException {
        channel.position(segmentSize);
        final var writeChannel = new EnvelopeWriteChannel(
                channel, scopedBuffer, segmentSize, checksum, START_INDEX, databaseTracer, logRotation);
        if (logRotation instanceof LogRotationForChannel rotator) {
            rotator.bindWriteChannel(writeChannel);
        }
        return writeChannel;
    }

    private Supplier<byte[]> header(int logHeaderSize) {
        return () -> bytes(random, logHeaderSize);
    }

    private static HeapScopedBuffer buffer() {
        return new HeapScopedBuffer(SEGMENT_SIZE, LITTLE_ENDIAN, INSTANCE);
    }

    private static HeapScopedBuffer buffer(int segmentSize) {
        return new HeapScopedBuffer(segmentSize, LITTLE_ENDIAN, INSTANCE);
    }

    private static void assertBytesArray(ByteBuffer buffer, byte[] expected) {
        final var actualBytes = new byte[expected.length];
        buffer.get(actualBytes);
        assertThat(actualBytes).isEqualTo(expected);
    }

    private static ByteBuffer slice(HeapScopedBuffer buffer) {
        return buffer.getBuffer().duplicate().order(LITTLE_ENDIAN).position(0);
    }

    private static ByteBuffer slice(HeapScopedBuffer buffer, int segmentSize) {
        return buffer.getBuffer().duplicate().order(LITTLE_ENDIAN).position(segmentSize);
    }

    private static ByteBuffer channelData(StoreChannel channel, int segmentSize) throws IOException {
        return channelData(channel, (int) channel.position(), segmentSize);
    }

    private static ByteBuffer channelData(StoreChannel channel, int channelSize, int segmentSize) throws IOException {
        final var buffer = ByteBuffer.wrap(new byte[channelSize]).order(LITTLE_ENDIAN);
        channel.position(0).readAll(buffer);
        return buffer.flip().position(segmentSize);
    }

    private static byte[] bytes(RandomSupport random, int size) {
        final var bytes = new byte[size];
        random.nextBytes(bytes);
        return bytes;
    }

    private static void skipHeader(ByteBuffer data) {
        data.position(data.position() + HEADER_SIZE);
    }

    private static RandomSupport random() {
        final var support = new RandomSupport();
        support.setSeed(1665587165007L);
        return support;
    }

    private static void assertEnvelopeContents(ByteBuffer data, EnvelopeChunk... envelopeChunks) {
        assertEnvelopeContents(data, BASE_TX_CHECKSUM, envelopeChunks);
    }

    private static void assertEnvelopeContents(ByteBuffer data, int initialChecksum, EnvelopeChunk... envelopeChunks) {
        int previousChecksum = initialChecksum;
        for (EnvelopeChunk chunk : envelopeChunks) {
            assertLogEnvelope(data, previousChecksum, chunk);
            if (chunk.type != EnvelopeType.ZERO && chunk.type != EnvelopeType.START_OFFSET) {
                previousChecksum = chunk.checksum;
            }
        }
    }

    private static void assertLogEnvelope(ByteBuffer buffer, int previousChecksum, EnvelopeChunk chunk) {
        if (chunk.type == EnvelopeType.ZERO) {
            byte[] padding = new byte[chunk.data.length];
            buffer.get(padding);
            assertThat(padding).as("zero padding").isEqualTo(chunk.data);
            return;
        }

        int payloadChecksum = buffer.getInt();

        assertThat(buffer.get()).as("type").isEqualTo(chunk.type.typeValue);
        assertThat(buffer.getInt()).as("payloadLength").isEqualTo(chunk.data.length);
        assertThat(buffer.getLong()).as("entryIndex").isEqualTo(chunk.entryIndex);
        assertThat(buffer.get()).as("kernelVersion").isEqualTo(chunk.kernelVersion);
        int previousPayloadChecksum = buffer.getInt();
        if (chunk.type != EnvelopeType.START_OFFSET) {
            assertThat(previousPayloadChecksum).as("previousChecksum").isEqualTo(previousChecksum);
        } else {
            // START_OFFSET envelopes do not participate in the checksum chain
            assertThat(previousPayloadChecksum).as("previousChecksum").isEqualTo(0);
        }
        assertBytesArray(buffer, chunk.data);

        // We verify the checksum by last, because it is easier to track down bugs/errors when we first detect
        // the mismatched component above. If everything matches the expected, but the checksum doesn't then it
        // is a sign that something strange is happening with the checksum calculation.
        assertChecksum(payloadChecksum, chunk.checksum);
    }

    private static void assertChecksum(int actual, int expected) {
        // We make the assertion as hex string, so if they don't match the produced error message is more clear
        // and easier to check against or update the current checksum values used on setting up the tests.
        assertThat(Integer.toHexString(actual)).as("checksum").isEqualTo(Integer.toHexString(expected));
    }

    private static final class EnvelopeChunk {
        private final EnvelopeType type;
        private final int checksum;
        private final byte[] data;
        private final long entryIndex;
        private final byte kernelVersion;

        private EnvelopeChunk(EnvelopeType type, long entryIndex, int checksum, byte[] data) {
            this(type, entryIndex, checksum, data, KERNEL_VERSION);
        }

        private EnvelopeChunk(EnvelopeType type, long entryIndex, int checksum, byte[] data, byte kernelVersion) {
            this.type = type;
            this.checksum = checksum;
            this.data = data;
            this.kernelVersion = kernelVersion;
            this.entryIndex = entryIndex;
        }

        @Override
        public String toString() {
            return String.format(
                    "EnvelopeChunk[type=%s,checksum=%s,length=%s,kernelVersion=%s,entryIndex=%s]",
                    type, checksum, data.length, kernelVersion, entryIndex);
        }
    }

    private static EnvelopeChunk envelope(EnvelopeType type, long entryIndex, byte[] payload, int checksum) {
        return new EnvelopeChunk(type, entryIndex, checksum, payload);
    }

    private static EnvelopeChunk envelope(
            EnvelopeType type, long entryIndex, byte[] payload, byte kernelVersion, int checksum) {
        return new EnvelopeChunk(type, entryIndex, checksum, payload, kernelVersion);
    }

    private static EnvelopeChunk padding(int size) {
        return new EnvelopeChunk(EnvelopeType.ZERO, EnvelopeWriteChannel.START_INDEX, 0, new byte[size]);
    }

    private static EnvelopeChunk startOffset(int length) {
        return new EnvelopeChunk(
                EnvelopeType.START_OFFSET, START_INDEX, expectedStartOffsetChecksum(length), new byte[length]);
    }

    /**
     * Checksums for start envelopes are quite easy to calculate, so we do it manually here to match what we see
     * from the writer channel.
     */
    private static int expectedStartOffsetChecksum(int length) {
        // Full header minus the 4 bytes for checksum (that we're computing now) plus 0's for length.
        final int checksumFieldsLength = HEADER_SIZE - Integer.BYTES + length;
        final byte[] checksumBuffer = new byte[checksumFieldsLength];
        final ByteBuffer checksumView = ByteBuffer.wrap(checksumBuffer)
                .order(LITTLE_ENDIAN)
                // Write the header without the checksum, as we're calculating it right now:
                .put(EnvelopeType.START_OFFSET.typeValue)
                .putInt(length)
                .putLong(0)
                .put(KERNEL_VERSION)
                .putInt(0); // Previous checksum is 0, as start offset does not participate in checksum chain.

        final var checksum = ChecksumWriter.CHECKSUM_FACTORY.get();
        checksum.reset();
        checksum.update(checksumView.clear().limit(checksumFieldsLength).position(0));
        return (int) checksum.getValue();
    }

    private static void assertZeroHeaderBytes(ByteBuffer buffer) {
        var pos = 0;
        while (pos++ < HEADER_SIZE) {
            assertThat(buffer.get()).isZero();
        }
    }

    private interface LogRotationForChannel extends LogRotation {
        void bindWriteChannel(EnvelopeWriteChannel channel);
    }
}
