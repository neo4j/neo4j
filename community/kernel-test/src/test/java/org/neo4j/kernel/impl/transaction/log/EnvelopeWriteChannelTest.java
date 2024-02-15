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
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.io.memory.ScopedBuffer;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
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
                    padding(paddingSize, START_INDEX));

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
                    padding(paddingSize, START_INDEX),
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
                    padding(paddingSize, START_INDEX),
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
            public long rotationSize() {
                return maxFileSize;
            }

            @Override
            public boolean rotateLogIfNeeded(LogRotateEvents logRotateEvents) {
                throw new UnsupportedOperationException("envelope channel rotation checks are done internally");
            }

            @Override
            public boolean batchedRotateLogIfNeeded(LogRotateEvents logRotateEvents, long lastTransactionId) {
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
            assertLogEnvelope(
                    data,
                    chunk.checksum,
                    chunk.type,
                    chunk.entryIndex,
                    chunk.data.length,
                    chunk.kernelVersion,
                    previousChecksum,
                    chunk.data);
            if (chunk.type != EnvelopeType.ZERO) {
                previousChecksum = chunk.checksum;
            }
        }
    }

    private static void assertLogEnvelope(
            ByteBuffer buffer,
            int checksum,
            EnvelopeType type,
            long entryIndex,
            int payloadLength,
            byte kernelVersion,
            int previousChecksum,
            byte[] payload) {
        if (type == EnvelopeType.ZERO) {
            byte[] padding = new byte[payloadLength];
            buffer.get(padding);
            assertThat(padding).as("zero padding").isEqualTo(payload);
            return;
        }

        int payloadChecksum = buffer.getInt();
        assertChecksum(payloadChecksum, checksum);

        assertThat(buffer.get()).as("type").isEqualTo(type.typeValue);
        assertThat(buffer.getInt()).as("payloadLength").isEqualTo(payloadLength);
        assertThat(buffer.getLong()).as("entryIndex").isEqualTo(entryIndex);
        assertThat(buffer.get()).as("kernelVersion").isEqualTo(kernelVersion);
        int previousPayloadChecksum = buffer.getInt();
        assertThat(previousPayloadChecksum).as("previousChecksum").isEqualTo(previousChecksum);
        assertBytesArray(buffer, payload);
    }

    private static void assertChecksum(int actual, int expected) {
        // System.out.printf("%d : 0x%08X%n", actual, actual);
        assertThat(actual).as("checksum").isEqualTo(expected);
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
    }

    private static EnvelopeChunk envelope(EnvelopeType type, long entryIndex, byte[] payload, int checksum) {
        return new EnvelopeChunk(type, entryIndex, checksum, payload);
    }

    private static EnvelopeChunk envelope(
            EnvelopeType type, long entryIndex, byte[] payload, byte kernelVersion, int checksum) {
        return new EnvelopeChunk(type, entryIndex, checksum, payload, kernelVersion);
    }

    private static EnvelopeChunk padding(int size, long entryIndex) {
        return new EnvelopeChunk(EnvelopeType.ZERO, entryIndex, 0, new byte[size]);
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
