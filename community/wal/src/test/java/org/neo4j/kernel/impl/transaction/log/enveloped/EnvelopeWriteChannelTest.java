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

import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Arrays.copyOfRange;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.KERNEL_CONTENT_TYPE;
import static org.neo4j.kernel.impl.transaction.log.rotation.LogRotation.NO_ROTATION;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.AppendTransactionEvent;
import org.neo4j.kernel.impl.transaction.log.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.log.LogFileCreateEvent;
import org.neo4j.kernel.impl.transaction.log.LogFileFlushEvent;
import org.neo4j.kernel.impl.transaction.log.LogForceEvent;
import org.neo4j.kernel.impl.transaction.log.LogForceWaitEvent;
import org.neo4j.kernel.impl.transaction.log.LogTracers;
import org.neo4j.kernel.impl.transaction.log.LogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.EnvelopeType;
import org.neo4j.kernel.impl.transaction.log.rotation.CountingLogRotateEvent;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotateEvent;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;

@TestDirectoryExtension
class EnvelopeWriteChannelTest extends EnvelopeWriteChannelTestSupport {

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void writeDataThatFitsWithinOneSegment(int segmentSize) throws IOException {
        final var version = (byte) 42;
        final var term = 24L;
        final var contentType = (byte) 12;
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

            channel.beginChecksumForWriting();
            channel.putVersion(version);
            channel.putTerm(term);
            channel.putContentType(contentType);
            channel.put(bValue);
            channel.putInt(iValue);
            channel.putLong(lValue);
            channel.put(SMALL_BYTES, SMALL_BYTES.length);
            channel.putAll(smallByteBuffer);

            final var payloadLength =
                    Byte.BYTES + Integer.BYTES + Long.BYTES + SMALL_BYTES.length + smallByteBuffer.capacity();
            assertThat(channel.getAppendedBytes()).isEqualTo(LogEnvelopeHeader.HEADER_SIZE + payloadLength);

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

            assertEnvelopeContents(
                    data, envelope(EnvelopeType.FULL, FIRST_INDEX, expected, version, 0x2C1FE8C4, term, contentType));
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
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
            channel.put(byteData, byteData.length);
            channel.endCurrentEntry();
            assertThat(channel.position())
                    .as("should have written the data AND the envelope")
                    .isEqualTo(segmentSize + chunkSize);

            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putContentType(CONTENT_TYPE);
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
                    envelope(EnvelopeType.FULL, FIRST_INDEX, byteData, 0x2C6784FE),
                    envelope(EnvelopeType.FULL, FIRST_INDEX + 1, byteData, 0x3BD77863));
        }
    }

    @Test
    void writeAndPutChecksumThatFitsWithinTheSameSegment() throws IOException {
        int segmentSize = 256;
        final var byteData = bytes(random, segmentSize / 6);
        final var chunkSize = byteData.length + HEADER_SIZE;
        final var checksums = new int[] {0x10B0836F, 0x36BE2291};

        final var fileChannel = storeChannel();
        try (var channel = writeChannel(fileChannel, segmentSize, buffer(256))) {
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
            channel.put(byteData, byteData.length);
            assertChecksum(channel.putChecksum(), checksums[0]);

            channel.beginChecksumForWriting();
            channel.putContentType(CONTENT_TYPE);
            channel.put(byteData, byteData.length);
            assertChecksum(channel.putChecksum(), checksums[1]);
            channel.prepareForFlush();

            assertThat(fileChannel.position())
                    .as("should have written the data to the file")
                    .isEqualTo(segmentSize + (chunkSize * 2L));

            assertEnvelopeContents(
                    channelData(fileChannel, segmentSize),
                    envelope(EnvelopeType.FULL, FIRST_INDEX, byteData, checksums[0]),
                    envelope(EnvelopeType.FULL, FIRST_INDEX + 1, byteData, checksums[1]));
        }
    }

    @Test
    void writeDataThatFitsExactlyWithinOneSegmentAndSomePartialData() throws IOException {
        int segmentSize = 256;
        final var byteData = bytes(random, segmentSize - HEADER_SIZE);

        final var fileChannel = storeChannel();

        final var buffer = buffer(segmentSize * 2);
        try (var channel = writeChannel(fileChannel, segmentSize, buffer)) {
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
            channel.put(byteData, byteData.length);
            channel.endCurrentEntry();
            channel.beginChecksumForWriting();
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
                    envelope(EnvelopeType.FULL, FIRST_INDEX, byteData, 0x765CDA5A));

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
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
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
                    envelope(EnvelopeType.BEGIN, FIRST_INDEX, firstEnvelope, 0xECDD40B9));

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

        final int[] checksums = new int[] {0x96215996, 0xEF468A9B};

        final var fileChannel = storeChannel();
        final var buffer = buffer(segmentSize * 2);
        try (var channel = writeChannel(fileChannel, segmentSize, buffer)) {
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
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
                    envelope(EnvelopeType.BEGIN, FIRST_INDEX, firstEnvelope, checksums[0]));

            assertEnvelopeContents(
                    slice(buffer), checksums[0], envelope(EnvelopeType.END, FIRST_INDEX, secondEnvelope, checksums[1]));
        }
    }

    @Test
    void writeAndPutChecksumThatDoesntFitWithinOneSegment() throws IOException {
        int segmentSize = 256;
        final var byteData = bytes(random, segmentSize);
        final int[] checksums = new int[] {0x4A372265, 0x41C311FD};
        final var firstPayloadLength = segmentSize - HEADER_SIZE;
        byte[] firstEnvelope = copyOfRange(byteData, 0, firstPayloadLength);
        byte[] secondEnvelope = copyOfRange(byteData, firstPayloadLength, segmentSize);

        final var fileChannel = storeChannel();
        final var buffer = buffer(segmentSize * 2);
        try (var channel = writeChannel(fileChannel, segmentSize, buffer)) {
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
            channel.put(byteData, byteData.length);
            channel.putChecksum();
            channel.prepareForFlush();

            assertThat(fileChannel.position())
                    .as("should have written the first segment to file")
                    .isEqualTo((segmentSize * 2) + (HEADER_SIZE * 2));

            assertEnvelopeContents(
                    channelData(fileChannel, segmentSize),
                    envelope(EnvelopeType.BEGIN, FIRST_INDEX, firstEnvelope, checksums[0]));

            assertEnvelopeContents(
                    slice(buffer), checksums[0], envelope(EnvelopeType.END, FIRST_INDEX, secondEnvelope, checksums[1]));
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

        final int[] checksums = new int[] {0xECDD40B9, 0xC47EF95E, 0xC1C91A2A};

        final var fileChannel = storeChannel();
        final var buffer = buffer(segmentSize);
        try (var channel = writeChannel(fileChannel, segmentSize, buffer)) {
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
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
                    envelope(EnvelopeType.BEGIN, FIRST_INDEX, firstEnvelope, checksums[0]),
                    envelope(EnvelopeType.MIDDLE, FIRST_INDEX, secondEnvelope, checksums[1]));

            assertEnvelopeContents(
                    slice(buffer), checksums[1], envelope(EnvelopeType.END, FIRST_INDEX, thirdEnvelope, checksums[2]));
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
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
            channel.put(byteData, byteData.length);
            channel.putChecksum();
            channel.prepareForFlush();

            assertThat(fileChannel.position())
                    .as("should have written the two segments to file")
                    .isEqualTo((segmentSize * 3) + (HEADER_SIZE * 3));

            assertEnvelopeContents(
                    channelData(fileChannel, segmentSize),
                    envelope(EnvelopeType.BEGIN, FIRST_INDEX, firstEnvelope, 0x4A372265),
                    envelope(EnvelopeType.MIDDLE, FIRST_INDEX, secondEnvelope, 0xEB4F1C95),
                    envelope(EnvelopeType.END, FIRST_INDEX, thirdEnvelope, 0x86AD93D));
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
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
            channel.putAll(ByteBuffer.wrap(byteData));
            channel.putChecksum();
            channel.prepareForFlush();

            assertThat(fileChannel.position())
                    .as("should have written the two segments to file")
                    .isEqualTo((segmentSize * 3) + (HEADER_SIZE * 3));

            assertEnvelopeContents(
                    channelData(fileChannel, segmentSize),
                    envelope(EnvelopeType.BEGIN, FIRST_INDEX, firstEnvelope, 0x4A372265),
                    envelope(EnvelopeType.MIDDLE, FIRST_INDEX, secondEnvelope, 0xEB4F1C95),
                    envelope(EnvelopeType.END, FIRST_INDEX, thirdEnvelope, 0x86AD93D));
        }
    }

    @Test
    void appendEntryToSegmentWithDataAndSpansAcrossSegments() throws IOException {
        int segmentSize = 256;
        final var byteData = bytes(random, segmentSize);
        final var beginChunkSize = segmentSize - (HEADER_SIZE * 2) - SMALL_BYTES.length;
        byte[] secondEnvelope = copyOfRange(byteData, 0, beginChunkSize);
        byte[] thirdEnvelope = copyOfRange(byteData, beginChunkSize, segmentSize);

        final int[] checksums = new int[] {0x838E4BE0, 0xF85609E3, 0xC97574E3};

        final var fileChannel = storeChannel();

        final var buffer = buffer(segmentSize * 2);
        try (var channel = writeChannel(fileChannel, segmentSize, buffer)) {
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
            channel.put(SMALL_BYTES, SMALL_BYTES.length);
            channel.endCurrentEntry();
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putContentType(CONTENT_TYPE);
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
                    envelope(EnvelopeType.FULL, FIRST_INDEX, SMALL_BYTES, KERNEL_VERSION, checksums[0]),
                    envelope(EnvelopeType.BEGIN, FIRST_INDEX + 1, secondEnvelope, KERNEL_VERSION, checksums[1]));

            assertEnvelopeContents(
                    slice(buffer),
                    checksums[1],
                    envelope(EnvelopeType.END, FIRST_INDEX + 1, thirdEnvelope, KERNEL_VERSION, checksums[2]));
        }
    }

    @Test
    void appendEntryToSegmentWithDataThatEndsCloseToTheSegmentBoundary() throws IOException {
        int segmentSize = 256;
        final var paddingSize = SMALL_BYTES.length - 1;
        final var byteData = bytes(random, segmentSize - HEADER_SIZE - paddingSize);

        final int[] checksums = new int[] {0xB0FADFF8, 0x5BA016AF};

        final var fileChannel = storeChannel();
        final var buffer = buffer(segmentSize);
        try (var channel = writeChannel(fileChannel, segmentSize, buffer)) {
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
            channel.put(byteData, byteData.length);
            channel.endCurrentEntry();
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putContentType(CONTENT_TYPE);
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
                    envelope(EnvelopeType.FULL, FIRST_INDEX, byteData, checksums[0]),
                    padding(paddingSize));

            assertEnvelopeContents(
                    slice(buffer),
                    checksums[0],
                    envelope(EnvelopeType.FULL, FIRST_INDEX + 1, SMALL_BYTES, checksums[1]));
        }
    }

    @Test
    void appendEntryToSegmentWithDataThatEndsCloseToTheSegmentBoundaryAndPutChecksum() throws IOException {
        int segmentSize = 128;
        int paddingSize = 3;
        byte[] byteData = bytes(random, SEGMENT_SIZE - HEADER_SIZE - paddingSize);

        final var fileChannel = storeChannel();
        try (var channel = writeChannel(fileChannel, segmentSize, buffer(segmentSize * 2))) {
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
            channel.put(byteData, byteData.length);
            channel.putChecksum();
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putContentType(CONTENT_TYPE);
            channel.put(SMALL_BYTES, SMALL_BYTES.length);
            channel.putChecksum();
            channel.prepareForFlush();

            assertThat(fileChannel.position())
                    .as("should have written the data, the two envelopes AND the zero-padding")
                    .isEqualTo(segmentSize + byteData.length + SMALL_BYTES.length + (HEADER_SIZE * 2) + paddingSize);

            assertEnvelopeContents(
                    channelData(fileChannel, segmentSize),
                    envelope(EnvelopeType.FULL, FIRST_INDEX, byteData, 0x7B3785CE),
                    padding(paddingSize),
                    envelope(EnvelopeType.FULL, FIRST_INDEX + 1, SMALL_BYTES, 0x7AB0EAD9));
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
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
            channel.put(byteData, byteData.length);
            channel.putChecksum();
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putContentType(CONTENT_TYPE);
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
                    envelope(EnvelopeType.FULL, FIRST_INDEX, byteData, 0xD1AEC2EF),
                    padding(paddingSize),
                    envelope(EnvelopeType.FULL, FIRST_INDEX + 1, valueBytes, 0xE5B080E));
        }
    }

    @Test
    void intermittentFlushingOfData() throws IOException {
        int segmentSize = 128;
        final var fileChannel = storeChannel();

        try (var channel = writeChannel(fileChannel, segmentSize, buffer(segmentSize * 3))) {
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
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

        final int[] checksums = new int[] {0x4A372265, 0xEB4F1C95, 0x200B651C};

        final var fileChannel = storeChannel(initialLogVersion);
        final var buffer = buffer(segmentSize);
        try (var channel = writeChannel(
                fileChannel,
                segmentSize,
                buffer,
                logRotation(fileChannel, header(segmentSize), maxLogFileSize),
                LogTracers.NULL)) {
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
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
                    envelope(EnvelopeType.BEGIN, FIRST_INDEX, firstEnvelope, checksums[0]),
                    envelope(EnvelopeType.MIDDLE, FIRST_INDEX, secondEnvelope, checksums[1]));

            try (var rotatedFileChannel = storeChannel(initialLogVersion + 1)) {
                assertEnvelopeContents(
                        channelData(rotatedFileChannel, segmentSize * 2, segmentSize),
                        checksums[1],
                        envelope(EnvelopeType.MIDDLE, FIRST_INDEX, thirdEnvelope, checksums[2]));
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

        final int[] checksums = new int[] {0x4A372265, 0xEB4F1C95, 0x200B651C, 0x5486F56B};

        final var initialLogVersion = 1L;
        final var fileChannel = storeChannel(initialLogVersion);
        final var buffer = buffer(segmentSize);
        try (var channel = writeChannel(
                fileChannel,
                segmentSize,
                buffer,
                logRotation(fileChannel, header(segmentSize), maxLogFileSize),
                LogTracers.NULL)) {
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
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
                    envelope(EnvelopeType.BEGIN, FIRST_INDEX, firstEnvelope, checksums[0]),
                    envelope(EnvelopeType.MIDDLE, FIRST_INDEX, secondEnvelope, checksums[1]));

            try (var rotatedFileChannel = storeChannel(initialLogVersion + 1)) {
                assertEnvelopeContents(
                        channelData(rotatedFileChannel, segmentSize * 2, segmentSize),
                        checksums[1],
                        envelope(EnvelopeType.MIDDLE, FIRST_INDEX, thirdEnvelope, checksums[2]));
            }

            assertEnvelopeContents(
                    slice(buffer), checksums[2], envelope(EnvelopeType.END, FIRST_INDEX, forthEnvelope, checksums[3]));
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

        final int[] checksums = new int[] {0x4A372265, 0xEB4F1C95, 0x200B651C, 0x5486F56B};

        final var fileChannel = storeChannel(initialLogVersion);
        final var rotatedPath = logPath(initialLogVersion + 1);

        try (var channel = writeChannel(
                fileChannel,
                segmentSize,
                buffer(segmentSize),
                logRotation(fileChannel, header(segmentSize), maxLogFileSize),
                LogTracers.NULL)) {
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
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
                    envelope(EnvelopeType.BEGIN, FIRST_INDEX, firstEnvelope, checksums[0]),
                    envelope(EnvelopeType.MIDDLE, FIRST_INDEX, secondEnvelope, checksums[1]));

            try (var rotatedFileChannel = storeChannel(initialLogVersion + 1)) {
                assertEnvelopeContents(
                        channelData(rotatedFileChannel, (int) rotatedFileSize, segmentSize),
                        checksums[1],
                        envelope(EnvelopeType.MIDDLE, FIRST_INDEX, thirdEnvelope, checksums[2]),
                        envelope(EnvelopeType.END, FIRST_INDEX, forthEnvelope, checksums[3]));
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

        final int[] checksums = new int[] {0xECDD40B9, 0xC47EF95E, 0x547635E3, 0xE3C1A2E, 0x22E9141};

        final var fileChannel = storeChannel(initialLogVersion);

        final var rotatedPath1 = logPath(initialLogVersion + 1);
        final var rotatedPath2 = logPath(initialLogVersion + 2);

        final var buffer = buffer(segmentSize * 3);
        try (var channel = writeChannel(
                fileChannel,
                segmentSize,
                buffer,
                logRotation(fileChannel, header(segmentSize), maxLogFileSize),
                LogTracers.NULL)) {
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
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
                    envelope(EnvelopeType.BEGIN, FIRST_INDEX, firstEnvelope, checksums[0]),
                    envelope(EnvelopeType.MIDDLE, FIRST_INDEX, secondEnvelope, checksums[1]));

            try (var rotatedFileChannel = storeChannel(initialLogVersion + 1)) {
                assertEnvelopeContents(
                        channelData(rotatedFileChannel, maxLogFileSize, segmentSize),
                        checksums[1],
                        envelope(EnvelopeType.MIDDLE, FIRST_INDEX, thirdEnvelope, checksums[2]),
                        envelope(EnvelopeType.MIDDLE, FIRST_INDEX, forthEnvelope, checksums[3]));
            }

            try (var rotatedFileChannel = storeChannel(initialLogVersion + 2)) {
                assertEnvelopeContents(
                        channelData(rotatedFileChannel, segmentSize * 2, segmentSize),
                        checksums[3],
                        envelope(EnvelopeType.END, FIRST_INDEX, fifthEnvelope, checksums[4]));
            }
        }
    }

    @Test
    void rotationHappensOnFirstDataGoingOverRotationLimit() throws IOException {
        int segmentSize = 128;
        final var maxLogFileSize = segmentSize * 3;
        final var chunkSize = segmentSize - HEADER_SIZE;
        final var byteData = bytes(random, chunkSize * 2);
        byte[] firstEnvelope = copyOfRange(byteData, 0, chunkSize);
        byte[] secondEnvelope = copyOfRange(byteData, chunkSize, chunkSize * 2);
        final var secondByteData = bytes(random, chunkSize * 2);
        byte[] thirdEnvelope = copyOfRange(secondByteData, 0, chunkSize);
        byte[] fourthEnvelope = copyOfRange(secondByteData, chunkSize, chunkSize * 2);

        final var initialLogVersion = 0L;

        final int[] checksums = new int[] {0xecdd40b9, 0x8df7e412, 0x119f4e2f, 0xac04407};

        final var fileChannel = storeChannel(initialLogVersion);

        final var rotatedPath1 = logPath(initialLogVersion + 1);
        final var rotatedPath2 = logPath(initialLogVersion + 2);

        final var buffer = buffer(segmentSize * 3);
        try (var channel = writeChannel(
                fileChannel,
                segmentSize,
                buffer,
                logRotation(fileChannel, header(segmentSize), maxLogFileSize),
                LogTracers.NULL)) {
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
            channel.put(byteData, byteData.length);
            channel.endCurrentEntry();
            channel.prepareForFlush();

            assertThat(fileChannel.position()).isEqualTo(maxLogFileSize);
            assertThat(fileSystem.fileExists(rotatedPath1))
                    .as("should not have created the second log file yet")
                    .isFalse();

            // Lets put the next one
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
            channel.put(secondByteData, secondByteData.length);
            channel.endCurrentEntry();
            channel.prepareForFlush();
            assertThat(fileChannel.position())
                    .as("should have filled the initial file")
                    .isEqualTo(maxLogFileSize);
            assertThat(fileSystem.fileExists(rotatedPath1))
                    .as("should have created the second log file")
                    .isTrue();
            assertThat(fileSystem.getFileSize(rotatedPath1))
                    .as("should have written the data to the new log file")
                    .isEqualTo(maxLogFileSize);
            assertThat(fileSystem.fileExists(rotatedPath2))
                    .as("should not have created the third log file")
                    .isFalse();

            assertEnvelopeContents(
                    channelData(fileChannel, segmentSize),
                    envelope(EnvelopeType.BEGIN, FIRST_INDEX, firstEnvelope, checksums[0]),
                    envelope(EnvelopeType.END, FIRST_INDEX, secondEnvelope, checksums[1]));

            try (var rotatedFileChannel = storeChannel(initialLogVersion + 1)) {
                assertEnvelopeContents(
                        channelData(rotatedFileChannel, maxLogFileSize, segmentSize),
                        checksums[1],
                        envelope(EnvelopeType.BEGIN, FIRST_INDEX + 1, thirdEnvelope, checksums[2]),
                        envelope(EnvelopeType.END, FIRST_INDEX + 1, fourthEnvelope, checksums[3]));
            }
        }
    }

    @Test
    void rotationHappensOnFirstDataGoingOverRotationLimitWithPadding() throws IOException {
        int segmentSize = 128;
        final var maxLogFileSize = segmentSize * 2;
        final var paddingBytes = 15;
        final var chunkSize =
                segmentSize - HEADER_SIZE - paddingBytes; // Just under segment size to trigger some padding
        final var byteData = bytes(random, chunkSize);

        final var initialLogVersion = 0L;

        final int[] checksums = new int[] {0x4cfd80dc, 0xc018135f};

        final var fileChannel = storeChannel(initialLogVersion);

        final var rotatedPath1 = logPath(initialLogVersion + 1);

        final var buffer = buffer(segmentSize * 3);
        try (var channel = writeChannel(
                fileChannel,
                segmentSize,
                buffer,
                logRotation(fileChannel, header(segmentSize), maxLogFileSize),
                LogTracers.NULL)) {
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
            channel.put(byteData, byteData.length);
            channel.endCurrentEntry();
            channel.prepareForFlush();

            assertThat(fileChannel.position())
                    .as("should have written the first envelope but no padding")
                    .isEqualTo(maxLogFileSize - paddingBytes);
            assertThat(fileSystem.fileExists(rotatedPath1))
                    .as("should not have created the second log file yet")
                    .isFalse();

            // Lets put the next one
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
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
                    .as("should have written the data to the new log file")
                    .isEqualTo(segmentSize + HEADER_SIZE + chunkSize);

            assertEnvelopeContents(
                    channelData(fileChannel, segmentSize),
                    envelope(EnvelopeType.FULL, FIRST_INDEX, byteData, checksums[0]),
                    padding(paddingBytes));

            try (var rotatedFileChannel = storeChannel(initialLogVersion + 1)) {
                assertEnvelopeContents(
                        channelData(rotatedFileChannel, (int) fileSystem.getFileSize(rotatedPath1), segmentSize),
                        checksums[0],
                        envelope(EnvelopeType.FULL, FIRST_INDEX + 1, byteData, checksums[1]));
            }
        }
    }

    @Test
    void dontMissRotationIfStartingOnRotationLimit() throws IOException {
        int segmentSize = 128;
        final var maxLogFileSize = segmentSize * 3;
        final var chunkSize = segmentSize - HEADER_SIZE;
        final var byteData = bytes(random, chunkSize * 2);
        byte[] firstEnvelope = copyOfRange(byteData, 0, chunkSize);
        byte[] secondEnvelope = copyOfRange(byteData, chunkSize, chunkSize * 2);
        final var secondByteData = bytes(random, chunkSize * 2);
        byte[] thirdEnvelope = copyOfRange(secondByteData, 0, chunkSize);
        byte[] fourthEnvelope = copyOfRange(secondByteData, chunkSize, chunkSize * 2);

        final var initialLogVersion = 0L;

        final int[] checksums = new int[] {0xecdd40b9, 0x8df7e412, 0x119f4e2f, 0xac04407};

        var fileChannel = storeChannel(initialLogVersion);

        final var rotatedPath1 = logPath(initialLogVersion + 1);
        final var rotatedPath2 = logPath(initialLogVersion + 2);

        var buffer = buffer(segmentSize * 3);
        try (var channel = writeChannel(
                fileChannel,
                segmentSize,
                buffer,
                logRotation(fileChannel, header(segmentSize), maxLogFileSize),
                LogTracers.NULL)) {
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
            channel.put(byteData, byteData.length);
            channel.endCurrentEntry();
            channel.prepareForFlush();

            assertThat(fileChannel.position()).isEqualTo(maxLogFileSize);
            assertThat(fileSystem.fileExists(rotatedPath1))
                    .as("should not have created the second log file yet")
                    .isFalse();
        }

        // Open the channel again and see that rotation is triggered on the next write
        fileChannel = storeChannel(initialLogVersion);
        buffer = buffer(segmentSize * 3);
        try (var channel = writeChannel(
                fileChannel,
                segmentSize,
                checksums[1],
                buffer,
                logRotation(fileChannel, header(segmentSize), maxLogFileSize),
                LogTracers.NULL,
                maxLogFileSize /* don't forget to position the channel to where we were */,
                FIRST_INDEX)) {

            // Lets put the next one
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
            channel.put(secondByteData, secondByteData.length);
            channel.endCurrentEntry();
            channel.prepareForFlush();
            assertThat(fileChannel.position())
                    .as("should have filled the initial file")
                    .isEqualTo(maxLogFileSize);
            assertThat(fileSystem.fileExists(rotatedPath1))
                    .as("should have created the second log file")
                    .isTrue();
            assertThat(fileSystem.getFileSize(rotatedPath1))
                    .as("should have written the data to the new log file")
                    .isEqualTo(maxLogFileSize);
            assertThat(fileSystem.fileExists(rotatedPath2))
                    .as("should not have created the third log file")
                    .isFalse();

            assertEnvelopeContents(
                    channelData(fileChannel, segmentSize),
                    envelope(EnvelopeType.BEGIN, FIRST_INDEX, firstEnvelope, checksums[0]),
                    envelope(EnvelopeType.END, FIRST_INDEX, secondEnvelope, checksums[1]));

            try (var rotatedFileChannel = storeChannel(initialLogVersion + 1)) {
                assertEnvelopeContents(
                        channelData(rotatedFileChannel, maxLogFileSize, segmentSize),
                        checksums[1],
                        envelope(EnvelopeType.BEGIN, FIRST_INDEX + 1, thirdEnvelope, checksums[2]),
                        envelope(EnvelopeType.END, FIRST_INDEX + 1, fourthEnvelope, checksums[3]));
            }
        }
    }

    @Test
    void dontTriggerRotationUntilNextBoundaryIfOverFileRotationLimit() throws IOException {
        int segmentSize = 128;
        final var higherMaxLogFileSize = segmentSize * 4;
        final var lowerMaxLogFileSize = segmentSize * 3;
        final var chunkSize = segmentSize - HEADER_SIZE;
        final var bytesInLastSegmentFirstTx = 15;
        final var lastEnvelopeFirstTxSize = 15 + HEADER_SIZE;
        final var totalSizeFirstTx = (chunkSize + HEADER_SIZE) * 2 + lastEnvelopeFirstTxSize;
        final var byteData = bytes(random, chunkSize * 2 + bytesInLastSegmentFirstTx);
        byte[] firstEnvelope = copyOfRange(byteData, 0, chunkSize);
        byte[] secondEnvelope = copyOfRange(byteData, chunkSize, chunkSize * 2);
        byte[] thirdEnvelope = copyOfRange(byteData, chunkSize * 2, byteData.length);
        final var secondByteData = bytes(random, chunkSize * 2 - lastEnvelopeFirstTxSize);
        byte[] fourthEnvelope = copyOfRange(secondByteData, 0, chunkSize - lastEnvelopeFirstTxSize);
        byte[] fifthEnvelope = copyOfRange(secondByteData, chunkSize - lastEnvelopeFirstTxSize, secondByteData.length);

        final var initialLogVersion = 0L;

        final int[] checksums = new int[] {0xecdd40b9, 0xc47ef95e, 0x2eb6537a, 0x37fb27e9, 0x5022958a};

        var fileChannel = storeChannel(initialLogVersion);

        final var rotatedPath1 = logPath(initialLogVersion + 1);
        final var rotatedPath2 = logPath(initialLogVersion + 2);

        var buffer = buffer(segmentSize * 3);
        try (var channel = writeChannel(
                fileChannel,
                segmentSize,
                buffer,
                logRotation(fileChannel, header(segmentSize), higherMaxLogFileSize),
                LogTracers.NULL)) {
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
            channel.put(byteData, byteData.length);
            channel.endCurrentEntry();
            channel.prepareForFlush();

            assertThat(fileChannel.position()).isEqualTo(segmentSize + totalSizeFirstTx);
            assertThat(fileSystem.fileExists(rotatedPath1))
                    .as("should not have created the second log file yet")
                    .isFalse();
        }

        // Open the channel again and see that rotation is not triggered until the segment boundary
        fileChannel = storeChannel(initialLogVersion);
        buffer = buffer(segmentSize * 3);
        try (var channel = writeChannel(
                fileChannel,
                segmentSize,
                checksums[2],
                buffer,
                logRotation(fileChannel, header(segmentSize), lowerMaxLogFileSize),
                LogTracers.NULL,
                segmentSize + totalSizeFirstTx /* don't forget to position the channel to where we were */,
                FIRST_INDEX)) {

            // Lets put the next one
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
            channel.put(secondByteData, secondByteData.length);
            channel.endCurrentEntry();
            channel.prepareForFlush();
            assertThat(fileChannel.position())
                    .as("should have filled the initial file to the next segment")
                    .isEqualTo(lowerMaxLogFileSize + segmentSize);
            assertThat(fileSystem.fileExists(rotatedPath1))
                    .as("should have created the second log file")
                    .isTrue();
            assertThat(fileSystem.getFileSize(rotatedPath1))
                    .as("should have written the data to the new log file")
                    .isEqualTo(segmentSize * 2);
            assertThat(fileSystem.fileExists(rotatedPath2))
                    .as("should not have created the third log file")
                    .isFalse();

            assertEnvelopeContents(
                    channelData(fileChannel, segmentSize),
                    envelope(EnvelopeType.BEGIN, FIRST_INDEX, firstEnvelope, checksums[0]),
                    envelope(EnvelopeType.MIDDLE, FIRST_INDEX, secondEnvelope, checksums[1]),
                    envelope(EnvelopeType.END, FIRST_INDEX, thirdEnvelope, checksums[2]),
                    envelope(EnvelopeType.BEGIN, FIRST_INDEX + 1, fourthEnvelope, checksums[3]));

            try (var rotatedFileChannel = storeChannel(initialLogVersion + 1)) {
                assertEnvelopeContents(
                        channelData(rotatedFileChannel, segmentSize * 2, segmentSize),
                        checksums[3],
                        envelope(EnvelopeType.END, FIRST_INDEX + 1, fifthEnvelope, checksums[4]));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 512, 1024})
    void spanningOverLogFileIsTraced(int segmentSize) throws IOException {
        final var maxLogFileSize = segmentSize * 4;
        final var byteData = bytes(random, segmentSize * 4);
        final var tracer = new TestLogTracer();

        final var fileChannel = storeChannel();
        try (var channel = writeChannel(
                fileChannel,
                segmentSize,
                buffer(segmentSize * 2),
                logRotation(fileChannel, header(segmentSize), maxLogFileSize),
                tracer)) {
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
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
                LogTracers.NULL)) {
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
            channel.putLong(100);
            channel.endCurrentEntry();
            long truncatePosition = channel.position();

            for (int i = 0; i < numberOfTruncatedLongs; i++) {
                channel.beginChecksumForWriting();
                channel.putVersion(KERNEL_VERSION);
                channel.putContentType(CONTENT_TYPE);
                channel.putLong(i);
                channel.endCurrentEntry();
            }
            channel.prepareForFlush();

            // Truncate to first entry
            channel.truncateToPosition(truncatePosition, 0xCF1AE743, FIRST_INDEX, TERM);

            // Channel should be usable after truncate
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putContentType(CONTENT_TYPE);
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
                    envelope(EnvelopeType.FULL, FIRST_INDEX, new byte[] {100, 0, 0, 0, 0, 0, 0, 0}, 0xF1A25568));

            try (var rotatedFileChannel = storeChannel(2)) {
                assertEnvelopeContents(
                        channelData(rotatedFileChannel, (int) secondFilePosition, segmentSize),
                        0xCF1AE743,
                        envelope(
                                EnvelopeType.FULL, FIRST_INDEX + 1, new byte[] {101, 0, 0, 0, 0, 0, 0, 0}, 0x819FB7C1));
            }
        }
    }

    @Test
    void failWhenTryingToCompleteAnEmptyEnvelope() throws IOException {
        final int segmentSize = 256;
        final var fileChannel = storeChannel();
        final var buffer = buffer(segmentSize);
        try (var channel = writeChannel(fileChannel, segmentSize, buffer)) {
            assertThat(channel.position())
                    .as("should start writing after header and zeroed first segment")
                    .isEqualTo(segmentSize);
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION); // Version is for the channel, not for a specific envelope.
            channel.putContentType(CONTENT_TYPE);

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

    private static class TestLogTracer implements LogTracers {
        private final CountingLogRotateEvent countingLogRotateEvent = new CountingLogRotateEvent();
        private int rotations = 0;
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
                rotations++;
                return countingLogRotateEvent;
            }
        };

        @Override
        public LogFileCreateEvent createLogFile() {
            return null;
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
            return null;
        }

        @Override
        public long appendedBytes() {
            return 0;
        }

        @Override
        public long numberOfLogRotations() {
            return rotations;
        }

        @Override
        public long logRotationAccumulatedTotalTimeMillis() {
            return 0;
        }

        @Override
        public long lastLogRotationTimeMillis() {
            return countingLogRotateEvent.lastLogRotationTimeMillis();
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

    private static Stream<Arguments> provideStartOffsetParameters() {
        return Stream.of(
                // segmentSize, offsetFullLength (header + length)
                Arguments.of(128, 33, 128),
                Arguments.of(256, 33, 256),
                // Test with bufferSize other than segmentLength
                Arguments.of(256, 32, 512),
                // Minimum START_OFFSET + 1, containing just the size of the header + 1.
                Arguments.of(128, HEADER_SIZE + 1, 128),
                Arguments.of(256, HEADER_SIZE + 1, 256),
                // Maximum START_OFFSET, spawning the whole segment but enough space for a small (header + 4 bytes)
                // envelope after.
                Arguments.of(128, 128 - HEADER_SIZE - Integer.BYTES, 128),
                Arguments.of(256, 256 - HEADER_SIZE - Integer.BYTES, 256));
    }

    @ParameterizedTest
    @MethodSource("provideStartOffsetParameters")
    void writeStartOffsetIntoTheFirstSegment(int segmentSize, int offsetFullLength, int bufferSize) throws IOException {
        final int mainPayloadValue = random.nextInt();
        final int mainPayloadLength = Integer.BYTES;

        final int startOffsetPayloadLength = offsetFullLength - HEADER_SIZE;

        final var fileChannel = storeChannel();
        final var buffer = buffer(bufferSize);
        try (var channel = writeChannel(fileChannel, segmentSize, buffer)) {
            assertThat(channel.position())
                    .as("should start writing after header and zeroed first segment")
                    .isEqualTo(segmentSize);

            channel.insertStartOffset(offsetFullLength);

            assertThat(channel.position())
                    .as("after inserting start offset position should be at the offset in the segment")
                    .isEqualTo(segmentSize + offsetFullLength);

            // Start offset should be possible to write without version & content type, but these are needed for "real"
            // envelopes
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putContentType(CONTENT_TYPE);
            channel.putTerm(TERM);

            // And we can keep adding envelopes as expected:
            channel.putInt(mainPayloadValue);
            assertThat(channel.getAppendedBytes())
                    .isEqualTo(LogEnvelopeHeader.HEADER_SIZE + mainPayloadLength + startOffsetPayloadLength);
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
                    envelope(EnvelopeType.FULL, FIRST_INDEX, expected, mainPayloadEnvelopeChecksum));
        }
    }

    @Test
    void shouldNotIncrementIndexWhenWritingOffset() throws IOException {
        final var buffer = buffer(SEGMENT_SIZE);
        final var fileChannel = storeChannel();
        try (var channel = writeChannel(fileChannel, SEGMENT_SIZE, buffer)) {
            assertThat(channel.currentIndex()).isEqualTo(FIRST_INDEX - 1);

            var offset = HEADER_SIZE + 8;
            channel.insertStartOffset(offset);
            channel.prepareForFlush().flush();
            var byteBuffer = channelData(fileChannel, SEGMENT_SIZE);
            assertEnvelopeContents(byteBuffer, startOffset(offset - HEADER_SIZE));

            assertThat(channel.currentIndex()).isEqualTo(FIRST_INDEX - 1);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void writeStartOffsetFailsIfNotAtTheBeginningOfASegment(int segmentSize) throws IOException {
        final var fileChannel = storeChannel();
        final var buffer = buffer(segmentSize);
        try (var channel = writeChannel(fileChannel, segmentSize, buffer)) {
            assertThat(channel.position())
                    .as("should start writing after header and zeroed first segment")
                    .isEqualTo(segmentSize);
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION); // Version is for the channel, not for a specific envelope.

            // Add a regular envelope first:
            channel.putContentType(CONTENT_TYPE);
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
            assertThat(channel.position())
                    .as("should start writing after header and zeroed first segment")
                    .isEqualTo(segmentSize);
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION); // Version is for the channel, not for a specific envelope.

            channel.putContentType(CONTENT_TYPE);
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
            assertThat(channel.position())
                    .as("should start writing after header and zeroed first segment")
                    .isEqualTo(segmentSize);
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION); // Version is for the channel, not for a specific envelope.

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
            assertThat(channel.position())
                    .as("should start writing after header and zeroed first segment")
                    .isEqualTo(segmentSize);

            // Lower bounds
            assertThatThrownBy(() -> channel.insertStartOffset(-1))
                    .as("trying to use a negative size for offset will be rejected")
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(
                            EnvelopeWriteChannel.ERROR_MSG_TEMPLATE_OFFSET_SIZE_TOO_SMALL.formatted(-1, HEADER_SIZE));
            assertThatThrownBy(() -> channel.insertStartOffset(0))
                    .isInstanceOf(IllegalArgumentException.class)
                    .as("trying to use a zero size for offset will be rejected")
                    .hasMessage(
                            EnvelopeWriteChannel.ERROR_MSG_TEMPLATE_OFFSET_SIZE_TOO_SMALL.formatted(0, HEADER_SIZE));
            assertThatThrownBy(() -> channel.insertStartOffset(HEADER_SIZE - 1))
                    .as("trying to use less than or equal to the size of an envelope header will be rejected")
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(EnvelopeWriteChannel.ERROR_MSG_TEMPLATE_OFFSET_SIZE_TOO_SMALL.formatted(
                            HEADER_SIZE - 1, HEADER_SIZE));
            assertThatThrownBy(() -> channel.insertStartOffset(HEADER_SIZE))
                    .as("trying to use less than or equal to the size of an envelope header will be rejected")
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(EnvelopeWriteChannel.ERROR_MSG_TEMPLATE_OFFSET_SIZE_TOO_SMALL.formatted(
                            HEADER_SIZE, HEADER_SIZE));

            // Upper bounds
            assertThatThrownBy(() -> channel.insertStartOffset(segmentSize * 3 / 2))
                    .as("trying to use an offset size that is bigger than segment will be rejected")
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(EnvelopeWriteChannel.ERROR_MSG_TEMPLATE_OFFSET_SIZE_TOO_LARGE.formatted(
                            segmentSize * 3 / 2, segmentSize));
            assertThatThrownBy(() -> channel.insertStartOffset(segmentSize))
                    .as("trying to offset size the whole segment will be rejected")
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(EnvelopeWriteChannel.ERROR_MSG_TEMPLATE_OFFSET_SIZE_TOO_LARGE.formatted(
                            segmentSize, segmentSize));
            assertThatThrownBy(() -> channel.insertStartOffset(segmentSize - HEADER_SIZE))
                    .as("trying to offset without leaving enough space for another envelope will be rejected")
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage(EnvelopeWriteChannel.ERROR_MSG_TEMPLATE_OFFSET_SIZE_TOO_LARGE.formatted(
                            segmentSize - HEADER_SIZE, segmentSize));

            channel.prepareForFlush();
            assertThat(channel.position())
                    .as("nothing was written to the channel after failed start offset calls")
                    .isEqualTo(segmentSize);
        }
    }

    @Test
    void directPutAllBeginningOfSegment() throws IOException {
        int segmentSize = 128;
        final var byteData = bytes(random, segmentSize / 8);
        final var chunkSize = byteData.length + HEADER_SIZE;
        final var checksums = new int[] {0x9AA4E607, 0x84C1B4};

        // Create some real envelopes to write as a chunk to an EnvelopeWriteChannel
        var fileChannel = storeChannel(0);
        HeapScopedBuffer buffer1 = buffer(segmentSize);
        ByteBuffer inData;
        try (var channel = writeChannel(fileChannel, segmentSize, buffer1)) {
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
            channel.put(byteData, byteData.length);
            assertChecksum(channel.putChecksum(), checksums[0]);

            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putContentType(CONTENT_TYPE);
            channel.put(byteData, byteData.length);
            assertChecksum(channel.putChecksum(), checksums[1]);
            inData = slice(buffer1);
            inData.limit(buffer1.getBuffer().position());
        }

        fileChannel = storeChannel();
        final var buffer = buffer(segmentSize);
        try (var channel = writeChannel(fileChannel, segmentSize, buffer)) {
            ByteBuffer realBuffer = buffer.getBuffer();
            assertThat(realBuffer.position())
                    .as("buffer is positioned at beginning")
                    .isZero();

            // Asking for segment size offset - should be turned into offset into current envelope
            channel.directPutAll(inData, segmentSize);

            // Should have positioned directly after
            assertThat(realBuffer.position()).isEqualTo(chunkSize * 2);
            channel.prepareForFlush();

            assertEnvelopeContents(
                    channelData(fileChannel, segmentSize),
                    envelope(EnvelopeType.FULL, FIRST_INDEX, byteData, checksums[0]),
                    envelope(EnvelopeType.FULL, FIRST_INDEX + 1, byteData, checksums[1]));
        }
    }

    @Test
    void directPutAllOffsetIntoSegment() throws IOException {
        int segmentSize = 256;
        final var byteData = bytes(random, segmentSize / 8);
        final var chunkSize = byteData.length + HEADER_SIZE;
        final var checksums = new int[] {0x2FBA1821, 0xCB9CD5D9};

        // Create some real envelopes to write as a chunk to an EnvelopeWriteChannel
        var fileChannel = storeChannel(0);
        HeapScopedBuffer buffer1 = buffer(segmentSize);
        ByteBuffer inData;
        try (var channel = writeChannel(fileChannel, segmentSize, buffer1)) {
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
            channel.put(byteData, byteData.length);
            assertChecksum(channel.putChecksum(), checksums[0]);

            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putContentType(CONTENT_TYPE);
            channel.put(byteData, byteData.length);
            assertChecksum(channel.putChecksum(), checksums[1]);
            inData = slice(buffer1);
            inData.limit(buffer1.getBuffer().position());
        }

        fileChannel = storeChannel();
        final var buffer = buffer(segmentSize);
        try (var channel = writeChannel(fileChannel, segmentSize, buffer)) {
            ByteBuffer realBuffer = buffer.getBuffer();
            assertThat(realBuffer.position())
                    .as("buffer is positioned at beginning")
                    .isZero();

            // Asking a bit in and don't have data written up to that point
            channel.directPutAll(inData, 33);
            // Should have positioned directly after
            assertThat(realBuffer.position()).isEqualTo(33 + chunkSize * 2);
            channel.prepareForFlush();

            assertEnvelopeContents(
                    channelData(fileChannel, segmentSize),
                    startOffset(33 - HEADER_SIZE),
                    envelope(EnvelopeType.FULL, FIRST_INDEX, byteData, checksums[0]),
                    envelope(EnvelopeType.FULL, FIRST_INDEX + 1, byteData, checksums[1]));
        }
    }

    @Test
    void directPutAllNoOffset() throws IOException {
        int segmentSize = 128;
        final var byteData = bytes(random, segmentSize / 8);
        final var chunkSize = byteData.length + HEADER_SIZE;
        final var checksums = new int[] {0x9AA4E607, 0x84C1B4};

        // Create some real envelopes to write as a chunk to an EnvelopeWriteChannel
        var fileChannel = storeChannel(0);
        HeapScopedBuffer buffer1 = buffer(segmentSize);
        ByteBuffer inData;
        try (var channel = writeChannel(fileChannel, segmentSize, buffer1)) {
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
            channel.put(byteData, byteData.length);
            assertChecksum(channel.putChecksum(), checksums[0]);

            channel.beginChecksumForWriting();
            channel.putContentType(CONTENT_TYPE);
            channel.put(byteData, byteData.length);
            assertChecksum(channel.putChecksum(), checksums[1]);
            inData = slice(buffer1);
            inData.limit(buffer1.getBuffer().position() - HEADER_SIZE);
        }

        fileChannel = storeChannel();
        final var buffer = buffer(segmentSize);
        try (var channel = writeChannel(fileChannel, segmentSize, buffer)) {
            ByteBuffer realBuffer = buffer.getBuffer();
            assertThat(realBuffer.position())
                    .as("buffer is positioned at beginning")
                    .isZero();

            // No offset on the second one, it should then put its data directly after previously written data.
            channel.directPutAll(inData.limit(chunkSize), 0);
            channel.directPutAll(inData.position(chunkSize).limit(chunkSize * 2), -1);

            // Should have positioned directly after
            assertThat(realBuffer.position()).isEqualTo(chunkSize * 2);
            channel.prepareForFlush();

            assertEnvelopeContents(
                    channelData(fileChannel, segmentSize),
                    envelope(EnvelopeType.FULL, FIRST_INDEX, byteData, checksums[0]),
                    envelope(EnvelopeType.FULL, FIRST_INDEX + 1, byteData, checksums[1]));
        }
    }

    @Test
    void directPutAllOffsetIntoSegmentButDataUpToOffset() throws IOException {
        int segmentSize = 128;
        final var byteData = bytes(random, segmentSize / 8);
        final var chunkSize = byteData.length + HEADER_SIZE;
        final var checksums = new int[] {0x9AA4E607, 0x84C1B4};

        // Create some real envelopes to write as a chunk to an EnvelopeWriteChannel
        var fileChannel = storeChannel(0);
        HeapScopedBuffer buffer1 = buffer(segmentSize);
        ByteBuffer inData;
        try (var channel = writeChannel(fileChannel, segmentSize, buffer1)) {
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
            channel.put(byteData, byteData.length);
            assertChecksum(channel.putChecksum(), checksums[0]);

            channel.beginChecksumForWriting();
            channel.putContentType(CONTENT_TYPE);
            channel.put(byteData, byteData.length);
            assertChecksum(channel.putChecksum(), checksums[1]);
            inData = slice(buffer1);
            inData.limit(buffer1.getBuffer().position() - HEADER_SIZE);
        }

        fileChannel = storeChannel();
        final var buffer = buffer(segmentSize);
        try (var channel = writeChannel(fileChannel, segmentSize, buffer)) {
            ByteBuffer realBuffer = buffer.getBuffer();
            assertThat(realBuffer.position())
                    .as("buffer is positioned at beginning")
                    .isZero();

            // Offset on second write, but there is data before it, so it should not insert a startOffset envelope
            channel.directPutAll(inData.limit(chunkSize), 0);
            channel.directPutAll(inData.position(chunkSize).limit(chunkSize * 2), chunkSize);

            // Should have positioned directly after
            assertThat(realBuffer.position()).isEqualTo(chunkSize * 2);
            channel.prepareForFlush();

            assertEnvelopeContents(
                    channelData(fileChannel, segmentSize),
                    envelope(EnvelopeType.FULL, FIRST_INDEX, byteData, checksums[0]),
                    envelope(EnvelopeType.FULL, FIRST_INDEX + 1, byteData, checksums[1]));
        }
    }

    @Test
    void directPutAllOverBufferSize() throws IOException {
        int segmentSize = 128;
        final var byteData = bytes(random, 38);
        final var checksums = new int[] {0x90A3889B, 0x4AA44A38, 0x62D78F2C};

        // Create some real envelopes to write as a chunk to an EnvelopeWriteChannel - split over a segment boundary
        var fileChannel = storeChannel(0);
        HeapScopedBuffer buffer1 = buffer(384);
        ByteBuffer inData;
        try (var channel = writeChannel(fileChannel, segmentSize, buffer1)) {
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
            channel.put(byteData, byteData.length);
            assertChecksum(channel.putChecksum(), checksums[0]);

            // This one will split on segment boundary
            channel.beginChecksumForWriting();
            channel.putContentType(CONTENT_TYPE);
            channel.put(byteData, byteData.length);
            assertChecksum(channel.putChecksum(), checksums[2]);
            inData = slice(buffer1).position(segmentSize);
            inData.limit(buffer1.getBuffer().position());
        }

        fileChannel = storeChannel();
        // Buffer as small as the segment size to trigger flushing in directPutAll
        final var buffer = buffer(segmentSize);
        try (var channel = writeChannel(fileChannel, segmentSize, buffer)) {
            ByteBuffer realBuffer = buffer.getBuffer();
            assertThat(realBuffer.position())
                    .as("buffer is positioned at beginning")
                    .isZero();

            // Asking for segment size offset - should be turned into offset into current envelope
            channel.directPutAll(inData, segmentSize);

            // Should have positioned directly after (header and data in end envelope)
            assertThat(realBuffer.position()).isEqualTo(HEADER_SIZE + 10);
            channel.prepareForFlush();

            assertEnvelopeContents(
                    channelData(fileChannel, segmentSize),
                    envelope(EnvelopeType.FULL, FIRST_INDEX, byteData, checksums[0]),
                    envelope(EnvelopeType.BEGIN, FIRST_INDEX + 1, Arrays.copyOfRange(byteData, 0, 28), checksums[1]),
                    envelope(
                            EnvelopeType.END,
                            FIRST_INDEX + 1,
                            Arrays.copyOfRange(byteData, 28, byteData.length),
                            checksums[2]));
        }
    }

    @Test
    void twoConsecutiveDirectPutAllGetsCorrectPosition() throws IOException {
        int segmentSize = 128;
        final var byteData = bytes(random, 4);
        final var checksums = new int[] {0x80715BD2, 0xFF3B2A15};

        // Create some real envelopes to write as a chunk to an EnvelopeWriteChannel
        var fileChannel = storeChannel(0);
        HeapScopedBuffer buffer1 = buffer(384);
        ByteBuffer inData;
        ByteBuffer inDataCopy;
        try (var channel = writeChannel(fileChannel, segmentSize, buffer1)) {
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
            channel.put(byteData, byteData.length);
            assertChecksum(channel.putChecksum(), checksums[0]);

            channel.beginChecksumForWriting();
            channel.putContentType(CONTENT_TYPE);
            channel.put(byteData, byteData.length);
            assertChecksum(channel.putChecksum(), checksums[1]);
            inData = slice(buffer1).position(segmentSize);
            inData.limit(buffer1.getBuffer().position());
            inDataCopy = slice(buffer1).position(segmentSize);
            inDataCopy.limit(buffer1.getBuffer().position());
        }

        fileChannel = storeChannel();
        final var buffer = buffer(segmentSize);
        try (var channel = writeChannel(fileChannel, segmentSize, buffer)) {
            ByteBuffer realBuffer = buffer.getBuffer();
            assertThat(realBuffer.position())
                    .as("buffer is positioned at beginning")
                    .isZero();

            int dataLimit = inData.limit();
            int splitPoint = inData.position() + 50;
            channel.directPutAll(inData.limit(splitPoint), -1);
            // Should have positioned directly after
            assertThat(realBuffer.position()).isEqualTo(50);

            // Do another directPut and see that it manages to put the bytes directly after the first put
            channel.directPutAll(inData.limit(dataLimit).position(splitPoint), -1);
            assertThat(realBuffer.position()).isEqualTo(dataLimit % segmentSize);

            channel.prepareForFlush();

            assertEnvelopeContents(
                    channelData(fileChannel, segmentSize),
                    envelope(EnvelopeType.FULL, FIRST_INDEX, byteData, checksums[0]),
                    envelope(EnvelopeType.FULL, FIRST_INDEX + 1, byteData, checksums[1]));
        }
    }

    @Test
    void directPutAllDoesNotRotateIfEndsAtFileMax() throws IOException {
        final var initialLogVersion = 1L;
        final var rotatedPath = logPath(initialLogVersion + 1);

        int segmentSize = 128;
        final var maxLogFileSize = segmentSize * 2;
        final var byteData = bytes(random, 33);
        final var checksums = new int[] {0x5DE2F72, 0x8F63B97A};

        // Create some real envelopes to write as a chunk to an EnvelopeWriteChannel
        var fileChannel = storeChannel(initialLogVersion);
        HeapScopedBuffer buffer1 = buffer(384);
        ByteBuffer inData;
        try (var channel = writeChannel(fileChannel, segmentSize, buffer1)) {
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
            channel.put(byteData, byteData.length);
            assertChecksum(channel.putChecksum(), checksums[0]);

            channel.beginChecksumForWriting();
            channel.putContentType(CONTENT_TYPE);
            channel.put(byteData, byteData.length);
            assertChecksum(channel.putChecksum(), checksums[1]);
            inData = slice(buffer1).position(segmentSize);
            inData.limit(buffer1.getBuffer().position());
        }

        fileChannel = storeChannel();
        final var buffer = buffer(segmentSize);
        try (var channel = writeChannel(
                fileChannel,
                segmentSize,
                buffer,
                logRotation(fileChannel, header(segmentSize), maxLogFileSize),
                LogTracers.NULL)) {
            ByteBuffer realBuffer = buffer.getBuffer();
            assertThat(realBuffer.position())
                    .as("buffer is positioned at beginning")
                    .isZero();

            // Asking for segment size offset - should be turned into offset into current envelope
            channel.directPutAll(inData, segmentSize);

            // Should have positioned directly after (header and data in end envelope)
            assertThat(realBuffer.position()).isEqualTo(segmentSize);
            channel.prepareForFlush();

            assertThat(fileChannel.position())
                    .as("should have ignored the rotation limit at end of directPutAll")
                    .isEqualTo(maxLogFileSize);
            assertThat(fileSystem.fileExists(rotatedPath))
                    .as("should not have created the new log file")
                    .isFalse();

            assertEnvelopeContents(
                    channelData(fileChannel, segmentSize),
                    envelope(EnvelopeType.FULL, FIRST_INDEX, byteData, checksums[0]),
                    envelope(EnvelopeType.FULL, FIRST_INDEX + 1, byteData, checksums[1]));
        }
    }

    @Test
    void directPutAllDoesNotRotateInMiddle() throws IOException {
        final var initialLogVersion = 1L;
        final var rotatedPath = logPath(initialLogVersion + 1);

        int segmentSize = 128;
        final var maxLogFileSize = segmentSize * 2;
        final var byteData = bytes(random, 38);
        final var checksums = new int[] {0x90A3889B, 0x4AA44A38, 0x62D78F2C};

        // Create some real envelopes to write as a chunk to an EnvelopeWriteChannel - split over a segment boundary
        var fileChannel = storeChannel(initialLogVersion);
        HeapScopedBuffer buffer1 = buffer(384);
        ByteBuffer inData;
        try (var channel = writeChannel(fileChannel, segmentSize, buffer1)) {
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
            channel.put(byteData, byteData.length);
            assertChecksum(channel.putChecksum(), checksums[0]);

            // This one will split on segment boundary
            channel.beginChecksumForWriting();
            channel.putContentType(CONTENT_TYPE);
            channel.put(byteData, byteData.length);
            assertChecksum(channel.putChecksum(), checksums[2]);
            inData = slice(buffer1).position(segmentSize);
            inData.limit(buffer1.getBuffer().position());
        }

        fileChannel = storeChannel();
        // Buffer as small as the segment size to trigger flushing in directPutAll
        final var buffer = buffer(segmentSize);
        try (var channel = writeChannel(
                fileChannel,
                segmentSize,
                buffer,
                logRotation(fileChannel, header(segmentSize), maxLogFileSize),
                LogTracers.NULL)) {
            ByteBuffer realBuffer = buffer.getBuffer();
            assertThat(realBuffer.position())
                    .as("buffer is positioned at beginning")
                    .isZero();

            // Asking for segment size offset - should be turned into offset into current envelope
            channel.directPutAll(inData, segmentSize);

            // Should have positioned directly after (header and data in end envelope)
            assertThat(realBuffer.position()).isEqualTo(HEADER_SIZE + 10);
            channel.prepareForFlush();

            assertThat(fileChannel.position())
                    .as("should have ignored the rotation limit in directPutAll")
                    .isGreaterThan(maxLogFileSize);
            assertThat(fileSystem.fileExists(rotatedPath))
                    .as("should not have created the new log file")
                    .isFalse();

            assertEnvelopeContents(
                    channelData(fileChannel, segmentSize),
                    envelope(EnvelopeType.FULL, FIRST_INDEX, byteData, checksums[0]),
                    envelope(EnvelopeType.BEGIN, FIRST_INDEX + 1, Arrays.copyOfRange(byteData, 0, 28), checksums[1]),
                    envelope(
                            EnvelopeType.END,
                            FIRST_INDEX + 1,
                            Arrays.copyOfRange(byteData, 28, byteData.length),
                            checksums[2]));
        }
    }

    @Test
    void termMustBeIncreasing() throws IOException {
        int segmentSize = 64;
        final var data = (byte) 14;

        var fileChannel = storeChannel(0);
        try (var channel = writeChannel(fileChannel, segmentSize, buffer())) {
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putContentType(CONTENT_TYPE);
            channel.putTerm(10);
            channel.put(data);
            channel.endCurrentEntry();

            assertThatThrownBy(() -> channel.putTerm(9))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(
                            "Failed to write entry to replication log, the entry's term was less than the "
                                    + "last appended term, terms must be monotonically increasing.");
        }
    }

    @Test
    void writeDataThatHasZeroChecksums() throws IOException {
        final var segmentSize = 256;
        final var term = 24L;

        final var fileChannel = storeChannel();
        final var buffer = buffer(segmentSize);
        byte[] payload1 = new byte[32];
        ByteBuffer.wrap(payload1)
                .order(BIG_ENDIAN)
                .position(payload1.length - Integer.BYTES)
                .putInt(0x73c30ece);
        byte[] payload2 = new byte[17];
        ByteBuffer.wrap(payload2)
                .order(BIG_ENDIAN)
                .position(payload2.length - Integer.BYTES)
                .putInt(0xd4a2e6e3);
        byte[] payload3 = new byte[364];
        ByteBuffer.wrap(payload3)
                .order(BIG_ENDIAN)
                .position(110)
                .putInt(0xefa6f5e1)
                .position(335)
                .putInt(0xc48f50e7)
                .position(360)
                .putInt(0x3bb12318);
        try (var channel = writeChannel(fileChannel, segmentSize, buffer)) {
            // write a simple FULL envelope which leads to zero checksum
            channel.beginChecksumForWriting();
            channel.putVersion(KernelVersion.V5_25.version());
            channel.putTerm(term);
            channel.putContentType(KERNEL_CONTENT_TYPE);
            channel.put(payload1, payload1.length);
            assertThat(channel.putChecksum()).isZero();

            // write a second simple FULL envelope which leads to zero checksum
            channel.beginChecksumForWriting();
            channel.putVersion(KernelVersion.V5_25.version());
            channel.putTerm(term);
            channel.putContentType(KERNEL_CONTENT_TYPE);
            channel.put(payload2, payload2.length);
            assertThat(channel.putChecksum()).isZero();

            // Write a multi-segment payload that will have zero checksum for each constituent envelope
            channel.beginChecksumForWriting();
            channel.putVersion(KernelVersion.V5_25.version());
            channel.putTerm(term);
            channel.putContentType(KERNEL_CONTENT_TYPE);
            channel.put(payload3, payload3.length);
            assertThat(channel.putChecksum()).isZero();
        }

        try (var readChannel = new EnvelopeReadChannel(
                storeChannel(), segmentSize, LogVersionBridge.NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // Validate first envelope
            var readBack1 = ByteBuffer.allocate(payload1.length);
            readChannel.beginChecksum();
            readChannel.read(readBack1);
            assertThat(readChannel.endChecksumAndValidate()).isZero();
            assertThat(readBack1.array()).isEqualTo(payload1);
            // Validate second envelope
            var readBack2 = ByteBuffer.allocate(payload2.length);
            readChannel.beginChecksum();
            readChannel.read(readBack2);
            assertThat(readChannel.endChecksumAndValidate()).isZero();
            assertThat(readBack2.array()).isEqualTo(payload2);
            // Validate third multi-segment payload
            var readBack3 = ByteBuffer.allocate(payload3.length);
            readChannel.beginChecksum();
            readChannel.read(readBack3);
            assertThat(readChannel.endChecksumAndValidate()).isZero();
            assertThat(readBack3.array()).isEqualTo(payload3);
        }
    }

    @ParameterizedTest
    @CsvSource({
        "256, 225, 20, 10", // entries each fill one segment
        "256, 20, 20, 5", // 1 byte short of first full segment after first part
        "256, 20, 20, 10", // 1 byte short of second full segment after first part
        "256, 194, 20, 1", // Max 31 bytes padding after each entry
        "256, 193, 20, 1", // 32 bytes space after first entry, so won't pad
        "256, 97, 20, 5", // Ends half into segment after first part, so won't pad
        "256, 1000, 20, 7", // Entries span multiple segments, then need padding
        "1024, 1, 20, 10", // Restart still within one segment, no padding
    })
    void directPutAllCanAppendAfterEntryLatePadded(int segmentSize, int dataSize, int entryCount, int splitIndex)
            throws IOException {
        // write first set of entries into the source log file
        final var part1State = writeLogFileEntries(
                segmentSize, splitIndex, dataSize, new LogContinuityInfo(segmentSize, BASE_TX_ID, BASE_TX_CHECKSUM));
        // setup enveloped write channel to push raw binary data into
        try (var copyWriter = writeChannel(storeChannel(2L), segmentSize, buffer(segmentSize))) {
            directCopyLogData(segmentSize, BASE_TX_ID, copyWriter);
        }
        assertThat(directory.fileContent(logPath(1)))
                .as("files should be binary identical after copy of first section")
                .isEqualTo(directory.fileContent(logPath(2)));

        // write remaining entries to source file
        writeLogFileEntries(segmentSize, entryCount - splitIndex, dataSize, part1State);

        // re-attach writer to append further catch up data
        try (var copyWriter = writeChannel(
                storeChannel(2L),
                segmentSize,
                part1State.lastChecksum(),
                buffer(segmentSize),
                NO_ROTATION,
                LogTracers.NULL,
                part1State.offset(),
                part1State.lastAppendIndex())) {
            // append the new range of entries onto
            directCopyLogData(segmentSize, part1State.lastAppendIndex() + 1L, copyWriter);
        }
        assertThat(directory.fileContent(logPath(1)))
                .as("files should still be binary identical after second catchup")
                .isEqualTo(directory.fileContent(logPath(2)));
    }

    @ParameterizedTest
    @CsvSource({
        "256, 225, 20, 10, 15", // no START_OFFSET needed, no padding
        "256, 20, 20, 5, 15", // START_OFFSET, then 1 byte pad
        "256, 20, 20, 8 , 10", // START_OFFSET, then 1 byte pad
        "256, 27, 20, 8, 12", // START_OFFSET, then 24 byte pad
        "256, 193, 20, 8, 15", // START_OFFSET, then no padding
        "256, 1024, 20, 11, 15", // Multi-segment data, START_OFFSET, then 16 byte pad
        "1024, 1, 20, 10, 15", // START_OFFSET, then no pad and continued in same segment
    })
    void directPutAllWithStartOffsetChecksumMatches(
            int segmentSize, int dataSize, int entryCount, int startIndex, int midIndex) throws IOException {
        // write full set of entries into the source log file
        final var initialState = writeLogFileEntries(
                segmentSize, midIndex, dataSize, new LogContinuityInfo(segmentSize, BASE_TX_ID, BASE_TX_CHECKSUM));

        // start copy from point within range
        try (var copyWriter = writeChannel(storeChannel(2L), segmentSize, buffer(segmentSize))) {
            directCopyLogData(segmentSize, startIndex + BASE_TX_ID, copyWriter);
        }
        int midChecksum = getEndChecksum(segmentSize);
        assertThat(midChecksum)
                .as("original and copy should have same checksum despite START_OFFSET")
                .isEqualTo(initialState.lastChecksum());

        // write remaining entries to source file
        final var finalState = writeLogFileEntries(segmentSize, entryCount - midIndex, dataSize, initialState);

        // re-attach writer to append further catch up data
        try (var copyWriter = writeChannel(
                storeChannel(2L),
                segmentSize,
                finalState.lastChecksum(),
                buffer(segmentSize),
                NO_ROTATION,
                LogTracers.NULL,
                (int) fileSystem.getFileSize(logPath(2L)),
                finalState.lastAppendIndex())) {
            // append the new range of entries onto
            directCopyLogData(segmentSize, initialState.lastAppendIndex() + 1L, copyWriter);
        }
        int finalChecksum = getEndChecksum(segmentSize);
        assertThat(finalChecksum)
                .as("original and copy should still have same checksum despite START_OFFSET and restarted append")
                .isEqualTo(finalState.lastChecksum());
    }

    @Test
    void directPutAllThrowsIfOffsetAlignedWhenCurrentOffsetBeforePadZone() throws IOException {
        // setup situation
        final var segmentSize = 256;
        final var dataSize = 20;
        final var splitIndex = 8;
        // write set of entries into the source log file that ends before padding range
        final var part1State = writeLogFileEntries(
                segmentSize, splitIndex, dataSize, new LogContinuityInfo(segmentSize, BASE_TX_ID, BASE_TX_CHECKSUM));
        assertThat(part1State.offset() % segmentSize)
                .as("After setup, end offset of file should be in early part of segment, but not zero")
                .isNotZero()
                .isLessThan(segmentSize - HEADER_SIZE);
        // setup enveloped write channel to push raw binary data into
        try (var copyWriter = writeChannel(storeChannel(2L), segmentSize, buffer(segmentSize))) {
            directCopyLogData(segmentSize, BASE_TX_ID, copyWriter);
        }

        // re-attach writer to append further catch up data
        try (var copyWriter = writeChannel(
                storeChannel(2L),
                segmentSize,
                part1State.lastChecksum(),
                buffer(segmentSize),
                NO_ROTATION,
                LogTracers.NULL,
                part1State.offset(),
                part1State.lastAppendIndex())) {
            var dummyBuf = ByteBuffer.allocate(10);
            assertThatThrownBy(() -> copyWriter.directPutAll(dummyBuf, 5L * segmentSize))
                    .as("Appending with segment aligned offset should throw if not aligned, or needing late padding")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(EnvelopeWriteChannel.ERROR_MSG_TEMPLATE_OFFSET_NOT_CONSISTENT);
        }
    }

    @Test
    void directPutAllDoesntThrowOnAlignedOffset() throws IOException {
        // setup situation
        final var segmentSize = 256;
        final var dataSize = (segmentSize / 2) - HEADER_SIZE; // two entries per segment
        final var splitIndex = 8; // even number to get alignment we want
        // write set of entries into the source log file that ends aligned
        final var part1State = writeLogFileEntries(
                segmentSize, splitIndex, dataSize, new LogContinuityInfo(segmentSize, BASE_TX_ID, BASE_TX_CHECKSUM));
        assertThat(part1State.offset() % segmentSize)
                .as("After setup, end offset of file should be on a segment boundary")
                .isZero();
        // setup enveloped write channel to push raw binary data into
        try (var copyWriter = writeChannel(storeChannel(2L), segmentSize, buffer(segmentSize))) {
            directCopyLogData(segmentSize, BASE_TX_ID, copyWriter);
        }

        // re-attach writer to append further catch up data
        try (var copyWriter = writeChannel(
                storeChannel(2L),
                segmentSize,
                part1State.lastChecksum(),
                buffer(segmentSize),
                NO_ROTATION,
                LogTracers.NULL,
                part1State.offset(),
                part1State.lastAppendIndex())) {
            var dummyBuf = ByteBuffer.allocate(10);
            var beforeOffset = copyWriter.position();
            assertThatCode(() -> copyWriter.directPutAll(dummyBuf, 5L * segmentSize))
                    .as("Appending with segment aligned offset and destination channel should not throw")
                    .doesNotThrowAnyException();
            assertThat(copyWriter.position() - beforeOffset)
                    .as("Only data with no padding should be added to copy channel")
                    .isEqualTo(dummyBuf.limit());
        }
    }

    @Test
    void directPutAllDoesntThrowOnPadSituation() throws IOException {
        // setup situation
        final var segmentSize = 256;
        final var dataSize = segmentSize - HEADER_SIZE - 15; // always requires pad at segment end
        final var splitIndex = 8;
        // write set of entries into the source log file that ends with padding at next write
        final var part1State = writeLogFileEntries(
                segmentSize, splitIndex, dataSize, new LogContinuityInfo(segmentSize, BASE_TX_ID, BASE_TX_CHECKSUM));
        assertThat(part1State.offset() % segmentSize)
                .isNotZero()
                .as("After setup, end offset should require padding to segment boundary")
                .isGreaterThanOrEqualTo(segmentSize - HEADER_SIZE);
        // setup enveloped write channel to push raw binary data into
        try (var copyWriter = writeChannel(storeChannel(2L), segmentSize, buffer(segmentSize))) {
            directCopyLogData(segmentSize, BASE_TX_ID, copyWriter);
        }

        // re-attach writer to append further catch up data
        try (var copyWriter = writeChannel(
                storeChannel(2L),
                segmentSize,
                part1State.lastChecksum(),
                buffer(segmentSize),
                NO_ROTATION,
                LogTracers.NULL,
                part1State.offset(),
                part1State.lastAppendIndex())) {
            var dummyBuf = ByteBuffer.allocate(10);
            var beforeOffset = copyWriter.position();
            // Appending with segment aligned offset when we expect padding should not throw
            assertThatCode(() -> copyWriter.directPutAll(dummyBuf, 5L * segmentSize))
                    .doesNotThrowAnyException();
            assertThat(copyWriter.position() - beforeOffset)
                    .as("Data appended to copy channel should also include extra padding")
                    .isEqualTo(dummyBuf.limit() + (segmentSize - (part1State.offset() % segmentSize)));
        }
    }

    @Test
    void prepareWriteWillPositionForWriting() throws IOException {
        int segmentSize = 128;
        int padding = 2;
        int maxLogFileSize = segmentSize * 3;
        int payloadSize = segmentSize - HEADER_SIZE - padding;
        byte[] byteData = bytes(random, payloadSize);

        long initialLogVersion = 0L;

        int[] checksums = new int[] {0x79978646, 0x93333558};

        final var fileChannel = storeChannel(initialLogVersion);

        Path rotatedPath = logPath(initialLogVersion + 1);

        final var buffer = buffer(segmentSize * 3);
        try (var channel = writeChannel(
                fileChannel,
                segmentSize,
                buffer,
                logRotation(fileChannel, header(segmentSize), maxLogFileSize),
                LogTracers.NULL)) {
            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
            channel.put(byteData, byteData.length);
            channel.endCurrentEntry();
            channel.prepareForFlush();

            assertThat(channel.position()).isEqualTo(segmentSize + segmentSize - padding);
            assertThat(fileSystem.fileExists(rotatedPath))
                    .as("should not have created the second log file yet")
                    .isFalse();

            // After this prepareWrite the padding should have been added and we should be at the next segment boundary
            channel.prepareWrite();
            assertThat(channel.position()).isEqualTo(segmentSize + segmentSize);

            channel.beginChecksumForWriting();
            channel.putVersion(KERNEL_VERSION);
            channel.putTerm(TERM);
            channel.putContentType(CONTENT_TYPE);
            channel.put(byteData, byteData.length);
            channel.endCurrentEntry();
            channel.prepareForFlush();

            assertThat(channel.position()).isEqualTo(segmentSize + segmentSize + segmentSize - padding);
            assertThat(fileSystem.fileExists(rotatedPath))
                    .as("should not have created the second log file yet")
                    .isFalse();

            // After this prepareWrite the padding should have been added and file rotated
            channel.prepareWrite();
            assertThat(fileSystem.fileExists(rotatedPath))
                    .as("should have created the second log file")
                    .isTrue();
            assertThat(channel.position()).isEqualTo(segmentSize);

            assertEnvelopeContents(
                    channelData(fileChannel, segmentSize),
                    envelope(EnvelopeType.FULL, FIRST_INDEX, byteData, checksums[0]),
                    envelope(EnvelopeType.ZERO, 0, new byte[padding], 0),
                    envelope(EnvelopeType.FULL, FIRST_INDEX + 1, byteData, checksums[1]));
        }
    }
}
