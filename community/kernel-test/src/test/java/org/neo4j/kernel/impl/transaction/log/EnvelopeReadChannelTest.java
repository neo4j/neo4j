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

import static java.lang.Math.toIntExact;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Arrays.copyOfRange;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.neo4j.io.ByteUnit.KibiByte;
import static org.neo4j.io.fs.ChecksumWriter.CHECKSUM_FACTORY;
import static org.neo4j.kernel.impl.transaction.log.EnvelopeWriteChannel.START_INDEX;
import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.IGNORE_KERNEL_VERSION;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.MAX_ZERO_PADDING_SIZE;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.zip.Checksum;
import org.apache.commons.lang3.mutable.MutableInt;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.io.fs.ChecksumMismatchException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.kernel.impl.transaction.log.entry.InvalidLogEnvelopeReadException;
import org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.EnvelopeType;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.files.LogFileChannelNativeAccessor;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@ExtendWith(RandomExtension.class)
class EnvelopeReadChannelTest {
    private final Checksum checksum = CHECKSUM_FACTORY.get();

    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private TestDirectory directory;

    @Inject
    private RandomSupport random;

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldReadSingleEnvelopeWithinOneSegment(int segmentSize) throws Exception {
        // GIVEN
        final var byteValue = (byte) random.nextInt();
        final var shortValue = (short) random.nextInt();
        final var intValue = random.nextInt();
        final var longValue = random.nextLong();
        final var floatValue = random.nextFloat();
        final var doubleValue = random.nextDouble();
        final var bytesValue = bytes(random, 9);
        final var byteBufferValue = ByteBuffer.wrap(bytes(random, 9));

        final var payloadLength = Byte.BYTES
                + Short.BYTES
                + Integer.BYTES
                + Long.BYTES
                + Float.BYTES
                + Double.BYTES
                + bytesValue.length
                + byteBufferValue.remaining();

        final var payloadChecksum =
                buildChecksum(EnvelopeType.FULL, payloadLength, BASE_TX_CHECKSUM, (buffer) -> buffer.put(byteValue)
                        .putShort(shortValue)
                        .putInt(intValue)
                        .putLong(longValue)
                        .putFloat(floatValue)
                        .putDouble(doubleValue)
                        .put(bytesValue)
                        .put(byteBufferValue.position(0)));

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);
            writeLogEnvelopeHeader(
                    buffer, payloadChecksum, EnvelopeType.FULL, payloadLength, BASE_TX_CHECKSUM, START_INDEX);
            buffer.put(byteValue);
            buffer.putShort(shortValue);
            buffer.putInt(intValue);
            buffer.putLong(longValue);
            buffer.putFloat(floatValue);
            buffer.putDouble(doubleValue);
            buffer.put(bytesValue);
            buffer.put(byteBufferValue.position(0));
        });

        final var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            assertThat(byteValue).isEqualTo(channel.get());
            assertThat(shortValue).isEqualTo(channel.getShort());
            assertThat(intValue).isEqualTo(channel.getInt());
            assertThat(longValue).isEqualTo(channel.getLong());
            assertThat(floatValue).isEqualTo(channel.getFloat(), Offset.offset(0.1f));
            assertThat(doubleValue).isEqualTo(channel.getDouble(), Offset.offset(0.1d));

            final var bytes = new byte[bytesValue.length];
            channel.get(bytes, bytesValue.length);
            assertThat(bytesValue).isEqualTo(bytes);

            final var byteBuffer = ByteBuffer.wrap(new byte[bytesValue.length]);
            channel.read(byteBuffer);
            assertThat(byteBuffer.array()).isEqualTo(byteBufferValue.array());

            assertThat(payloadChecksum).isEqualTo(channel.getChecksum());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldReadMultipleEnvelopesWithinOneSegment(int segmentSize) throws Exception {
        // GIVEN
        final var size = (segmentSize / 4) - HEADER_SIZE;
        final var bytes1 = bytes(random, size);
        final var bytes2 = bytes(random, size);
        final var bytes3 = bytes(random, size);
        final var checksums = new int[3];

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);
            checksums[0] = writeHeaderAndPayload(buffer, EnvelopeType.FULL, BASE_TX_CHECKSUM, bytes1, START_INDEX);
            checksums[1] = writeHeaderAndPayload(buffer, EnvelopeType.FULL, checksums[0], bytes2, START_INDEX + 1);
            checksums[2] = writeHeaderAndPayload(buffer, EnvelopeType.FULL, checksums[1], bytes3, START_INDEX + 2);
        });

        final var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            final var bytesRead = new byte[size];
            channel.get(bytesRead, size);
            assertThat(bytes1).isEqualTo(bytesRead);
            assertThat(checksums[0]).isEqualTo(channel.getChecksum());
            assertThat(channel.entryIndex()).isEqualTo(START_INDEX);
            channel.get(bytesRead, size);
            assertThat(bytes2).isEqualTo(bytesRead);
            assertThat(checksums[1]).isEqualTo(channel.getChecksum());
            assertThat(channel.entryIndex()).isEqualTo(START_INDEX + 1);
            channel.get(bytesRead, size);
            assertThat(bytes3).isEqualTo(bytesRead);
            assertThat(checksums[2]).isEqualTo(channel.getChecksum());
            assertThat(channel.entryIndex()).isEqualTo(START_INDEX + 2);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldReadEnvelopeThatFillsSingleSegment(int segmentSize) throws Exception {
        // GIVEN
        final var bytes = bytes(random, segmentSize - HEADER_SIZE);

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);
            writeHeaderAndPayload(buffer, EnvelopeType.FULL, BASE_TX_CHECKSUM, bytes, START_INDEX);
        });

        final var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            final var bytesRead = new byte[bytes.length];
            channel.get(bytesRead, bytes.length);
            assertThat(bytes).isEqualTo(bytesRead);
            assertThat(channel.entryIndex()).isEqualTo(START_INDEX);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldReadDataThatSpansAcrossTwoSegment(int segmentSize) throws Exception {
        // GIVEN
        final var beginChunkSize = segmentSize - HEADER_SIZE;
        final var bytes = bytes(random, segmentSize);

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);

            final var checksum = writeHeaderAndPayload(
                    buffer, EnvelopeType.BEGIN, BASE_TX_CHECKSUM, copyOfRange(bytes, 0, beginChunkSize), START_INDEX);
            writeHeaderAndPayload(
                    buffer, EnvelopeType.END, checksum, copyOfRange(bytes, beginChunkSize, bytes.length), START_INDEX);
        });

        final var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            final var bytesRead = new byte[bytes.length];
            channel.get(bytesRead, bytes.length);
            assertThat(bytes).isEqualTo(bytesRead);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldReadDataInChunksThatSpansAcrossSingleSegment(int segmentSize) throws Exception {
        // GIVEN
        final var beginChunkSize = segmentSize - HEADER_SIZE;
        final var bytes = bytes(random, segmentSize);

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);

            final var checksum = writeHeaderAndPayload(
                    buffer, EnvelopeType.BEGIN, BASE_TX_CHECKSUM, copyOfRange(bytes, 0, beginChunkSize), START_INDEX);
            writeHeaderAndPayload(
                    buffer, EnvelopeType.END, checksum, copyOfRange(bytes, beginChunkSize, bytes.length), START_INDEX);
        });

        final var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            final var readChunks = 4;
            final var readChunk = segmentSize / readChunks;
            final var bytesRead = new byte[readChunk];
            for (var i = 0; i < segmentSize; i += readChunk) {
                channel.get(bytesRead, readChunk);
                assertThat(copyOfRange(bytes, i, i + readChunk)).isEqualTo(bytesRead);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldReadEnvelopesAcrossSegmentsWithZeroPaddingPresent(int segmentSize) throws Exception {
        // GIVEN
        final var zeros = random.nextInt(HEADER_SIZE / 4, HEADER_SIZE / 2);
        final var chunkSize1 = segmentSize - HEADER_SIZE - zeros;
        final var chunkSize2 = HEADER_SIZE * 2;
        final var bytes1 = bytes(random, chunkSize1);
        final var bytes2 = bytes(random, chunkSize2);

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);

            final var checksum =
                    writeHeaderAndPayload(buffer, EnvelopeType.FULL, BASE_TX_CHECKSUM, bytes1, START_INDEX);
            buffer.put(new byte[zeros]);
            writeHeaderAndPayload(buffer, EnvelopeType.FULL, checksum, bytes2, START_INDEX);
        });

        final var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            var bytesRead = new byte[bytes1.length];
            channel.get(bytesRead, bytes1.length);
            assertThat(bytes1).isEqualTo(bytesRead);

            bytesRead = new byte[bytes2.length];
            channel.get(bytesRead, bytes2.length);
            assertThat(bytes2).isEqualTo(bytesRead);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldReadEnvelopesAcrossSegmentsWhenEnvelopeForcesZeroPadding(int segmentSize) throws Exception {
        // GIVEN
        final var zeros = new byte[Integer.BYTES];
        final var bytes1 = bytes(random, segmentSize - (HEADER_SIZE * 2) - Long.BYTES - zeros.length);
        final var l1 = random.nextLong();
        final var l2 = random.nextLong();
        final var l3 = random.nextLong();

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);

            final var checksum =
                    writeHeaderAndPayload(buffer, EnvelopeType.FULL, BASE_TX_CHECKSUM, bytes1, START_INDEX);

            final var beginChecksum =
                    buildChecksum(EnvelopeType.BEGIN, Long.BYTES, checksum, (crcBuffer) -> crcBuffer.putLong(l1));
            writeLogEnvelopeHeader(buffer, beginChecksum, EnvelopeType.BEGIN, Long.BYTES, checksum, START_INDEX);
            buffer.putLong(l1);
            buffer.put(zeros);

            final var endChecksum =
                    buildChecksum(EnvelopeType.END, Long.BYTES * 2, beginChecksum, (crcBuffer) -> crcBuffer
                            .putLong(l2)
                            .putLong(l3));
            writeLogEnvelopeHeader(buffer, endChecksum, EnvelopeType.END, Long.BYTES * 2, beginChecksum, START_INDEX);
            buffer.putLong(l2).putLong(l3);
        });

        final var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            var bytesRead = new byte[bytes1.length];
            channel.get(bytesRead, bytes1.length);
            assertThat(bytes1).isEqualTo(bytesRead);

            assertThat(channel.getLong()).isEqualTo(l1);
            assertThat(channel.getLong()).isEqualTo(l2);
            assertThat(channel.getLong()).isEqualTo(l3);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldReadEnvelopesWithMaximumZeroPadding(int segmentSize) throws Exception {
        // GIVEN
        final var zeros = new byte[MAX_ZERO_PADDING_SIZE - 1];
        final var bytes1 = bytes(random, segmentSize - HEADER_SIZE - zeros.length);
        final var l1 = random.nextLong();

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);

            final var checksum =
                    writeHeaderAndPayload(buffer, EnvelopeType.FULL, BASE_TX_CHECKSUM, bytes1, START_INDEX);
            buffer.put(zeros);

            final var fullChecksum =
                    buildChecksum(EnvelopeType.FULL, Long.BYTES, checksum, (crcBuffer) -> crcBuffer.putLong(l1));
            writeLogEnvelopeHeader(buffer, fullChecksum, EnvelopeType.FULL, Long.BYTES, checksum, START_INDEX);
            buffer.putLong(l1);
        });

        final var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            var bytesRead = new byte[bytes1.length];
            channel.get(bytesRead, bytes1.length);
            assertThat(bytes1).isEqualTo(bytesRead);

            assertThat(channel.getLong()).isEqualTo(l1);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldEnforceZeroEnvelopesBiggerThanMaxPaddingContainsOnlyZeroes(int segmentSize) throws Exception {
        // GIVEN
        final var zerosWithGibberish = new byte[segmentSize];
        final var gibberishPosition = random.nextInt(segmentSize / 4, 3 * segmentSize / 4);
        zerosWithGibberish[gibberishPosition] = 42;

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);
            buffer.put(zerosWithGibberish);
        });

        final var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            assertThatThrownBy(channel::get)
                    .isInstanceOf(InvalidLogEnvelopeReadException.class)
                    .hasMessageContaining("Expecting only zeros at this point");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void readingPreAllocatedFile(int segmentSize) throws Exception {
        final var zeros = new byte[segmentSize * 3];
        writeSomeData(buffer -> buffer.put(zeros));

        final var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            assertThatThrownBy(channel::get).isInstanceOf(ReadPastEndException.class);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldFailWhenReadingAnEmptyFile(int segmentSize) throws IOException {
        final var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            assertThatThrownBy(channel::get).isInstanceOf(ReadPastEndException.class);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldFailWhenEndOfSegmentAfterAnEnvelopeIsNotZeros(int segmentSize) throws Exception {
        // GIVEN
        final var bytes = bytes(random, segmentSize - (HEADER_SIZE * 2));
        byte garbage = (byte) random.nextInt(1, 127);

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);
            writeHeaderAndPayload(buffer, EnvelopeType.FULL, BASE_TX_CHECKSUM, bytes, START_INDEX);
            buffer.put(garbage); // Should be zero padding here
        });

        final var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            final var bytesRead = new byte[bytes.length];
            channel.get(bytesRead, bytes.length);

            assertThatThrownBy(channel::get)
                    .isInstanceOf(InvalidLogEnvelopeReadException.class)
                    .hasMessageContainingAll(
                            "end of buffer", "Expecting only zeros at this point", "[" + garbage + "]");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldFailWhenInvalidPayloadLengthIsSpecified(int segmentSize) throws IOException {
        final var bytes = bytes(random, segmentSize / 2);
        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);

            final var payloadChecksum = buildChecksum(
                    checksum, EnvelopeType.FULL, BASE_TX_CHECKSUM, LATEST_KERNEL_VERSION.version(), bytes, START_INDEX);
            writeLogEnvelopeHeader(
                    buffer, payloadChecksum, EnvelopeType.FULL, segmentSize, BASE_TX_CHECKSUM, START_INDEX);
            buffer.put(bytes);
        });

        final var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            assertThatThrownBy(channel::get)
                    .isInstanceOf(InvalidLogEnvelopeReadException.class)
                    .hasMessageContaining("Envelope span segment boundary");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldFailWhenEnvelopeNotCompleted(int segmentSize) throws IOException {
        final var beginSize = segmentSize / 2;
        final var bytes = bytes(random, beginSize);
        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);
            writeHeaderAndPayload(buffer, EnvelopeType.BEGIN, BASE_TX_CHECKSUM, bytes, START_INDEX);
        });

        final var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            final var bytesRead = new byte[beginSize];
            channel.get(bytesRead, beginSize);
            assertThat(bytes).isEqualTo(bytesRead);

            assertThatThrownBy(channel::get)
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining(
                            "Log file with version 0 ended with an incomplete record type(BEGIN) and no following log file could be found.");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldFailWhenStartOfNewSegmentIsInvalid(int segmentSize) throws Exception {
        // GIVEN
        final var bytes = bytes(random, segmentSize - HEADER_SIZE);

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);
            writeHeaderAndPayload(buffer, EnvelopeType.FULL, BASE_TX_CHECKSUM, bytes, START_INDEX);
            buffer.put((byte) 13);
        });

        final var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            final var bytesRead = new byte[bytes.length];
            channel.get(bytesRead, bytes.length);

            assertThatThrownBy(channel::get)
                    .isInstanceOf(InvalidLogEnvelopeReadException.class)
                    .hasMessageContainingAll("start of buffer", "expecting a valid header", "[13]");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldReadDataThatSpansAcrossLogFiles(int segmentSize) throws Exception {
        // GIVEN
        final var path1 = file(0);
        final var path2 = file(1);

        final var segment1Size = segmentSize - HEADER_SIZE;
        final var segment2Size = 42;
        final var bytes0 = bytes(random, segment1Size);
        final var bytes1 = bytes(random, segment1Size);
        final var bytes2 = bytes(random, segment2Size);

        final var endChecksum = new MutableInt();
        writeSomeData(path1, buffer -> {
            writeZeroSegment(buffer, segmentSize);

            var checksum = writeHeaderAndPayload(buffer, EnvelopeType.BEGIN, BASE_TX_CHECKSUM, bytes0, START_INDEX);
            checksum = writeHeaderAndPayload(buffer, EnvelopeType.MIDDLE, checksum, bytes1, START_INDEX);
            endChecksum.setValue(checksum);
        });
        writeSomeData(path2, buffer -> {
            writeZeroSegment(buffer, segmentSize, endChecksum.intValue());
            writeHeaderAndPayload(buffer, EnvelopeType.END, endChecksum.intValue(), bytes2, START_INDEX);
        });

        final var logChannel = logChannel(path1);
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, new TwoFileLogVersionBridge(path2), EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            final var bytesRead = new byte[segment1Size + segment1Size + segment2Size];
            channel.get(bytesRead, bytesRead.length);
            assertThat(bytes0).isEqualTo(copyOfRange(bytesRead, 0, segment1Size));
            assertThat(bytes1).isEqualTo(copyOfRange(bytesRead, segment1Size, (segment1Size * 2)));
            assertThat(bytes2).isEqualTo(copyOfRange(bytesRead, (segment1Size * 2), bytesRead.length));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldReadByteBufferThatSpansAcrossLogFiles(int segmentSize) throws Exception {
        // GIVEN
        final var path1 = file(0);
        final var path2 = file(1);

        final var segment1Size = segmentSize - HEADER_SIZE;
        final var segment2Size = 42;
        final var bytes0 = bytes(random, segment1Size);
        final var bytes1 = bytes(random, segment1Size);
        final var bytes2 = bytes(random, segment2Size);

        final var endChecksum = new MutableInt();
        writeSomeData(path1, buffer -> {
            writeZeroSegment(buffer, segmentSize);

            var checksum = writeHeaderAndPayload(buffer, EnvelopeType.BEGIN, BASE_TX_CHECKSUM, bytes0, START_INDEX);
            checksum = writeHeaderAndPayload(buffer, EnvelopeType.MIDDLE, checksum, bytes1, START_INDEX);
            endChecksum.setValue(checksum);
        });
        writeSomeData(path2, buffer -> {
            writeZeroSegment(buffer, segmentSize, endChecksum.intValue());
            writeHeaderAndPayload(buffer, EnvelopeType.END, endChecksum.intValue(), bytes2, START_INDEX);
        });

        final var logChannel = logChannel(path1);
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, new TwoFileLogVersionBridge(path2), EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            int totalSize = segment1Size + segment1Size + segment2Size;
            final var bytesRead = ByteBuffer.wrap(new byte[totalSize]);
            channel.read(bytesRead);
            assertThat(bytes0).isEqualTo(copyOfRange(bytesRead.array(), 0, segment1Size));
            assertThat(bytes1).isEqualTo(copyOfRange(bytesRead.array(), segment1Size, (segment1Size * 2)));
            assertThat(bytes2).isEqualTo(copyOfRange(bytesRead.array(), (segment1Size * 2), totalSize));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldBeAbleToReadFileThatStartsWithNonBaseChecksum(int segmentSize) throws Exception {
        // GIVEN
        final var bytes = bytes(random, segmentSize / 2);

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize, 42);
            writeHeaderAndPayload(buffer, EnvelopeType.FULL, 666, bytes, START_INDEX);
        });

        final var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            assertThatThrownBy(() -> channel.get(bytes, bytes.length))
                    .isInstanceOf(ChecksumMismatchException.class)
                    .hasMessageContainingAll("checksum chain is broken", "42", "666");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldFailWhenPayloadChecksumIsInvalid(int segmentSize) throws IOException {
        // GIVEN
        final var bytes = bytes(random, segmentSize / 4);

        checksum.reset();
        checksum.update(bytes);
        final var payloadChecksum = (int) checksum.getValue();
        final var invalid = random.nextBoolean() ? payloadChecksum + 1 : payloadChecksum - 1;

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);
            writeHeaderAndPayload(
                    buffer,
                    EnvelopeType.FULL,
                    BASE_TX_CHECKSUM,
                    invalid,
                    LATEST_KERNEL_VERSION.version(),
                    bytes,
                    START_INDEX);
        });

        final var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            final var bytesRead = new byte[bytes.length];
            assertThatThrownBy(() -> channel.get(bytesRead, bytes.length))
                    .isInstanceOf(ChecksumMismatchException.class);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldReadDataThatSpansAcrossMultipleSegments(int segmentSize) throws Exception {
        // GIVEN
        final var totalSize = segmentSize * 2;
        final var chunkSize = segmentSize - HEADER_SIZE;
        final var bytes = bytes(random, totalSize);

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);

            var checksum = writeHeaderAndPayload(
                    buffer, EnvelopeType.BEGIN, BASE_TX_CHECKSUM, copyOfRange(bytes, 0, chunkSize), START_INDEX);
            checksum = writeHeaderAndPayload(
                    buffer, EnvelopeType.MIDDLE, checksum, copyOfRange(bytes, chunkSize, chunkSize * 2), START_INDEX);
            writeHeaderAndPayload(
                    buffer, EnvelopeType.END, checksum, copyOfRange(bytes, chunkSize * 2, bytes.length), START_INDEX);
        });

        final var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            final var bytesRead = new byte[totalSize];
            channel.get(bytesRead, totalSize);
            assertThat(bytes).isEqualTo(bytesRead);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldReadDataInChunksThatSpansAcrossMultipleSegments(int segmentSize) throws Exception {
        // GIVEN
        final var chunkCount = 3;
        final var chunkSize = segmentSize - HEADER_SIZE;
        final var bytes = bytes(random, chunkSize * 3);

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);

            var checksum = writeHeaderAndPayload(
                    buffer, EnvelopeType.BEGIN, BASE_TX_CHECKSUM, copyOfRange(bytes, 0, chunkSize), START_INDEX);
            checksum = writeHeaderAndPayload(
                    buffer, EnvelopeType.MIDDLE, checksum, copyOfRange(bytes, chunkSize, chunkSize * 2), START_INDEX);
            writeHeaderAndPayload(
                    buffer, EnvelopeType.END, checksum, copyOfRange(bytes, chunkSize * 2, chunkSize * 3), START_INDEX);
        });

        final var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            final var bytesRead = new byte[chunkSize];
            for (var i = 0; i < chunkCount; i += chunkSize) {
                channel.get(bytesRead, chunkSize);
                assertThat(copyOfRange(bytes, i, i + chunkSize))
                        .as("Should have read the chunk of data at offset %d", i)
                        .isEqualTo(bytesRead);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldSkipStartOffsetEnvelope(int segmentSize) throws Exception {
        // GIVEN
        final var startOffsetLength = random.nextInt(1, 64);

        final var longValue = random.nextLong();
        final var mainPayloadLength = Long.BYTES;
        final var mainPayloadChecksum = buildChecksum(
                EnvelopeType.FULL, mainPayloadLength, BASE_TX_CHECKSUM, (buffer) -> buffer.putLong(longValue));

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);
            writeStartOffsetEnvelope(buffer, startOffsetLength, false);
            writeLogEnvelopeHeader(
                    buffer, mainPayloadChecksum, EnvelopeType.FULL, mainPayloadLength, BASE_TX_CHECKSUM, START_INDEX);
            buffer.putLong(longValue);
        });

        final var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            assertThat(longValue).isEqualTo(channel.getLong());
            assertThat(mainPayloadChecksum).isEqualTo(channel.getChecksum());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldSkipStartOffsetEnvelopeWhenOverridingChannelPosition(int segmentSize) throws Exception {
        // GIVEN
        final var startOffsetLength = random.nextInt(1, 64);

        final var longValue = random.nextLong();
        final var mainPayloadLength = Long.BYTES;
        final var mainPayloadChecksum = buildChecksum(
                EnvelopeType.FULL, mainPayloadLength, BASE_TX_CHECKSUM, (buffer) -> buffer.putLong(longValue));

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);
            writeStartOffsetEnvelope(buffer, startOffsetLength, false);
            writeLogEnvelopeHeader(
                    buffer, mainPayloadChecksum, EnvelopeType.FULL, mainPayloadLength, BASE_TX_CHECKSUM, START_INDEX);
            buffer.putLong(longValue);
        });

        final var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            assertThat(longValue).isEqualTo(channel.getLong());
            assertThat(mainPayloadChecksum).isEqualTo(channel.getChecksum());

            // Now let's override channel position to the beginning of the segment again:
            channel.position(segmentSize);
            assertThat(longValue).isEqualTo(channel.getLong());
            assertThat(mainPayloadChecksum).isEqualTo(channel.getChecksum());

            // Now let's override channel position to the middle of the START_OFFSET envelope:
            channel.position(segmentSize + HEADER_SIZE / 2);
            assertThat(longValue).isEqualTo(channel.getLong());
            assertThat(mainPayloadChecksum).isEqualTo(channel.getChecksum());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldFailIfStartOffsetEnvelopePayloadHasNonZerosContent(int segmentSize) throws Exception {
        // GIVEN
        final var startOffsetLength = random.nextInt(1, 64);

        final var longValue = random.nextLong();
        final var mainPayloadLength = Long.BYTES;
        final var mainPayloadChecksum = buildChecksum(
                EnvelopeType.FULL, mainPayloadLength, BASE_TX_CHECKSUM, (buffer) -> buffer.putLong(longValue));

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);
            writeStartOffsetEnvelope(buffer, startOffsetLength, true);
            writeLogEnvelopeHeader(buffer, mainPayloadChecksum, EnvelopeType.FULL, mainPayloadLength, 0, START_INDEX);
            buffer.putLong(longValue);
        });

        final var logChannel = logChannel();
        assertThatThrownBy(() -> new EnvelopeReadChannel(
                        logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false))
                .isInstanceOf(InvalidLogEnvelopeReadException.class)
                .hasMessageContaining("Expecting only zeros at this point.");
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldFailIfStartOffsetIsInAnotherSegmentThatNotTheFirstOne(int segmentSize) throws Exception {
        // GIVEN
        // First segment content: FULL
        final var firstPayloadLength = segmentSize - HEADER_SIZE;
        final var firstPayloadBytesValue = bytes(random, firstPayloadLength);
        final var firstPayloadChecksum = buildChecksum(
                EnvelopeType.FULL,
                firstPayloadLength,
                BASE_TX_CHECKSUM,
                (buffer) -> buffer.put(firstPayloadBytesValue));

        // Second segment: OFFSET + FULL
        final var startOffsetLength = random.nextInt(1, 16);

        final var secondPayloadLongValue = random.nextLong();
        final var secondPayloadLength = Long.BYTES;
        final var secondPayloadChecksum = buildChecksum(
                EnvelopeType.FULL,
                secondPayloadLength,
                firstPayloadChecksum,
                (buffer) -> buffer.putLong(secondPayloadLongValue));

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);
            // Write first segment:
            writeLogEnvelopeHeader(
                    buffer, firstPayloadChecksum, EnvelopeType.FULL, firstPayloadLength, BASE_TX_CHECKSUM, START_INDEX);
            buffer.put(firstPayloadBytesValue);

            // Write second segment:
            // - First the offset:
            writeStartOffsetEnvelope(buffer, startOffsetLength, false);
            // - Then the second full:
            writeLogEnvelopeHeader(
                    buffer,
                    secondPayloadChecksum,
                    EnvelopeType.FULL,
                    secondPayloadLength,
                    firstPayloadChecksum,
                    START_INDEX + 1);
            buffer.putLong(secondPayloadLongValue);
        });

        final var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            // We can read the first payload just fine:
            byte[] readFirstPayload = new byte[firstPayloadLength];
            channel.get(readFirstPayload, firstPayloadLength);
            assertThat(firstPayloadBytesValue).isEqualTo(readFirstPayload);
            assertThat(firstPayloadChecksum).isEqualTo(channel.getChecksum());

            // But we fail to read the second one, because there is a START_OFFSET envelope at the beginning of the
            // the second segment:
            assertThatThrownBy(channel::getLong)
                    .isInstanceOf(InvalidLogEnvelopeReadException.class)
                    .hasMessage(
                            "Unable to read log envelope data: unexpected chunk type 'START_OFFSET' at position 0 of "
                                    + "segment 2");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldFailIfStartOffsetEnvelopeIsNotFirstEnvelopeOfSegment(int segmentSize) throws Exception {
        // GIVEN
        final var firstPayloadIntValue = random.nextInt();
        final var firstPayloadLength = Integer.BYTES;
        final var firstPayloadChecksum = buildChecksum(
                EnvelopeType.FULL,
                firstPayloadLength,
                BASE_TX_CHECKSUM,
                (buffer) -> buffer.putInt(firstPayloadIntValue));

        final var startOffsetLength = random.nextInt(1, 32);

        final var secondPayloadLongValue = random.nextLong();
        final var secondPayloadLength = Long.BYTES;
        final var secondPayloadChecksum = buildChecksum(
                EnvelopeType.FULL,
                secondPayloadLength,
                firstPayloadChecksum,
                (buffer) -> buffer.putLong(secondPayloadLongValue));

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);
            // Write one payload:
            writeLogEnvelopeHeader(
                    buffer, firstPayloadChecksum, EnvelopeType.FULL, firstPayloadLength, BASE_TX_CHECKSUM, START_INDEX);
            buffer.putInt(firstPayloadIntValue);

            // Write the offset positioned wrong:
            writeStartOffsetEnvelope(buffer, startOffsetLength, false);

            // Write another payload:
            writeLogEnvelopeHeader(
                    buffer,
                    secondPayloadChecksum,
                    EnvelopeType.FULL,
                    secondPayloadLength,
                    firstPayloadChecksum,
                    START_INDEX + 1);
            buffer.putLong(secondPayloadLongValue);
        });

        final var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            // We can read the first payload just fine:
            assertThat(firstPayloadIntValue).isEqualTo(channel.getInt());
            assertThat(firstPayloadChecksum).isEqualTo(channel.getChecksum());

            // But when we try to read the second payload, since it needs to go through the START_OFFSET one
            // it will blow-up as it is not at the beginning of the segment.
            assertThatThrownBy(channel::getLong)
                    .isInstanceOf(InvalidLogEnvelopeReadException.class)
                    .hasMessage(
                            "Unable to read log envelope data: unexpected chunk type 'START_OFFSET' at position 26 of segment 1");
        }
    }

    @Test
    void rawReadAheadChannelOpensRawChannelOnNext() throws IOException {
        // GIVEN
        final var path = file(0);
        directory.createFile(path.getFileName().toString());

        final var logChannel = logChannel();
        final var capturingLogVersionBridge = new RawCapturingLogVersionBridge();

        try (var channel = new EnvelopeReadChannel(
                logChannel, 128, capturingLogVersionBridge, EmptyMemoryTracker.INSTANCE, true)) {
            // WHEN
            assertThatThrownBy(channel::get).isInstanceOf(ReadPastEndException.class);
        }
        // THEN
        assertThat(capturingLogVersionBridge.isRaw).isTrue();
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void setPositionAcrossOneSegment(int segmentSize) throws IOException {
        // GIVEN
        final var payloadSize = (segmentSize / 8);
        final var envelopeSize = payloadSize + HEADER_SIZE;
        final var bytes1 = bytes(random, payloadSize);
        final var bytes2 = bytes(random, payloadSize);
        final var bytes3 = bytes(random, payloadSize);

        final var positions =
                IntStream.range(0, 3).map(i -> segmentSize + (i * envelopeSize)).toArray();

        final var checksums = new int[3];
        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);

            checksums[0] = writeHeaderAndPayload(buffer, EnvelopeType.FULL, BASE_TX_CHECKSUM, bytes1, START_INDEX);
            checksums[1] = writeHeaderAndPayload(buffer, EnvelopeType.FULL, checksums[0], bytes2, START_INDEX + 1);
            checksums[2] = writeHeaderAndPayload(buffer, EnvelopeType.FULL, checksums[1], bytes3, START_INDEX + 2);
        });

        final var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            final var bytesRead = new byte[payloadSize];
            channel.position(positions[2]);
            channel.get(bytesRead, bytesRead.length);
            assertThat(bytes3).isEqualTo(bytesRead);
            assertThat(checksums[2]).isEqualTo(channel.getChecksum());
            assertThat(channel.entryIndex()).isEqualTo(START_INDEX + 2);

            channel.position(positions[1]);
            channel.get(bytesRead, bytesRead.length);
            assertThat(bytes2).isEqualTo(bytesRead);
            assertThat(checksums[1]).isEqualTo(channel.getChecksum());
            assertThat(channel.entryIndex()).isEqualTo(START_INDEX + 1);

            channel.position(positions[0]);
            channel.get(bytesRead, bytesRead.length);
            assertThat(bytes1).isEqualTo(bytesRead);
            assertThat(checksums[0]).isEqualTo(channel.getChecksum());
            assertThat(channel.entryIndex()).isEqualTo(START_INDEX);
        }
    }

    @ParameterizedTest
    @EnumSource(names = {"FULL", "END"})
    void shouldFailForReadsOutsideOfTerminatingEnvelope(EnvelopeType envelopeType) throws Exception {
        int segmentSize = 128;
        // GIVEN
        final var bytes = bytes(random, segmentSize / 2);

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);
            writeHeaderAndPayload(buffer, envelopeType, BASE_TX_CHECKSUM, bytes, START_INDEX);
        });

        final var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            final var bytesRead = new byte[bytes.length];
            channel.get(bytesRead, bytes.length - 1);

            assertThatThrownBy(channel::getShort)
                    .isInstanceOf(InvalidLogEnvelopeReadException.class)
                    .hasMessageContaining("Entry underflow. 2 bytes was requested but only 1 are available.");
        }
    }

    @Test
    void nextEntry() throws IOException {
        int segmentSize = 128;
        final var bytes = bytes(random, 8);
        int entrySize = HEADER_SIZE + 8;

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);
            int checksum = writeHeaderAndPayload(buffer, EnvelopeType.FULL, BASE_TX_CHECKSUM, bytes, START_INDEX);
            checksum = writeHeaderAndPayload(buffer, EnvelopeType.BEGIN, checksum, bytes, START_INDEX + 1);
            checksum = writeHeaderAndPayload(buffer, EnvelopeType.END, checksum, bytes, START_INDEX + 1);
            checksum = writeHeaderAndPayload(buffer, EnvelopeType.BEGIN, checksum, bytes, START_INDEX + 2);

            buffer.put(new byte[8]); // padding
            assertThat(buffer.position()).isEqualTo(segmentSize * 2);

            checksum = writeHeaderAndPayload(buffer, EnvelopeType.MIDDLE, checksum, bytes, START_INDEX + 2);
            checksum = writeHeaderAndPayload(buffer, EnvelopeType.END, checksum, bytes, START_INDEX + 2);
            checksum = writeHeaderAndPayload(buffer, EnvelopeType.FULL, checksum, bytes, START_INDEX + 3);
            writeHeaderAndPayload(buffer, EnvelopeType.FULL, checksum, bytes, START_INDEX + 4);
        });

        int[] positions = new int[] {
            segmentSize,
            segmentSize + entrySize,
            segmentSize + entrySize * 3,
            segmentSize * 2 + entrySize * 2,
            segmentSize * 2 + entrySize * 3,
        };

        var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            for (int i = 0; i < positions.length; i++) {
                var position = positions[i];
                channel.goToNextEntry();
                assertThat(channel.entryIndex()).isEqualTo(START_INDEX + i);
                assertThat(channel.position() - HEADER_SIZE).isEqualTo(position);
            }

            assertThatThrownBy(channel::goToNextEntry).isInstanceOf(ReadPastEndException.class);
        }
    }

    @Test
    void allowOpenOfEmptyFile() throws IOException {
        final var file = file(0);
        writeSomeData(file, buffer -> {});

        var logChannel = logChannel();
        try (var channel =
                new EnvelopeReadChannel(logChannel, 512, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            assertThatThrownBy(channel::get).isInstanceOf(ReadPastEndException.class);
        }
    }

    private Path file(int index) {
        return directory.homePath().resolve(String.valueOf(index));
    }

    /**
     * Write data to file(0).
     * A shortcut for the most common case, see {@link #writeSomeData(Path, ThrowingConsumer)} to write to other files.
     */
    private void writeSomeData(ThrowingConsumer<ByteBuffer, IOException> consumer) throws IOException {
        writeSomeData(file(0), consumer);
    }

    private void writeSomeData(Path file, ThrowingConsumer<ByteBuffer, IOException> consumer) throws IOException {
        fileSystem.deleteFile(file);
        try (var channel = fileSystem.write(file)) {
            var buffer =
                    ByteBuffers.allocate(toIntExact(KibiByte.toBytes(1)), LITTLE_ENDIAN, EmptyMemoryTracker.INSTANCE);
            consumer.accept(buffer);
            buffer.flip();
            channel.writeAll(buffer);
            channel.flush();
        }
    }

    private int buildChecksum(
            EnvelopeType type, int payloadLength, int previousChecksum, Consumer<ByteBuffer> builder) {
        final var version = type.isStarting() ? LATEST_KERNEL_VERSION.version() : IGNORE_KERNEL_VERSION;
        return buildChecksum(type, payloadLength, version, previousChecksum, builder, START_INDEX);
    }

    private static int buildChecksum(
            Checksum checksum,
            EnvelopeType type,
            int previousChecksum,
            byte kernelVersion,
            byte[] payload,
            long entryIndex) {
        return buildChecksum(
                checksum,
                ByteBuffer.allocate(HEADER_SIZE - Integer.BYTES + payload.length)
                        .order(LITTLE_ENDIAN)
                        .put(type.typeValue)
                        .putInt(payload.length)
                        .putLong(entryIndex)
                        .put(kernelVersion)
                        .putInt(previousChecksum)
                        .put(payload)
                        .flip());
    }

    private int buildChecksum(
            EnvelopeType type,
            int payloadLength,
            byte kernelVersion,
            int previousChecksum,
            Consumer<ByteBuffer> builder,
            long entryIndex) {
        final var buffer = ByteBuffer.allocate(HEADER_SIZE - Integer.BYTES + payloadLength)
                .order(LITTLE_ENDIAN)
                // header data
                .put(type.typeValue)
                .putInt(payloadLength)
                .putLong(entryIndex)
                .put(kernelVersion)
                .putInt(previousChecksum);
        builder.accept(buffer);
        return buildChecksum(checksum, buffer.flip());
    }

    private static int buildChecksum(Checksum checksum, ByteBuffer payload) {
        checksum.reset();
        checksum.update(payload);
        return (int) checksum.getValue();
    }

    private static void writeLogEnvelopeHeader(
            ByteBuffer buffer,
            int payloadChecksum,
            EnvelopeType type,
            int payloadLength,
            int previousChecksum,
            long entryIndex) {
        final var version = type.isStarting() ? LATEST_KERNEL_VERSION.version() : IGNORE_KERNEL_VERSION;
        writeLogEnvelopeHeader(buffer, payloadChecksum, type, payloadLength, version, previousChecksum, entryIndex);
    }

    private int writeHeaderAndPayload(
            ByteBuffer buffer, EnvelopeType type, int previousChecksum, byte[] payload, long startIndex) {
        final var version = type.isStarting() ? LATEST_KERNEL_VERSION.version() : IGNORE_KERNEL_VERSION;
        return writeHeaderAndPayload(buffer, type, previousChecksum, version, payload, startIndex);
    }

    private static void writeZeroSegment(ByteBuffer buffer, int segmentSize) {
        writeZeroSegment(buffer, segmentSize, BASE_TX_CHECKSUM);
    }

    private static void writeZeroSegment(ByteBuffer buffer, int segmentSize, int previousLogFileChecksum) {
        try {
            LogFormat.V9.serializeHeader(
                    buffer,
                    LogFormat.V9.newHeader(
                            42, 1, StoreId.UNKNOWN, segmentSize, previousLogFileChecksum, LATEST_KERNEL_VERSION));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        buffer.put(new byte[segmentSize - buffer.position()]);
    }

    private int writeHeaderAndPayload(
            ByteBuffer buffer,
            EnvelopeType type,
            int previousChecksum,
            byte kernelVersion,
            byte[] payload,
            long startIndex) {
        final var payloadChecksum = buildChecksum(checksum, type, previousChecksum, kernelVersion, payload, startIndex);
        return writeHeaderAndPayload(
                buffer, type, previousChecksum, payloadChecksum, kernelVersion, payload, startIndex);
    }

    private int writeHeaderAndPayload(
            ByteBuffer buffer,
            EnvelopeType type,
            int previousChecksum,
            int payloadChecksum,
            byte kernelVersion,
            byte[] payload,
            long entryIndex) {
        writeLogEnvelopeHeader(
                buffer, payloadChecksum, type, payload.length, kernelVersion, previousChecksum, entryIndex);
        buffer.put(payload);
        return payloadChecksum;
    }

    private static void writeLogEnvelopeHeader(
            ByteBuffer buffer,
            int payloadChecksum,
            EnvelopeType type,
            int payloadLength,
            byte kernelVersion,
            int previousChecksum,
            long entryIndex) {
        buffer.putInt(payloadChecksum)
                .put(type.typeValue)
                .putInt(payloadLength)
                .putLong(entryIndex)
                .put(kernelVersion)
                .putInt(previousChecksum);
    }

    private void writeStartOffsetEnvelope(ByteBuffer buffer, int length, boolean gibberish) {
        final var payload = gibberish ? bytes(random, length) : new byte[length];
        writeHeaderAndPayload(buffer, EnvelopeType.START_OFFSET, 0, payload, 0);
    }

    private static byte[] bytes(RandomSupport random, int size) {
        final var bytes = new byte[size];
        random.nextBytes(bytes);
        return bytes;
    }

    /**
     * Get a PhysicalLogVersionedStoreChannel to log on file(0).
     * Shortcut for the most common use, see {@link #logChannel(Path)} for other files.
     */
    private PhysicalLogVersionedStoreChannel logChannel() throws IOException {
        return logChannel(file(0));
    }

    private PhysicalLogVersionedStoreChannel logChannel(Path file) throws IOException {
        return new PhysicalLogVersionedStoreChannel(
                fileSystem.write(file),
                0,
                LATEST_LOG_FORMAT,
                file,
                mock(LogFileChannelNativeAccessor.class),
                DatabaseTracer.NULL);
    }

    private class TwoFileLogVersionBridge implements LogVersionBridge {

        private final Path path;

        private boolean returned;

        private TwoFileLogVersionBridge(Path path) {
            this.path = path;
        }

        @Override
        public LogVersionedStoreChannel next(LogVersionedStoreChannel channel, boolean raw) throws IOException {
            if (!returned) {
                returned = true;
                channel.close();

                return logChannel(path);
            }
            return channel;
        }
    }

    private static class RawCapturingLogVersionBridge implements LogVersionBridge {
        private boolean isRaw = false;

        @Override
        public LogVersionedStoreChannel next(LogVersionedStoreChannel channel, boolean raw) throws IOException {
            isRaw = raw;
            return channel;
        }
    }
}
