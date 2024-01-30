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
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
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
        final var file = file(0);
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

        writeSomeData(file, buffer -> {
            writeZeroSegment(buffer, segmentSize);
            writeLogEnvelopeHeader(buffer, payloadChecksum, EnvelopeType.FULL, payloadLength, BASE_TX_CHECKSUM);
            buffer.put(byteValue);
            buffer.putShort(shortValue);
            buffer.putInt(intValue);
            buffer.putLong(longValue);
            buffer.putFloat(floatValue);
            buffer.putDouble(doubleValue);
            buffer.put(bytesValue);
            buffer.put(byteBufferValue.position(0));
        });

        final var logChannel = logChannel(fileSystem, file);
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
        final var file = file(0);
        final var size = (segmentSize - HEADER_SIZE) / 4;
        final var bytes1 = bytes(random, size);
        final var bytes2 = bytes(random, size);
        final var bytes3 = bytes(random, size);
        final var checksums = new int[3];

        writeSomeData(file, buffer -> {
            writeZeroSegment(buffer, segmentSize);
            checksums[0] = writeHeaderAndPayload(buffer, EnvelopeType.FULL, BASE_TX_CHECKSUM, bytes1);
            checksums[1] = writeHeaderAndPayload(buffer, EnvelopeType.FULL, checksums[0], bytes2);
            checksums[2] = writeHeaderAndPayload(buffer, EnvelopeType.FULL, checksums[1], bytes3);
        });

        final var logChannel = logChannel(fileSystem, file);
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            final var bytesRead = new byte[size];
            channel.get(bytesRead, size);
            assertThat(bytes1).isEqualTo(bytesRead);
            assertThat(checksums[0]).isEqualTo(channel.getChecksum());

            channel.get(bytesRead, size);
            assertThat(bytes2).isEqualTo(bytesRead);
            assertThat(checksums[1]).isEqualTo(channel.getChecksum());

            channel.get(bytesRead, size);
            assertThat(bytes3).isEqualTo(bytesRead);
            assertThat(checksums[2]).isEqualTo(channel.getChecksum());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldReadEnvelopeThatFillsSingleSegment(int segmentSize) throws Exception {
        // GIVEN
        final var file = file(0);
        final var bytes = bytes(random, segmentSize - HEADER_SIZE);

        writeSomeData(file, buffer -> {
            writeZeroSegment(buffer, segmentSize);
            writeHeaderAndPayload(buffer, EnvelopeType.FULL, BASE_TX_CHECKSUM, bytes);
        });

        final var logChannel = logChannel(fileSystem, file);
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
    void shouldReadDataThatSpansAcrossTwoSegment(int segmentSize) throws Exception {
        // GIVEN
        final var file = file(0);
        final var beginChunkSize = segmentSize - HEADER_SIZE;
        final var bytes = bytes(random, segmentSize);

        writeSomeData(file, buffer -> {
            writeZeroSegment(buffer, segmentSize);

            final var checksum = writeHeaderAndPayload(
                    buffer, EnvelopeType.BEGIN, BASE_TX_CHECKSUM, copyOfRange(bytes, 0, beginChunkSize));
            writeHeaderAndPayload(buffer, EnvelopeType.END, checksum, copyOfRange(bytes, beginChunkSize, bytes.length));
        });

        final var logChannel = logChannel(fileSystem, file);
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
        final var file = file(0);
        final var beginChunkSize = segmentSize - HEADER_SIZE;
        final var bytes = bytes(random, segmentSize);

        writeSomeData(file, buffer -> {
            writeZeroSegment(buffer, segmentSize);

            final var checksum = writeHeaderAndPayload(
                    buffer, EnvelopeType.BEGIN, BASE_TX_CHECKSUM, copyOfRange(bytes, 0, beginChunkSize));
            writeHeaderAndPayload(buffer, EnvelopeType.END, checksum, copyOfRange(bytes, beginChunkSize, bytes.length));
        });

        final var logChannel = logChannel(fileSystem, file);
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
        final var file = file(0);
        final var zeros = random.nextInt(HEADER_SIZE / 4, HEADER_SIZE / 2);
        final var chunkSize1 = segmentSize - HEADER_SIZE - zeros;
        final var chunkSize2 = HEADER_SIZE * 2;
        final var bytes1 = bytes(random, chunkSize1);
        final var bytes2 = bytes(random, chunkSize2);

        writeSomeData(file, buffer -> {
            writeZeroSegment(buffer, segmentSize);

            final var checksum = writeHeaderAndPayload(buffer, EnvelopeType.FULL, BASE_TX_CHECKSUM, bytes1);
            buffer.put(new byte[zeros]);
            writeHeaderAndPayload(buffer, EnvelopeType.FULL, checksum, bytes2);
        });

        final var logChannel = logChannel(fileSystem, file);
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
        final var file = file(0);
        final var zeros = new byte[Integer.BYTES];
        final var bytes1 = bytes(random, segmentSize - (HEADER_SIZE * 2) - Long.BYTES - zeros.length);
        final var l1 = random.nextLong();
        final var l2 = random.nextLong();
        final var l3 = random.nextLong();

        writeSomeData(file, buffer -> {
            writeZeroSegment(buffer, segmentSize);

            final var checksum = writeHeaderAndPayload(buffer, EnvelopeType.FULL, BASE_TX_CHECKSUM, bytes1);

            final var beginChecksum =
                    buildChecksum(EnvelopeType.BEGIN, Long.BYTES, checksum, (crcBuffer) -> crcBuffer.putLong(l1));
            writeLogEnvelopeHeader(buffer, beginChecksum, EnvelopeType.BEGIN, Long.BYTES, checksum);
            buffer.putLong(l1);
            buffer.put(zeros);

            final var endChecksum =
                    buildChecksum(EnvelopeType.END, Long.BYTES * 2, beginChecksum, (crcBuffer) -> crcBuffer
                            .putLong(l2)
                            .putLong(l3));
            writeLogEnvelopeHeader(buffer, endChecksum, EnvelopeType.END, Long.BYTES * 2, beginChecksum);
            buffer.putLong(l2).putLong(l3);
        });

        final var logChannel = logChannel(fileSystem, file);

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
        final var file = file(0);
        final var zeros = new byte[MAX_ZERO_PADDING_SIZE - 1];
        final var bytes1 = bytes(random, segmentSize - HEADER_SIZE - zeros.length);
        final var l1 = random.nextLong();

        writeSomeData(file, buffer -> {
            writeZeroSegment(buffer, segmentSize);

            final var checksum = writeHeaderAndPayload(buffer, EnvelopeType.FULL, BASE_TX_CHECKSUM, bytes1);
            buffer.put(zeros);

            final var fullChecksum =
                    buildChecksum(EnvelopeType.FULL, Long.BYTES, checksum, (crcBuffer) -> crcBuffer.putLong(l1));
            writeLogEnvelopeHeader(buffer, fullChecksum, EnvelopeType.FULL, Long.BYTES, checksum);
            buffer.putLong(l1);
        });

        final var logChannel = logChannel(fileSystem, file);

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
        final var file = file(0);
        final var zerosWithGibberish = new byte[segmentSize];
        final var gibberishPosition = random.nextInt(segmentSize / 4, 3 * segmentSize / 4);
        zerosWithGibberish[gibberishPosition] = 42;

        writeSomeData(file, buffer -> {
            writeZeroSegment(buffer, segmentSize);
            buffer.put(zerosWithGibberish);
        });

        final var logChannel = logChannel(fileSystem, file);
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            assertThatThrownBy(channel::get)
                    .isInstanceOf(InvalidLogEnvelopeReadException.class)
                    .hasMessageContaining("Expecting only zero padding at this point");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void readingPreAllocatedFile(int segmentSize) throws Exception {
        final var file = file(0);
        final var zeros = new byte[segmentSize * 3];
        writeSomeData(file, buffer -> buffer.put(zeros));

        final var logChannel = logChannel(fileSystem, file);
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            assertThatThrownBy(channel::get).isInstanceOf(ReadPastEndException.class);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldFailWhenReadingAnEmptyFile(int segmentSize) throws IOException {
        final var logChannel = logChannel(fileSystem, file(0));

        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            assertThatThrownBy(channel::get).isInstanceOf(ReadPastEndException.class);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldFailWhenEndOfSegmentAfterAnEnvelopeIsNotZeros(int segmentSize) throws Exception {
        // GIVEN
        final var file = file(0);
        final var bytes = bytes(random, segmentSize - (HEADER_SIZE * 2));
        byte garbage = (byte) random.nextInt(1, 127);

        writeSomeData(file, buffer -> {
            writeZeroSegment(buffer, segmentSize);
            writeHeaderAndPayload(buffer, EnvelopeType.FULL, BASE_TX_CHECKSUM, bytes);
            buffer.put(garbage); // Should be zero padding here
        });

        final var logChannel = logChannel(fileSystem, file);
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            final var bytesRead = new byte[bytes.length];
            channel.get(bytesRead, bytes.length);

            assertThatThrownBy(channel::get)
                    .isInstanceOf(InvalidLogEnvelopeReadException.class)
                    .hasMessageContainingAll(
                            "end of buffer", "Expecting only zero padding at this point", "[" + garbage + "]");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldFailWhenInvalidPayloadLengthIsSpecified(int segmentSize) throws IOException {
        final var bytes = bytes(random, segmentSize / 2);
        final var path = file(0);
        writeSomeData(path, buffer -> {
            writeZeroSegment(buffer, segmentSize);

            final var payloadChecksum = buildChecksum(
                    checksum, EnvelopeType.FULL, BASE_TX_CHECKSUM, LATEST_KERNEL_VERSION.version(), bytes);
            writeLogEnvelopeHeader(buffer, payloadChecksum, EnvelopeType.FULL, segmentSize, BASE_TX_CHECKSUM);
            buffer.put(bytes);
        });

        final var logChannel = logChannel(fileSystem, path);
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
        final var path = file(0);
        writeSomeData(path, buffer -> {
            writeZeroSegment(buffer, segmentSize);
            writeHeaderAndPayload(buffer, EnvelopeType.BEGIN, BASE_TX_CHECKSUM, bytes);
        });

        final var logChannel = logChannel(fileSystem, path);

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
        final var file = file(0);
        final var bytes = bytes(random, segmentSize - HEADER_SIZE);

        writeSomeData(file, buffer -> {
            writeZeroSegment(buffer, segmentSize);
            writeHeaderAndPayload(buffer, EnvelopeType.FULL, BASE_TX_CHECKSUM, bytes);
            buffer.put((byte) 13);
        });

        final var logChannel = logChannel(fileSystem, file);

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

            var checksum = writeHeaderAndPayload(buffer, EnvelopeType.BEGIN, BASE_TX_CHECKSUM, bytes0);
            checksum = writeHeaderAndPayload(buffer, EnvelopeType.MIDDLE, checksum, bytes1);
            endChecksum.setValue(checksum);
        });
        writeSomeData(path2, buffer -> {
            writeZeroSegment(buffer, segmentSize, endChecksum.intValue());
            writeHeaderAndPayload(buffer, EnvelopeType.END, endChecksum.intValue(), bytes2);
        });

        final var logChannel = logChannel(fileSystem, path1);

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

            var checksum = writeHeaderAndPayload(buffer, EnvelopeType.BEGIN, BASE_TX_CHECKSUM, bytes0);
            checksum = writeHeaderAndPayload(buffer, EnvelopeType.MIDDLE, checksum, bytes1);
            endChecksum.setValue(checksum);
        });
        writeSomeData(path2, buffer -> {
            writeZeroSegment(buffer, segmentSize, endChecksum.intValue());
            writeHeaderAndPayload(buffer, EnvelopeType.END, endChecksum.intValue(), bytes2);
        });

        final var logChannel = logChannel(fileSystem, path1);

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
        final var file = file(0);
        final var bytes = bytes(random, segmentSize / 2);

        writeSomeData(file, buffer -> {
            writeZeroSegment(buffer, segmentSize, 42);
            writeHeaderAndPayload(buffer, EnvelopeType.FULL, 666, bytes);
        });

        final var logChannel = logChannel(fileSystem, file);
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
        final var file = file(0);
        final var bytes = bytes(random, segmentSize / 4);

        checksum.reset();
        checksum.update(bytes);
        final var payloadChecksum = (int) checksum.getValue();
        final var invalid = random.nextBoolean() ? payloadChecksum + 1 : payloadChecksum - 1;

        writeSomeData(file, buffer -> {
            writeZeroSegment(buffer, segmentSize);
            writeHeaderAndPayload(
                    buffer, EnvelopeType.FULL, BASE_TX_CHECKSUM, invalid, LATEST_KERNEL_VERSION.version(), bytes);
        });

        final var logChannel = logChannel(fileSystem, file);
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
        final var file = file(0);
        final var totalSize = segmentSize * 2;
        final var chunkSize = segmentSize - HEADER_SIZE;
        final var bytes = bytes(random, totalSize);

        writeSomeData(file, buffer -> {
            writeZeroSegment(buffer, segmentSize);

            var checksum = writeHeaderAndPayload(
                    buffer, EnvelopeType.BEGIN, BASE_TX_CHECKSUM, copyOfRange(bytes, 0, chunkSize));
            checksum = writeHeaderAndPayload(
                    buffer, EnvelopeType.MIDDLE, checksum, copyOfRange(bytes, chunkSize, chunkSize * 2));
            writeHeaderAndPayload(buffer, EnvelopeType.END, checksum, copyOfRange(bytes, chunkSize * 2, bytes.length));
        });

        final var logChannel = logChannel(fileSystem, file);

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
        final var file = file(0);
        final var chunkCount = 3;
        final var chunkSize = segmentSize - HEADER_SIZE;
        final var bytes = bytes(random, chunkSize * 3);

        writeSomeData(file, buffer -> {
            writeZeroSegment(buffer, segmentSize);

            var checksum = writeHeaderAndPayload(
                    buffer, EnvelopeType.BEGIN, BASE_TX_CHECKSUM, copyOfRange(bytes, 0, chunkSize));
            checksum = writeHeaderAndPayload(
                    buffer, EnvelopeType.MIDDLE, checksum, copyOfRange(bytes, chunkSize, chunkSize * 2));
            writeHeaderAndPayload(buffer, EnvelopeType.END, checksum, copyOfRange(bytes, chunkSize * 2, chunkSize * 3));
        });

        final var logChannel = logChannel(fileSystem, file);
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

    @Test
    void rawReadAheadChannelOpensRawChannelOnNext() throws IOException {
        // GIVEN
        final var path = file(0);
        directory.createFile(path.getFileName().toString());

        final var logChannel = logChannel(fileSystem, path);
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
        final var path = file(0);

        final var payloadSize = segmentSize / 6;
        final var envelopeSize = payloadSize + HEADER_SIZE;
        final var bytes1 = bytes(random, payloadSize);
        final var bytes2 = bytes(random, payloadSize);
        final var bytes3 = bytes(random, payloadSize);

        final var positions =
                IntStream.range(0, 3).map(i -> segmentSize + (i * envelopeSize)).toArray();

        final var checksums = new int[3];
        writeSomeData(path, buffer -> {
            writeZeroSegment(buffer, segmentSize);

            checksums[0] = writeHeaderAndPayload(buffer, EnvelopeType.FULL, BASE_TX_CHECKSUM, bytes1);
            checksums[1] = writeHeaderAndPayload(buffer, EnvelopeType.FULL, checksums[0], bytes2);
            checksums[2] = writeHeaderAndPayload(buffer, EnvelopeType.FULL, checksums[1], bytes3);
        });

        final var logChannel = logChannel(fileSystem, path);
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            final var bytesRead = new byte[payloadSize];
            channel.position(positions[2]);
            channel.get(bytesRead, bytesRead.length);
            assertThat(bytes3).isEqualTo(bytesRead);
            assertThat(checksums[2]).isEqualTo(channel.getChecksum());

            channel.position(positions[1]);
            channel.get(bytesRead, bytesRead.length);
            assertThat(bytes2).isEqualTo(bytesRead);
            assertThat(checksums[1]).isEqualTo(channel.getChecksum());

            channel.position(positions[0]);
            channel.get(bytesRead, bytesRead.length);
            assertThat(bytes1).isEqualTo(bytesRead);
            assertThat(checksums[0]).isEqualTo(channel.getChecksum());
        }
    }

    @ParameterizedTest
    @EnumSource(names = {"FULL", "END"})
    void shouldFailForReadsOutsideOfTerminatingEnvelope(EnvelopeType envelopeType) throws Exception {
        int segmentSize = 128;
        // GIVEN
        final var file = file(0);
        final var bytes = bytes(random, segmentSize / 2);

        writeSomeData(file, buffer -> {
            writeZeroSegment(buffer, segmentSize);
            writeHeaderAndPayload(buffer, envelopeType, BASE_TX_CHECKSUM, bytes);
        });

        final var logChannel = logChannel(fileSystem, file);
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
        final var file = file(0);
        final var bytes = bytes(random, 8);
        int entrySize = HEADER_SIZE + 8;

        writeSomeData(file, buffer -> {
            writeZeroSegment(buffer, segmentSize);
            int checksum = writeHeaderAndPayload(buffer, EnvelopeType.FULL, BASE_TX_CHECKSUM, bytes);
            checksum = writeHeaderAndPayload(buffer, EnvelopeType.BEGIN, checksum, bytes);
            checksum = writeHeaderAndPayload(buffer, EnvelopeType.END, checksum, bytes);
            checksum = writeHeaderAndPayload(buffer, EnvelopeType.BEGIN, checksum, bytes);
            checksum = writeHeaderAndPayload(buffer, EnvelopeType.MIDDLE, checksum, bytes);

            buffer.put(new byte[HEADER_SIZE + 4]); // padding
            assertThat(buffer.position()).isEqualTo(segmentSize * 2);

            checksum = writeHeaderAndPayload(buffer, EnvelopeType.END, checksum, bytes);
            checksum = writeHeaderAndPayload(buffer, EnvelopeType.FULL, checksum, bytes);
            writeHeaderAndPayload(buffer, EnvelopeType.FULL, checksum, bytes);
        });

        int[] positions = new int[] {
            segmentSize,
            segmentSize + entrySize,
            segmentSize + entrySize * 3,
            segmentSize * 2 + entrySize,
            segmentSize * 2 + entrySize * 2,
        };

        var logChannel = logChannel(fileSystem, file);
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            for (int position : positions) {
                channel.goToNextEntry();
                assertThat(channel.position() - HEADER_SIZE).isEqualTo(position);
            }
            assertThatThrownBy(channel::goToNextEntry).isInstanceOf(ReadPastEndException.class);
        }
    }

    @Test
    void allowOpenOfEmptyFile() throws IOException {
        final var file = file(0);
        writeSomeData(file, buffer -> {});

        var logChannel = logChannel(fileSystem, file);
        try (var channel =
                new EnvelopeReadChannel(logChannel, 512, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            assertThatThrownBy(channel::get).isInstanceOf(ReadPastEndException.class);
        }
    }

    private Path file(int index) {
        return directory.homePath().resolve(String.valueOf(index));
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
        return buildChecksum(type, payloadLength, version, previousChecksum, builder);
    }

    private static int buildChecksum(
            Checksum checksum, EnvelopeType type, int previousChecksum, byte kernelVersion, byte[] payload) {
        return buildChecksum(
                checksum,
                ByteBuffer.allocate(HEADER_SIZE - Integer.BYTES + payload.length)
                        .order(LITTLE_ENDIAN)
                        .put(type.typeValue)
                        .putInt(payload.length)
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
            Consumer<ByteBuffer> builder) {
        final var buffer = ByteBuffer.allocate(HEADER_SIZE - Integer.BYTES + payloadLength)
                .order(LITTLE_ENDIAN)
                // header data
                .put(type.typeValue)
                .putInt(payloadLength)
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
            ByteBuffer buffer, int payloadChecksum, EnvelopeType type, int payloadLength, int previousChecksum) {
        final var version = type.isStarting() ? LATEST_KERNEL_VERSION.version() : IGNORE_KERNEL_VERSION;
        writeLogEnvelopeHeader(buffer, payloadChecksum, type, payloadLength, version, previousChecksum);
    }

    private int writeHeaderAndPayload(ByteBuffer buffer, EnvelopeType type, int previousChecksum, byte[] payload) {
        final var version = type.isStarting() ? LATEST_KERNEL_VERSION.version() : IGNORE_KERNEL_VERSION;
        return writeHeaderAndPayload(buffer, type, previousChecksum, version, payload);
    }

    private static void writeZeroSegment(ByteBuffer buffer, int segmentSize) {
        writeZeroSegment(buffer, segmentSize, BASE_TX_CHECKSUM);
    }

    private static void writeZeroSegment(ByteBuffer buffer, int segmentSize, int previousLogFileChecksum) {
        try {
            LogFormat.V9
                    .getHeaderWriter()
                    .write(
                            buffer,
                            new LogHeader(
                                    LogFormat.V9,
                                    42,
                                    1,
                                    StoreId.UNKNOWN,
                                    segmentSize,
                                    previousLogFileChecksum,
                                    LATEST_KERNEL_VERSION));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        buffer.put(new byte[segmentSize - buffer.position()]);
    }

    private int writeHeaderAndPayload(
            ByteBuffer buffer, EnvelopeType type, int previousChecksum, byte kernelVersion, byte[] payload) {
        final var payloadChecksum = buildChecksum(checksum, type, previousChecksum, kernelVersion, payload);
        return writeHeaderAndPayload(buffer, type, previousChecksum, payloadChecksum, kernelVersion, payload);
    }

    private int writeHeaderAndPayload(
            ByteBuffer buffer,
            EnvelopeType type,
            int previousChecksum,
            int payloadChecksum,
            byte kernelVersion,
            byte[] payload) {
        writeLogEnvelopeHeader(buffer, payloadChecksum, type, payload.length, kernelVersion, previousChecksum);
        buffer.put(payload);
        return payloadChecksum;
    }

    private static void writeLogEnvelopeHeader(
            ByteBuffer buffer,
            int payloadChecksum,
            EnvelopeType type,
            int payloadLength,
            byte kernelVersion,
            int previousChecksum) {
        buffer.putInt(payloadChecksum)
                .put(type.typeValue)
                .putInt(payloadLength)
                .put(kernelVersion)
                .putInt(previousChecksum);
    }

    private static byte[] bytes(RandomSupport random, int size) {
        final var bytes = new byte[size];
        random.nextBytes(bytes);
        return bytes;
    }

    private static PhysicalLogVersionedStoreChannel logChannel(FileSystemAbstraction fileSystem, Path file)
            throws IOException {
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

                return logChannel(fileSystem, path);
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
