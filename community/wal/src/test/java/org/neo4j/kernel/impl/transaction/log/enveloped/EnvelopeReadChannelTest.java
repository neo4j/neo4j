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

import static java.lang.Math.toIntExact;
import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Arrays.copyOfRange;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.io.ByteUnit.KibiByte;
import static org.neo4j.io.fs.ChecksumWriter.CHECKSUM_FACTORY;
import static org.neo4j.io.fs.ReadableChannel.UNSPECIFIED_CONTENT_TYPE;
import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.KERNEL_CONTENT_TYPE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.MAX_ZERO_PADDING_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.REPLICATED_TX_CONTENT_TYPE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.UNSPECIFIED_CREATION_TIME;
import static org.neo4j.kernel.impl.transaction.log.enveloped.EnvelopeWriteChannelTest.buffer;
import static org.neo4j.kernel.impl.transaction.log.enveloped.EnvelopeWriteChannelTest.writeChannel;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.zip.Checksum;
import org.apache.commons.lang3.mutable.MutableInt;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.configuration.Config;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.io.fs.ChannelNativeAccessor;
import org.neo4j.io.fs.ChecksumMismatchException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.LogTracers;
import org.neo4j.kernel.impl.transaction.log.LogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.EnvelopeType;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.CommandReader;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StoreIdentifier;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@RandomSupportExtension
class EnvelopeReadChannelTest {
    private static final long START_INDEX = 0;
    private static final byte CONTENT_TYPE = KERNEL_CONTENT_TYPE;
    private static final long TERM = 1L;
    private static final int TEST_DATA_SIZE = 32;
    private static final int TEST_ENTRY_SIZE = TEST_DATA_SIZE + HEADER_SIZE;
    private final Checksum checksum = CHECKSUM_FACTORY.get();

    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private TestDirectory directory;

    @Inject
    private RandomSupport random;

    private static void writeZeroSegment(ByteBuffer buffer, int segmentSize) {
        writeZeroSegment(buffer, segmentSize, BASE_TX_CHECKSUM);
    }

    private static void writeZeroSegment(ByteBuffer buffer, int segmentSize, int previousLogFileChecksum) {
        try {
            LogFormat.V10.serializeHeader(
                    buffer,
                    LogFormat.V10.newHeader(
                            42,
                            1,
                            10,
                            StoreIdentifier.UNKNOWN,
                            segmentSize,
                            previousLogFileChecksum,
                            LatestVersions.LATEST_KERNEL_VERSION,
                            UNSPECIFIED_CREATION_TIME));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        buffer.put(new byte[segmentSize - buffer.position()]);
    }

    private static byte[] bytes(RandomSupport random, int size) {
        final var bytes = new byte[size];
        random.nextBytes(bytes);
        return bytes;
    }

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

        final var payloadChecksum = buildChecksum(
                EnvelopeType.FULL,
                payloadLength,
                BASE_TX_CHECKSUM,
                (buffer) -> buffer.put(byteValue)
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
            assertThat(floatValue).isCloseTo(channel.getFloat(), Offset.offset(0.1f));
            assertThat(doubleValue).isCloseTo(channel.getDouble(), Offset.offset(0.1d));

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

            final var endChecksum = buildChecksum(
                    EnvelopeType.END,
                    Long.BYTES * 2,
                    beginChecksum,
                    (crcBuffer) -> crcBuffer.putLong(l2).putLong(l3));
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
        final var file = file(0);
        final var zeros = new byte[segmentSize * 3];
        writeSomeData(buffer -> writeZeroSegment(buffer, segmentSize));
        writeSomeData(file, buffer -> buffer.put(zeros));

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
                    checksum,
                    EnvelopeType.FULL,
                    BASE_TX_CHECKSUM,
                    LatestVersions.LATEST_KERNEL_VERSION.version(),
                    bytes,
                    START_INDEX,
                    CONTENT_TYPE,
                    TERM);
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
                            "Log file with version 0 ended with an incomplete record type (BEGIN) and no following log file could be found.");
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
                    .isInstanceOf(IncompleteEnvelopeReadException.class)
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
                    LatestVersions.LATEST_KERNEL_VERSION.version(),
                    bytes,
                    START_INDEX,
                    CONTENT_TYPE,
                    TERM);
        });

        final var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            final var bytesRead = new byte[bytes.length];
            assertThatThrownBy(() -> channel.get(bytesRead, bytes.length))
                    .isInstanceOf(IncompleteEnvelopeReadException.class)
                    .cause()
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
        final var startOffsetLength = random.nextInt(1, 59);

        final var longValue = random.nextLong();
        final var mainPayloadLength = Long.BYTES;
        final var mainPayloadChecksum = buildChecksum(
                EnvelopeType.FULL, mainPayloadLength, BASE_TX_CHECKSUM, (buffer) -> buffer.putLong(longValue));

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);
            writeHeaderAndPayload(buffer, EnvelopeType.START_OFFSET, 0, new byte[startOffsetLength], 0);
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
    @ValueSource(ints = {256, 512})
    void shouldSkipStartOffsetEnvelopeInSecondFile(int bufferSize) throws Exception {
        // GIVEN
        int segmentSize = 256;
        final var startOffsetLength = random.nextInt(1, 59);
        int dataLength = 64;

        final var path1 = file(0);
        final var path2 = file(1);

        final var endChecksum = new MutableInt();
        byte[] bytes1 = bytes(random, dataLength);
        byte[] bytes2 = bytes(random, dataLength);
        writeSomeData(path1, buffer -> {
            writeZeroSegment(buffer, segmentSize);
            writeHeaderAndPayload(buffer, EnvelopeType.START_OFFSET, 0, new byte[startOffsetLength], 0);
            endChecksum.setValue(
                    writeHeaderAndPayload(buffer, EnvelopeType.FULL, BASE_TX_CHECKSUM, bytes1, START_INDEX));
        });
        writeSomeData(path2, buffer -> {
            writeZeroSegment(buffer, segmentSize, endChecksum.intValue());
            writeHeaderAndPayload(buffer, EnvelopeType.START_OFFSET, 0, new byte[startOffsetLength], 0);
            writeHeaderAndPayload(buffer, EnvelopeType.FULL, endChecksum.intValue(), bytes2, START_INDEX);
        });

        final var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, new TwoFileLogVersionBridge(path2), EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            byte[] result = new byte[dataLength];
            channel.get(result, dataLength);
            assertThat(bytes1).isEqualTo(result);
            channel.get(result, dataLength);
            assertThat(bytes2).isEqualTo(result);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldSkipStartOffsetEnvelopeWhenOverridingChannelPosition(int segmentSize) throws Exception {
        // GIVEN
        final var startOffsetLength = random.nextInt(1, 59);

        final var longValue = random.nextLong();
        final var mainPayloadLength = Long.BYTES;
        final var mainPayloadChecksum = buildChecksum(
                EnvelopeType.FULL, mainPayloadLength, BASE_TX_CHECKSUM, (buffer) -> buffer.putLong(longValue));

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);
            writeHeaderAndPayload(buffer, EnvelopeType.START_OFFSET, 0, new byte[startOffsetLength], 0);
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
            byte[] payload;
            do {
                payload = bytes(random, startOffsetLength);
            } while (Arrays.equals(payload, new byte[startOffsetLength])); // make sure it's not all zeros
            writeHeaderAndPayload(buffer, EnvelopeType.START_OFFSET, 0, payload, 0);
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
            writeHeaderAndPayload(buffer, EnvelopeType.START_OFFSET, 0, new byte[startOffsetLength], 0);
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
            writeHeaderAndPayload(buffer, EnvelopeType.START_OFFSET, 0, new byte[startOffsetLength], 0);

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
                            "Unable to read log envelope data: unexpected chunk type 'START_OFFSET' at position 35 of segment 1");
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
        final var payloadSize = (segmentSize / 11);
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

    @Test
    void setPositionAtExactlySegmentBoundary() throws IOException {
        // GIVEN
        final var segmentSize = 128;
        final var payloadSize = segmentSize - HEADER_SIZE;
        final var envelopeSize = payloadSize + HEADER_SIZE;
        final var bytes1 = bytes(random, payloadSize);
        final var bytes2 = bytes(random, payloadSize);

        final var positions =
                IntStream.range(0, 2).map(i -> segmentSize + (i * envelopeSize)).toArray();

        final var checksums = new int[2];
        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);

            checksums[0] = writeHeaderAndPayload(buffer, EnvelopeType.FULL, BASE_TX_CHECKSUM, bytes1, START_INDEX);
            checksums[1] = writeHeaderAndPayload(buffer, EnvelopeType.FULL, checksums[0], bytes2, START_INDEX + 1);
        });

        final var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            final var bytesRead = new byte[payloadSize];
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

    @Test
    void setPositionAtExactlySegmentBoundaryAndMoveToNextFile() throws Exception {
        // GIVEN
        final var segmentSize = 128;
        final var path1 = file(0);
        final var path2 = file(1);

        final var payloadSize = segmentSize - HEADER_SIZE;
        final var envelope1Size = payloadSize + HEADER_SIZE;

        final var bytes1 = bytes(random, payloadSize);
        final var bytes2 = bytes(random, payloadSize);
        final var bytes3 = bytes(random, payloadSize);

        final var positions = IntStream.range(0, 3)
                .map(i -> segmentSize + (i * envelope1Size))
                .toArray();

        final var endChecksum = new MutableInt();
        writeSomeData(path1, buffer -> {
            writeZeroSegment(buffer, segmentSize);

            var checksum = writeHeaderAndPayload(buffer, EnvelopeType.FULL, BASE_TX_CHECKSUM, bytes1, START_INDEX);
            checksum = writeHeaderAndPayload(buffer, EnvelopeType.FULL, checksum, bytes2, START_INDEX + 1);
            endChecksum.setValue(checksum);
        });
        writeSomeData(path2, buffer -> {
            writeZeroSegment(buffer, segmentSize, endChecksum.intValue());
            writeHeaderAndPayload(buffer, EnvelopeType.FULL, endChecksum.intValue(), bytes3, START_INDEX + 2);
        });

        final var logChannel = logChannel(path1);
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, new TwoFileLogVersionBridge(path2), EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            final var bytesRead = new byte[payloadSize];
            channel.position(positions[1]);
            channel.get(bytesRead, bytesRead.length);
            assertThat(bytes2).isEqualTo(bytesRead);
            assertThat(channel.entryIndex()).isEqualTo(START_INDEX + 1);

            channel.position(positions[2]);
            channel.get(bytesRead, bytesRead.length);
            assertThat(bytes3).isEqualTo(bytesRead);
            assertThat(channel.entryIndex()).isEqualTo(START_INDEX + 2);
        }
    }

    @ParameterizedTest
    @EnumSource(names = {"FULL", "END"})
    void shouldFailForReadsOutsideOfTerminatingEnvelope(EnvelopeType envelopeType) throws Exception {
        int segmentSize = 256;
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
                    .isInstanceOf(IncompleteEnvelopeReadException.class)
                    .hasMessageContaining("Entry underflow. 2 bytes was requested, but only 1 are available.");
        }
    }

    @Test
    void shouldFailForReadsThatHasPassedEnvelopeBoundary() throws Exception {
        // Hacked command reader factory to take us over an envelope boundary. In the real world it could
        // potentially happen if starting to read a tx in a middle/end envelope and bytes happen to match
        // valid type codes.
        CommandReaderFactory commandReaderFactory = version -> new CommandReader() {
            @Override
            public StorageCommand read(ReadableChannel channel, MemoryTracker memoryTracker) throws IOException {
                channel.getLong(); // Read that goes past envelope boundary
                channel.get(); // The following read that triggers the exception
                return null;
            }

            @Override
            public KernelVersion kernelVersion() {
                return null;
            }
        };

        int segmentSize = 128;
        byte[] bytes = new byte[] {3, 0, 0}; // valid command type and some bytes to not read second header on first get

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);
            int checksum = writeHeaderAndPayload(buffer, EnvelopeType.MIDDLE, BASE_TX_CHECKSUM, bytes, START_INDEX);
            writeHeaderAndPayload(buffer, EnvelopeType.MIDDLE, checksum, new byte[50], START_INDEX + 1);
        });

        try (var channel = new EnvelopeReadChannel(
                logChannel(), segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            VersionAwareLogEntryReader logEntryReader = new VersionAwareLogEntryReader(
                    commandReaderFactory,
                    new BinarySupportedKernelVersions(Config.defaults()),
                    EmptyMemoryTracker.INSTANCE);
            assertThatThrownBy(() -> logEntryReader.readLogEntry(channel))
                    .isInstanceOf(InvalidLogEnvelopeReadException.class)
                    .hasMessageContaining("Read has gone past an envelope boundary");
        }
    }

    @ParameterizedTest
    @ValueSource(bytes = {KERNEL_CONTENT_TYPE, CONTENT_TYPE, REPLICATED_TX_CONTENT_TYPE})
    void getContentType(byte contentType) throws IOException {
        // GIVEN an envelope with specific content type
        int segmentSize = 256;
        final var bytes = bytes(random, TEST_DATA_SIZE);
        final HeapScopedBuffer buffer = buffer(segmentSize);
        try (var channel = writeChannel(logChannel(), segmentSize, buffer)) {
            channel.resetAppendedBytesCounter();
            channel.beginChecksumForWriting();
            channel.putVersion(LatestVersions.LATEST_KERNEL_VERSION.version());
            channel.putTerm(24L);
            channel.put(bytes, bytes.length);
            channel.putContentType(contentType);
            channel.endCurrentEntry();
        }

        // WHEN reading the envelope
        try (var channel = new EnvelopeReadChannel(
                logChannel(), segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN the content type is as expected
            assertThat(channel.getContentType()).isEqualTo(contentType);
        }
    }

    @Test
    void nextTransactionEntry() throws IOException {
        int segmentSize = 256;
        final var bytes = bytes(random, TEST_DATA_SIZE);
        final byte version = LatestVersions.LATEST_KERNEL_VERSION.version();
        byte nonKernelContentType = (byte) 1;

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);
            int checksum = writeHeaderAndPayload(
                    buffer, EnvelopeType.FULL, BASE_TX_CHECKSUM, version, bytes, START_INDEX, KERNEL_CONTENT_TYPE);
            checksum = writeHeaderAndPayload(
                    buffer, EnvelopeType.BEGIN, checksum, version, bytes, START_INDEX + 1, UNSPECIFIED_CONTENT_TYPE);
            checksum = writeHeaderAndPayload(
                    buffer, EnvelopeType.END, checksum, version, bytes, START_INDEX + 1, UNSPECIFIED_CONTENT_TYPE);
            checksum = writeHeaderAndPayload(
                    buffer, EnvelopeType.BEGIN, checksum, version, bytes, START_INDEX + 2, nonKernelContentType);

            buffer.put(new byte[4]); // padding
            assertThat(buffer.position()).isEqualTo(segmentSize * 2);

            checksum = writeHeaderAndPayload(
                    buffer, EnvelopeType.MIDDLE, checksum, version, bytes, START_INDEX + 2, nonKernelContentType);
            checksum = writeHeaderAndPayload(
                    buffer, EnvelopeType.END, checksum, version, bytes, START_INDEX + 2, nonKernelContentType);
            checksum = writeHeaderAndPayload(
                    buffer, EnvelopeType.FULL, checksum, version, bytes, START_INDEX + 3, nonKernelContentType);
            writeHeaderAndPayload(
                    buffer, EnvelopeType.FULL, checksum, version, bytes, START_INDEX + 4, REPLICATED_TX_CONTENT_TYPE);
        });

        var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // WHEN at beginning of file, THEN it skips to first transaction entry
            assertThat(channel.goToNextTransactionEntry()).isEqualTo(segmentSize);
            assertThat(channel.entryIndex()).isEqualTo(START_INDEX);
            assertThat(channel.getContentType()).isEqualTo(KERNEL_CONTENT_TYPE);

            // WHEN called again, THEN it moves to subsequent entries of tx-associated content types (skipping others)
            assertThat(channel.goToNextTransactionEntry()).isEqualTo(segmentSize + TEST_ENTRY_SIZE);
            assertThat(channel.entryIndex()).isEqualTo(START_INDEX + 1);
            assertThat(channel.getContentType()).isEqualTo(UNSPECIFIED_CONTENT_TYPE);

            assertThat(channel.goToNextTransactionEntry()).isEqualTo(segmentSize * 2 + TEST_ENTRY_SIZE * 3);
            assertThat(channel.entryIndex()).isEqualTo(START_INDEX + 4);
            assertThat(channel.getContentType()).isEqualTo(REPLICATED_TX_CONTENT_TYPE);

            // WHEN called at position in last transaction entry, THEN it will ReadPastEndException
            assertThatThrownBy(channel::goToNextTransactionEntry).isInstanceOf(ReadPastEndException.class);
        }
    }

    @Test
    void nextEntry() throws IOException {
        int segmentSize = 256;
        final var bytes = bytes(random, TEST_DATA_SIZE);

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);
            int checksum = writeHeaderAndPayload(buffer, EnvelopeType.FULL, BASE_TX_CHECKSUM, bytes, START_INDEX);
            checksum = writeHeaderAndPayload(buffer, EnvelopeType.BEGIN, checksum, bytes, START_INDEX + 1);
            checksum = writeHeaderAndPayload(buffer, EnvelopeType.END, checksum, bytes, START_INDEX + 1);
            checksum = writeHeaderAndPayload(buffer, EnvelopeType.BEGIN, checksum, bytes, START_INDEX + 2);

            buffer.put(new byte[4]); // padding
            assertThat(buffer.position()).isEqualTo(segmentSize * 2);

            checksum = writeHeaderAndPayload(buffer, EnvelopeType.MIDDLE, checksum, bytes, START_INDEX + 2);
            checksum = writeHeaderAndPayload(buffer, EnvelopeType.END, checksum, bytes, START_INDEX + 2);
            checksum = writeHeaderAndPayload(buffer, EnvelopeType.FULL, checksum, bytes, START_INDEX + 3);
            writeHeaderAndPayload(buffer, EnvelopeType.FULL, checksum, bytes, START_INDEX + 4);
        });

        int[] positions = new int[] {
            segmentSize,
            segmentSize + TEST_ENTRY_SIZE,
            segmentSize + TEST_ENTRY_SIZE * 3,
            segmentSize * 2 + TEST_ENTRY_SIZE * 2,
            segmentSize * 2 + TEST_ENTRY_SIZE * 3,
        };

        var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            for (int i = 0; i < positions.length; i++) {
                var position = positions[i];
                assertThat(channel.goToNextEntry()).isEqualTo(position);
                assertThat(channel.entryIndex()).isEqualTo(START_INDEX + i);
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

    @ParameterizedTest
    @EnumSource(
            value = EnvelopeType.class,
            mode = Mode.INCLUDE,
            names = {"FULL", "BEGIN"})
    void shouldAlignFromStartToNextNewEntry(EnvelopeType envelopeType) throws IOException {
        int segmentSize = 256;
        final var bytes = bytes(random, TEST_DATA_SIZE);

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);
            int checksum = writeHeaderAndPayload(buffer, EnvelopeType.MIDDLE, BASE_TX_CHECKSUM, bytes, START_INDEX);
            checksum = writeHeaderAndPayload(buffer, EnvelopeType.MIDDLE, checksum, bytes, START_INDEX);
            checksum = writeHeaderAndPayload(buffer, EnvelopeType.END, checksum, bytes, START_INDEX);
            checksum = writeHeaderAndPayload(buffer, envelopeType, checksum, bytes, START_INDEX + 1);
            writeHeaderAndPayload(buffer, EnvelopeType.FULL, checksum, bytes, START_INDEX + 2);
        });

        try (var channel = new EnvelopeReadChannel(
                logChannel(), segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            assertThat(channel.alignWithStartEntry())
                    .isEqualTo(segmentSize + TEST_ENTRY_SIZE * 3); // first full or begin
        }
    }

    @ParameterizedTest
    @EnumSource(
            value = EnvelopeType.class,
            mode = Mode.INCLUDE,
            names = {"FULL", "BEGIN"})
    void shouldAlignFromMiddleToNextNewEntry(EnvelopeType envelopeType) throws IOException {
        int segmentSize = 256;
        final var bytes = bytes(random, TEST_DATA_SIZE);

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);
            int checksum = writeHeaderAndPayload(buffer, EnvelopeType.MIDDLE, BASE_TX_CHECKSUM, bytes, START_INDEX);
            checksum = writeHeaderAndPayload(buffer, EnvelopeType.MIDDLE, checksum, bytes, START_INDEX);
            checksum = writeHeaderAndPayload(buffer, EnvelopeType.END, checksum, bytes, START_INDEX);
            checksum = writeHeaderAndPayload(buffer, envelopeType, checksum, bytes, START_INDEX + 1);
            writeHeaderAndPayload(buffer, EnvelopeType.FULL, checksum, bytes, START_INDEX + 2);
        });

        try (var channel = new EnvelopeReadChannel(
                logChannel(), segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            channel.position(segmentSize + TEST_ENTRY_SIZE);
            // first full or begin
            assertThat(channel.alignWithStartEntry()).isEqualTo(segmentSize + TEST_ENTRY_SIZE * 3);
        }
    }

    @Test
    void shouldAlignResettingPositionToStartOfAlignedEntry() throws IOException {
        int segmentSize = 256;
        final var bytes = bytes(random, TEST_DATA_SIZE);

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);
            int checksum = writeHeaderAndPayload(buffer, EnvelopeType.MIDDLE, BASE_TX_CHECKSUM, bytes, START_INDEX);
            checksum = writeHeaderAndPayload(buffer, EnvelopeType.MIDDLE, checksum, bytes, START_INDEX);
            checksum = writeHeaderAndPayload(buffer, EnvelopeType.END, checksum, bytes, START_INDEX);
            checksum = writeHeaderAndPayload(
                    buffer,
                    EnvelopeType.BEGIN,
                    checksum,
                    bytes,
                    START_INDEX + 1); // Illegal begin but for test purposes
            writeHeaderAndPayload(buffer, EnvelopeType.FULL, checksum, bytes, START_INDEX + 2);
        });

        try (var channel = new EnvelopeReadChannel(
                logChannel(), segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            channel.position(segmentSize + TEST_ENTRY_SIZE * 3 + 5); // partly read content
            assertThat(channel.alignWithStartEntry())
                    .isEqualTo(segmentSize + TEST_ENTRY_SIZE * 3); // first full or begin
        }
    }

    @Test
    void shouldAlignPositionAtTheEndIfNoNewEntries() throws IOException {
        int segmentSize = 256;
        final var bytes = bytes(random, TEST_DATA_SIZE);

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);
            int checksum = writeHeaderAndPayload(buffer, EnvelopeType.MIDDLE, BASE_TX_CHECKSUM, bytes, START_INDEX);
            checksum = writeHeaderAndPayload(buffer, EnvelopeType.MIDDLE, checksum, bytes, START_INDEX);
            writeHeaderAndPayload(buffer, EnvelopeType.END, checksum, bytes, START_INDEX);
        });

        try (var channel = new EnvelopeReadChannel(
                logChannel(), segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            assertThat(channel.alignWithStartEntry()).isEqualTo(segmentSize + TEST_ENTRY_SIZE * 3); // end of last entry
            assertThat(channel.position()).isEqualTo(segmentSize + TEST_ENTRY_SIZE * 3); // no new header to read
        }
    }

    @Test
    void shouldAlignPositionAtTheEndIfNoNewEntriesAndPartialEndEnvelope() throws IOException {
        int segmentSize = 256;

        final var fullSegmentPayload = segmentSize - HEADER_SIZE;
        final var partialSegmentPayload = segmentSize - HEADER_SIZE - 10;

        final var fullBytesPayload = bytes(random, fullSegmentPayload);
        final var partialBytesPayload = bytes(random, partialSegmentPayload);

        final var fullEntrySize = fullSegmentPayload + HEADER_SIZE;
        final var partialEntrySize = partialSegmentPayload + HEADER_SIZE;

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);
            int checksum =
                    writeHeaderAndPayload(buffer, EnvelopeType.MIDDLE, BASE_TX_CHECKSUM, fullBytesPayload, START_INDEX);
            checksum = writeHeaderAndPayload(buffer, EnvelopeType.MIDDLE, checksum, fullBytesPayload, START_INDEX);
            writeHeaderAndPayload(buffer, EnvelopeType.END, checksum, partialBytesPayload, START_INDEX);

            // pad the rest of the segment with zeros
            int remaining = segmentSize - buffer.position() % segmentSize;
            if (remaining > 0) {
                buffer.put(new byte[remaining]);
            }
        });

        try (var channel = new EnvelopeReadChannel(
                logChannel(), segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            assertThat(channel.alignWithStartEntry()).isEqualTo(segmentSize + fullEntrySize * 2 + partialEntrySize);
            assertThat(channel.position()).isEqualTo(segmentSize + fullEntrySize * 2 + partialEntrySize);
            var readData = new byte[1];
            assertThatThrownBy(() -> channel.get(readData, readData.length)).isInstanceOf(ReadPastEndException.class);
        }
    }

    @Test
    void markAndGetVersionShouldReturnPreEnvelopePosition() throws IOException {
        int segmentSize = 256;
        final var bytes = bytes(random, TEST_DATA_SIZE);
        final var bytes2 = bytes(random, TEST_DATA_SIZE);
        final byte TEST_VERSION_1 = (byte) 0x50;
        final byte TEST_VERSION_2 = (byte) 0x51;

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);
            var checksum = writeHeaderAndPayload(
                    buffer, EnvelopeType.FULL, BASE_TX_CHECKSUM, TEST_VERSION_1, bytes, START_INDEX + 1, CONTENT_TYPE);
            writeHeaderAndPayload(
                    buffer, EnvelopeType.FULL, checksum, TEST_VERSION_2, bytes2, START_INDEX + 2, CONTENT_TYPE);
        });

        try (var channel = new EnvelopeReadChannel(
                logChannel(), segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // immediately after open we expect the position to be ready to read
            // first envelope at position = segmentSize
            var marker1 = new LogPositionMarker();
            var logPosition1 = channel.getCurrentLogPosition();
            var version1 = channel.markAndGetVersion(marker1);
            assertThat(version1).isEqualTo(TEST_VERSION_1);
            var expected = new LogPosition(0L, segmentSize);
            assertThat(marker1.newPosition()).isEqualTo(expected);
            assertThat(logPosition1).isEqualTo(expected);

            // read first envelope
            final var bytesRead = new byte[TEST_DATA_SIZE];
            channel.get(bytesRead, bytesRead.length);
            assertThat(bytesRead).isEqualTo(bytes);

            // capture position of second envelope
            var marker2 = new LogPositionMarker();
            var logPosition3 = channel.getCurrentLogPosition();
            assertThat(logPosition3.getByteOffset()).isEqualTo(logPosition1.getByteOffset() + TEST_ENTRY_SIZE);
            var version2 = channel.markAndGetVersion(marker2);
            assertThat(version2).isEqualTo(TEST_VERSION_2);
            assertThat(marker2.newPosition()).isEqualTo(logPosition3);

            // read second envelope as last envelope of file
            channel.get(bytesRead, bytesRead.length);
            assertThat(bytesRead).isEqualTo(bytes2);

            var marker3 = new LogPositionMarker();
            // read ahead off end of file should fail
            assertThatThrownBy(() -> channel.markAndGetVersion(marker3)).isInstanceOf(ReadPastEndException.class);

            // rewind to second position
            channel.setLogPosition(marker2);
            // reread second envelope again
            channel.get(bytesRead, bytesRead.length);
            assertThat(bytesRead).isEqualTo(bytes2);

            // rewind to first position
            channel.setLogPosition(marker1);
            // reread first envelope again
            channel.get(bytesRead, bytesRead.length);
            assertThat(bytesRead).isEqualTo(bytes);
        }
    }

    @Test
    void markAndGetVersionShouldWorkOverFileBoundaries() throws IOException {
        int segmentSize = 256;
        final var bytes = bytes(random, TEST_DATA_SIZE);
        final var bytes2 = bytes(random, TEST_DATA_SIZE);
        final var bytes3 = bytes(random, TEST_DATA_SIZE);
        final var bytes4 = bytes(random, TEST_DATA_SIZE);
        final byte TEST_VERSION_1 = (byte) 0x50;
        final byte TEST_VERSION_2 = (byte) 0x51;
        final byte TEST_VERSION_3 = (byte) 0x52;
        final byte TEST_VERSION_4 = (byte) 0x53;

        final var checksum = new MutableInt(BASE_TX_CHECKSUM);
        var path1 = file(0);
        writeSomeData(path1, buffer -> {
            writeZeroSegment(buffer, segmentSize);
            checksum.setValue(writeHeaderAndPayload(
                    buffer,
                    EnvelopeType.FULL,
                    checksum.getValue(),
                    TEST_VERSION_1,
                    bytes,
                    START_INDEX + 1,
                    CONTENT_TYPE));
            checksum.setValue(writeHeaderAndPayload(
                    buffer,
                    EnvelopeType.FULL,
                    checksum.getValue(),
                    TEST_VERSION_2,
                    bytes2,
                    START_INDEX + 2,
                    CONTENT_TYPE));
        });

        var path2 = file(1);
        writeSomeData(path2, buffer -> {
            writeZeroSegment(buffer, segmentSize, checksum.getValue());
            checksum.setValue(writeHeaderAndPayload(
                    buffer,
                    EnvelopeType.FULL,
                    checksum.getValue(),
                    TEST_VERSION_3,
                    bytes3,
                    START_INDEX + 3,
                    CONTENT_TYPE));
            checksum.setValue(writeHeaderAndPayload(
                    buffer,
                    EnvelopeType.FULL,
                    checksum.getValue(),
                    TEST_VERSION_4,
                    bytes4,
                    START_INDEX + 4,
                    CONTENT_TYPE));
        });

        try (var channel = new EnvelopeReadChannel(
                logChannel(), segmentSize, new TwoFileLogVersionBridge(path2), EmptyMemoryTracker.INSTANCE, false)) {

            var marker1 = new LogPositionMarker();
            var version1 = channel.markAndGetVersion(marker1);
            assertThat(version1).isEqualTo(TEST_VERSION_1);
            assertThat(marker1.getByteOffset()).isEqualTo(segmentSize);
            assertThat(marker1.getLogVersion()).isZero();

            // read first envelope
            final var bytesRead = new byte[TEST_DATA_SIZE];
            channel.get(bytesRead, bytesRead.length);
            assertThat(bytesRead).isEqualTo(bytes);

            var marker2 = new LogPositionMarker();
            var version2 = channel.markAndGetVersion(marker2);
            assertThat(version2).isEqualTo(TEST_VERSION_2);
            assertThat(marker2.getByteOffset()).isEqualTo(marker1.getByteOffset() + TEST_ENTRY_SIZE);
            assertThat(marker2.getLogVersion()).isZero();

            // read second envelope as last envelope of file
            channel.get(bytesRead, bytesRead.length);
            assertThat(bytesRead).isEqualTo(bytes2);

            var marker3 = new LogPositionMarker();
            var version3 = channel.markAndGetVersion(marker3);
            assertThat(version3).isEqualTo(TEST_VERSION_3);
            assertThat(marker3.getByteOffset()).isEqualTo(segmentSize);
            assertThat(marker3.getLogVersion()).isOne();

            // read third envelope in new file
            channel.get(bytesRead, bytesRead.length);
            assertThat(bytesRead).isEqualTo(bytes3);

            var marker4 = new LogPositionMarker();
            var version4 = channel.markAndGetVersion(marker4);
            assertThat(version4).isEqualTo(TEST_VERSION_4);
            assertThat(marker4.getByteOffset()).isEqualTo(marker3.getByteOffset() + TEST_ENTRY_SIZE);
            assertThat(marker4.getLogVersion()).isOne();

            // read fourth envelope in new file
            channel.get(bytesRead, bytesRead.length);
            assertThat(bytesRead).isEqualTo(bytes4);

            // read ahead off end of file should fail
            assertThatThrownBy(() -> channel.markAndGetVersion(marker3)).isInstanceOf(ReadPastEndException.class);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldFailWhenChecksumMismatchesInFirstEnvelope(int segmentSize) throws Exception {
        // GIVEN
        final var bytes = bytes(random, segmentSize - HEADER_SIZE);

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);
            int checksum = writeHeaderAndPayload(buffer, EnvelopeType.FULL, BASE_TX_CHECKSUM, bytes, START_INDEX);
            // ensure we don't reach end of file in read
            var zero = new byte[1];
            writeHeaderAndPayload(buffer, EnvelopeType.FULL, checksum, zero, START_INDEX + 1);
        });

        // Then corrupt data in first envelope
        try (var channel = fileSystem.write(file(0))) {
            int randomPayloadPosition = random.nextInt(bytes.length);
            int modificationOffset = segmentSize // skip file header
                    + LogEnvelopeHeader.HEADER_SIZE // skip envelope header
                    + randomPayloadPosition;
            var buffer = ByteBuffer.allocate(1);
            // xor a modified value
            byte corrupted = (byte) (random.nextInt(1, 256) ^ bytes[randomPayloadPosition]);
            buffer.put(corrupted);
            buffer.flip();
            // modify file data
            channel.position(modificationOffset);
            channel.write(buffer);
        }

        final var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            final var bytesRead = new byte[bytes.length];
            assertThatThrownBy(() -> channel.get(bytesRead, bytesRead.length))
                    .isInstanceOf(ChecksumMismatchException.class)
                    .hasMessageContaining("checksum");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldFailWhenChecksumMismatchesInLaterEnvelope(int segmentSize) throws Exception {
        // GIVEN
        final var bytes = bytes(random, segmentSize - HEADER_SIZE);
        final var numberOfGoodEnvelopes = 5;
        var envelopeStartPosition = new MutableInt();

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);
            int checksum = BASE_TX_CHECKSUM;
            for (int i = 0; i < numberOfGoodEnvelopes; ++i) {
                checksum = writeHeaderAndPayload(buffer, EnvelopeType.FULL, checksum, bytes, START_INDEX + i);
            }
            // capture start of envelope we will corrupt
            envelopeStartPosition.setValue(buffer.position());
            // write the data we will modify
            checksum = writeHeaderAndPayload(
                    buffer, EnvelopeType.FULL, checksum, bytes, START_INDEX + numberOfGoodEnvelopes);

            // ensure we don't reach end of file in read
            var zero = new byte[1];
            writeHeaderAndPayload(buffer, EnvelopeType.FULL, checksum, zero, START_INDEX + 1);
        });

        // Then corrupt data
        try (var channel = fileSystem.write(file(0))) {
            int randomPayloadPosition = random.nextInt(bytes.length);
            int modificationOffset =
                    envelopeStartPosition.intValue() + LogEnvelopeHeader.HEADER_SIZE + randomPayloadPosition;
            var buffer = ByteBuffer.allocate(1);
            // xor a modified value
            byte corrupted = (byte) (random.nextInt(1, 256) ^ bytes[randomPayloadPosition]);
            buffer.put(corrupted);
            buffer.flip();
            // modify file data
            channel.position(modificationOffset);
            channel.write(buffer);
        }

        final var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            final var bytesRead = new byte[bytes.length];
            // earlier envelopes read ok
            for (int i = 0; i < numberOfGoodEnvelopes; ++i) {
                channel.get(bytesRead, bytesRead.length);
                assertThat(bytesRead).isEqualTo(bytes);
            }
            // should spot checksum mismatch
            assertThatThrownBy(() -> channel.get(bytesRead, bytesRead.length))
                    .isInstanceOf(ChecksumMismatchException.class)
                    .hasMessageContaining("checksum");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldRestartChecksumValidationAfterSetPosition(int segmentSize) throws Exception {
        // GIVEN
        final var bytes = bytes(random, segmentSize - HEADER_SIZE);
        final var numberOfGoodEnvelopes = 5;
        var envelopeStartPosition = new MutableInt();

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);
            int checksum = BASE_TX_CHECKSUM;
            for (int i = 0; i < numberOfGoodEnvelopes; ++i) {
                checksum = writeHeaderAndPayload(buffer, EnvelopeType.FULL, checksum, bytes, START_INDEX + i);
            }
            // capture start of envelope we will corrupt
            envelopeStartPosition.setValue(buffer.position());
            // write the data we will modify
            checksum = writeHeaderAndPayload(
                    buffer, EnvelopeType.FULL, checksum, bytes, START_INDEX + numberOfGoodEnvelopes);

            // ensure we don't reach end of file in read
            var zero = new byte[1];
            writeHeaderAndPayload(buffer, EnvelopeType.FULL, checksum, zero, START_INDEX + 1);
        });

        // Then corrupt data
        try (var channel = fileSystem.write(file(0))) {
            int randomPayloadPosition = random.nextInt(bytes.length);
            int modificationOffset =
                    envelopeStartPosition.intValue() + LogEnvelopeHeader.HEADER_SIZE + randomPayloadPosition;
            var buffer = ByteBuffer.allocate(1);
            // xor a modified value
            byte corrupted = (byte) (random.nextInt(1, 256) ^ bytes[randomPayloadPosition]);
            buffer.put(corrupted);
            buffer.flip();
            // modify file data
            channel.position(modificationOffset);
            channel.write(buffer);
        }

        final var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            final var bytesRead = new byte[bytes.length];
            // jump to just before corrupted envelope
            channel.position(envelopeStartPosition.intValue());
            // should spot checksum mismatch
            assertThatThrownBy(() -> channel.get(bytesRead, bytesRead.length))
                    .isInstanceOf(ChecksumMismatchException.class)
                    .hasMessageContaining("checksum");
        }
    }

    @Test
    void readFileWithZeroChecksumEnvelope() throws IOException {
        byte contentType = (byte) 1;
        var segmentSize = 256;
        var fit = (segmentSize / 3) - HEADER_SIZE;
        final var bytes1 = new byte[fit];
        ByteBuffer.wrap(bytes1)
                .order(BIG_ENDIAN)
                .position(bytes1.length - Integer.BYTES)
                .putInt(0x19cb1879);
        final var bytes2 = new byte[fit];
        ByteBuffer.wrap(bytes2)
                .order(BIG_ENDIAN)
                .position(bytes2.length - Integer.BYTES)
                .putInt(0x906538bf);
        final var bytes3 = new byte[fit];
        ByteBuffer.wrap(bytes3)
                .order(BIG_ENDIAN)
                .position(bytes3.length - Integer.BYTES)
                .putInt(0x2ab24cf5);
        final var bytes4 = new byte[segmentSize - HEADER_SIZE];
        ByteBuffer.wrap(bytes4)
                .order(BIG_ENDIAN)
                .position(bytes4.length - Integer.BYTES)
                .putInt(0xa0ab58e9);
        final var bytes5 = new byte[segmentSize - HEADER_SIZE];
        ByteBuffer.wrap(bytes5)
                .order(BIG_ENDIAN)
                .position(bytes5.length - Integer.BYTES)
                .putInt(0x2680b850);
        final var bytes6 = new byte[10];
        ByteBuffer.wrap(bytes6)
                .order(BIG_ENDIAN)
                .position(bytes6.length - Integer.BYTES)
                .putInt(0x734b7ab5);
        final var bytes7 = new byte[TEST_DATA_SIZE];
        ByteBuffer.wrap(bytes7)
                .order(BIG_ENDIAN)
                .position(bytes7.length - Integer.BYTES)
                .putInt(0x7c19290f);
        writeSomeData(buffer -> {
            int checksum = BASE_TX_CHECKSUM;
            writeZeroSegment(buffer, segmentSize);
            checksum = writeHeaderAndPayload(
                    buffer,
                    EnvelopeType.FULL,
                    checksum,
                    KernelVersion.V5_25.version(),
                    bytes1,
                    START_INDEX,
                    contentType);
            assertThat(checksum).isZero();
            checksum = writeHeaderAndPayload(
                    buffer,
                    EnvelopeType.FULL,
                    checksum,
                    KernelVersion.V5_25.version(),
                    bytes2,
                    START_INDEX + 1,
                    contentType);
            assertThat(checksum).isZero();
            checksum = writeHeaderAndPayload(
                    buffer,
                    EnvelopeType.FULL,
                    checksum,
                    KernelVersion.V5_25.version(),
                    bytes3,
                    START_INDEX + 2,
                    contentType);
            assertThat(checksum).isZero();
            // zero pad
            while (buffer.position() % segmentSize != 0) {
                buffer.put((byte) 0);
            }
            // write multiple envelope data that spans several segments
            checksum = writeHeaderAndPayload(
                    buffer,
                    EnvelopeType.BEGIN,
                    checksum,
                    KernelVersion.V5_25.version(),
                    bytes4,
                    START_INDEX + 3,
                    contentType);
            assertThat(checksum).isZero();
            checksum = writeHeaderAndPayload(
                    buffer,
                    EnvelopeType.MIDDLE,
                    checksum,
                    KernelVersion.V5_25.version(),
                    bytes5,
                    START_INDEX + 3,
                    contentType);
            assertThat(checksum).isZero();
            checksum = writeHeaderAndPayload(
                    buffer,
                    EnvelopeType.END,
                    checksum,
                    KernelVersion.V5_25.version(),
                    bytes6,
                    START_INDEX + 3,
                    contentType);
            assertThat(checksum).isZero();
        });

        var path2 = file(1);
        writeSomeData(path2, buffer -> {
            int checksum = 0;
            writeZeroSegment(buffer, segmentSize, checksum);
            checksum = writeHeaderAndPayload(
                    buffer,
                    EnvelopeType.FULL,
                    checksum,
                    KernelVersion.V5_25.version(),
                    bytes7,
                    START_INDEX + 4,
                    contentType);
            assertThat(checksum).isZero();
            // 'preallocate' some trailing zeros
            while (buffer.position() % segmentSize != 0) {
                buffer.put((byte) 0);
            }
            buffer.put((byte) 0);
            while (buffer.position() % segmentSize != 0) {
                buffer.put((byte) 0);
            }
        });

        var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, new TwoFileLogVersionBridge(path2), EmptyMemoryTracker.INSTANCE, false)) {
            // Read first full envelope in first data segment
            var readBytes1 = ByteBuffer.allocate(bytes1.length);
            channel.beginChecksum();
            channel.read(readBytes1);
            assertThat(channel.endChecksumAndValidate()).isZero();
            assertThat(readBytes1.array()).isEqualTo(bytes1);
            // Read second full envelope in first data segment
            var readBytes2 = ByteBuffer.allocate(bytes2.length);
            channel.beginChecksum();
            channel.read(readBytes2);
            assertThat(channel.endChecksumAndValidate()).isZero();
            assertThat(readBytes2.array()).isEqualTo(bytes2);
            // Read third full envelope in first data segment
            var readBytes3 = ByteBuffer.allocate(bytes3.length);
            channel.beginChecksum();
            channel.read(readBytes3);
            assertThat(channel.endChecksumAndValidate()).isZero();
            assertThat(readBytes3.array()).isEqualTo(bytes3);
            // Read data that spans multiple envelopes and segments as one and also skips past padding
            byte[] concat = new byte[bytes4.length + bytes5.length + bytes6.length];
            System.arraycopy(bytes4, 0, concat, 0, bytes4.length);
            System.arraycopy(bytes5, 0, concat, bytes4.length, bytes5.length);
            System.arraycopy(bytes6, 0, concat, bytes4.length + bytes5.length, bytes6.length);
            var readBytesSpanning = ByteBuffer.allocate(concat.length);
            channel.beginChecksum();
            channel.read(readBytesSpanning);
            assertThat(channel.endChecksumAndValidate()).isZero();
            assertThat(readBytesSpanning.array()).isEqualTo(concat);
            // Read data and bridge into next file with prevChecksum == 0
            var readBytes7 = ByteBuffer.allocate(bytes7.length);
            channel.beginChecksum();
            channel.read(readBytes7);
            assertThat(channel.endChecksumAndValidate()).isZero();
            assertThat(channel.logHeader().getPreviousLogFileChecksum()).isZero();
            assertThat(readBytes7.array()).isEqualTo(bytes7);
            var readBytes8 = ByteBuffer.allocate(4);
            // should correctly conclude rest of file is just preallocated zeros
            channel.beginChecksum();
            assertThatThrownBy(() -> channel.read(readBytes8)).isInstanceOf(ReadPastEndException.class);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldSkipWithinOneEnvelope(int segmentSize) throws Exception {
        // GIVEN
        final var bytesValue = bytes(random, 39);

        final var intValue = 55555;
        final var payloadLength = Integer.BYTES + bytesValue.length + Long.BYTES + Integer.BYTES;

        final var payloadChecksum = buildChecksum(
                EnvelopeType.FULL,
                payloadLength,
                BASE_TX_CHECKSUM,
                (buffer) -> buffer.putInt(intValue).put(bytesValue).putLong(1L).putInt(intValue));

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);
            writeLogEnvelopeHeader(
                    buffer, payloadChecksum, EnvelopeType.FULL, payloadLength, BASE_TX_CHECKSUM, START_INDEX);
            buffer.putInt(intValue);
            buffer.put(bytesValue);
            buffer.putLong(1L);
            buffer.putInt(intValue);
        });

        final var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN
            assertThat(intValue).isEqualTo(channel.getInt());
            channel.skip(bytesValue.length + Long.BYTES);
            assertThat(intValue).isEqualTo(channel.getInt());

            assertThat(payloadChecksum).isEqualTo(channel.getChecksum());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldSkipAcrossEnvelopes(int segmentSize) throws Exception {
        // GIVEN
        final var bytesValue = bytes(random, segmentSize + 20);
        final var beginChunkSize = segmentSize - HEADER_SIZE;

        var endChecksum = new MutableInt();

        writeSomeData(buffer -> {
            writeZeroSegment(buffer, segmentSize);

            var checksum = writeHeaderAndPayload(
                    buffer,
                    EnvelopeType.BEGIN,
                    BASE_TX_CHECKSUM,
                    copyOfRange(bytesValue, 0, beginChunkSize),
                    START_INDEX);
            checksum = writeHeaderAndPayload(
                    buffer,
                    EnvelopeType.END,
                    checksum,
                    copyOfRange(bytesValue, beginChunkSize, bytesValue.length),
                    START_INDEX);
            endChecksum.setValue(checksum);
        });

        final var logChannel = logChannel();
        try (var channel = new EnvelopeReadChannel(
                logChannel, segmentSize, NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            // THEN skip over into next envelope
            var skipInto = beginChunkSize + 10;
            channel.skip(skipInto);
            var remainingBytes = copyOfRange(bytesValue, skipInto, bytesValue.length);
            var readPart = new byte[bytesValue.length - skipInto];
            // remaining bytes should match
            channel.read(ByteBuffer.wrap(readPart));
            assertThat(remainingBytes).isEqualTo(readPart);

            assertThat(endChecksum.intValue()).isEqualTo(channel.getChecksum());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void shouldSkipAcrossLogFiles(int segmentSize) throws Exception {
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
            final var part2Skip = segment2Size / 2;
            final var skipInto = segment1Size + segment1Size + part2Skip;
            // skip into middle of final envelope in second file
            channel.skip(skipInto);
            final var bytesRead = new byte[segment2Size - part2Skip];
            channel.get(bytesRead, bytesRead.length);
            assertThat(bytesRead).isEqualTo(copyOfRange(bytes2, part2Skip, bytes2.length));
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
                    ByteBuffers.allocate(toIntExact(KibiByte.toBytes(2)), LITTLE_ENDIAN, EmptyMemoryTracker.INSTANCE);
            consumer.accept(buffer);
            buffer.flip();
            channel.writeAll(buffer);
            channel.flush();
        }
    }

    private int buildChecksum(
            EnvelopeType type, int payloadLength, int previousChecksum, Consumer<ByteBuffer> builder) {
        return buildChecksum(
                type,
                payloadLength,
                LatestVersions.LATEST_KERNEL_VERSION.version(),
                previousChecksum,
                builder,
                START_INDEX);
    }

    private static ByteBuffer buildChecksumContent(
            EnvelopeType type,
            int payloadLength,
            byte kernelVersion,
            int previousChecksum,
            Consumer<ByteBuffer> builder,
            long entryIndex,
            byte contentType,
            long term) {
        final var buffer = ByteBuffer.allocate(HEADER_SIZE - Integer.BYTES + payloadLength)
                .order(LITTLE_ENDIAN)
                // header data
                .put(type.typeValue)
                .putInt(payloadLength)
                .putLong(entryIndex)
                .put(kernelVersion)
                .putInt(previousChecksum)
                .putLong(term)
                .put(contentType);
        builder.accept(buffer);
        return buffer.flip();
    }

    private static int buildChecksum(
            Checksum checksum,
            EnvelopeType type,
            int previousChecksum,
            byte kernelVersion,
            byte[] payload,
            long entryIndex,
            byte contentType,
            long term) {
        return buildChecksum(
                checksum,
                buildChecksumContent(
                        type,
                        payload.length,
                        kernelVersion,
                        previousChecksum,
                        (buffer) -> buffer.put(payload),
                        entryIndex,
                        contentType,
                        term));
    }

    private int buildChecksum(
            EnvelopeType type,
            int payloadLength,
            byte kernelVersion,
            int previousChecksum,
            Consumer<ByteBuffer> builder,
            long entryIndex) {
        final var buffer = buildChecksumContent(
                type, payloadLength, kernelVersion, previousChecksum, builder, entryIndex, CONTENT_TYPE, TERM);
        return buildChecksum(checksum, buffer);
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
        writeLogEnvelopeHeader(
                buffer,
                payloadChecksum,
                type,
                payloadLength,
                LatestVersions.LATEST_KERNEL_VERSION.version(),
                previousChecksum,
                entryIndex,
                CONTENT_TYPE,
                TERM);
    }

    private int writeHeaderAndPayload(
            ByteBuffer buffer, EnvelopeType type, int previousChecksum, byte[] payload, long startIndex) {
        return writeHeaderAndPayload(
                buffer,
                type,
                previousChecksum,
                LatestVersions.LATEST_KERNEL_VERSION.version(),
                payload,
                startIndex,
                CONTENT_TYPE);
    }

    private int writeHeaderAndPayload(
            ByteBuffer buffer,
            EnvelopeType type,
            int previousChecksum,
            byte kernelVersion,
            byte[] payload,
            long startIndex,
            byte contentType) {
        final var payloadChecksum =
                buildChecksum(checksum, type, previousChecksum, kernelVersion, payload, startIndex, contentType, TERM);
        return writeHeaderAndPayload(
                buffer, type, previousChecksum, payloadChecksum, kernelVersion, payload, startIndex, contentType, TERM);
    }

    private int writeHeaderAndPayload(
            ByteBuffer buffer,
            EnvelopeType type,
            int previousChecksum,
            int payloadChecksum,
            byte kernelVersion,
            byte[] payload,
            long entryIndex,
            byte contentType,
            long term) {
        writeLogEnvelopeHeader(
                buffer,
                payloadChecksum,
                type,
                payload.length,
                kernelVersion,
                previousChecksum,
                entryIndex,
                contentType,
                term);
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
            long entryIndex,
            byte contentType,
            long term) {
        buffer.putInt(payloadChecksum)
                .put(type.typeValue)
                .putInt(payloadLength)
                .putLong(entryIndex)
                .put(kernelVersion)
                .putInt(previousChecksum)
                .putLong(term)
                .put(contentType);
    }

    private static class RawCapturingLogVersionBridge implements LogVersionBridge {
        private boolean isRaw = false;

        @Override
        public LogVersionedStoreChannel next(LogVersionedStoreChannel channel, boolean raw) {
            isRaw = raw;
            return channel;
        }
    }

    /**
     * Get a PhysicalLogVersionedStoreChannel to log on file(0).
     * Shortcut for the most common use, see {@link #logChannel(Path)} for other files.
     */
    private PhysicalLogVersionedStoreChannel logChannel() throws IOException {
        return logChannel(file(0));
    }

    private PhysicalLogVersionedStoreChannel logChannel(Path file) throws IOException {
        return logChannel(file, 0L);
    }

    private PhysicalLogVersionedStoreChannel logChannel(Path file, long fileVersion) throws IOException {
        return new PhysicalLogVersionedStoreChannel(
                fileSystem.write(file),
                fileVersion,
                LatestVersions.LATEST_LOG_FORMAT,
                file,
                ChannelNativeAccessor.EMPTY_ACCESSOR,
                LogTracers.NULL);
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

                return logChannel(path, 1L);
            }
            return channel;
        }
    }
}
