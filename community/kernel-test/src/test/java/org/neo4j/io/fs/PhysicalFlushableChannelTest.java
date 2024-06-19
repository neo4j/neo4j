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
package org.neo4j.io.fs;

import static java.util.Arrays.copyOfRange;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.neo4j.io.ByteUnit.bytes;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.io.fs.ChecksumWriter.CHECKSUM_FACTORY;
import static org.neo4j.io.memory.HeapScopedBuffer.EMPTY_BUFFER;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.test.LatestVersions.LATEST_KERNEL_VERSION;
import static org.neo4j.test.LatestVersions.LATEST_LOG_FORMAT;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import java.util.zip.Checksum;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.io.memory.ScopedBuffer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.api.tracer.DefaultTracer;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalFlushableLogPositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.files.LogFileChannelNativeAccessor;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class PhysicalFlushableChannelTest {
    @Inject
    private DefaultFileSystemAbstraction fileSystem;

    @Inject
    private TestDirectory directory;

    private final LogFileChannelNativeAccessor nativeChannelAccessor = mock(LogFileChannelNativeAccessor.class);
    private final DatabaseTracer databaseTracer = DatabaseTracer.NULL;

    @Test
    void countChannelFlushEvents() throws IOException {
        var path = directory.homePath().resolve("countChannelFlushEvents");
        var storeChannel = fileSystem.write(path);
        DefaultTracer databaseTracer = new DefaultTracer(PageCacheTracer.NULL);
        try (var channel = new PhysicalLogVersionedStoreChannel(
                storeChannel, 1, LATEST_LOG_FORMAT, path, nativeChannelAccessor, databaseTracer, true)) {
            channel.flush();
            channel.flush();
            channel.flush();
        }
        assertEquals(3, databaseTracer.numberOfFlushes());
    }

    @Test
    void rawChannelDoesNotEvictDataOnClose() throws IOException {
        var rawPath = directory.homePath().resolve("fileRaw");
        var storeChannel = fileSystem.write(rawPath);
        try (var channel = new PhysicalLogVersionedStoreChannel(
                storeChannel, 1, LATEST_LOG_FORMAT, rawPath, nativeChannelAccessor, databaseTracer, true)) {
            // empty
        }
        verifyNoInteractions(nativeChannelAccessor);
    }

    @Test
    void shouldBeAbleToWriteSmallNumberOfBytes() throws IOException {
        final Path firstFile = directory.homePath().resolve("file1");
        StoreChannel storeChannel = fileSystem.write(firstFile);
        PhysicalLogVersionedStoreChannel versionedStoreChannel = new PhysicalLogVersionedStoreChannel(
                storeChannel, 1, LATEST_LOG_FORMAT, firstFile, nativeChannelAccessor, databaseTracer);
        int length = 26_145;
        byte[] bytes;
        try (PhysicalFlushableChannel channel = new PhysicalFlushableChannel(
                versionedStoreChannel, new HeapScopedBuffer(100, ByteOrder.LITTLE_ENDIAN, INSTANCE))) {
            bytes = generateBytes(length);
            channel.put(bytes, length);
        }

        byte[] writtenBytes = Files.readAllBytes(firstFile);
        assertArrayEquals(bytes, writtenBytes);
    }

    @Test
    void shouldBeAbleToWriteValuesGreaterThanHalfTheBufferSize() throws IOException {
        final Path firstFile = directory.homePath().resolve("file1");
        StoreChannel storeChannel = fileSystem.write(firstFile);
        PhysicalLogVersionedStoreChannel versionedStoreChannel = new PhysicalLogVersionedStoreChannel(
                storeChannel, 1, LATEST_LOG_FORMAT, firstFile, nativeChannelAccessor, databaseTracer);
        int length = 262_145;
        byte[] bytes;
        try (PhysicalFlushableChannel channel = new PhysicalFlushableChannel(versionedStoreChannel, INSTANCE)) {
            bytes = generateBytes(length);
            channel.put(bytes, length);
        }

        byte[] writtenBytes = Files.readAllBytes(firstFile);
        assertArrayEquals(bytes, writtenBytes);
    }

    @Test
    void releaseBufferMemoryOnClose() throws IOException {
        var memoryTracker = new LocalMemoryTracker();
        final Path firstFile = directory.homePath().resolve("file2");
        StoreChannel storeChannel = fileSystem.write(firstFile);
        PhysicalLogVersionedStoreChannel versionedStoreChannel = new PhysicalLogVersionedStoreChannel(
                storeChannel, 1, LATEST_LOG_FORMAT, firstFile, nativeChannelAccessor, databaseTracer);

        assertThat(memoryTracker.estimatedHeapMemory()).isZero();
        assertThat(memoryTracker.usedNativeMemory()).isZero();

        try (PhysicalFlushableChannel channel = new PhysicalFlushableChannel(versionedStoreChannel, memoryTracker)) {
            channel.put((byte) 1);
            assertThat(memoryTracker.usedNativeMemory()).isZero();
            assertThat(memoryTracker.estimatedHeapMemory()).isGreaterThan(0);
        }

        assertThat(memoryTracker.estimatedHeapMemory()).isZero();
        assertThat(memoryTracker.usedNativeMemory()).isZero();
    }

    @Test
    void shouldBeAbleToWriteValuesGreaterThanTheBufferSize() throws IOException {
        final Path firstFile = directory.homePath().resolve("file1");
        StoreChannel storeChannel = fileSystem.write(firstFile);
        PhysicalLogVersionedStoreChannel versionedStoreChannel = new PhysicalLogVersionedStoreChannel(
                storeChannel, 1, LATEST_LOG_FORMAT, firstFile, nativeChannelAccessor, databaseTracer);
        int length = 1_000_000;
        byte[] bytes;
        try (PhysicalFlushableChannel channel = new PhysicalFlushableChannel(versionedStoreChannel, INSTANCE)) {
            bytes = generateBytes(length);
            channel.put(bytes, length);
        }

        byte[] writtenBytes = Files.readAllBytes(firstFile);
        assertArrayEquals(bytes, writtenBytes);
    }

    @Test
    void writeSmallByteBuffer() throws IOException {
        final Path firstFile = directory.homePath().resolve("file1");
        StoreChannel storeChannel = fileSystem.write(firstFile);
        PhysicalLogVersionedStoreChannel versionedStoreChannel = new PhysicalLogVersionedStoreChannel(
                storeChannel, 1, LATEST_LOG_FORMAT, firstFile, nativeChannelAccessor, databaseTracer);

        ByteBuffer smallBuffer = ByteBuffer.wrap(generateBytes(100));
        try (PhysicalFlushableChannel channel = new PhysicalFlushableChannel(versionedStoreChannel, INSTANCE)) {
            channel.putAll(smallBuffer);
        }
        byte[] writtenBytes = Files.readAllBytes(firstFile);
        assertArrayEquals(smallBuffer.array(), writtenBytes);
    }

    @Test
    void writeLargerThanBufferByteBuffer() throws IOException {
        final Path firstFile = directory.homePath().resolve("file1");
        StoreChannel storeChannel = fileSystem.write(firstFile);
        PhysicalLogVersionedStoreChannel versionedStoreChannel = new PhysicalLogVersionedStoreChannel(
                storeChannel, 1, LATEST_LOG_FORMAT, firstFile, nativeChannelAccessor, databaseTracer);

        int bufferSize = 512;
        ByteBuffer largeBuffer = ByteBuffer.wrap(generateBytes(bufferSize * 2));
        HeapScopedBuffer scopedBuffer = new HeapScopedBuffer(bufferSize, ByteOrder.LITTLE_ENDIAN, INSTANCE);
        try (PhysicalFlushableChannel channel = new PhysicalFlushableChannel(versionedStoreChannel, scopedBuffer)) {
            channel.putAll(largeBuffer);
        }
        byte[] writtenBytes = Files.readAllBytes(firstFile);
        assertArrayEquals(largeBuffer.array(), writtenBytes);
    }

    @Test
    void writeByteBufferShouldNotClobberExistingData() throws IOException {
        final Path firstFile = directory.homePath().resolve("file1");
        StoreChannel storeChannel = fileSystem.write(firstFile);
        PhysicalLogVersionedStoreChannel versionedStoreChannel = new PhysicalLogVersionedStoreChannel(
                storeChannel, 1, LATEST_LOG_FORMAT, firstFile, nativeChannelAccessor, databaseTracer);

        int bufferSize = 512;
        int small = 64;
        byte[] smallArray = generateBytes(small);
        ByteBuffer largeBuffer = ByteBuffer.wrap(generateBytes(bufferSize * 2));
        HeapScopedBuffer scopedBuffer = new HeapScopedBuffer(bufferSize, ByteOrder.LITTLE_ENDIAN, INSTANCE);
        try (PhysicalFlushableChannel channel = new PhysicalFlushableChannel(versionedStoreChannel, scopedBuffer)) {
            channel.put(smallArray, smallArray.length);
            channel.putAll(largeBuffer);
        }
        byte[] writtenBytes = Files.readAllBytes(firstFile);
        assertArrayEquals(smallArray, copyOfRange(writtenBytes, 0, small));
        assertArrayEquals(largeBuffer.array(), copyOfRange(writtenBytes, small, small + largeBuffer.capacity()));
    }

    @MethodSource("bytesToChannelParameters")
    @ParameterizedTest
    void writeBytesOverChannel(int length, ScopedBuffer buffer) throws IOException {
        var file = directory.homePath().resolve("fileWithBytes");
        StoreChannel storeChannel = fileSystem.write(file);
        PhysicalLogVersionedStoreChannel versionedStoreChannel = new PhysicalLogVersionedStoreChannel(
                storeChannel, 1, LATEST_LOG_FORMAT, file, nativeChannelAccessor, databaseTracer);
        byte[] bytes = generateBytes(length);
        try (PhysicalFlushableChannel channel = new PhysicalFlushableChannel(versionedStoreChannel, buffer)) {
            channel.put(bytes, length);
        }

        byte[] writtenBytes = Files.readAllBytes(file);
        assertArrayEquals(bytes, writtenBytes);
    }

    @Test
    void shouldWriteThroughRotation() throws Exception {
        // GIVEN
        final Path firstFile = directory.homePath().resolve("file1");
        final Path secondFile = directory.homePath().resolve("file2");
        StoreChannel storeChannel = fileSystem.write(firstFile);
        PhysicalLogVersionedStoreChannel versionedStoreChannel = new PhysicalLogVersionedStoreChannel(
                storeChannel, 1, LATEST_LOG_FORMAT, firstFile, nativeChannelAccessor, databaseTracer);
        PhysicalFlushableLogChannel channel = new PhysicalFlushableLogChannel(
                versionedStoreChannel, new HeapScopedBuffer(100, ByteOrder.LITTLE_ENDIAN, INSTANCE));

        // WHEN writing a transaction, of sorts
        byte byteValue = (byte) 4;
        short shortValue = (short) 10;
        int intValue = 3545;
        long longValue = 45849589L;
        float floatValue = 45849.332f;
        double doubleValue = 458493343D;
        byte[] byteArrayValue = new byte[] {1, 4, 2, 5, 3, 6};

        channel.put(byteValue);
        channel.putShort(shortValue);
        channel.putInt(intValue);
        channel.putLong(longValue);
        channel.prepareForFlush().flush();
        versionedStoreChannel.close();

        // "Rotate" and continue
        storeChannel = fileSystem.write(secondFile);
        channel.setChannel(new PhysicalLogVersionedStoreChannel(
                storeChannel, 2, LATEST_LOG_FORMAT, secondFile, nativeChannelAccessor, databaseTracer));
        channel.putFloat(floatValue);
        channel.putDouble(doubleValue);
        channel.put(byteArrayValue, byteArrayValue.length);
        channel.close();

        // The two chunks of values should end up in two different files
        ByteBuffer firstFileContents = readFile(firstFile);
        assertEquals(byteValue, firstFileContents.get());
        assertEquals(shortValue, firstFileContents.getShort());
        assertEquals(intValue, firstFileContents.getInt());
        assertEquals(longValue, firstFileContents.getLong());
        ByteBuffer secondFileContents = readFile(secondFile);
        assertEquals(floatValue, secondFileContents.getFloat(), 0.001f);
        assertEquals(doubleValue, secondFileContents.getDouble(), 0.001d);

        byte[] readByteArray = new byte[byteArrayValue.length];
        secondFileContents.get(readByteArray);
        assertArrayEquals(byteArrayValue, readByteArray);
    }

    @Test
    void shouldSeeCorrectPositionEvenBeforeEmptyingDataIntoChannel() throws Exception {
        // GIVEN
        final Path file = directory.homePath().resolve("file");
        StoreChannel storeChannel = fileSystem.write(file);
        PhysicalLogVersionedStoreChannel versionedStoreChannel = new PhysicalLogVersionedStoreChannel(
                storeChannel, 1, LATEST_LOG_FORMAT, file, nativeChannelAccessor, databaseTracer);
        final var logHeader =
                LATEST_LOG_FORMAT.newHeader(1, 1, StoreId.UNKNOWN, 1024, BASE_TX_CHECKSUM, LATEST_KERNEL_VERSION);
        try (var channel = new PhysicalFlushableLogPositionAwareChannel(versionedStoreChannel, logHeader, INSTANCE)) {
            LogPosition initialPosition = channel.getCurrentLogPosition();

            // WHEN
            channel.putLong(67);
            channel.putInt(1234);
            LogPosition positionAfterSomeData = channel.getCurrentLogPosition();

            // THEN
            assertEquals(12, positionAfterSomeData.getByteOffset() - initialPosition.getByteOffset());
        }
    }

    @Test
    void shouldThrowIllegalStateExceptionOnFlushAfterClosed() throws Exception {
        // GIVEN
        final Path file = directory.homePath().resolve("file");
        StoreChannel storeChannel = fileSystem.write(file);
        PhysicalLogVersionedStoreChannel versionedStoreChannel = new PhysicalLogVersionedStoreChannel(
                storeChannel, 1, LATEST_LOG_FORMAT, file, nativeChannelAccessor, databaseTracer);
        PhysicalFlushableChannel channel = new PhysicalFlushableChannel(versionedStoreChannel, INSTANCE);
        channel.put((byte) 1);

        // closing the WritableLogChannel, then the underlying channel is what PhysicalLogFile does
        channel.close();
        storeChannel.close();

        // WHEN wanting to empty buffer into the channel
        assertThatThrownBy(() -> channel.flushToChannel(storeChannel, EMPTY_BUFFER.getBuffer()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("This log channel has been closed");
    }

    @Test
    void shouldThrowsOnWriteAfterClosed() throws Exception {
        // GIVEN
        final Path file = directory.homePath().resolve("file");
        StoreChannel storeChannel = fileSystem.write(file);
        PhysicalLogVersionedStoreChannel versionedStoreChannel = new PhysicalLogVersionedStoreChannel(
                storeChannel, 1, LATEST_LOG_FORMAT, file, nativeChannelAccessor, databaseTracer);
        PhysicalFlushableChannel channel = new PhysicalFlushableChannel(versionedStoreChannel, INSTANCE);

        // closing the WritableLogChannel, then the underlying channel is what PhysicalLogFile does
        channel.close();
        storeChannel.close();

        // WHEN just appending something to the buffer
        // attempt to write to closed channel throws different exceptions if assertions enabled or not
        assertThatThrownBy(() -> channel.put((byte) 0)).isInstanceOf(Throwable.class);
    }

    @Test
    void shouldThrowClosedChannelExceptionWhenChannelUnexpectedlyClosed() throws Exception {
        // GIVEN
        final Path file = directory.homePath().resolve("file");
        StoreChannel storeChannel = fileSystem.write(file);
        PhysicalLogVersionedStoreChannel versionedStoreChannel = new PhysicalLogVersionedStoreChannel(
                storeChannel, 1, LATEST_LOG_FORMAT, file, nativeChannelAccessor, databaseTracer);
        PhysicalFlushableChannel channel = new PhysicalFlushableChannel(versionedStoreChannel, INSTANCE);

        // just close the underlying channel
        storeChannel.close();

        // WHEN just appending something to the buffer
        channel.put((byte) 0);
        // and wanting to empty that into the channel
        assertThrows(ClosedChannelException.class, channel::prepareForFlush);
    }

    @Test
    void calculateChecksum() throws IOException {
        final Path firstFile = directory.homePath().resolve("file1");
        StoreChannel storeChannel = fileSystem.write(firstFile);
        int channelChecksum;
        try (var channel = new PhysicalFlushableLogChannel(
                storeChannel, new HeapScopedBuffer(100, ByteOrder.LITTLE_ENDIAN, INSTANCE))) {
            channel.beginChecksumForWriting();
            channel.put((byte) 10);
            channelChecksum = channel.putChecksum();
        }

        int fileSize = (int) fileSystem.getFileSize(firstFile);
        assertEquals(Byte.BYTES + Integer.BYTES, fileSize);
        byte[] writtenBytes = new byte[fileSize];
        try (InputStream in = Files.newInputStream(firstFile)) {
            in.read(writtenBytes);
        }
        ByteBuffer buffer = ByteBuffer.wrap(writtenBytes).order(ByteOrder.LITTLE_ENDIAN);

        Checksum checksum = CHECKSUM_FACTORY.get();
        checksum.update(10);

        assertEquals(checksum.getValue(), channelChecksum);
        assertEquals(10, buffer.get());
        assertEquals(checksum.getValue(), buffer.getInt());
    }

    @Test
    void beginChecksumShouldResetCalculations() throws IOException {
        final Path firstFile = directory.homePath().resolve("file1");
        StoreChannel storeChannel = fileSystem.write(firstFile);
        int channelChecksum;
        try (var channel = new PhysicalFlushableLogChannel(
                storeChannel, new HeapScopedBuffer(100, ByteOrder.LITTLE_ENDIAN, INSTANCE))) {
            channel.put((byte) 5);
            channel.beginChecksumForWriting();
            channel.put((byte) 10);
            channelChecksum = channel.putChecksum();
        }

        int fileSize = (int) fileSystem.getFileSize(firstFile);
        assertEquals(Byte.BYTES + Byte.BYTES + Integer.BYTES, fileSize);
        byte[] writtenBytes = new byte[fileSize];
        try (InputStream in = Files.newInputStream(firstFile)) {
            in.read(writtenBytes);
        }
        ByteBuffer buffer = ByteBuffer.wrap(writtenBytes).order(ByteOrder.LITTLE_ENDIAN);

        Checksum checksum = CHECKSUM_FACTORY.get();
        checksum.update(10);

        assertEquals(checksum.getValue(), channelChecksum);
        assertEquals(5, buffer.get());
        assertEquals(10, buffer.get());
        assertEquals(checksum.getValue(), buffer.getInt());
    }

    private ByteBuffer readFile(Path file) throws IOException {
        try (StoreChannel channel = fileSystem.read(file)) {
            ByteBuffer buffer = ByteBuffers.allocate((int) channel.size(), ByteOrder.LITTLE_ENDIAN, INSTANCE);
            channel.readAll(buffer);
            buffer.flip();
            return buffer;
        }
    }

    private static Stream<Arguments> bytesToChannelParameters() {
        return Stream.of(
                Arguments.of(128, new HeapScopedBuffer(128, ByteOrder.LITTLE_ENDIAN, INSTANCE)),
                Arguments.of(256, new HeapScopedBuffer(128, ByteOrder.LITTLE_ENDIAN, INSTANCE)),
                Arguments.of(258, new HeapScopedBuffer(128, ByteOrder.LITTLE_ENDIAN, INSTANCE)),
                Arguments.of(512, new HeapScopedBuffer(128, ByteOrder.LITTLE_ENDIAN, INSTANCE)),
                Arguments.of(12, new HeapScopedBuffer(512, ByteOrder.LITTLE_ENDIAN, INSTANCE)),
                Arguments.of(120, new HeapScopedBuffer(512, ByteOrder.LITTLE_ENDIAN, INSTANCE)),
                Arguments.of(1200, new HeapScopedBuffer(512, ByteOrder.LITTLE_ENDIAN, INSTANCE)),
                Arguments.of(512 * 3 + 1, new HeapScopedBuffer(512, ByteOrder.LITTLE_ENDIAN, INSTANCE)),
                Arguments.of(512 * 3 - 1, new HeapScopedBuffer(512, ByteOrder.LITTLE_ENDIAN, INSTANCE)),
                Arguments.of((int) kibiBytes(24), new HeapScopedBuffer(512, ByteOrder.LITTLE_ENDIAN, INSTANCE)),
                Arguments.of((int) kibiBytes(1024), new HeapScopedBuffer(512, ByteOrder.LITTLE_ENDIAN, INSTANCE)),
                Arguments.of((int) kibiBytes(5024), new HeapScopedBuffer(512, ByteOrder.LITTLE_ENDIAN, INSTANCE)),
                Arguments.of((int) kibiBytes(10024), new HeapScopedBuffer(512, ByteOrder.LITTLE_ENDIAN, INSTANCE)),
                Arguments.of((int) kibiBytes(10024), new HeapScopedBuffer(1024, ByteOrder.LITTLE_ENDIAN, INSTANCE)),
                Arguments.of((int) kibiBytes(11024), new HeapScopedBuffer(1024, ByteOrder.LITTLE_ENDIAN, INSTANCE)),
                Arguments.of((int) kibiBytes(11424), new HeapScopedBuffer(1024, ByteOrder.LITTLE_ENDIAN, INSTANCE)),
                Arguments.of(
                        (int) bytes(ThreadLocalRandom.current().nextInt(1024, (int) mebiBytes(100))),
                        new HeapScopedBuffer(
                                ThreadLocalRandom.current().nextInt(128, (int) mebiBytes(2)),
                                ByteOrder.LITTLE_ENDIAN,
                                INSTANCE)));
    }

    private static byte[] generateBytes(int length) {
        Random random = new Random();
        char[] validCharacters = new char[] {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o'};
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte) validCharacters[random.nextInt(validCharacters.length)];
        }
        return bytes;
    }
}
