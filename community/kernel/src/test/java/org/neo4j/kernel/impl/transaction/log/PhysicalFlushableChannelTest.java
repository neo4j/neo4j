/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.log;

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
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.PhysicalFlushableChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.io.memory.NativeScopedBuffer;
import org.neo4j.io.memory.ScopedBuffer;
import org.neo4j.kernel.impl.api.tracer.DefaultTracer;
import org.neo4j.kernel.impl.transaction.log.files.LogFileChannelNativeAccessor;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.memory.LocalMemoryTracker;
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
        DefaultTracer databaseTracer = new DefaultTracer();
        try (var channel = new PhysicalLogVersionedStoreChannel(
                storeChannel, 1, (byte) -1, path, nativeChannelAccessor, databaseTracer, true)) {
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
                storeChannel, 1, (byte) -1, rawPath, nativeChannelAccessor, databaseTracer, true)) {
            // empty
        }
        verifyNoInteractions(nativeChannelAccessor);
    }

    @Test
    void shouldBeAbleToWriteSmallNumberOfBytes() throws IOException {
        final Path firstFile = directory.homePath().resolve("file1");
        StoreChannel storeChannel = fileSystem.write(firstFile);
        PhysicalLogVersionedStoreChannel versionedStoreChannel = new PhysicalLogVersionedStoreChannel(
                storeChannel, 1, (byte) -1, firstFile, nativeChannelAccessor, databaseTracer);
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
                storeChannel, 1, (byte) -1, firstFile, nativeChannelAccessor, databaseTracer);
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
                storeChannel, 1, (byte) -1, firstFile, nativeChannelAccessor, databaseTracer);

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
                storeChannel, 1, (byte) -1, firstFile, nativeChannelAccessor, databaseTracer);
        int length = 1_000_000;
        byte[] bytes;
        try (PhysicalFlushableChannel channel = new PhysicalFlushableChannel(versionedStoreChannel, INSTANCE)) {
            bytes = generateBytes(length);
            channel.put(bytes, length);
        }

        byte[] writtenBytes = Files.readAllBytes(firstFile);
        assertArrayEquals(bytes, writtenBytes);
    }

    @MethodSource("bytesToChannelParameters")
    @ParameterizedTest
    void writeBytesOverChannel(int length, ScopedBuffer buffer) throws IOException {
        var file = directory.homePath().resolve("fileWithBytes");
        StoreChannel storeChannel = fileSystem.write(file);
        PhysicalLogVersionedStoreChannel versionedStoreChannel = new PhysicalLogVersionedStoreChannel(
                storeChannel, 1, (byte) -1, file, nativeChannelAccessor, databaseTracer);
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
                storeChannel, 1, (byte) -1, firstFile, nativeChannelAccessor, databaseTracer);
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
                storeChannel, 2, (byte) -1, secondFile, nativeChannelAccessor, databaseTracer));
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
                storeChannel, 1, (byte) -1, file, nativeChannelAccessor, databaseTracer);
        try (var channel = new PositionAwarePhysicalFlushableChecksumChannel(
                versionedStoreChannel, new NativeScopedBuffer(1024, ByteOrder.LITTLE_ENDIAN, INSTANCE))) {
            LogPosition initialPosition = channel.getCurrentPosition();

            // WHEN
            channel.putLong(67);
            channel.putInt(1234);
            LogPosition positionAfterSomeData = channel.getCurrentPosition();

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
                storeChannel, 1, (byte) -1, file, nativeChannelAccessor, databaseTracer);
        PhysicalFlushableChannel channel = new PhysicalFlushableChannel(versionedStoreChannel, INSTANCE);

        // closing the WritableLogChannel, then the underlying channel is what PhysicalLogFile does
        channel.close();
        storeChannel.close();

        // WHEN wanting to empty buffer into the channel
        assertThatThrownBy(channel::prepareForFlush)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("This log channel has been closed");
    }

    @Test
    void shouldThrowsOnWriteAfterClosed() throws Exception {
        // GIVEN
        final Path file = directory.homePath().resolve("file");
        StoreChannel storeChannel = fileSystem.write(file);
        PhysicalLogVersionedStoreChannel versionedStoreChannel = new PhysicalLogVersionedStoreChannel(
                storeChannel, 1, (byte) -1, file, nativeChannelAccessor, databaseTracer);
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
                storeChannel, 1, (byte) -1, file, nativeChannelAccessor, databaseTracer);
        PhysicalFlushableChannel channel = new PhysicalFlushableChannel(versionedStoreChannel, INSTANCE);

        // just close the underlying channel
        storeChannel.close();

        // WHEN just appending something to the buffer
        channel.put((byte) 0);
        // and wanting to empty that into the channel
        assertThrows(ClosedChannelException.class, channel::prepareForFlush);
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
