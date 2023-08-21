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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.io.fs.ChecksumWriter.CHECKSUM_FACTORY;
import static org.neo4j.io.fs.ReadAheadChannel.DEFAULT_READ_AHEAD_SIZE;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.zip.Checksum;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.io.fs.ChecksumMismatchException;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.ReadAheadChannel;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.io.memory.NativeScopedBuffer;
import org.neo4j.io.memory.ScopedBuffer;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith({EphemeralFileSystemExtension.class, RandomExtension.class})
class ReadAheadChannelTest {
    @Inject
    private EphemeralFileSystemAbstraction fileSystem;

    @Inject
    private RandomSupport random;

    @ParameterizedTest
    @EnumSource(Constructors.class)
    void shouldThrowExceptionForReadAfterEOFIfNotEnoughBytesExist(Constructor constructor) throws Exception {
        // Given
        Path bytesReadTestFile = Path.of("bytesReadTest.txt");
        try (StoreChannel storeChannel = fileSystem.write(bytesReadTestFile)) {
            ByteBuffer buffer = ByteBuffers.allocate(1, ByteOrder.LITTLE_ENDIAN, INSTANCE);
            buffer.put((byte) 1);
            buffer.flip();
            storeChannel.writeAll(buffer);
            storeChannel.force(false);
        }

        ReadAheadChannel<StoreChannel> channel =
                constructor.apply(fileSystem.read(bytesReadTestFile), DEFAULT_READ_AHEAD_SIZE);
        assertEquals((byte) 1, channel.get());

        assertThrows(ReadPastEndException.class, channel::get);
        assertThrows(ReadPastEndException.class, channel::get);
    }

    @ParameterizedTest
    @EnumSource(Constructors.class)
    void shouldReturnValueIfSufficientBytesAreBufferedEvenIfEOFHasBeenEncountered(Constructor constructor)
            throws Exception {
        // Given
        Path shortReadTestFile = Path.of("shortReadTest.txt");
        try (StoreChannel storeChannel = fileSystem.write(shortReadTestFile)) {
            ByteBuffer buffer = ByteBuffers.allocate(1, ByteOrder.LITTLE_ENDIAN, INSTANCE);
            buffer.put((byte) 1);
            buffer.flip();
            storeChannel.writeAll(buffer);
            storeChannel.force(false);
        }

        ReadAheadChannel<StoreChannel> channel =
                constructor.apply(fileSystem.read(shortReadTestFile), DEFAULT_READ_AHEAD_SIZE);
        assertThrows(ReadPastEndException.class, channel::getShort);
        assertEquals((byte) 1, channel.get());
        assertThrows(ReadPastEndException.class, channel::get);
    }

    @ParameterizedTest
    @EnumSource(Constructors.class)
    void shouldHandleRunningOutOfBytesWhenRequestSpansMultipleFiles(Constructor constructor) throws Exception {
        // Given

        Path file1 = Path.of("foo.1");
        try (StoreChannel storeChannel1 = fileSystem.write(file1)) {
            ByteBuffer buffer = ByteBuffers.allocate(2, ByteOrder.LITTLE_ENDIAN, INSTANCE);
            buffer.put((byte) 1);
            buffer.put((byte) 0);
            buffer.flip();
            storeChannel1.writeAll(buffer);
            storeChannel1.force(false);
        }

        Path file2 = Path.of("foo.2");
        try (StoreChannel storeChannel2 = fileSystem.read(file2)) {
            ByteBuffer buffer = ByteBuffers.allocate(2, ByteOrder.LITTLE_ENDIAN, INSTANCE);
            buffer.put((byte) 0);
            buffer.put((byte) 1);
            buffer.flip();
            storeChannel2.writeAll(buffer);
            storeChannel2.force(false);
        }

        HookedReadAheadChannel channel = constructor.apply(fileSystem.read(file1), DEFAULT_READ_AHEAD_SIZE);
        channel.nextChannelHook = fileSystem.read(file2);

        assertThrows(ReadPastEndException.class, channel::getLong);
        assertEquals(0x0100_0001, channel.getInt());
        assertThrows(ReadPastEndException.class, channel::get);
    }

    @ParameterizedTest
    @EnumSource(Constructors.class)
    void shouldReturnPositionWithinBufferedStream(Constructor constructor) throws Exception {
        // given
        Path file = Path.of("foo.txt");

        int readAheadSize = 512;
        int fileSize = readAheadSize * 8;

        createFile(fileSystem, file, fileSize);
        ReadAheadChannel<StoreChannel> bufferedReader = constructor.apply(fileSystem.read(file), readAheadSize);

        // when
        for (int i = 0; i < fileSize / Long.BYTES; i++) {
            assertEquals(Long.BYTES * i, bufferedReader.position());
            bufferedReader.getLong();
        }

        assertEquals(fileSize, bufferedReader.position());
        assertThrows(ReadPastEndException.class, bufferedReader::getLong);
        assertEquals(fileSize, bufferedReader.position());
    }

    @ParameterizedTest
    @EnumSource(Constructors.class)
    void validateChecksumOverStream(Constructor constructor) throws Exception {
        // given
        Checksum checksum = CHECKSUM_FACTORY.get();
        int checksumValue;
        Path file = Path.of("foo.1");
        try (StoreChannel storeChannel = fileSystem.write(file)) {
            ByteBuffer buffer = ByteBuffers.allocate(6, ByteOrder.LITTLE_ENDIAN, INSTANCE);
            buffer.put((byte) 1);
            checksum.update(1);
            buffer.put((byte) 2);
            checksum.update(2);
            checksumValue = (int) checksum.getValue();
            buffer.putInt(checksumValue);
            buffer.flip();
            storeChannel.writeAll(buffer);
            storeChannel.force(false);
        }

        ReadAheadChannel<StoreChannel> bufferedReader =
                constructor.apply(fileSystem.read(file), DEFAULT_READ_AHEAD_SIZE);

        assertEquals(1, bufferedReader.get());
        assertEquals(2, bufferedReader.get());
        assertEquals(checksumValue, bufferedReader.endChecksumAndValidate());
        assertEquals(6, bufferedReader.position());
    }

    @ParameterizedTest
    @EnumSource(Constructors.class)
    void throwOnInvalidChecksum(Constructor constructor) throws Exception {
        // given
        Checksum checksum = CHECKSUM_FACTORY.get();
        Path file = Path.of("foo.1");
        try (StoreChannel storeChannel = fileSystem.write(file)) {
            ByteBuffer buffer = ByteBuffers.allocate(6, ByteOrder.LITTLE_ENDIAN, INSTANCE);
            buffer.put((byte) 1);
            checksum.update(1);
            buffer.put((byte) 2);
            checksum.update(2);
            int notChecksumValue = (int) checksum.getValue() + 1;
            buffer.putInt(notChecksumValue);
            buffer.flip();
            storeChannel.writeAll(buffer);
            storeChannel.force(false);
        }

        ReadAheadChannel<StoreChannel> bufferedReader =
                constructor.apply(fileSystem.read(file), DEFAULT_READ_AHEAD_SIZE);

        assertEquals(1, bufferedReader.get());
        assertEquals(2, bufferedReader.get());
        assertThrows(ChecksumMismatchException.class, bufferedReader::endChecksumAndValidate);
    }

    @ParameterizedTest
    @EnumSource(Constructors.class)
    void checksumIsCalculatedCorrectlyOverBuffersLargerThanReadAheadSize(Constructor constructor) throws Exception {
        // given
        Checksum checksum = CHECKSUM_FACTORY.get();
        int checksumValue;
        Path file = Path.of("foo.1");
        int testSize = 100;
        try (StoreChannel storeChannel = fileSystem.write(file)) {
            ByteBuffer buffer = ByteBuffers.allocate(testSize + 4, ByteOrder.LITTLE_ENDIAN, INSTANCE);
            for (int i = 0; i < testSize; i++) {
                buffer.put((byte) i);
                checksum.update(i);
            }
            checksumValue = (int) checksum.getValue();
            buffer.putInt(checksumValue);
            buffer.flip();
            storeChannel.writeAll(buffer);
            storeChannel.force(false);
        }

        ReadAheadChannel<StoreChannel> bufferedReader = constructor.apply(fileSystem.read(file), testSize / 2);

        byte[] in = new byte[testSize];
        bufferedReader.get(in, testSize);
        for (int i = 0; i < testSize; i++) {
            assertEquals(i, in[i]);
        }
        assertEquals(checksumValue, bufferedReader.endChecksumAndValidate());
    }

    @ParameterizedTest
    @EnumSource(Constructors.class)
    void readBytes(Constructor constructor) throws IOException {
        int chunkSize = 64;
        int totalSize = chunkSize + Byte.BYTES + Byte.BYTES + Integer.BYTES;
        Checksum checksum = CHECKSUM_FACTORY.get();
        byte b1 = (byte) random.nextInt();
        byte b2 = (byte) random.nextInt();
        byte[] randomBytes = random.nextBytes(new byte[chunkSize]);
        checksum.update(b1);
        checksum.update(b2);
        checksum.update(randomBytes);
        int checksumValue = (int) checksum.getValue();

        Path file = Path.of("bar.1");
        try (StoreChannel storeChannel = fileSystem.write(file)) {
            ByteBuffer buffer = ByteBuffers.allocate(totalSize, ByteOrder.LITTLE_ENDIAN, INSTANCE);

            buffer.put(b1);
            buffer.put(b2);
            buffer.put(randomBytes);
            buffer.putInt(checksumValue);

            buffer.flip();
            storeChannel.writeAll(buffer);
            storeChannel.force(false);
        }

        ReadAheadChannel<StoreChannel> bufferedReader =
                constructor.apply(fileSystem.read(file), DEFAULT_READ_AHEAD_SIZE);

        assertEquals(b1, bufferedReader.get());
        assertEquals(b2, bufferedReader.get());
        byte[] readBytes = new byte[chunkSize];
        ByteBuffer buffer = ByteBuffer.wrap(readBytes);
        bufferedReader.read(buffer);
        assertThat(readBytes).containsExactly(randomBytes);
        assertThat(buffer.position()).isEqualTo(readBytes.length);
        assertEquals(checksumValue, bufferedReader.endChecksumAndValidate());
        assertEquals(totalSize, bufferedReader.position());
    }

    @ParameterizedTest
    @EnumSource(Constructors.class)
    void readBytesBiggerThanBuffer(Constructor constructor) throws IOException {
        int chunkSize = DEFAULT_READ_AHEAD_SIZE * 2;
        int totalSize = chunkSize + Byte.BYTES + Byte.BYTES + Integer.BYTES;
        Checksum checksum = CHECKSUM_FACTORY.get();
        byte b1 = (byte) random.nextInt();
        byte b2 = (byte) random.nextInt();
        byte[] randomBytes = random.nextBytes(new byte[chunkSize]);
        checksum.update(b1);
        checksum.update(b2);
        checksum.update(randomBytes);
        int checksumValue = (int) checksum.getValue();

        Path file = Path.of("baz.1");
        try (StoreChannel storeChannel = fileSystem.write(file)) {
            ByteBuffer buffer = ByteBuffers.allocate(totalSize, ByteOrder.LITTLE_ENDIAN, INSTANCE);

            buffer.put(b1);
            buffer.put(b2);
            buffer.put(randomBytes);
            buffer.putInt(checksumValue);

            buffer.flip();
            storeChannel.writeAll(buffer);
            storeChannel.force(false);
        }

        ReadAheadChannel<StoreChannel> bufferedReader =
                constructor.apply(fileSystem.read(file), DEFAULT_READ_AHEAD_SIZE);

        assertEquals(b1, bufferedReader.get());
        assertEquals(b2, bufferedReader.get());
        byte[] readBytes = new byte[chunkSize];
        ByteBuffer buffer = ByteBuffer.wrap(readBytes);
        bufferedReader.read(buffer);
        assertThat(readBytes).containsExactly(randomBytes);
        assertThat(buffer.position()).isEqualTo(readBytes.length);
        assertEquals(checksumValue, bufferedReader.endChecksumAndValidate());
        assertEquals(totalSize, bufferedReader.position());
    }

    @ParameterizedTest
    @EnumSource(Constructors.class)
    void readBytesSpanningMultipleFiles(Constructor constructor) throws IOException {
        int chunkSize = DEFAULT_READ_AHEAD_SIZE * 2;
        int totalSize = chunkSize + Byte.BYTES + Byte.BYTES + Integer.BYTES;
        Checksum checksum = CHECKSUM_FACTORY.get();
        byte b1 = (byte) random.nextInt();
        byte b2 = (byte) random.nextInt();
        byte[] randomBytes = random.nextBytes(new byte[chunkSize]);
        checksum.update(b1);
        checksum.update(b2);
        checksum.update(randomBytes);
        int checksumValue = (int) checksum.getValue();

        Path file1 = Path.of("foobar.1");
        int firstChunk = DEFAULT_READ_AHEAD_SIZE / 4;
        try (StoreChannel storeChannel = fileSystem.write(file1)) {
            ByteBuffer buffer = ByteBuffers.allocate(totalSize, ByteOrder.LITTLE_ENDIAN, INSTANCE);

            buffer.put(b1);
            buffer.put(b2);
            buffer.put(randomBytes, 0, firstChunk);

            buffer.flip();
            storeChannel.writeAll(buffer);
            storeChannel.force(false);
        }

        Path file2 = Path.of("foobar.2");
        try (StoreChannel storeChannel = fileSystem.write(file2)) {
            ByteBuffer buffer = ByteBuffers.allocate(totalSize, ByteOrder.LITTLE_ENDIAN, INSTANCE);

            buffer.put(randomBytes, firstChunk, randomBytes.length - firstChunk);
            buffer.putInt(checksumValue);

            buffer.flip();
            storeChannel.writeAll(buffer);
            storeChannel.force(false);
        }

        HookedReadAheadChannel bufferedReader = constructor.apply(fileSystem.read(file1), DEFAULT_READ_AHEAD_SIZE);
        bufferedReader.nextChannelHook = fileSystem.read(file2);

        assertEquals(b1, bufferedReader.get());
        assertEquals(b2, bufferedReader.get());
        byte[] readBytes = new byte[chunkSize];
        ByteBuffer buffer = ByteBuffer.wrap(readBytes);
        bufferedReader.read(buffer);
        assertThat(readBytes).containsExactly(randomBytes);
        assertThat(buffer.position()).isEqualTo(readBytes.length);
        assertEquals(checksumValue, bufferedReader.endChecksumAndValidate());
    }

    private static void createFile(EphemeralFileSystemAbstraction fsa, Path name, int bufferSize) throws IOException {
        StoreChannel storeChannel = fsa.write(name);
        ByteBuffer buffer = ByteBuffers.allocate(bufferSize, ByteOrder.LITTLE_ENDIAN, INSTANCE);
        for (int i = 0; i < bufferSize; i++) {
            buffer.put((byte) i);
        }
        buffer.flip();
        storeChannel.writeAll(buffer);
        storeChannel.close();
    }

    private static class HookedReadAheadChannel extends ReadAheadChannel<StoreChannel> {
        StoreChannel nextChannelHook;

        HookedReadAheadChannel(StoreChannel channel, ScopedBuffer scopedBuffer) {
            super(channel, scopedBuffer);
        }

        @Override
        protected StoreChannel next(StoreChannel channel) throws IOException {
            if (nextChannelHook != null) {
                StoreChannel next = nextChannelHook;
                nextChannelHook = null;
                return next;
            }
            return super.next(channel);
        }
    }

    interface Constructor {
        HookedReadAheadChannel apply(StoreChannel channel, int readAheadSize);
    }

    enum Constructors implements Constructor {
        HEAP_BUFFER {
            @Override
            public HookedReadAheadChannel apply(StoreChannel channel, int readAheadSize) {
                return new HookedReadAheadChannel(
                        channel, new HeapScopedBuffer(readAheadSize, ByteOrder.LITTLE_ENDIAN, INSTANCE));
            }
        },
        DIRECT_BUFFER {
            @Override
            public HookedReadAheadChannel apply(StoreChannel channel, int readAheadSize) {
                return new HookedReadAheadChannel(
                        channel, new NativeScopedBuffer(readAheadSize, ByteOrder.LITTLE_ENDIAN, INSTANCE));
            }
        },
    }
}
