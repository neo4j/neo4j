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
package org.neo4j.cloud.storage;

import static java.util.Arrays.copyOfRange;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.io.fs.FileSystemAbstraction.INVALID_FILE_DESCRIPTOR;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
@ExtendWith(RandomExtension.class)
class StorageChannelTest {

    @Inject
    private TestDirectory directory;

    @Inject
    private RandomSupport rnd;

    private Path output;

    @BeforeEach
    void setup() {
        output = directory.file("output");
    }

    @Test
    void read() throws IOException {
        final var bytes = rnd.nextBytes(new byte[rnd.nextInt(123, 666)]);
        Files.write(output, bytes, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);

        try (var channel = readChannel()) {
            assertThat(channel.size()).isEqualTo(bytes.length);
            assertThat(channel.position()).isZero();

            final var buffer = ByteBuffer.allocate(42);
            final var expected = new byte[bytes.length];
            for (var pos = 0; pos < bytes.length; pos += buffer.capacity()) {
                final var read = channel.read(buffer.position(0).limit(buffer.capacity()));
                assertThat(read).isEqualTo(buffer.flip().remaining());
                assertThat(channel.position()).isEqualTo(pos + read);

                System.arraycopy(bytes, pos, expected, pos, read);
            }

            assertThat(expected).isEqualTo(bytes);
        }
    }

    @Test
    void readAll() throws IOException {
        final var bufferSize = 42;
        final var padding = 13;
        final var bytes = rnd.nextBytes(new byte[(bufferSize * 8) + padding]);
        Files.write(output, bytes, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);

        final var buffer1 = ByteBuffer.allocate(bufferSize);
        final var buffer2 = ByteBuffer.allocate(bufferSize);
        final var buffer3 = ByteBuffer.allocate(bufferSize);
        final var buffers = new ByteBuffer[] {buffer1, buffer2, buffer3};

        try (var channel = readChannel()) {
            assertThat(channel.size()).isEqualTo(bytes.length);
            assertThat(channel.position()).isZero();

            final var expected = new byte[bytes.length];
            var read = channel.read(buffers, 0, 2);
            assertThat(read).as("should have filled the 2 buffers").isEqualTo(bufferSize * 2);
            assertThat(buffer1.hasRemaining()).isFalse();
            assertThat(buffer2.hasRemaining()).isFalse();
            assertThat(buffer3.remaining()).isEqualTo(bufferSize);

            System.arraycopy(buffer1.array(), 0, expected, 0, bufferSize);
            System.arraycopy(buffer2.array(), 0, expected, bufferSize, bufferSize);

            buffer1.flip();
            buffer2.flip();
            read += channel.read(buffers, 1, 2);
            assertThat(read).as("should have filled the 2 buffers").isEqualTo(bufferSize * 4);
            assertThat(buffer1.remaining()).isEqualTo(bufferSize);
            assertThat(buffer2.hasRemaining()).isFalse();
            assertThat(buffer3.hasRemaining()).isFalse();

            System.arraycopy(buffer2.array(), 0, expected, bufferSize * 2, bufferSize);
            System.arraycopy(buffer3.array(), 0, expected, bufferSize * 3, bufferSize);

            buffer2.flip();
            buffer3.flip();

            read += channel.read(buffers);
            assertThat(read).as("should have filled all 3 buffers").isEqualTo(bufferSize * 7L);
            assertThat(buffer1.hasRemaining()).isFalse();
            assertThat(buffer2.hasRemaining()).isFalse();
            assertThat(buffer3.hasRemaining()).isFalse();

            System.arraycopy(buffer1.array(), 0, expected, bufferSize * 4, bufferSize);
            System.arraycopy(buffer2.array(), 0, expected, bufferSize * 5, bufferSize);
            System.arraycopy(buffer3.array(), 0, expected, bufferSize * 6, bufferSize);

            buffer1.flip();
            buffer2.flip();
            buffer3.flip();
            read += channel.read(buffers);
            assertThat(read)
                    .as("should have (partially) filled the first 2 buffers")
                    .isEqualTo(bytes.length);
            assertThat(buffer1.hasRemaining()).isFalse();
            assertThat(buffer2.remaining()).isEqualTo(bufferSize - padding);
            assertThat(buffer3.remaining()).isEqualTo(bufferSize);

            System.arraycopy(buffer1.array(), 0, expected, bufferSize * 7, bufferSize);
            System.arraycopy(buffer2.array(), 0, expected, bufferSize * 8, padding);

            buffer1.flip();
            buffer2.flip();
            assertThat(channel.read(buffers))
                    .as("should return that the EOF has been reached")
                    .isEqualTo(-1L);

            assertThat(expected).isEqualTo(bytes);
        }
    }

    @Test
    void readFromPosition() throws IOException {
        final var bytes = rnd.nextBytes(new byte[rnd.nextInt(123, 666)]);
        Files.write(output, bytes, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);

        try (var channel = readChannel()) {
            assertThat(channel.size()).isEqualTo(bytes.length);

            final var offset = rnd.nextInt(10, 69);
            channel.position(offset);
            assertThat(channel.position()).isEqualTo(offset);

            final var buffer = ByteBuffer.allocate(42);
            final var expected = new byte[bytes.length];
            // preload the skipped content
            System.arraycopy(bytes, 0, expected, 0, offset);

            for (var pos = offset; pos < bytes.length; pos += buffer.capacity()) {
                final var read = channel.read(buffer.position(0).limit(buffer.capacity()));
                assertThat(read).isEqualTo(buffer.flip().remaining());
                assertThat(channel.position()).isEqualTo(pos + read);

                System.arraycopy(bytes, pos, expected, pos, read);
            }

            assertThat(expected).isEqualTo(bytes);
        }
    }

    @Test
    void write() throws IOException {
        final var bytes = rnd.nextBytes(new byte[rnd.nextInt(1, 666)]);

        try (var channel = writeChannel()) {
            assertThat(channel.position()).isZero();
            assertThat(channel.size()).isZero();

            channel.write(ByteBuffer.wrap(bytes));
            assertThat(channel.position()).isEqualTo(bytes.length);
            assertThat(channel.size()).isEqualTo(bytes.length);

            channel.write(ByteBuffer.wrap(bytes));
            assertThat(channel.position()).isEqualTo(bytes.length * 2L);
            assertThat(channel.size()).isEqualTo(bytes.length * 2L);
        }

        final var content = Files.readAllBytes(output);
        assertThat(content.length).isEqualTo(bytes.length * 2);
        assertThat(copyOfRange(content, 0, bytes.length)).isEqualTo(bytes);
        assertThat(copyOfRange(content, bytes.length, bytes.length * 2)).isEqualTo(bytes);
    }

    @Test
    void writeAll() throws IOException {
        final var bufferSize = rnd.nextInt(1, 123);
        final var buffer1 = ByteBuffer.wrap(rnd.nextBytes(new byte[bufferSize]));
        final var buffer2 = ByteBuffer.wrap(rnd.nextBytes(new byte[bufferSize]));
        final var buffer3 = ByteBuffer.wrap(rnd.nextBytes(new byte[bufferSize]));

        try (var channel = writeChannel()) {
            assertThat(channel.position()).isZero();
            assertThat(channel.size()).isZero();

            channel.write(buffer1);
            assertThat(channel.position()).isEqualTo(bufferSize);
            assertThat(channel.size()).isEqualTo(bufferSize);

            channel.write(buffer2, buffer3);
            assertThat(channel.position()).isEqualTo(bufferSize * 3L);
            assertThat(channel.size()).isEqualTo(bufferSize * 3L);

            channel.write(new ByteBuffer[] {buffer1.flip()}, 0, 1);
            assertThat(channel.position()).isEqualTo(bufferSize * 4L);
            assertThat(channel.size()).isEqualTo(bufferSize * 4L);
        }

        final var content = Files.readAllBytes(output);
        assertThat(content.length).isEqualTo(bufferSize * 4L);
        assertThat(copyOfRange(content, 0, bufferSize)).isEqualTo(buffer1.array());
        assertThat(copyOfRange(content, bufferSize, bufferSize * 2)).isEqualTo(buffer2.array());
        assertThat(copyOfRange(content, bufferSize * 2, bufferSize * 3)).isEqualTo(buffer3.array());
        assertThat(copyOfRange(content, bufferSize * 3, bufferSize * 4)).isEqualTo(buffer1.array());
    }

    @Test
    void lifecycle() throws IOException {
        final var channel = writeChannel();
        assertThat(channel.isOpen()).isTrue();
        channel.close();
        assertThat(channel.isOpen()).isFalse();
    }

    @Test
    void getFileDescriptor() throws IOException {
        try (var channel = writeChannel()) {
            assertThat(channel.getFileDescriptor()).isEqualTo(INVALID_FILE_DESCRIPTOR);
        }
    }

    @Test
    void lockMethods() throws IOException {
        try (var channel = writeChannel()) {
            assertThat(channel.hasPositionLock()).isFalse();
            assertThatThrownBy(channel::tryLock).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(channel::getPositionLock).isInstanceOf(UnsupportedOperationException.class);
        }
    }

    private StorageChannel writeChannel() throws IOException {
        return channel(StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
    }

    private StorageChannel readChannel() throws IOException {
        return channel(StandardOpenOption.READ);
    }

    private StorageChannel channel(OpenOption... options) throws IOException {
        return new StorageChannel(Files.newByteChannel(output, options));
    }
}
