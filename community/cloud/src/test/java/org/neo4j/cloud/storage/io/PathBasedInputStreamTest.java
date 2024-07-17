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
package org.neo4j.cloud.storage.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.io.ByteUnit.kibiBytes;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class PathBasedInputStreamTest {
    private static final String CONTENT = "some content";

    @Inject
    private TestDirectory directory;

    private Path inputPath;

    @BeforeEach
    void setup() throws IOException {
        inputPath = directory.file("input");
        Files.writeString(inputPath, CONTENT);
    }

    @Test
    void transferTo() throws IOException {
        final var outPath = directory.file("transfer");
        try (var input = new PathBasedInputStream(inputPath)) {
            try (var output = Files.newOutputStream(outPath)) {
                assertThat(input.transferTo(output)).isGreaterThan(0);
            }
        }

        assertThat(Files.readString(outPath)).isEqualTo(CONTENT);
    }

    @Test
    void transferToChannel() throws IOException {
        final var outPath = directory.file("transfer");
        try (var input = new PathBasedInputStream(inputPath)) {
            try (var output = new TestWriteableChannel(outPath)) {
                assertThat(input.transferTo(output)).isGreaterThan(0);
                assertThat(output.transferred).isTrue();
            }
        }

        assertThat(Files.readString(outPath)).isEqualTo(CONTENT);
    }

    @Test
    void read() throws IOException {
        try (var input = new PathBasedInputStream(inputPath)) {
            assertThat(new String(input.readAllBytes(), StandardCharsets.UTF_8)).isEqualTo(CONTENT);
        }
    }

    @Test
    void cannotTransferIfAlreadyRead() throws IOException {
        try (var input = new PathBasedInputStream(inputPath)) {
            assertThat(input.read()).isGreaterThan(0);

            try (var output = Files.newOutputStream(directory.file("transfer"))) {
                assertThatThrownBy(() -> input.transferTo(output)).isInstanceOf(IOException.class);
            }
        }
    }

    @Test
    void cannotReadIfAlreadyTransferring() throws IOException {
        try (var input = new PathBasedInputStream(inputPath)) {
            try (var output = new TestWriteableChannel(directory.file("transfer"))) {
                assertThat(input.transferTo(output)).isGreaterThan(0);
            }

            assertThatThrownBy(input::read).isInstanceOf(IOException.class);
        }
    }

    @Test
    void lifecycle() throws IOException {
        final var input = new PathBasedInputStream(inputPath);
        input.close();

        assertThatThrownBy(input::read).isInstanceOf(IOException.class).hasMessage("Stream is already closed");

        try (var output = Files.newOutputStream(directory.file("transfer"))) {
            assertThatThrownBy(() -> input.transferTo(output))
                    .isInstanceOf(IOException.class)
                    .hasMessage("Stream is already closed");
        }
    }

    private static class TestWriteableChannel extends WriteableChannel {

        private final FileChannel channel;

        private boolean transferred;

        protected TestWriteableChannel(Path path) throws IOException {
            super((int) kibiBytes(8), EmptyMemoryTracker.INSTANCE);
            this.channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
        }

        @Override
        public long transferFrom(Path path) throws IOException {
            transferred = true;
            return super.transferFrom(path);
        }

        @Override
        protected long internalGetSize() {
            try {
                return channel.size();
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
        }

        @Override
        protected void completeWriteProcess() throws IOException {
            if (buffer.hasRemaining()) {
                //noinspection ResultOfMethodCallIgnored
                channel.write(buffer.flip());
            }
            channel.close();
        }

        @Override
        protected void doBufferWrite(ByteBuffer data) {
            buffer.put(data);
        }

        @Override
        protected void reportChunksWritten(long chunks) {
            // no-op
        }

        @Override
        protected boolean hasBeenReplicated() {
            return false;
        }
    }
}
