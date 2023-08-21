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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.memory.ByteBuffers.allocate;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.io.IOUtils;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
public class FileSystemAbstractionInterruptionTest {
    @Inject
    private TestDirectory testdir;

    private FileSystemAbstraction fs;
    private Path file;
    private StoreChannel channel;
    private boolean channelShouldBeClosed;

    protected FileSystemAbstraction createFileSystem() {
        return new DefaultFileSystemAbstraction();
    }

    @BeforeEach
    void createWorkingDirectoryAndTestFile() throws IOException {
        fs = createFileSystem();
        fs.mkdirs(testdir.homePath());
        file = testdir.file("a");
        fs.write(file).close();
        channel = null;
        channelShouldBeClosed = false;
        Thread.currentThread().interrupt();
    }

    @AfterEach
    void verifyInterruptionAndChannelState() throws IOException {
        assertTrue(Thread.interrupted());
        assertThat(channel.isOpen())
                .describedAs("channelShouldBeClosed? " + channelShouldBeClosed)
                .isEqualTo(!channelShouldBeClosed);

        if (channelShouldBeClosed) {
            assertThrows(ClosedChannelException.class, () -> channel.force(true));
        }
        IOUtils.closeAll(channel, fs);
    }

    @Test
    void fsOpenClose() throws IOException {
        channel(true).close();
    }

    @Test
    void channelTryLock() throws IOException {
        channel(false).tryLock().release();
    }

    @Test
    void channelSetPosition() {
        assertThrows(ClosedByInterruptException.class, () -> channel(true).position(0));
    }

    @Test
    void channelGetPosition() {
        assertThrows(ClosedByInterruptException.class, () -> channel(true).position());
    }

    @Test
    void channelTruncate() {
        assertThrows(ClosedByInterruptException.class, () -> channel(true).truncate(0));
    }

    @Test
    void channelForce() {
        assertThrows(ClosedByInterruptException.class, () -> channel(true).force(true));
    }

    @Test
    void channelWriteAllByteBuffer() {
        assertThrows(ClosedByInterruptException.class, () -> channel(true)
                .writeAll(allocate(1, ByteOrder.LITTLE_ENDIAN, INSTANCE)));
    }

    @Test
    void channelWriteAllByteBufferPosition() {
        assertThrows(ClosedByInterruptException.class, () -> channel(true)
                .writeAll(allocate(1, ByteOrder.LITTLE_ENDIAN, INSTANCE), 1));
    }

    @Test
    void channelReadByteBuffer() {
        assertThrows(ClosedByInterruptException.class, () -> channel(true)
                .read(allocate(1, ByteOrder.LITTLE_ENDIAN, INSTANCE)));
    }

    @Test
    void channelWriteByteBuffer() {
        assertThrows(ClosedByInterruptException.class, () -> channel(true)
                .write(allocate(1, ByteOrder.LITTLE_ENDIAN, INSTANCE)));
    }

    @Test
    void channelSize() {
        assertThrows(ClosedByInterruptException.class, () -> channel(true).size());
    }

    @Test
    void channelIsOpen() throws IOException {
        assertTrue(channel(false).isOpen());
    }

    @Test
    void channelWriteByteBuffersOffsetLength() {
        assertThrows(ClosedByInterruptException.class, () -> channel(true)
                .write(new ByteBuffer[] {allocate(1, ByteOrder.LITTLE_ENDIAN, INSTANCE)}, 0, 1));
    }

    @Test
    void channelWriteByteBuffers() {
        assertThrows(ClosedByInterruptException.class, () -> channel(true)
                .write(new ByteBuffer[] {allocate(1, ByteOrder.LITTLE_ENDIAN, INSTANCE)}));
    }

    @Test
    void channelReadByteBuffersOffsetLength() {
        assertThrows(ClosedByInterruptException.class, () -> channel(true)
                .read(new ByteBuffer[] {allocate(1, ByteOrder.LITTLE_ENDIAN, INSTANCE)}, 0, 1));
    }

    @Test
    void channelReadByteBuffers() {
        assertThrows(ClosedByInterruptException.class, () -> channel(true)
                .read(new ByteBuffer[] {allocate(1, ByteOrder.LITTLE_ENDIAN, INSTANCE)}));
    }

    private StoreChannel channel(boolean channelShouldBeClosed) throws IOException {
        this.channelShouldBeClosed = channelShouldBeClosed;
        channel = fs.write(file);
        return channel;
    }
}
