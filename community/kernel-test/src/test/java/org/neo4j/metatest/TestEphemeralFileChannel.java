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
package org.neo4j.metatest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.io.memory.ByteBuffers.allocate;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;

@ExtendWith(EphemeralFileSystemExtension.class)
class TestEphemeralFileChannel {

    @Inject
    private EphemeralFileSystemAbstraction fileSystem;

    @Test
    void smoke() throws Exception {
        StoreChannel channel = fileSystem.write(Path.of("yo"));

        // Clear it because we depend on it to be zeros where we haven't written
        ByteBuffer buffer = allocate(23, ByteOrder.LITTLE_ENDIAN, INSTANCE);
        buffer.put(new byte[23]); // zeros
        buffer.flip();
        channel.write(buffer);
        channel = fileSystem.write(Path.of("yo"));
        long longValue = 1234567890L;

        // [1].....[2]........[1234567890L]...

        buffer.clear();
        buffer.limit(1);
        buffer.put((byte) 1);
        buffer.flip();
        channel.write(buffer);

        buffer.clear();
        buffer.limit(1);
        buffer.put((byte) 2);
        buffer.flip();
        channel.position(6);
        channel.write(buffer);

        buffer.clear();
        buffer.limit(8);
        buffer.putLong(longValue);
        buffer.flip();
        channel.position(15);
        channel.write(buffer);
        assertEquals(23, channel.size());

        // Read with position
        // byte 0
        buffer.clear();
        buffer.limit(1);
        channel.read(buffer, 0);
        buffer.flip();
        assertEquals((byte) 1, buffer.get());

        // bytes 5-7
        buffer.clear();
        buffer.limit(3);
        channel.read(buffer, 5);
        buffer.flip();
        assertEquals((byte) 0, buffer.get());
        assertEquals((byte) 2, buffer.get());
        assertEquals((byte) 0, buffer.get());

        // bytes 15-23
        buffer.clear();
        buffer.limit(8);
        channel.read(buffer, 15);
        buffer.flip();
        assertEquals(longValue, buffer.getLong());
    }

    @Test
    void absoluteVersusRelative() throws Exception {
        // GIVEN
        Path file = Path.of("myfile").toAbsolutePath();
        StoreChannel channel = fileSystem.write(file);
        byte[] bytes = "test".getBytes();
        channel.write(ByteBuffer.wrap(bytes));
        channel.close();

        // WHEN
        channel = fileSystem.read(file);
        byte[] readBytes = new byte[bytes.length];
        channel.readAll(ByteBuffer.wrap(readBytes));

        // THEN
        assertArrayEquals(bytes, readBytes);
    }

    @Test
    void listFiles() throws Exception {
        /* GIVEN
         *                        root
         *                       /    \
         *         ----------- dir1   dir2
         *        /       /     |       \
         *    subdir1  file  file2      file
         *       |
         *     file
         */
        Path root = Path.of("/root").toAbsolutePath().normalize();
        Path dir1 = root.resolve("dir1");
        Path dir2 = root.resolve("dir2");
        Path subdir1 = dir1.resolve("sub");
        Path file1 = dir1.resolve("file");
        Path file2 = dir1.resolve("file2");
        Path file3 = dir2.resolve("file");
        Path file4 = subdir1.resolve("file");

        fileSystem.mkdirs(dir2);
        fileSystem.mkdirs(dir1);
        fileSystem.mkdirs(subdir1);

        fileSystem.write(file1);
        fileSystem.write(file2);
        fileSystem.write(file3);
        fileSystem.write(file4);

        // THEN
        assertThat(fileSystem.listFiles(root)).containsExactlyInAnyOrder(dir1, dir2);
        assertThat(fileSystem.listFiles(dir1)).containsExactlyInAnyOrder(subdir1, file1, file2);
        assertThat(fileSystem.listFiles(dir2)).containsExactly(file3);
        assertThat(fileSystem.listFiles(subdir1)).containsExactly(file4);
    }
}
