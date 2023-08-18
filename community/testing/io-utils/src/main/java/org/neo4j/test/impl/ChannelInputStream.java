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
package org.neo4j.test.impl;

import static java.lang.Math.toIntExact;
import static org.neo4j.io.ByteUnit.KibiByte;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.io.memory.ScopedBuffer;
import org.neo4j.memory.MemoryTracker;

public class ChannelInputStream extends InputStream {
    private final StoreChannel channel;
    private final ScopedBuffer scopedBuffer;
    private final ByteBuffer buffer;
    private int position;

    public ChannelInputStream(StoreChannel channel, MemoryTracker memoryTracker) {
        this.channel = channel;
        this.scopedBuffer =
                new HeapScopedBuffer(toIntExact(KibiByte.toBytes(8)), ByteOrder.LITTLE_ENDIAN, memoryTracker);
        this.buffer = scopedBuffer.getBuffer();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        buffer.clear();
        buffer.limit(Math.min(len, buffer.capacity()));
        int totalRead = 0;
        boolean eof = false;
        while (buffer.hasRemaining() && !eof) {
            int bytesRead = channel.read(buffer);
            if (bytesRead < 0) {
                eof = true;
                if (totalRead == 0) {
                    return -1;
                }
            } else {
                totalRead += bytesRead;
            }
        }

        buffer.flip();
        position += totalRead;

        buffer.get(b, off, totalRead);
        return totalRead;
    }

    @Override
    public int read() throws IOException {
        buffer.clear();
        buffer.limit(1);
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer);

            if (read == -1) {
                return -1;
            }
        }
        buffer.flip();
        position++;
        // Return the *unsigned* byte value as an integer
        return buffer.get() & 0x000000FF;
    }

    @Override
    public int available() throws IOException {
        return (int) (channel.size() - position);
    }

    @Override
    public void close() throws IOException {
        scopedBuffer.close();
        channel.close();
    }
}
