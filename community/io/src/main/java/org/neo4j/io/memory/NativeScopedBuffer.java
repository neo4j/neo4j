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
package org.neo4j.io.memory;

import static java.lang.Math.toIntExact;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.neo4j.memory.MemoryTracker;

/**
 * A life-time scope for the contained direct byte buffer.
 */
public final class NativeScopedBuffer implements ScopedBuffer {
    private final ByteBuffer buffer;
    private final MemoryTracker memoryTracker;
    private boolean closed;

    public NativeScopedBuffer(long capacity, ByteOrder byteOrder, MemoryTracker memoryTracker) {
        this(toIntExact(capacity), byteOrder, memoryTracker);
    }

    public NativeScopedBuffer(int capacity, ByteOrder byteOrder, MemoryTracker memoryTracker) {
        this.buffer = ByteBuffers.allocateDirect(capacity, byteOrder, memoryTracker);
        this.memoryTracker = memoryTracker;
    }

    @Override
    public ByteBuffer getBuffer() {
        return buffer;
    }

    @Override
    public void close() {
        if (!closed) {
            ByteBuffers.releaseBuffer(buffer, memoryTracker);
            closed = true;
        }
    }
}
