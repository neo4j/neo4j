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
package org.neo4j.kernel.impl.api.state;

import static java.lang.Math.toIntExact;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.kernel.impl.util.collection.Memory;
import org.neo4j.kernel.impl.util.collection.MemoryAllocator;
import org.neo4j.memory.MemoryTracker;

class TestMemoryAllocator implements MemoryAllocator {
    @Override
    public Memory allocate(long size, boolean zeroed, MemoryTracker memoryTracker) {
        final ByteBuffer buf = ByteBuffers.allocate(toIntExact(size), ByteOrder.LITTLE_ENDIAN, INSTANCE);
        if (zeroed) {
            Arrays.fill(buf.array(), (byte) 0);
        }
        return new MemoryImpl(buf, memoryTracker);
    }

    static class MemoryImpl implements Memory {
        final ByteBuffer buf;

        MemoryImpl(ByteBuffer buf, MemoryTracker memoryTracker) {
            this.buf = buf;
            memoryTracker.allocateNative(buf.capacity());
        }

        @Override
        public long readLong(long offset) {
            return buf.getLong(toIntExact(offset));
        }

        @Override
        public void writeLong(long offset, long value) {
            buf.putLong(toIntExact(offset), value);
        }

        @Override
        public void clear() {
            Arrays.fill(buf.array(), (byte) 0);
        }

        @Override
        public long size() {
            return buf.capacity();
        }

        @Override
        public void free(MemoryTracker memoryTracker) {
            memoryTracker.releaseNative(buf.capacity());
        }

        @Override
        public Memory copy(MemoryTracker memoryTracker) {
            ByteBuffer copyBuf = ByteBuffer.wrap(Arrays.copyOf(buf.array(), buf.array().length));
            return new MemoryImpl(copyBuf, memoryTracker);
        }

        @Override
        public ByteBuffer asByteBuffer() {
            return buf;
        }
    }
}
