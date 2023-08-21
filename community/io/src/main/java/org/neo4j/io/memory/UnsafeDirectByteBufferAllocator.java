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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import org.neo4j.internal.unsafe.NativeMemoryAllocationRefusedError;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.Preconditions;

/**
 * Allocates {@link ByteBuffer} instances using {@link UnsafeUtil#newDirectByteBuffer(long, int)}/{@link
 * UnsafeUtil#initDirectByteBuffer(ByteBuffer, long, int)} and frees all allocated memory in {@link #close()}.
 */
public class UnsafeDirectByteBufferAllocator implements ByteBufferFactory.Allocator {
    private final List<ScopedBuffer> allocations = new ArrayList<>();
    private boolean closed;

    @Override
    public synchronized ScopedBuffer allocate(int bufferSize, MemoryTracker memoryTracker) {
        assertOpen();
        try {
            var byteBuffer = new NativeScopedBuffer(bufferSize, ByteOrder.LITTLE_ENDIAN, memoryTracker);
            allocations.add(byteBuffer);
            return byteBuffer;
        } catch (NativeMemoryAllocationRefusedError allocationRefusedError) {
            // What ever went wrong fallback to on-heap buffer.
            return new HeapScopedBuffer(bufferSize, ByteOrder.LITTLE_ENDIAN, memoryTracker);
        }
    }

    @Override
    public synchronized void close() {
        // Idempotent close due to the way the population lifecycle works sometimes
        if (!closed) {
            allocations.forEach(ScopedBuffer::close);
            closed = true;
        }
    }

    private void assertOpen() {
        Preconditions.checkState(!closed, "Already closed");
    }
}
