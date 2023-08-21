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
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.memory.MemoryTracker;

public final class ByteBuffers {
    private ByteBuffers() {}

    /**
     * Allocate on heap byte buffer with requested byte order
     * @param capacity byte buffer capacity
     * @param order byte buffer order
     * @param memoryTracker underlying buffers allocation memory tracker
     * @return byte buffer with requested size
     */
    public static ByteBuffer allocate(int capacity, ByteOrder order, MemoryTracker memoryTracker) {
        memoryTracker.allocateHeap(capacity);
        try {
            return ByteBuffer.allocate(capacity).order(order);
        } catch (Throwable any) {
            memoryTracker.releaseHeap(capacity);
            throw any;
        }
    }

    /**
     * Allocate direct byte buffer with default byte order
     *
     * Allocated memory will be tracked by global memory allocator.
     * @param capacity byte buffer capacity
     * @param order byte order
     * @param memoryTracker memory tracker
     * @return byte buffer with requested size
     */
    public static ByteBuffer allocateDirect(int capacity, ByteOrder order, MemoryTracker memoryTracker) {
        if (UnsafeUtil.unsafeByteBufferAccessAvailable()) {
            return UnsafeUtil.allocateByteBuffer(capacity, memoryTracker).order(order);
        } else {
            return allocateDirectFallback(capacity, memoryTracker).order(order);
        }
    }

    /**
     * Release all the memory that was allocated for the buffer in case its native.
     * @param byteBuffer byte buffer to release
     */
    public static void releaseBuffer(ByteBuffer byteBuffer, MemoryTracker memoryTracker) {
        if (UnsafeUtil.unsafeByteBufferAccessAvailable()) {
            UnsafeUtil.releaseBuffer(byteBuffer, memoryTracker);
        } else {
            releaseBufferFallback(byteBuffer, memoryTracker);
        }
    }

    private static ByteBuffer allocateDirectFallback(int capacity, MemoryTracker memoryTracker) {
        memoryTracker.allocateNative(capacity);
        try {
            return ByteBuffer.allocateDirect(capacity);
        } catch (Throwable any) {
            memoryTracker.releaseNative(capacity);
            throw any;
        }
    }

    private static void releaseBufferFallback(ByteBuffer byteBuffer, MemoryTracker memoryTracker) {
        if (!byteBuffer.isDirect()) {
            memoryTracker.releaseHeap(byteBuffer.capacity());
            return;
        }
        var capacity = byteBuffer.capacity();
        UnsafeUtil.invokeCleaner(byteBuffer);
        memoryTracker.releaseNative(capacity);
    }
}
