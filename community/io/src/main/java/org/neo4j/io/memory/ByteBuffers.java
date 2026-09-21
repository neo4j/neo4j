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

import static org.neo4j.io.memory.BufferLeakTracker.DISABLED_TRACKER;
import static org.neo4j.io.memory.BufferLeakTracker.ENABLED_TRACKER;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.Preconditions;

public final class ByteBuffers {

    public static final BufferLeakTracker BUFFER_LEAK_TRACKER =
            Boolean.getBoolean("org.neo4j.ByteBuffers.TRACK_BUFFERS") ? ENABLED_TRACKER : DISABLED_TRACKER;

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
     * <br>
     * Allocated memory will be tracked by global memory allocator.
     * @param capacity byte buffer capacity
     * @param order byte order
     * @param memoryTracker memory tracker
     * @return byte buffer with requested size
     */
    public static ByteBuffer allocateDirect(int capacity, ByteOrder order, MemoryTracker memoryTracker) {
        if (UnsafeUtil.unsafeByteBufferAccessAvailable()) {
            return BUFFER_LEAK_TRACKER.track(
                    UnsafeUtil.allocateByteBuffer(capacity, memoryTracker).order(order));
        } else {
            return BUFFER_LEAK_TRACKER.track(
                    allocateDirectFallback(capacity, memoryTracker).order(order));
        }
    }

    /**
     * Release all the memory that was allocated for the buffer in case its native.
     * @param byteBuffer byte buffer to release
     */
    public static void releaseBuffer(ByteBuffer byteBuffer, MemoryTracker memoryTracker) {
        BUFFER_LEAK_TRACKER.release(byteBuffer);
        if (UnsafeUtil.unsafeByteBufferAccessAvailable()) {
            UnsafeUtil.releaseBuffer(byteBuffer, memoryTracker);
        } else {
            releaseBufferFallback(byteBuffer, memoryTracker);
        }
    }

    public static boolean directBufferContainsNonZeroData(ByteBuffer byteBuffer) {
        Preconditions.checkState(byteBuffer.isDirect(), "Only direct buffers are supported.");
        int longCount = byteBuffer.remaining() >> 3;
        if (longCount > 0) {
            LongBuffer longBuffer = byteBuffer.asLongBuffer();
            for (int i = 0; i < longCount; i++) {
                if (longBuffer.get() != 0L) {
                    return true;
                }
            }
            byteBuffer.position(byteBuffer.position() + (longCount << 3));
        }
        while (byteBuffer.hasRemaining()) {
            if (byteBuffer.get() != 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * Sorts a buffer, where the contents of the buffer between {@link ByteBuffer#position()} and
     * {@link ByteBuffer#limit()} <strong>MUST</strong> all be {@link Long}s
     * @param buffer the buffer to sort
     * @param memoryTracker a memory tracker - to use in the case when a merge sort needs to occur. Otherwise, all
     *                      sorting is done directly in-place in the provided buffer
     */
    public static void sortLongs(ByteBuffer buffer, MemoryTracker memoryTracker) {
        ByteBufferQuicksort.sort(buffer, memoryTracker);
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
