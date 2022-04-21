/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.io.bufferpool.impl;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.VisibleForTesting;

/**
 * A storage of buffers of the same capacity.
 */
class Bucket {
    private final int bufferCapacity;
    private final Slice[] slices;
    private final MemoryTracker memoryTracker;
    private final AtomicLong currentTick = new AtomicLong();

    Bucket(int bufferCapacity, int sliceCount, MemoryTracker memoryTracker) {
        this.bufferCapacity = bufferCapacity;
        this.memoryTracker = memoryTracker;

        slices = IntStream.range(0, sliceCount).mapToObj(i -> new Slice()).toArray(Slice[]::new);
    }

    ByteBuffer acquire() {
        var slice = getSlice();
        var idleBuffer = slice.stack.pollFirst();
        if (idleBuffer != null) {
            return idleBuffer.byteBuffer;
        }

        return ByteBuffers.allocateDirect(bufferCapacity, memoryTracker);
    }

    void release(ByteBuffer buffer) {
        var idleBuffer = new IdleBuffer(buffer, currentTick.get());
        var slice = getSlice();
        slice.stack.offerFirst(idleBuffer);
    }

    int getBufferCapacity() {
        return bufferCapacity;
    }

    void prunePooledBuffers() {
        long previousTick = currentTick.getAndIncrement();

        for (Slice slice : slices) {
            while (true) {
                var idleBuffer = slice.stack.pollLast();
                if (idleBuffer == null) {
                    // There is nothing more in this slice, so the buffer collecting in this slice is done.
                    break;
                }

                if (idleBuffer.lastUsedTick >= previousTick) {
                    // We have encountered a buffer that has been used since the last collection cycle.
                    // Let's put it back and since we are dealing with a stack, the buffers closer
                    // to the top have been used, too, so the buffer collecting of this slice is done.
                    slice.stack.offerLast(idleBuffer);
                    break;
                }

                ByteBuffers.releaseBuffer(idleBuffer.byteBuffer, memoryTracker);
            }
        }
    }

    void releasePooledBuffers() {
        for (Slice slice : slices) {
            while (true) {
                var idleBuffer = slice.stack.pollFirst();
                if (idleBuffer == null) {
                    break;
                }

                ByteBuffers.releaseBuffer(idleBuffer.byteBuffer, memoryTracker);
            }
        }
    }

    private Slice getSlice() {
        if (slices.length == 1) {
            return slices[0];
        }

        return slices[ThreadLocalRandom.current().nextInt(slices.length)];
    }

    @VisibleForTesting
    List<Slice> getSlices() {
        return Arrays.asList(slices);
    }

    private static class Slice {
        // The buffers are stored in a stack-like data structure.
        // They are acquired from and released to the top of the stack.
        // The periodic collection of buffers not used recently processes buffers
        // from the bottom until in encounters a buffer used recently.
        private final Deque<IdleBuffer> stack = new ConcurrentLinkedDeque<>();
    }

    private record IdleBuffer(ByteBuffer byteBuffer, long lastUsedTick) {}
}
