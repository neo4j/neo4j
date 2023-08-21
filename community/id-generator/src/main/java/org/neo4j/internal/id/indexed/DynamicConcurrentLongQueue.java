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
package org.neo4j.internal.id.indexed;

import static java.lang.Math.max;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;

class DynamicConcurrentLongQueue implements ConcurrentLongQueue {
    private final AtomicReference<Chunk> head;
    private final AtomicReference<Chunk> tail;
    private final AtomicInteger numChunks = new AtomicInteger(1);
    private final int chunkSize;
    private final int maxNumChunks;

    DynamicConcurrentLongQueue(int chunkSize, int maxNumChunks) {
        this.chunkSize = chunkSize;
        this.maxNumChunks = maxNumChunks;

        var chunk = new Chunk(chunkSize);
        this.head = new AtomicReference<>(chunk);
        this.tail = new AtomicReference<>(chunk);
    }

    @Override
    public boolean offer(long v) {
        var chunk = tail.get();
        if (!chunk.offer(v)) {
            if (numChunks.get() >= maxNumChunks) {
                return false;
            }

            var next = new Chunk(chunkSize);
            chunk.next.set(next);
            tail.set(next);
            numChunks.incrementAndGet();
            return next.offer(v);
        }
        return true;
    }

    @Override
    public long takeOrDefault(long defaultValue) {
        Chunk chunk;
        Chunk next;
        do {
            chunk = head.get();
            next = chunk.next.get();
            var candidate = chunk.takeOrDefault(defaultValue);
            if (candidate != defaultValue) {
                return candidate;
            }
            if (next != null) {
                if (head.compareAndSet(chunk, next)) {
                    numChunks.decrementAndGet();
                }
            }
        } while (next != null);
        return defaultValue;
    }

    @Override
    public long takeInRange(long minBoundary, long maxBoundary) {
        Chunk chunk;
        Chunk next;
        do {
            chunk = head.get();
            next = chunk.next.get();
            var candidate = chunk.takeInRange(minBoundary, maxBoundary);
            if (candidate >= 0 && candidate < maxBoundary) {
                return candidate;
            } else if (candidate > maxBoundary) {
                return Long.MAX_VALUE;
            }
            if (candidate == -1 && next != null) {
                if (head.compareAndSet(chunk, next)) {
                    numChunks.decrementAndGet();
                }
            }
        } while (next != null);
        return Long.MAX_VALUE;
    }

    private int capacity() {
        return chunkSize * maxNumChunks;
    }

    @Override
    public int size() {
        Chunk firstChunk;
        int size;
        do {
            firstChunk = head.get();
            size = firstChunk.size();
            var numChunks = this.numChunks.get();
            if (numChunks > 1) {
                size += (numChunks - 2) * chunkSize;
                size += tail.get().size();
            }
        } while (firstChunk != head.get());
        return size;
    }

    @Override
    public int availableSpace() {
        int capacity = capacity();
        var lastChunk = tail.get();
        int occupied = (numChunks.get() - 1) * chunkSize + lastChunk.occupied();
        return capacity - occupied;
    }

    @Override
    public void clear() {
        var chunk = new Chunk(chunkSize);
        head.set(chunk);
        tail.set(chunk);
    }

    private static class Chunk {
        private final AtomicLongArray array;
        private final int capacity;
        private final AtomicInteger readSeq = new AtomicInteger();
        private final AtomicInteger writeSeq = new AtomicInteger();
        private final AtomicReference<Chunk> next = new AtomicReference<>();

        Chunk(int capacity) {
            this.array = new AtomicLongArray(capacity);
            this.capacity = capacity;
        }

        boolean offer(long v) {
            var currentWriteSeq = writeSeq.get();
            if (currentWriteSeq == capacity) {
                return false;
            }
            array.set(currentWriteSeq, v);
            writeSeq.incrementAndGet();
            return true;
        }

        long takeInRange(long minBoundary, long maxBoundary) {
            int currentReadSeq;
            int currentWriteSeq;
            long value;
            do {
                currentReadSeq = readSeq.get();
                currentWriteSeq = writeSeq.get();
                if (currentReadSeq == currentWriteSeq) {
                    return -1;
                }
                value = array.get(currentReadSeq);
                if (value >= maxBoundary || value < minBoundary) {
                    return Long.MAX_VALUE;
                }
            } while (!readSeq.compareAndSet(currentReadSeq, currentReadSeq + 1));
            return value;
        }

        long takeOrDefault(long defaultValue) {
            int currentReadSeq;
            int currentWriteSeq;
            long value;
            do {
                currentReadSeq = readSeq.get();
                currentWriteSeq = writeSeq.get();
                if (currentReadSeq == currentWriteSeq) {
                    return defaultValue;
                }
                value = array.get(currentReadSeq);
            } while (!readSeq.compareAndSet(currentReadSeq, currentReadSeq + 1));
            return value;
        }

        int size() {
            return max(0, writeSeq.intValue() - readSeq.intValue());
        }

        int occupied() {
            return writeSeq.intValue();
        }
    }
}
