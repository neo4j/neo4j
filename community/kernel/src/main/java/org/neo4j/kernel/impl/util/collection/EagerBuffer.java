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
package org.neo4j.kernel.impl.util.collection;

import static org.neo4j.memory.HeapEstimator.SCOPED_MEMORY_TRACKER_SHALLOW_SIZE;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfObjectArray;

import java.util.Iterator;
import java.util.function.IntUnaryOperator;
import org.neo4j.internal.helpers.ArrayUtil;
import org.neo4j.internal.kernel.api.DefaultCloseListenable;
import org.neo4j.memory.Measurable;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.VisibleForTesting;

/**
 * A specialized heap tracking buffer of Measurable elements, which grows in chunks without array copy as a linked list.
 * Its use case is append only, and sequential read from beginning to end.
 *
 * Each new chunk can grow by a configurable factor, up to a maximum size
 *
 * @param <T> element type
 */
public class EagerBuffer<T extends Measurable> extends DefaultCloseListenable {
    public static final IntUnaryOperator KEEP_CONSTANT_CHUNK_SIZE = size -> size;
    public static final IntUnaryOperator GROW_NEW_CHUNKS_BY_50_PCT = size -> size + (size >> 1);
    public static final IntUnaryOperator GROW_NEW_CHUNKS_BY_100_PCT = size -> size << 1;

    private static final long SHALLOW_SIZE = shallowSizeOfInstance(EagerBuffer.class);

    private final MemoryTracker scopedMemoryTracker;
    private final IntUnaryOperator growthStrategy;
    private final ChunkMemoryEstimator<T> memoryEstimator;

    private EagerBuffer.Chunk<T> first;
    private EagerBuffer.Chunk<T> current;
    private long size;
    private final int maxChunkSize;

    public static <T extends Measurable> EagerBuffer<T> createEagerBuffer(MemoryTracker memoryTracker) {
        return createEagerBuffer(
                memoryTracker, 1024, ArrayUtil.MAX_ARRAY_SIZE, GROW_NEW_CHUNKS_BY_50_PCT); // Grow by 50%
    }

    public static <T extends Measurable> EagerBuffer<T> createEagerBuffer(
            MemoryTracker memoryTracker, int initialChunkSize) {
        return createEagerBuffer(
                memoryTracker, initialChunkSize, ArrayUtil.MAX_ARRAY_SIZE, GROW_NEW_CHUNKS_BY_50_PCT); // Grow by 50%
    }

    public static <T extends Measurable> EagerBuffer<T> createEagerBuffer(
            MemoryTracker memoryTracker, int initialChunkSize, int maxChunkSize, IntUnaryOperator growthStrategy) {
        return createEagerBuffer(
                memoryTracker, initialChunkSize, maxChunkSize, growthStrategy, ChunkMemoryEstimator.createDefault());
    }

    public static <T extends Measurable> EagerBuffer<T> createEagerBuffer(
            MemoryTracker memoryTracker,
            int initialChunkSize,
            int maxChunkSize,
            IntUnaryOperator growthStrategy,
            ChunkMemoryEstimator<T> memoryEstimator) {
        MemoryTracker scopedMemoryTracker = memoryTracker.getScopedMemoryTracker();
        scopedMemoryTracker.allocateHeap(SHALLOW_SIZE
                + SCOPED_MEMORY_TRACKER_SHALLOW_SIZE
                + shallowSizeOfInstance(IntUnaryOperator.class)
                + shallowSizeOfInstance(ChunkMemoryEstimator.class));
        return new EagerBuffer<>(scopedMemoryTracker, initialChunkSize, maxChunkSize, growthStrategy, memoryEstimator);
    }

    private EagerBuffer(
            MemoryTracker scopedMemoryTracker,
            int initialChunkSize,
            int maxChunkSize,
            IntUnaryOperator growthStrategy,
            ChunkMemoryEstimator<T> memoryEstimator) {
        this.scopedMemoryTracker = scopedMemoryTracker;
        this.maxChunkSize = maxChunkSize;
        this.growthStrategy = growthStrategy;
        this.memoryEstimator = memoryEstimator;
        first = new EagerBuffer.Chunk<>(
                initialChunkSize, scopedMemoryTracker.getScopedMemoryTracker(), memoryEstimator);
        current = first;
    }

    public void add(T element) {
        if (!current.add(element)) {
            int newChunkSize = grow(current.elements.length);
            EagerBuffer.Chunk<T> newChunk = new EagerBuffer.Chunk<>(
                    newChunkSize, scopedMemoryTracker.getScopedMemoryTracker(), memoryEstimator);
            current.next = newChunk;
            current = newChunk;
            current.add(element);
        }
        size++;
    }

    public long size() {
        return size;
    }

    /**
     * Non-closing iterator.
     */
    public Iterator<T> iterator() {
        return new EagerBufferIterator(false);
    }

    public Iterator<T> autoClosingIterator() {
        return new EagerBufferIterator(true);
    }

    @Override
    public void closeInternal() {
        first = null;
        current = null;
        scopedMemoryTracker.close();
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @VisibleForTesting
    public int numberOfChunks() {
        int i = 0;
        var chunk = first;
        while (chunk != null) {
            chunk = chunk.next;
            i++;
        }
        return i;
    }

    private int grow(int size) {
        if (size == maxChunkSize) {
            return size;
        }
        int newSize = growthStrategy.applyAsInt(size);
        if (newSize <= 0 || newSize > maxChunkSize) // Check overflow
        {
            return maxChunkSize;
        }
        return newSize;
    }

    private class EagerBufferIterator implements Iterator<T> {
        private final boolean autoClosing;
        private EagerBuffer.Chunk<T> chunk;
        private int index;

        EagerBufferIterator(boolean autoClosing) {
            this.autoClosing = autoClosing;
            chunk = first;
            if (autoClosing) {
                EagerBuffer.this.first = null;
                EagerBuffer.this.current = null;
            }
        }

        @Override
        public boolean hasNext() {
            if (chunk == null || index >= chunk.cursor) {
                if (autoClosing) {
                    EagerBuffer.this.close();
                }
                return false;
            }
            return true;
        }

        @SuppressWarnings("unchecked")
        @Override
        public T next() {
            Object element = chunk.elements[index++];
            if (index >= chunk.cursor) {
                var chunkToRelease = chunk;
                chunk = chunk.next;
                index = 0;
                if (autoClosing) {
                    chunkToRelease.close();
                }
            }
            return (T) element;
        }
    }

    public interface ChunkMemoryEstimator<T extends Measurable> {

        long estimateHeapUsage(T element, T previous);

        static <T extends Measurable> ChunkMemoryEstimator<T> createDefault() {
            return (element, previous) -> element.estimatedHeapUsage();
        }
    }

    private static class Chunk<T extends Measurable> {
        private static final long SHALLOW_SIZE = shallowSizeOfInstance(EagerBuffer.Chunk.class);

        private final Object[] elements;
        private final ChunkMemoryEstimator<T> memoryEstimator;
        private final MemoryTracker memoryTracker;
        private EagerBuffer.Chunk<T> next;
        private int cursor;

        Chunk(int size, MemoryTracker memoryTracker, ChunkMemoryEstimator<T> memoryEstimator) {
            memoryTracker.allocateHeap(
                    SHALLOW_SIZE + SCOPED_MEMORY_TRACKER_SHALLOW_SIZE + shallowSizeOfObjectArray(size));

            elements = new Object[size];
            this.memoryTracker = memoryTracker;
            this.memoryEstimator = memoryEstimator;
        }

        boolean add(T element) {
            if (cursor < elements.length) {
                @SuppressWarnings("unchecked")
                final var previous = cursor == 0 ? null : (T) elements[cursor - 1];
                memoryTracker.allocateHeap(memoryEstimator.estimateHeapUsage(element, previous));
                elements[cursor++] = element;
                return true;
            }
            return false;
        }

        void close() {
            memoryTracker.close();
        }
    }
}
