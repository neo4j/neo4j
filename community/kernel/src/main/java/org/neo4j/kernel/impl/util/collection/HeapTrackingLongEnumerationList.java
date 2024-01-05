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
import java.util.NoSuchElementException;
import java.util.function.BiConsumer;
import org.eclipse.collections.api.block.procedure.primitive.LongObjectProcedure;
import org.neo4j.internal.kernel.api.DefaultCloseListenable;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.Preconditions;

/**
 * A heap-tracking list that also provides a limited ordered map-interface where the keys are
 * strictly increasing primitive longs, starting from 0.
 * <p>
 * Heap-tracking is only for the internal structure, not the elements within.
 *
 * <p>
 * Elements are inserted in a single-linked chunk array list to allow traversal from first to last in the order of insertion.
 * Elements can only be appended at the last index and no replacement of existing elements is possible,
 * but removing elements is possible at any index.
 * If a range of first elements are removed so that a chunk becomes empty, its heap memory will be reclaimed.
 * <p>
 * The chunk size is configurable at creation, but must be a power of two. It is fixed for every chunk.
 * <p>
 * The ideal use case is to remove elements at the beginning and adding new elements at the end,
 * like a sliding window.
 * Optimal memory usage is achieved if this sliding window size is below the configured chunk size,
 * since we reuse the same chunk in this case and no additional chunks needs to be allocated.
 * E.g. a pattern: add(0..c-1), get(0..c-1), remove(0..c-1), add(c..2c-1), get(c..2c-1), remove(c..2c-1), ...
 * <p>
 * Indexed access with {@link #get(long)} is fast when the index is in the range of the first or the last chunk.
 * Indexed access in between the first and the last chunk is also possible, but has access complexity linear to
 * the number of chunks traversed.
 * <p>
 * Fast access to the last chunk and the second to last chunk avoids linear traverse in cases where the elements accessed by index are
 * in the range of a sliding window at the end of the list, even if no elements are removed.
 * E.g. the pattern: add(0..c-1), get(0..c-1), add(c..2c-1), get(c..2c-1), ...
 * (This should be fast, but memory usage will build up)
 *
 * @param <V> value type
 */
public class HeapTrackingLongEnumerationList<V> extends DefaultCloseListenable {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(HeapTrackingLongEnumerationList.class);
    public static final int DEFAULT_CHUNK_SIZE = 1024; // Must be a power of 2

    private final int chunkSize;
    private final int chunkShiftAmount;
    private final MemoryTracker scopedMemoryTracker;

    // Linked chunk list used to store values
    private Chunk<V> firstChunk;
    private Chunk<V> lastChunk;
    private Chunk<V> secondLastChunk;

    // The range of the enumeration that the chunk list currently contains values for
    private long firstKey;
    private long lastKey; // NOTE: This is the last added key in the entire list _plus_ 1, i.e. the next key to be added
    private long lastKeyInFirstChunk; // NOTE: This is the last added key in the first chunk _plus_ 1

    public static <V> HeapTrackingLongEnumerationList<V> create(MemoryTracker memoryTracker) {
        return create(memoryTracker, DEFAULT_CHUNK_SIZE);
    }

    public static <V> HeapTrackingLongEnumerationList<V> create(MemoryTracker memoryTracker, int chunkSize) {
        Preconditions.requirePowerOfTwo(chunkSize);
        MemoryTracker scopedMemoryTracker = memoryTracker.getScopedMemoryTracker();
        scopedMemoryTracker.allocateHeap(SHALLOW_SIZE + SCOPED_MEMORY_TRACKER_SHALLOW_SIZE);
        return new HeapTrackingLongEnumerationList<>(scopedMemoryTracker, chunkSize);
    }

    private HeapTrackingLongEnumerationList(MemoryTracker scopedMemoryTracker, int chunkSize) {
        this.chunkSize = chunkSize;
        this.chunkShiftAmount = Integer.numberOfTrailingZeros(chunkSize);
        this.scopedMemoryTracker = scopedMemoryTracker;
        firstChunk = new Chunk<>(scopedMemoryTracker, chunkSize);
        lastChunk = firstChunk;
    }

    /**
     * Get a value by key
     *
     * @return the value at `key` index in the enumeration
     */
    @SuppressWarnings("unchecked")
    public V get(long key) {
        if (key < firstKey || key >= lastKey) {
            return null;
        }
        Chunk<V> chunk = findChunk(key);
        int indexInChunk = ((int) key) & (chunkSize - 1);
        return (V) chunk.values[indexInChunk];
    }

    /**
     * Get the first value
     *
     * @return the value of the first added key in the enumeration that has not been removed, or null if no keys exist
     */
    @SuppressWarnings("unchecked")
    public V getFirst() {
        if (firstKey < lastKey) {
            return (V) firstChunk.values[((int) firstKey) & (chunkSize - 1)];
        } else {
            return null;
        }
    }

    /**
     * Put a value at the given key, within or beyond the current enumeration.
     *
     * <p>
     * Replaces the existing value if it exists.
     * Putting after the last key is the same as adding the value with {@link #add}, after first adding `key - lastKey() - 1` number of null values as padding.
     * Putting before the first key is not allowed and will throw {@link IndexOutOfBoundsException}
     *
     * @param key   the key to put at
     * @param value the value to be inserted
     * @return the old value at the index or null
     */
    @SuppressWarnings("unchecked")
    public V put(long key, V value) {
        if (key < firstKey) {
            throw new IndexOutOfBoundsException(String.format("Cannot put key %s before first key %s", key, firstKey));
        }
        if (key >= lastKey) {
            // Use add() for padding
            // If the difference is huge we may want to implement an optimized way of adding entire new chunks
            while (lastKey < key) {
                add(null);
            }
            add(value);
            return null;
        }
        // Replace value
        Chunk<V> chunk = findChunk(key);
        int indexInChunk = ((int) key) & (chunkSize - 1);
        V oldValue = (V) chunk.values[indexInChunk];
        chunk.values[indexInChunk] = value;
        return oldValue;
    }

    /**
     * Add a value at the end of the enumeration, accessible at index {@link #lastKey()}, which is increased by 1.
     * <p>
     * Adds to the last chunk if possible, otherwise creates a new chunk and inserts the value in the new chunk.
     * If all the elements in the last chunk has already been removed, it will be reused instead of allocating a new chunk.
     *
     * @param value the value to be inserted
     */
    public void add(V value) {
        if (firstChunk == lastChunk) {
            addToSingleChunk(value);
        } else {
            addToTailChunk(value);
        }
    }

    // When we have only a single chunk, we pack it like a ring-buffer
    private void addToSingleChunk(V value) {
        int chunkMask = chunkSize - 1;
        int firstIndexInChunk = ((int) firstKey) & chunkMask;
        int lastIndexInChunk = ((int) lastKey) & chunkMask;
        boolean addedNewChunk = false;

        if (lastIndexInChunk == firstIndexInChunk) {
            if (!isEmpty()) {
                // The chunk is full. We need to allocate a new chunk
                Chunk<V> newChunk = new Chunk<>(scopedMemoryTracker, chunkSize);
                secondLastChunk = lastChunk;
                lastChunk.next = newChunk;
                lastChunk = newChunk;
                addedNewChunk = true;
            } else {
                if (value
                        == null) // Special case if null is added as the first key, the list should still be considered
                // empty
                {
                    firstKey++;
                }
            }
        }

        // Set the value
        lastChunk.values[lastIndexInChunk] = value;
        lastKey++;
        if (!addedNewChunk) {
            lastKeyInFirstChunk = lastKey;
        }
    }

    private void addToTailChunk(V value) {
        int indexInChunk = ((int) lastKey) & (chunkSize - 1);

        // The last chunk may be full
        if (indexInChunk == 0) {
            // We need to allocate a new chunk
            Chunk<V> newChunk = new Chunk<>(scopedMemoryTracker, chunkSize);
            secondLastChunk = lastChunk;
            lastChunk.next = newChunk;
            lastChunk = newChunk;
        }

        // Set the value
        lastChunk.values[indexInChunk] = value;

        // Update last
        lastKey++;
    }

    /**
     * Remove the value at `key` index in the enumeration.
     *
     * @param key The enumeration
     * @return the value that was removed, or null if it was not found or has already been removed.
     */
    public V remove(long key) {
        if (key < firstKey || key >= lastKey) {
            return null;
        }
        if (firstChunk == lastChunk) {
            return removeInSingleChunk(key);
        } else {
            return removeInMultipleChunks(key);
        }
    }

    @SuppressWarnings("unchecked")
    private V removeInSingleChunk(long key) {
        Chunk<V> chunk = firstChunk;
        int chunkMask = chunkSize - 1;
        int firstIndexInChunk = ((int) firstKey) & chunkMask;
        int removeIndexInChunk = ((int) key) & chunkMask;

        V removedValue = (V) chunk.values[removeIndexInChunk];
        chunk.values[removeIndexInChunk] = null;

        // Update first in single chunk
        while (firstKey < lastKey && firstChunk.values[firstIndexInChunk] == null) {
            firstKey++;
            firstIndexInChunk = ((int) firstKey) & chunkMask;
        }

        return removedValue;
    }

    @SuppressWarnings("unchecked")
    private V removeInMultipleChunks(long key) {
        Chunk<V> chunk = findChunk(key);

        int chunkMask = chunkSize - 1;
        int indexInChunk = ((int) key) & chunkMask;
        V removedValue = (V) chunk.values[indexInChunk];
        chunk.values[indexInChunk] = null;

        // If we removed the first key we need to move the references to the first element
        if (key == firstKey) {
            updateFirstOfMultipleChunks(chunk, chunkMask);
        }

        return removedValue;
    }

    private Chunk<V> findChunk(long key) {
        // Check if the key is within the first chunk
        if (key < lastKeyInFirstChunk) {
            return firstChunk;
        }

        long keyChunkNumber = key >>> chunkShiftAmount;
        long lastChunkNumber = (lastKey - 1) >>> chunkShiftAmount;

        // Check if the key is within the last chunk
        if (keyChunkNumber == lastChunkNumber) {
            return lastChunk;
        }
        // Or the second to last chunk
        else if (keyChunkNumber == lastChunkNumber - 1) {
            return secondLastChunk;
        } else {
            // Otherwise traverse from the second chunk
            Chunk<V> chunk = firstChunk.next;

            // We need to align the key offset since tail chunk boundaries are always fixed in the enumeration key
            // space:
            //   [0..chunkSize-1][chunkSize..2*chunkSize-1]...
            long offset = key - lastKeyInFirstChunk;
            long alignment = lastKeyInFirstChunk & (chunkSize - 1);
            long index = offset + alignment;

            long nChunkHops = index >>> chunkShiftAmount;
            for (int i = 0; i < nChunkHops; i++) {
                chunk = chunk.next;
            }
            return chunk;
        }
    }

    /*
     * Updates `firstKey` to be the index of the first value which has not been removed.
     *
     * E.g.
     * if we have [null, null, 12, null, 3] -> then firstKey = 2
     *
     * if we remove index 2 we get [null, null, null, null, 3] -> then firstKey = 4
     */
    private void updateFirstOfMultipleChunks(Chunk<V> chunk, int chunkMask) {
        int firstIndexInChunk = ((int) firstKey) & chunkMask;
        while (firstKey < lastKey && chunk.values[firstIndexInChunk] == null) {
            firstKey++;
            boolean moveToNextChunk;

            if (chunk == firstChunk) {
                firstIndexInChunk = (firstIndexInChunk + 1) & chunkMask;
                moveToNextChunk = firstKey >= lastKeyInFirstChunk;
            } else {
                firstIndexInChunk++;
                moveToNextChunk = firstIndexInChunk >= chunkSize;
            }
            if (moveToNextChunk && chunk != lastChunk) {
                firstIndexInChunk = ((int) firstKey) & chunkMask;
                chunk.close(scopedMemoryTracker);
                chunk = chunk.next;
            }
        }

        if (chunk != firstChunk) {
            // Update references to the new first chunk
            if (secondLastChunk == firstChunk) {
                secondLastChunk = null;
            }
            firstChunk = chunk;

            // Update lastKeyInFirstChunk
            if (chunk == lastChunk) {
                lastKeyInFirstChunk = lastKey;
            } else {
                // Middle chunks are always aligned so that lastKeyInFirstChunk should be at the chunk boundary
                lastKeyInFirstChunk = (firstKey & ~chunkMask) + chunkSize;
            }
        }
    }

    /*
     * Do we have any values
     */
    public boolean isEmpty() {
        return firstKey == lastKey;
    }

    /**
     * Apply the function for each key-value pair in the list, but skipping over null values.
     */
    @SuppressWarnings("unchecked")
    public void foreach(LongObjectProcedure<V> fun) {
        Chunk<V> chunk = firstChunk;
        final int cs = chunkSize;
        long key = firstKey;
        int chunkMask = cs - 1;
        int index = ((int) key) & chunkMask;
        final long lkifc = lastKeyInFirstChunk;

        // Iterate over the first chunk
        while (key < lkifc) {
            V value = (V) chunk.values[index];
            if (value != null) {
                fun.value(key, value);
            }
            index = (index + 1) & chunkMask;
            key++;
        }

        // Iterate over remaining chunks
        for (chunk = chunk.next; chunk != null; chunk = chunk.next) {
            for (int i = index; i < cs; i++, key++) {
                V value = (V) chunk.values[i];
                if (value != null) {
                    fun.value(key, value);
                }
            }
            index = 0;
        }
    }

    /**
     * Remove each key starting from the first key until we reach the given key or the end of the list,
     * and apply the given function to each removed value, skipping over null values.
     */
    @SuppressWarnings("unchecked")
    public void removeUntil(long untilKey, BiConsumer<Long, V> fun) {
        long until = Math.min(untilKey, lastKey);
        while (firstKey < until) {
            long key = firstKey;
            V value = remove(key);
            fun.accept(key, value);
        }
    }

    /**
     * @return The last added key or -1 if no keys exist (i.e. if nothing was ever added to the list)
     */
    public long lastKey() {
        return lastKey - 1;
    }

    public MemoryTracker scopedMemoryTracker() {
        return scopedMemoryTracker;
    }

    @Override
    public void closeInternal() {
        if (!isClosed()) {
            firstChunk = null;
            lastChunk = null;
            secondLastChunk = null;
            scopedMemoryTracker.close();
        }
    }

    @Override
    public boolean isClosed() {
        return firstChunk == null;
    }

    /**
     * Iterator of all non-null values in the list
     * Warning: not safe to modify during iteration.
     */
    public Iterator<V> valuesIterator() {
        if (isEmpty()) {
            return java.util.Collections.emptyIterator();
        } else {
            if (firstChunk == lastChunk) {
                return new SingleChunkValuesIterator();
            } else {
                return new ValuesIterator();
            }
        }
    }

    private class SingleChunkValuesIterator implements Iterator<V> {
        private final Chunk<V> chunk;
        private long key;

        {
            chunk = firstChunk;
            key = firstKey;
        }

        @Override
        public boolean hasNext() {
            int index = ((int) key) & (chunkSize - 1);
            return key < lastKeyInFirstChunk && chunk.values[index] != null;
        }

        @Override
        @SuppressWarnings("unchecked")
        public V next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }

            int chunkMask = chunkSize - 1;

            int index = ((int) key) & chunkMask;
            Object value = chunk.values[index];

            // Advance next entry
            do {
                key++;
                index = (index + 1) & chunkMask;
            } while (key < lastKeyInFirstChunk && chunk.values[index] == null);

            return (V) value;
        }
    }

    private class ValuesIterator implements Iterator<V> {
        private Chunk<V> chunk;
        private int index;
        private long key;

        {
            chunk = firstChunk;
            key = firstKey;
            index = ((int) firstKey) & (chunkSize - 1);
        }

        @Override
        public boolean hasNext() {
            return chunk != null && chunk.values[index] != null;
        }

        @Override
        @SuppressWarnings("unchecked")
        public V next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }

            Object value = chunk.values[index];

            // Advance next entry
            if (chunk == firstChunk) {
                findNextInFirstChunk();
            } else {
                findNextInTailChunks();
            }

            return (V) value;
        }

        private void findNextInFirstChunk() {

            boolean hasRemainingElements;
            do {
                key++;
                index = (index + 1) & (chunkSize - 1);
                hasRemainingElements = key < lastKeyInFirstChunk;
            } while (hasRemainingElements && chunk.values[index] == null);

            if (hasRemainingElements) {
                // We still have elements in the first chunk (chunk.values[index] != null)
                return;
            }

            // Move to the next chunk
            chunk = chunk.next;

            if (chunk != null && chunk.values[index] == null && key < lastKey) {
                findNextInTailChunks();
            }
        }

        private void findNextInTailChunks() {
            do {
                key++;
                index++;
                if (index >= chunkSize) {
                    index = 0;
                    chunk = chunk.next;
                }
            } while (chunk != null && chunk.values[index] == null && key < lastKey);
        }
    }

    private static class Chunk<V> {
        private static final long SHALLOW_SIZE = shallowSizeOfInstance(Chunk.class);

        private final Object[] values;
        private Chunk<V> next;

        Chunk(MemoryTracker memoryTracker, int chunkSize) {
            memoryTracker.allocateHeap(SHALLOW_SIZE + shallowSizeOfObjectArray(chunkSize));
            values = new Object[chunkSize];
        }

        void close(MemoryTracker memoryTracker) {
            memoryTracker.releaseHeap(SHALLOW_SIZE + shallowSizeOfObjectArray(values.length));
        }
    }
}
