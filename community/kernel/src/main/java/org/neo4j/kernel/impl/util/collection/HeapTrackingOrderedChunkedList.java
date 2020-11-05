/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.util.collection;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.BiConsumer;

import org.neo4j.internal.kernel.api.DefaultCloseListenable;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.Preconditions;

import static org.neo4j.memory.HeapEstimator.SCOPED_MEMORY_TRACKER_SHALLOW_SIZE;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfObjectArray;

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
 * Fast access to the last chunk avoids linear traverse in cases where the elements accessed by index are
 * in the range of a sliding window at the end of the list, even if no elements are removed.
 * E.g. the pattern: add(0..c-1), get(0..c-1), add(c..2c-1), get(c..2c-1), ...
 * (This should be fast, but memory usage will build up)
 *
 * @param <V> value type
 */
public class HeapTrackingOrderedChunkedList<V> extends DefaultCloseListenable
{
    private static final long SHALLOW_SIZE = shallowSizeOfInstance( HeapTrackingOrderedChunkedList.class );
    static final int DEFAULT_CHUNK_SIZE = 4096; // Must be a power of 2

    private final int chunkSize;
    private final int chunkShiftAmount;
    private final MemoryTracker scopedMemoryTracker;

    // Linked chunk list used to store values
    private Chunk<V> firstChunk;
    private Chunk<V> lastChunk;
    private int indexInLastChunk;

    // The range of the enumeration that the chunk list currently contains values for
    private long firstKey;
    private long lastKey;

    public static <V> HeapTrackingOrderedChunkedList<V> create( MemoryTracker memoryTracker )
    {
        return create( memoryTracker, DEFAULT_CHUNK_SIZE );
    }

    public static <V> HeapTrackingOrderedChunkedList<V> create( MemoryTracker memoryTracker, int chunkSize )
    {
        Preconditions.requirePowerOfTwo( chunkSize );
        MemoryTracker scopedMemoryTracker = memoryTracker.getScopedMemoryTracker();
        scopedMemoryTracker.allocateHeap( SHALLOW_SIZE + SCOPED_MEMORY_TRACKER_SHALLOW_SIZE );
        return new HeapTrackingOrderedChunkedList<>( scopedMemoryTracker, chunkSize );
    }

    private HeapTrackingOrderedChunkedList( MemoryTracker scopedMemoryTracker, int chunkSize )
    {
        this.chunkSize = chunkSize;
        this.chunkShiftAmount = Integer.numberOfTrailingZeros( chunkSize );
        this.firstKey = -1;
        this.lastKey = -1;
        this.indexInLastChunk = 0;
        this.scopedMemoryTracker = scopedMemoryTracker;
        firstChunk = new Chunk<>( scopedMemoryTracker, chunkSize );
        lastChunk = firstChunk;
    }

    /**
     * Get a value by key
     *
     * @return the value at `key` index in the enumeration
     */
    @SuppressWarnings( "unchecked" )
    public V get( long key )
    {
        long index = key - firstKey;
        if ( index < 0 || index >= size() ) // NOTE: This should work even when firstKey == -1, because size() will be 0
        {
            return null;
        }

        Chunk<V> chunk;
        int chunkMask = chunkSize - 1;

        // Check if the key is in the last chunk
        if ( key >>> chunkShiftAmount == lastKey >>> chunkShiftAmount )
        {
            chunk = lastChunk;
            index = key & chunkMask;
        }
        else // Start looking from the first chunk
        {
            chunk = firstChunk;
            int removedInFirstChunk = (int) (firstKey & chunkMask);
            index += removedInFirstChunk;
        }
        while ( index >= chunkSize ) // find chunk in which the value should be removed
        {
            chunk = chunk.next;
            index -= chunkSize;
        }
        return (V) chunk.values[(int) index];
    }

    /**
     * Get the first value
     *
     * @return the value of the first added key in the enumeration that has not been removed, or null if no keys exist
     */
    @SuppressWarnings( "unchecked" )
    public V getFirst()
    {
        if ( firstKey >= 0 && firstChunk != null )
        {
            return (V) firstChunk.values[(int) (firstKey & (chunkSize - 1))];
        }
        else
        {
            return null;
        }
    }

    /**
     * Add a value at the end of the enumeration, accessible at index {@link #lastKey()}, which is increased by 1.
     * <p>
     * Adds to the last chunk if possible, otherwise creates a new chunk and inserts the value in the new chunk.
     * If all the elements in the last chunk has already been removed, it will be reused instead of allocating a new chunk.
     *
     * @param value the value to be inserted
     */
    public void add( V value )
    {
        boolean isEmpty = isEmpty();

        // The last chunk may be full
        if ( indexInLastChunk >= chunkSize )
        {
            // If the list is empty we can reuse the current chunk (which is expected to be the common case under ideal usage conditions)
            if ( !isEmpty )
            {
                // Otherwise we need to allocate a new chunk
                Chunk<V> newChunk = new Chunk<>( scopedMemoryTracker, chunkSize );
                lastChunk.next = newChunk;
                lastChunk = newChunk;
            }
            indexInLastChunk = 0;
        }

        if ( isEmpty )
        {
            // If the list is empty we need to update first
            firstKey++;
            firstChunk = lastChunk;
        }

        // Set the value
        lastChunk.values[indexInLastChunk] = value;

        // Update last
        lastKey++;
        indexInLastChunk++;
    }

    /**
     * Remove the value at `key` index in the enumeration.
     *
     * @param key The enumeration
     * @return the value that was removed, or null if it was not found or has already been removed.
     */
    @SuppressWarnings( "unchecked" )
    public V remove( long key )
    {
        if ( key < firstKey || key > lastKey )
        {
            return null;
        }
        Chunk<V> chunk = firstChunk;

        // Find chunk and index where the value should be removed
        long i = (key - firstKey) + (firstKey & (chunkSize - 1));
        while ( i >= chunkSize )
        {
            chunk = chunk.next;
            i -= chunkSize;
        }
        int indexInChunk = (int) i; // Now indexInChunk is [0..chunkSize[ so can safely be cast to int
        V removedValue = (V) chunk.values[indexInChunk];
        chunk.values[indexInChunk] = null;

        updateFirstKey();

        return removedValue;
    }

    /*
     * Updates `firstKey` to be the index of the first value which has not been removed.
     *
     * E.g.
     * if we have [null, null, 12, null, 3] -> then firstKey = 2
     *
     * if we remove index 2 we get [null, null, null, null, 3] -> then firstKey = 4
     */
    private void updateFirstKey()
    {
        int removedInFirstChunk = ((int) firstKey) & (chunkSize - 1);
        while ( firstKey < lastKey && firstChunk.values[removedInFirstChunk] == null )
        {
            firstKey++;
            removedInFirstChunk++;
            if ( removedInFirstChunk >= chunkSize )
            {
                removedInFirstChunk = 0;
                firstChunk.close( scopedMemoryTracker );
                firstChunk = firstChunk.next;
            }
        }
    }

    /*
     * The current size of the range of accessible elements.
     *
     * NOTE: Elements that have been removed _within_ this range are included in the size,
     * so it is _not_ the same as numberOfElements
     *
     * E.g. if we have 3 chunks with chunk size 4:
     *
     * [null, null, null, 4] -> [null, 3, 2, 4] -> [4, 3, null, null]
     *
     * then
     * firstKey = 3
     * lastKey = 9
     * size = 7 ([4, null, 3, 2, 4, 4, 3])
     */
    private long size()
    {
        return isEmpty() ? 0 : lastKey - firstKey + 1;
    }

    /*
     * Do we have any values
     */
    private boolean isEmpty()
    {
        return getFirst() == null;
    }

    /**
     * Apply the function for each key-value pair in the list.
     */
    @SuppressWarnings( "unchecked" )
    public void foreach( BiConsumer<Long,V> fun )
    {
        Chunk<V> chunk = firstChunk;
        long key = firstKey;
        int index = ((int) key) & (chunkSize - 1);
        while ( key <= lastKey )
        {
            if ( index >= chunkSize )
            {
                chunk = chunk.next;
                index = 0;
            }
            V value = (V) chunk.values[index];
            if ( value != null )
            {
                fun.accept( key, value );
            }
            index += 1;
            key += 1;
        }
    }

    /**
     * @return The last added key or -1 if no keys exist
     */
    public long lastKey()
    {
        return lastKey;
    }

    public MemoryTracker scopedMemoryTracker()
    {
        return scopedMemoryTracker;
    }

    @Override
    public void closeInternal()
    {
        firstChunk = null;
        lastChunk = null;
        scopedMemoryTracker.close();
    }

    @Override
    public boolean isClosed()
    {
        return firstChunk == null;
    }

    public Iterator<V> iterator()
    {
        if ( isEmpty() )
        {
            return java.util.Collections.emptyIterator();
        }
        else
        {
            return new ValuesIterator();
        }
    }

    private class ValuesIterator implements Iterator<V>
    {
        private Chunk<V> chunk;
        private int index;

        {
            chunk = firstChunk;
            index = (int) (firstKey & (chunkSize - 1));
        }

        @Override
        public boolean hasNext()
        {
            return chunk != null && chunk.values[index] != null;
        }

        @Override
        @SuppressWarnings( "unchecked" )
        public V next()
        {
            if ( !this.hasNext() )
            {
                throw new NoSuchElementException();
            }

            Object value = chunk.values[index];

            // Advance next entry
            findNext();

            return (V) value;
        }

        private void findNext()
        {
            do
            {
                index++;
                if ( index >= chunkSize )
                {
                    index = 0;
                    chunk = chunk.next;
                }
            }
            while ( chunk != null && chunk.values[index] == null );
        }
    }

    private static class Chunk<V>
    {
        private static final long SHALLOW_SIZE = shallowSizeOfInstance( Chunk.class );

        private final Object[] values;
        private Chunk<V> next;

        Chunk( MemoryTracker memoryTracker, int chunkSize )
        {
            memoryTracker.allocateHeap( SHALLOW_SIZE + shallowSizeOfObjectArray( chunkSize ) );
            values = new Object[chunkSize];
        }

        void close( MemoryTracker memoryTracker )
        {
            memoryTracker.releaseHeap( SHALLOW_SIZE + shallowSizeOfObjectArray( values.length ) );
        }
    }
}
