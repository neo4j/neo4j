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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import org.neo4j.internal.kernel.api.DefaultCloseListenable;
import org.neo4j.memory.MemoryTracker;

import static org.neo4j.kernel.impl.util.collection.LongProbeTable.SCOPED_MEMORY_TRACKER_SHALLOW_SIZE;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfObjectArray;

/**
 * A heap tracking, ordered list. It only tracks the internal structure, not the elements within.
 * <p>
 * Elements are also inserted in a single-linked chunked list to allow traversal from first to last in the order of insertion. No replacement of existing
 * elements is
 * possible.
 *
 * @param <V> value type
 */
public class HeapTrackingOrderedChunkedList<V> extends DefaultCloseListenable
{
    //-----------------------------------------------------------------------------------------------------------------
    // TODO: Remove statistics
    private static final boolean DEBUG = true;
    public static AtomicLong totalChunkHopCounter = new AtomicLong( 0 );
    public static AtomicLong totalNewChunkCounter = new AtomicLong( 0 );
    private int chunkHopCounter;
    private int newChunkCounter;
    //-----------------------------------------------------------------------------------------------------------------

    private static final long SHALLOW_SIZE = shallowSizeOfInstance( HeapTrackingOrderedChunkedList.class );
    //static final int DEFAULT_CHUNK_SIZE = 4096; // Must be a power of 2
    static final int DEFAULT_CHUNK_SIZE = 64; // Must be a power of 2 // TODO: Remove statistics gathering

    private final int chunkSize;
    private final MemoryTracker scopedMemoryTracker;

    // Linked chunk list used to store key-value pairs in order
    private Chunk<V> first;
    private Chunk<V> current;
    private int indexInCurrentChunk;
    private long firstKey;
    private long lastKey;

    public static <V> HeapTrackingOrderedChunkedList<V> createOrderedMap( MemoryTracker memoryTracker )
    {
        return createOrderedMap( memoryTracker, DEFAULT_CHUNK_SIZE );
    }

    public static <V> HeapTrackingOrderedChunkedList<V> createOrderedMap( MemoryTracker memoryTracker, int chunkSize )
    {
        assert chunkSize > 0 && ((chunkSize & (chunkSize - 1)) == 0); // Must be a power of 2
        MemoryTracker scopedMemoryTracker = memoryTracker.getScopedMemoryTracker();
        scopedMemoryTracker.allocateHeap( SHALLOW_SIZE + SCOPED_MEMORY_TRACKER_SHALLOW_SIZE );
        return new HeapTrackingOrderedChunkedList<>( scopedMemoryTracker, chunkSize );
    }

    private HeapTrackingOrderedChunkedList( MemoryTracker scopedMemoryTracker, int chunkSize )
    {
        this.chunkSize = chunkSize;
        this.firstKey = -1;
        this.lastKey = -1;
        this.indexInCurrentChunk = 0;
        this.scopedMemoryTracker = scopedMemoryTracker;
        first = new Chunk<>( scopedMemoryTracker, chunkSize );
        current = first;
    }

    /**
     * @param key
     * @return the value at `key` index
     */
    @SuppressWarnings( "unchecked" )
    public V get( long key )
    {
        long index = key - firstKey;
        int removedInFirstChunk = (int) (firstKey % chunkSize);

        if ( index < 0 || index >= size() )
        {
            return null;
        }

        Chunk<V> chunk = first;
        index = index + removedInFirstChunk;
        //-----------------------------------------------------------------
        // TODO: Remove statistics gathering
        if ( DEBUG )
        {
            System.out.println( "get(" + key + ")" );
            if ( index >= chunkSize )
            {
                int chunkHops = 0;
                while ( index >= chunkSize ) // find chunk in which the value should be removed
                {
                    chunk = chunk.next;
                    index -= chunkSize;
                    chunkHops++;
                }
                chunkHopCounter += chunkHops;
                var total = totalChunkHopCounter.addAndGet( chunkHops );
                System.out.println( String.format( ">>> Chunk hops (get) +%s this count: %s total: %s", chunkHops, chunkHopCounter, total ) );
            }
        }
        else
        //-----------------------------------------------------------------
        {
            while ( index >= chunkSize ) // find chunk in which the value should be removed
            {
                chunk = chunk.next;
                index -= chunkSize;
            }
        }
        return (V) chunk.values[(int) index];
    }

    @SuppressWarnings( "unchecked" )
    public V getFirst()
    {
        if ( firstKey >= 0 && first != null )
        {
            return (V) first.values[(int) (firstKey % chunkSize)];
        }
        else
        {
            return null;
        }
    }

    /**
     * Adds the value to the current chunk if possible, otherwise creates a new chunk and inserts the value in the new chunk.
     *
     * @param value the value to be inserted
     */
    public void add( V value )
    {
        if ( DEBUG )
        {
            System.out.println( "add(" + (lastKey + 1) + ")" );
        }
        assert value != null;
        if ( indexInCurrentChunk >= chunkSize )
        {
            Chunk<V> newChunk = new Chunk<>( scopedMemoryTracker, chunkSize );
            current.next = newChunk;
            current = newChunk;
            indexInCurrentChunk = 0;

            //-----------------------------------------------------------------
            // TODO: Remove statistics gathering
            if ( DEBUG )
            {
                newChunkCounter++;
                var total = totalNewChunkCounter.addAndGet( 1 );
                System.out.println( String.format( "### New chunk count: %s total: %s", newChunkCounter, total ) );
            }
            //-----------------------------------------------------------------
        }
        if ( isEmpty() )
        {
            // If the list is empty we need to update first
            firstKey++;
            first = current;
        }

        // Set the value
        current.values[indexInCurrentChunk] = value;

        lastKey++;
        indexInCurrentChunk++;
    }

    /**
     * Remove the value at `key` index.
     *
     * @param key
     * @return the value that was removed.
     */
    @SuppressWarnings( "unchecked" )
    public V remove( long key )
    {
        //-----------------------------------------------------------------
        // TODO: Remove statistics gathering
        if ( DEBUG )
        {
            System.out.println( "remove(" + key + ")" );
        }
        int chunkHops = 0;
        //-----------------------------------------------------------------

        if ( key < firstKey || key > lastKey )
        {
            return null;
        }
        Chunk<V> chunk = first;

        // Find chunk and index where the value should be removed
        long i = (key - firstKey) + (firstKey % chunkSize);
        while ( i >= chunkSize )
        {
            chunk = chunk.next;
            i -= chunkSize;
            chunkHops++; // TODO: Remove statistics gathering
        }
        int indexInChunk = (int) i; // Now indexInChunk is [0..chunkSize[ so can safely be cast to int
        V removedValue = (V) chunk.values[indexInChunk];
        chunk.values[indexInChunk] = null;

        //-----------------------------------------------------------------
        // TODO: Remove statistics gathering
        if ( DEBUG && chunkHops > 0 )
        {
            chunkHopCounter += chunkHops;
            var total = totalChunkHopCounter.addAndGet( chunkHops );
            System.out.println( String.format( ">>> Chunk hops (remove) +%s this count: %s total: %s", chunkHops, chunkHopCounter, total ) );
        }
        //-----------------------------------------------------------------

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
        int chunkHops = 0; // TODO: Remove statistics gathering
        int removedInFirstChunk = (int) (firstKey % chunkSize);
        while ( firstKey < lastKey && first.values[removedInFirstChunk] == null )
        {
            firstKey++;
            removedInFirstChunk++;
            if ( removedInFirstChunk >= chunkSize )
            {
                removedInFirstChunk = 0;
                first.close( scopedMemoryTracker );
                first = first.next;
                chunkHops++; // TODO: Remove statistics gathering
            }
        }
        //-----------------------------------------------------------------
        // TODO: Remove statistics gathering
        if ( DEBUG && chunkHops > 0 )
        {
            chunkHopCounter += chunkHops;
            var total = totalChunkHopCounter.addAndGet( chunkHops );
            System.out.println( String.format( ">>> Chunk hops (update first) +%s this count: %s total: %s", chunkHops, chunkHopCounter, total ) );
        }
        //-----------------------------------------------------------------
    }

    /*
     * The size of the
     *
     * E.g. if we have 3 chunks with chunk size 4:
     *
     * [null, null, null, 4] -> [null, 3, 2, 4] -> [4, 3, null, null]
     *
     * then
     * firstKey = 3
     * indexInCurrentChunk = 2
     * size = 7 ([4, null, 3, 2, 4, 4, 3])
     */
    private long size()
    {
        return isEmpty() ? 0 : lastKey - firstKey + 1;
    }

    private boolean isEmpty()
    {
        // TODO: Optimize?
        return getFirst() == null;
    }

    /**
     * Apply the function for each index-value pair in the list.
     */
    @SuppressWarnings( "unchecked" )
    public void foreach( BiConsumer<Long,V> fun )
    {
        //-----------------------------------------------------------------
        // TODO: Remove statistics gathering
        if ( DEBUG )
        {
            System.out.println( "foreach" );
        }
        int chunkHops = 0;
        //-----------------------------------------------------------------

        Chunk<V> chunk = first;
        long key = firstKey;
        int index = (int) (key % chunkSize);
        while ( key <= lastKey )
        {
            if ( index >= chunkSize )
            {
                chunk = chunk.next;
                assert chunk != null; // TODO: Remove?
                index = 0;
                chunkHops++; // TODO: Remove statistics gathering
            }
            V value = (V) chunk.values[index];
            if ( value != null )
            {
                fun.accept( key, value );
            }
            index += 1;
            key += 1;
        }

        //-----------------------------------------------------------------
        // TODO: Remove statistics gathering
        if ( DEBUG && chunkHops > 0 )
        {
            chunkHopCounter += chunkHops;
            var total = totalChunkHopCounter.addAndGet( chunkHops );
            System.out.println( String.format( ">>> Chunk hops (foreach) +%s this count: %s total: %s", chunkHops, chunkHopCounter, total ) );
        }
        //-----------------------------------------------------------------
    }

    /**
     * @return The last added key or -1 if no key exists
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
        first = null;
        current = null;
        scopedMemoryTracker.close();
    }

    @Override
    public boolean isClosed()
    {
        return first == null;
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
            chunk = first;
            index = (int) (firstKey % chunkSize);
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
            int chunkHops = 0; // TODO: Remove statistics gathering
            do
            {
                index++;
                if ( index >= chunkSize )
                {
                    index = 0;
                    chunk = chunk.next;
                    chunkHops++; // TODO: Remove statistics gathering
                }
            }
            while ( chunk != null && chunk.values[index] == null );
            //-----------------------------------------------------------------
            // TODO: Remove statistics gathering
            if ( DEBUG && chunkHops > 0 )
            {
                chunkHopCounter += chunkHops;
                var total = totalChunkHopCounter.addAndGet( chunkHops );
                System.out.println( String.format( ">>> Chunk hops (iterator) +%s this count: %s total: %s", chunkHops, chunkHopCounter, total ) );
            }
            //-----------------------------------------------------------------
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
