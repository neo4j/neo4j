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

import static org.neo4j.kernel.impl.util.collection.LongProbeTable.SCOPED_MEMORY_TRACKER_SHALLOW_SIZE;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfObjectArray;

/**
 * A heap tracking, ordered list. It only tracks the internal structure, not the elements within.
 * <p>
 * Elements are also inserted in a single-linked chunked list to allow traversal from first to last in the order of insertion. No replacement of existing elements is
 * possible.
 *
 * @param <V> value type
 */
public class HeapTrackingOrderedChunkedList<V> extends DefaultCloseListenable
{
    private static final long SHALLOW_SIZE = shallowSizeOfInstance( HeapTrackingOrderedChunkedList.class );
    private static final int CHUNK_SIZE = 4096; // Must be even, preferably a power of 2

    private final MemoryTracker scopedMemoryTracker;

    // Linked chunk list used to store key-value pairs in order
    private Chunk<V> first;
    private Chunk<V> current;
    private int indexInCurrentChunk;
    private long firstKey;
    private long nbrChunksInMemory;

    public static <V> HeapTrackingOrderedChunkedList<V> createOrderedMap( MemoryTracker memoryTracker )
    {
        MemoryTracker scopedMemoryTracker = memoryTracker.getScopedMemoryTracker();
        scopedMemoryTracker.allocateHeap( SHALLOW_SIZE + SCOPED_MEMORY_TRACKER_SHALLOW_SIZE );
        return new HeapTrackingOrderedChunkedList<>( scopedMemoryTracker );
    }

    private HeapTrackingOrderedChunkedList( MemoryTracker scopedMemoryTracker )
    {
        this.firstKey = 0;
        this.indexInCurrentChunk = 0;
        this.scopedMemoryTracker = scopedMemoryTracker;
        first = new Chunk<>( scopedMemoryTracker );
        nbrChunksInMemory = 1;
        current = first;
    }

    /**
     * @param key
     * @return the value at `key` index
     */
    public V get( long key )
    {
        long index = key - firstKey;
        int removedInFirstChunk = (int) (firstKey % CHUNK_SIZE);

        if ( index < 0 || index >= size() )
        {
            return null;
        }

        Chunk<V> chunk = first;
        index = index + removedInFirstChunk;
        while ( index >= CHUNK_SIZE ) // find chunk in which the value should be removed
        {
            chunk = chunk.next;
            index -= CHUNK_SIZE;
        }

        return (V) chunk.values[(int) index];
    }

    public V getFirst()
    {
        return (V) first.values[(int) (firstKey % CHUNK_SIZE)];
    }

    /**
     * Adds the value to the current chunk if possible, otherwise creates a new chunk and inserts the value in the new chunk.
     * @param value the value to be inserted
     */
    public void add( V value )
    {
        if ( !current.add( indexInCurrentChunk, value ) )
        {
            Chunk<V> newChunk = new Chunk<>( scopedMemoryTracker );
            current.next = newChunk;
            current = newChunk;
            nbrChunksInMemory++;
            indexInCurrentChunk = 0;
            current.add( indexInCurrentChunk, value );
        }
        indexInCurrentChunk++;
    }

    /**
     * Remove the value at `key` index.
     * @param key
     * @return the value that was removed.
     */
    public V remove( long key )
    {
        long index = key - firstKey;
        Chunk<V> chunk = first;

        if ( index < 0 || index >= size() )
        {
            return null;
        }

        index = index + (firstKey % CHUNK_SIZE);
        while ( index >= CHUNK_SIZE ) // find chunk in which the value should be removed
        {
            chunk = chunk.next;
            index -= CHUNK_SIZE;
        }

        V removedValue = (V) chunk.values[(int) index];
        chunk.remove( (int) index );
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
        int removedInFirstChunk = (int) (firstKey % CHUNK_SIZE);
        while ( firstKey < size() && first.values[removedInFirstChunk] == null )
        {
            firstKey++;
            removedInFirstChunk++;
            if ( removedInFirstChunk >= CHUNK_SIZE )
            {
                nbrChunksInMemory--;
                removedInFirstChunk = 0;
                first.close( scopedMemoryTracker );
                first = first.next;
            }
        }
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
        return (CHUNK_SIZE - firstKey % CHUNK_SIZE) + (nbrChunksInMemory - 2) * CHUNK_SIZE + indexInCurrentChunk;
    }

    /**
     * Apply the procedure for each value in the list.
     */
//    @SuppressWarnings( "unchecked" )
//    public void forEachValue( Procedure<? super V> p )
//    {
//        Chunk chunk = first;
//        while ( chunk != null )
//        {
//            chunk.forEachValue( p );
//        }
//    }

    /**
     * Apply the function for each index-value pair in the list.
     */
    public void foreach( BiConsumer<Long,V> fun )
    {
        Chunk<V> currentChunk = first;
        long chunkIndex = firstKey;
        while ( null != currentChunk )
        {
            currentChunk.foreach( chunkIndex, (int) (chunkIndex % CHUNK_SIZE), fun );
            currentChunk = currentChunk.next;
            chunkIndex += CHUNK_SIZE;
        }
    }

    public long lastKey() {
        return firstKey + size() - 1;
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
        return new ValuesIterator();
    }

    private class ValuesIterator implements Iterator<V>
    {
        private Chunk<V> chunk;
        private int index;

        {
            chunk = first;
            index = (int) (firstKey % CHUNK_SIZE);
        }

        @Override
        public boolean hasNext()
        {
            return chunk != null;
        }

        @Override
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
                if ( index >= CHUNK_SIZE )
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

        Chunk( MemoryTracker memoryTracker )
        {
            memoryTracker.allocateHeap( SHALLOW_SIZE + shallowSizeOfObjectArray( CHUNK_SIZE ) );
            values = new Object[CHUNK_SIZE];
        }

//        public void forEachValue( Procedure<V> p )
//        {
//            for ( Object value : values )
//            {
//                V currentValue = (V) value;
//                if ( currentValue != null )
//                {
//                    p.accept( currentValue );
//                }
//            }
//        }

        public void foreach( long trueIndexStart, int indexStart, BiConsumer<Long,V> f )
        {
            for ( int i = indexStart; i < values.length; i++ )
            {
                V currentValue = (V) values[i];
                if ( currentValue != null )
                {
                    f.accept( trueIndexStart + i, currentValue );
                }
            }
        }

        boolean add( int key, V value )
        {
            if ( key < CHUNK_SIZE )
            {
                values[key] = value;
                return true;
            }
            return false;
        }

        Object remove( int key )
        {
            Object v = values[key];
            values[key] = null;

            return v;
        }

        void close( MemoryTracker memoryTracker )
        {
            memoryTracker.releaseHeap( SHALLOW_SIZE + shallowSizeOfObjectArray( CHUNK_SIZE ) );
        }
    }
}
