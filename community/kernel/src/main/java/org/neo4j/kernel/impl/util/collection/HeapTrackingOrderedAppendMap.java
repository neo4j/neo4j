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

import org.eclipse.collections.api.block.function.Function;
import org.eclipse.collections.api.block.function.Function2;
import org.eclipse.collections.api.block.procedure.Procedure;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

import org.neo4j.collection.trackable.HeapTrackingUnifiedMap;
import org.neo4j.internal.kernel.api.DefaultCloseListenable;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.CalledFromGeneratedCode;
import org.neo4j.util.VisibleForTesting;

import static org.neo4j.collection.trackable.HeapTrackingCollections.newMap;
import static org.neo4j.memory.HeapEstimator.SCOPED_MEMORY_TRACKER_SHALLOW_SIZE;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfObjectArray;

/**
 * A heap tracking, ordered, append-only, map. It only tracks the internal structure, not the elements within.
 *
 * Elements are also inserted in a single-linked list to allow traversal from first to last in the order of insertion.
 * No replacement of existing elements is possible.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class HeapTrackingOrderedAppendMap<K, V> extends DefaultCloseListenable
{
    private static final long SHALLOW_SIZE = shallowSizeOfInstance( HeapTrackingOrderedAppendMap.class );
    private static final int INITIAL_CHUNK_SIZE = 32; // Must be even, preferably a power of 2 (32 matches the HeapTrackingUnifiedMap initial size)
    private static final int MAX_CHUNK_SIZE = 8192; // Must be even, preferably a power of 2

    private final MemoryTracker scopedMemoryTracker;
    private HeapTrackingUnifiedMap<K,V> map;

    // Linked chunk list used to store key-value pairs in order
    private Chunk first;
    private Chunk current;

    public static <K, V> HeapTrackingOrderedAppendMap<K,V> createOrderedMap( MemoryTracker memoryTracker )
    {
        MemoryTracker scopedMemoryTracker = memoryTracker.getScopedMemoryTracker();
        scopedMemoryTracker.allocateHeap( SHALLOW_SIZE + SCOPED_MEMORY_TRACKER_SHALLOW_SIZE );
        return new HeapTrackingOrderedAppendMap<>( scopedMemoryTracker );
    }

    private HeapTrackingOrderedAppendMap( MemoryTracker scopedMemoryTracker )
    {
        this.scopedMemoryTracker = scopedMemoryTracker;
        this.map = newMap( scopedMemoryTracker );
        first = new Chunk( INITIAL_CHUNK_SIZE, scopedMemoryTracker );
        current = first;
    }

    /**
     * Get and return the value in the Map at the specified key. Alternatively, if there is no value in the map for that key
     * return the result of evaluating the specified Function given the internal scoped memory tracker, and put that value in the
     * map at the specified key.
     *
     * @param key The key to look up or insert a new value for
     * @param function A function that takes a memory tracker and returns a value.
     * @return The value for the given key
     */
    public V getIfAbsentPutWithMemoryTracker( K key, Function<MemoryTracker,? extends V> function )
    {
        return map.getIfAbsentPutWith( key, p ->
        {
            MemoryTracker memoryTracker = scopedMemoryTracker;
            V value = p.valueOf( memoryTracker );
            addToBuffer( key, value );
            return value;
        }, function );
    }

    /**
     * Get and return the value in the Map at the specified key. Alternatively, if there is no value in the map for that key
     * return the result of evaluating the specified Function given the internal scoped memory tracker, and put that value in the
     * map at the specified key.
     *
     * @param key The key to look up or insert a new value for
     * @param function A function that takes the key and a memory tracker and returns a value.
     * @return The value for the given key
     */
    public V getIfAbsentPutWithMemoryTracker2( K key, Function2<K, MemoryTracker,? extends V> function )
    {
        // NOTE: Based on profiling it seems that because of the overhead of creating an linking a lambda in this method it is faster to do
        //       separate gets and puts, especially when we expect more gets to happen on existing values.
        V value = map.get( key );
        if ( value != null )
        {
            return value;
        }
        // Put a new value
        V newValue = function.value( key, scopedMemoryTracker );
        map.put( key, newValue );
        addToBuffer( key, newValue );
        return newValue;
    }

    @CalledFromGeneratedCode
    public V get( K key )
    {
        return map.get( key );
    }

    /**
     * WARNING: Use only from generated code where we always first call get( key ) to check that the key does not already exist.
     *          Will throw if you accidentally replace a value!
     *          (This is to avoid having to unnecessarily implement a slow linear scan through the singly-linked list to find and replace the entry)
     */
    @CalledFromGeneratedCode
    public void put( K key, V value )
    {
        addToBuffer( key, value );
        if ( map.put( key, value ) != null )
        {
            throw new UnsupportedOperationException( "Replacing an existing value is not supported." );
        }
    }

    /**
     * Apply the procedure for each value in the map.
     */
    @SuppressWarnings( "unchecked" )
    public void forEachValue( Procedure<? super V> p )
    {
        Chunk chunk = first;
        while ( chunk != null )
        {
            // Value is at odd indicies (1, 3, 5, ...)
            for ( int i = 1; i < chunk.cursor; i += 2 )
            {
                p.accept( (V) chunk.elements[i] );
            }
            chunk = chunk.next;
        }
    }

    /**
     * After calling this you can only consume the existing entries through the returned iterator.
     * The map will be closed and no further entries can be added.
     *
     * When the iterator is exhausted it will call close() automatically.
     * (Everything allocated by the function given to getIfAbsentPutWithMemoryTracker() will then
     *  also be released when the scoped memory tracker is closed.)
     *
     * WARNING: The entries returned by next() are transient and must be consumed before calling next() again!
     */
    public Iterator<Map.Entry<K, V>> autoClosingEntryIterator()
    {
        // At this point we are not expecting updates so we do not need the map anymore
        map.close();
        map = null;

        return new AutoClosingTransientEntryIterator();
    }

    public MemoryTracker scopedMemoryTracker()
    {
        return scopedMemoryTracker;
    }

    @Override
    public void closeInternal()
    {
        map = null;
        first = null;
        current = null;
        scopedMemoryTracker.close();
    }

    @Override
    public boolean isClosed()
    {
        return first == null;
    }

    public void addToBuffer( Object key, Object value )
    {
        if ( !current.add( key, value ) )
        {
            int newChunkSize = grow( current.elements.length );
            Chunk newChunk = new Chunk( newChunkSize, scopedMemoryTracker );
            current.next = newChunk;
            current = newChunk;
            current.add( key, value );
        }
    }

    private class AutoClosingTransientEntryIterator implements Iterator<Map.Entry<K,V>>, Map.Entry<K,V>
    {
        private Chunk chunk;
        private Chunk nextChunk;
        private int index;
        private int nextIndex;

        {
            chunk = nextChunk = first;
            first = null;
            current = null;
        }

        @Override
        public boolean hasNext()
        {
            if ( nextChunk == null || nextIndex >= nextChunk.cursor )
            {
                close();
                return false;
            }
            return true;
        }

        @Override
        public Map.Entry<K,V> next()
        {
            if ( nextChunk == null )
            {
                throw new NoSuchElementException();
            }

            // Set current entry
            index = nextIndex;
            chunk = nextChunk;

            // Advance next entry
            nextIndex += 2;
            if ( nextIndex >= nextChunk.cursor )
            {
                nextChunk = nextChunk.next;
                nextIndex = 0;
            }

            // This is now a view of the current entry
            return this;
        }

        @Override
        @SuppressWarnings( "unchecked" )
        public K getKey()
        {
            return (K) chunk.elements[index];
        }

        @Override
        @SuppressWarnings( "unchecked" )
        public V getValue()
        {
            return (V) chunk.elements[index + 1];
        }

        @Override
        public V setValue( V value )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals( Object o )
        {
            if ( o instanceof Map.Entry )
            {
                Map.Entry<?,?> that = (Map.Entry<?,?>) o;
                return Objects.equals( this.getKey(), that.getKey() ) && Objects.equals( this.getValue(), that.getValue() );
            }
            return false;
        }

        @Override
        public int hashCode()
        {
            K key = this.getKey();
            V value = this.getValue();
            return (key == null ? 0 : key.hashCode()) ^ (value == null ? 0 : value.hashCode());
        }
    }

    @VisibleForTesting
    int numberOfChunks()
    {
        int i = 0;
        var chunk = first;
        while ( chunk != null )
        {
            chunk = chunk.next;
            i++;
        }
        return i;
    }

    private int grow( int size )
    {
        if ( size == MAX_CHUNK_SIZE )
        {
            return size;
        }
        int newSize = size << 1;
        if ( newSize <= 0 || newSize > MAX_CHUNK_SIZE ) // Check overflow
        {
            return MAX_CHUNK_SIZE;
        }
        return newSize;
    }

    private static class Chunk
    {
        private static final long SHALLOW_SIZE = shallowSizeOfInstance( Chunk.class );

        private final Object[] elements;
        private Chunk next;
        private int cursor;

        Chunk( int size, MemoryTracker memoryTracker )
        {
            memoryTracker.allocateHeap( SHALLOW_SIZE + shallowSizeOfObjectArray( size ) );
            elements = new Object[size];
        }

        boolean add( Object key, Object value )
        {
            if ( cursor < elements.length )
            {
                elements[cursor] = key;
                elements[cursor + 1] = value;
                cursor += 2;
                return true;
            }
            return false;
        }
    }
}
