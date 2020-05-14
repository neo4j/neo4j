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
import org.eclipse.collections.impl.tuple.AbstractImmutableEntry;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import org.neo4j.collection.trackable.HeapTrackingUnifiedMap;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.memory.ScopedMemoryTracker;
import org.neo4j.util.CalledFromGeneratedCode;

import static org.neo4j.collection.trackable.HeapTrackingCollections.newMap;
import static org.neo4j.kernel.impl.util.collection.LongProbeTable.SCOPED_MEMORY_TRACKER_SHALLOW_SIZE;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

/**
 * A heap tracking, ordered, append-only, map. It only tracks the internal structure, not the elements within.
 *
 * Elements are also inserted in a single-linked list to allow traversal from first to last in the order of insertion.
 * No replacement of existing elements is possible.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class HeapTrackingOrderedAppendMap<K, V> implements AutoCloseable
{
    private static final long SHALLOW_SIZE = shallowSizeOfInstance( HeapTrackingOrderedAppendMap.class );

    private final ScopedMemoryTracker scopedMemoryTracker;
    private HeapTrackingUnifiedMap<K,Entry<K,V>> map;
    private Entry<K,V> first;
    private Entry<K,V> last;

    public static <K, V> HeapTrackingOrderedAppendMap<K,V> createOrderedMap( MemoryTracker memoryTracker )
    {
        ScopedMemoryTracker scopedMemoryTracker = new ScopedMemoryTracker( memoryTracker );
        scopedMemoryTracker.allocateHeap( SHALLOW_SIZE + SCOPED_MEMORY_TRACKER_SHALLOW_SIZE );
        return new HeapTrackingOrderedAppendMap<>( scopedMemoryTracker );
    }

    private HeapTrackingOrderedAppendMap( ScopedMemoryTracker scopedMemoryTracker )
    {
        this.scopedMemoryTracker = scopedMemoryTracker;
        this.map = newMap( scopedMemoryTracker );
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
            V result = p.valueOf( memoryTracker );
            memoryTracker.allocateHeap( Entry.SHALLOW_SIZE );
            Entry<K,V> entry = new Entry<>( key, result );
            append( entry );
            return entry;
        }, function ).getValue();
    }

    @CalledFromGeneratedCode
    public V get( K key )
    {
        Entry<K,V> entry = map.get( key );
        if ( entry != null )
        {
            return entry.getValue();
        }
        return null;
    }

    /**
     * WARNING: Use only from generated code where we always first call get( key ) to check that the key does not already exist.
     *          Will throw if you accidentally replace a value!
     *          (This is to avoid having to unnecessarily implement a slow linear scan through the singly-linked list to find and replace the entry)
     */
    @CalledFromGeneratedCode
    public void put( K key, V value )
    {
        scopedMemoryTracker.allocateHeap( Entry.SHALLOW_SIZE );
        Entry<K,V> entry = new Entry( key, value );
        append( entry );
        if ( map.put( key, entry ) != null )
        {
            throw new UnsupportedOperationException( "Replacing an existing value is not supported." );
        }
    }

    /**
     * After calling this you can only consume the existing entries through the returned iterator.
     * The map will be closed and no further entries can be added.
     *
     * When the iterator is exhausted it will call close() automatically.
     * (Everything allocated by the function given to getIfAbsentPutWithMemoryTracker() will then
     *  also be released when the scoped memory tracker is closed.)
     */
    public Iterator<Map.Entry<K, V>> autoClosingEntryIterator()
    {
        // At this point we are not expecting updates so we do not need the map anymore
        map.close();
        map = null;

        return new Iterator<>()
        {
            private Entry<K,V> current;

            {
                current = first;
                first = null;
                last = null;
            }

            @Override
            public boolean hasNext()
            {
                if ( current == null )
                {
                    close();
                    return false;
                }
                return true;
            }

            @Override
            public Entry<K,V> next()
            {
                var entry = current;
                current = current.next;
                return entry;
            }
        };
    }

    public ScopedMemoryTracker scopedMemoryTracker()
    {
        return scopedMemoryTracker;
    }

    @Override
    public void close()
    {
        first = null;
        last = null;
        map = null;
        scopedMemoryTracker.close();
    }

    private void append( Entry<K, V> entry )
    {
        if ( first == null )
        {
            first = last = entry;
        }
        else
        {
            last.next = entry;
            last = entry;
        }
    }

    private static class Entry<K, V> extends AbstractImmutableEntry<K, V>
    {
        private static final long SHALLOW_SIZE = shallowSizeOfInstance( Entry.class );

        Entry<K, V> next;

        Entry( K key, V value )
        {
            super( key, value );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( o instanceof Map.Entry )
            {
                Map.Entry<?,?> that = (Map.Entry<?,?>) o;
                return Objects.equals( this.key, that.getKey() ) && Objects.equals( this.value, that.getValue() );
            }
            return false;
        }

        @Override
        public int hashCode()
        {
            K key = this.key;
            V value = this.value;
            return (key == null ? 0 : key.hashCode()) ^ (value == null ? 0 : value.hashCode());
        }
    }
}
