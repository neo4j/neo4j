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
package org.neo4j.collection.trackable;

import java.util.Iterator;

import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.memory.MemoryTracker;

import static org.neo4j.internal.helpers.ArrayUtil.MAX_ARRAY_SIZE;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfObjectArray;

/**
 * A heap tracking append list. It only tracks the internal structure, not the elements within.
 *
 * @param <T> element type
 */
@SuppressWarnings( "unchecked" )
public class HeapTrackingAppendList<T> implements Iterable<T>, AutoCloseable
{
    private static final long SHALLOW_SIZE = shallowSizeOfInstance( HeapTrackingAppendList.class );
    private static final long ARRAY_INITIAL_SIZE = shallowSizeOfObjectArray( 1 );

    private final MemoryTracker memoryTracker;

    private long trackedSize;
    private int size;
    private T[] items = (T[]) new Object[1];

    /**
     * @return a new heap tracking append list with initial size 1
     */
    public static <T> HeapTrackingAppendList<T> newAppendList( MemoryTracker memoryTracker )
    {
        memoryTracker.allocateHeap( SHALLOW_SIZE + ARRAY_INITIAL_SIZE );
        return new HeapTrackingAppendList<>( memoryTracker, ARRAY_INITIAL_SIZE );
    }

    private HeapTrackingAppendList( MemoryTracker memoryTracker, long trackedSize )
    {
        this.memoryTracker = memoryTracker;
        this.trackedSize = trackedSize;
    }

    public void add( T item )
    {
        if ( items.length == size )
        {
            grow( size + 1 );
        }
        items[size++] = item;
    }

    @Override
    public Iterator<T> iterator()
    {
        return Iterators.iterator( size, items );
    }

    @Override
    public void close()
    {
        if ( items != null )
        {
            memoryTracker.releaseHeap( trackedSize + SHALLOW_SIZE );
            items = null;
        }
    }

    /**
     * Grow and report size change to tracker
     */
    private void grow( int minimumCapacity )
    {
        int newCapacity = size + (size >> 1) + 1; // Grow by 50%
        if ( newCapacity > MAX_ARRAY_SIZE || newCapacity < 0 ) // Check for overflow
        {
            if ( minimumCapacity > MAX_ARRAY_SIZE )
            {
                // Nothing left to do here. We have failed to prevent an overflow.
                throw new OutOfMemoryError();
            }
            newCapacity = MAX_ARRAY_SIZE;
        }

        long oldHeapUsage = trackedSize;
        trackedSize = shallowSizeOfObjectArray( newCapacity );
        memoryTracker.allocateHeap( trackedSize );
        T[] newItems = (T[]) new Object[newCapacity];
        System.arraycopy( items, 0, newItems, 0, Math.min( size, newCapacity ) );
        items = newItems;
        memoryTracker.releaseHeap( oldHeapUsage );
    }
}
