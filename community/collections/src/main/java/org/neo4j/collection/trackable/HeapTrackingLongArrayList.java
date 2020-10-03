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

import java.util.Arrays;
import java.util.EmptyStackException;
import java.util.Objects;

import org.neo4j.collection.PrimitiveLongResourceCollections;
import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.memory.MemoryTracker;

import static org.neo4j.collection.trackable.HeapTrackingArrayList.newCapacity;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
import static org.neo4j.memory.HeapEstimator.sizeOfLongArray;
import static org.neo4j.util.Preconditions.requireNonNegative;

public class HeapTrackingLongArrayList implements LongArrayList, HeapTrackingLongStack
{
    private static final long SHALLOW_SIZE = shallowSizeOfInstance( HeapTrackingLongArrayList.class );

    private final MemoryTracker memoryTracker;

    private long trackedSize;
    private int size;
    private long[] elementData;

    /**
     * @return a new heap tracking long array list with initial size 1
     */
    public static HeapTrackingLongArrayList newLongArrayList( MemoryTracker memoryTracker )
    {
        return newLongArrayList( 1, memoryTracker );
    }

    /**
     * @return a new heap tracking long array list with the specified initial size
     */
    public static HeapTrackingLongArrayList newLongArrayList( int initialSize, MemoryTracker memoryTracker )
    {
        requireNonNegative( initialSize );
        requireNonNegative( initialSize );
        long trackedSize = sizeOfLongArray( initialSize );
        memoryTracker.allocateHeap( SHALLOW_SIZE + trackedSize );
        return new HeapTrackingLongArrayList( initialSize, memoryTracker, trackedSize );
    }

    /**
     * @return a new heap tracking long stack with initial size 1
     */
    public static HeapTrackingLongStack newLongStack( MemoryTracker memoryTracker )
    {
        return newLongArrayList( 1, memoryTracker );
    }

    /**
     * @return a new heap tracking long stack with the specified initial size
     */
    public static HeapTrackingLongStack newLongStack( int initialSize, MemoryTracker memoryTracker )
    {
        return newLongArrayList( initialSize, memoryTracker );
    }

    private HeapTrackingLongArrayList( int initialSize, MemoryTracker memoryTracker, long trackedSize )
    {
        this.trackedSize = sizeOfLongArray( initialSize );
        this.elementData = new long[initialSize];
        this.memoryTracker = memoryTracker;
    }

    @Override
    public boolean add( long item )
    {
        add( item, elementData, size );
        return true;
    }

    @Override
    public void add( int index, long element )
    {
        rangeCheckForAdd( index );
        final int s;
        long[] elementData;
        if ( (s = size) == (elementData = this.elementData).length )
        {
            elementData = grow( size + 1 );
        }
        System.arraycopy( elementData, index, elementData, index + 1, s - index );
        elementData[index] = element;
        size = s + 1;
    }

    @Override
    public long get( int index )
    {
        Objects.checkIndex( index, size );
        return elementData[index];
    }

    @Override
    public long set( int index, long element )
    {
        Objects.checkIndex( index, size );
        long oldValue = elementData[index];
        elementData[index] = element;
        return oldValue;
    }

    @Override
    public void add( long e, long[] elementData, int s )
    {
        if ( s == elementData.length )
        {
            elementData = grow( size + 1 );
        }
        elementData[s] = e;
        size = s + 1;
    }

    @Override
    public int size()
    {
        return size;
    }

    @Override
    public void clear()
    {
        Arrays.fill( this.elementData, 0, size, 0L);
        this.size = 0;
    }

    @Override
    public void close()
    {
        if ( elementData != null )
        {
            memoryTracker.releaseHeap( trackedSize + SHALLOW_SIZE );
            elementData = null;
        }
    }

    public PrimitiveLongResourceIterator autoClosingIterator()
    {
        return new PrimitiveLongResourceCollections.AbstractPrimitiveLongBaseResourceIterator( this )
        {
            private int index = -1;

            @Override
            protected boolean fetchNext()
            {
                index++;
                return index < size && next( elementData[index] );
            }
        };
    }

    @Override
    public long peek()
    {
        if ( size == 0 )
        {
            throw new EmptyStackException();
        }
        return elementData[size - 1];
    }

    @Override
    public void push( long item )
    {
        add( item );
    }

    @Override
    public long pop()
    {
        long previous = elementData[size - 1];
        --this.size;
        this.elementData[this.size] = 0L;
        return previous;
    }

    @Override
    public boolean addAll( long... longs )
    {
        int numNew = longs.length;
        if ( numNew == 0 )
        {
            return false;
        }
        long[] elementData;
        final int s;
        if ( numNew > (elementData = this.elementData).length - (s = size) )
        {
            elementData = grow( s + numNew );
        }
        System.arraycopy( longs, 0, elementData, s, numNew );
        size = s + numNew;
        return true;
    }
    /**
     * Grow and report size change to tracker
     */
    private long[] grow( int minimumCapacity )
    {
        int newCapacity = newCapacity( minimumCapacity, elementData.length );
        long oldHeapUsage = trackedSize;
        trackedSize = sizeOfLongArray( newCapacity );
        memoryTracker.allocateHeap( trackedSize );
        long[] newItems = new long[newCapacity];
        System.arraycopy( elementData, 0, newItems, 0, Math.min( size, newCapacity ) );
        elementData = newItems;
        memoryTracker.releaseHeap( oldHeapUsage );
        return elementData;
    }

    private void rangeCheckForAdd( int index )
    {
        if ( index > size || index < 0 )
        {
            throw new IndexOutOfBoundsException( "Index: " + index + ", Size: " + size );
        }
    }
}
