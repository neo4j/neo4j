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
package org.neo4j.cypher.internal.collection;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.memory.MemoryTracker;

import static java.util.Objects.requireNonNull;
import static org.neo4j.internal.helpers.ArrayUtil.MAX_ARRAY_SIZE;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfObjectArray;
import static org.neo4j.util.Preconditions.checkArgument;

/**
 * A partial implementation (only supports what is necessary for our use cases)
 * of a memory tracking heap data structure (https://en.wikipedia.org/wiki/Heap_(data_structure))
 *
 * By default this is a max-heap. Use a reverse comparator to get a min-heap.
 */
abstract class MemoryTrackingHeap<T> implements AutoCloseable
{
    protected final Comparator<? super T> comparator;
    protected final MemoryTracker memoryTracker;

    private long trackedSize;
    protected int size;
    protected T[] heap;

    @SuppressWarnings( "unchecked" )
    protected MemoryTrackingHeap( Comparator<? super T> comparator, int initialSize, MemoryTracker memoryTracker )
    {
        this.comparator = requireNonNull( comparator );
        this.memoryTracker = memoryTracker;
        checkArgument( initialSize > 0, "Table size must be greater than 0" );

        trackedSize = shallowSizeOfObjectArray( initialSize );
        memoryTracker.allocateHeap( instanceSize() + trackedSize );
        heap = (T[]) new Object[initialSize];
    }

    protected abstract long instanceSize();

    public boolean insert( T e )
    {
        int n = size;
        if ( n >= heap.length )
        {
            grow( n + 1 );
        }
        siftUp( n, e );
        size = n + 1;
        return true;
    }

    public boolean replace( T e )
    {
        T head = heap[0];
        if ( comparator.compare( head, e ) > 0 )
        {
            heap[0] = e;
            siftDown( 0, e, size );
            return true;
        }
        return false;
    }

    public void sort()
    {
        // Heap sort the array
        int n = size - 1;
        while ( n > 0 )
        {
            T tmp = heap[n];
            heap[n] = heap[0];
            heap[0] = tmp;
            siftDown( 0, tmp, n );
            n--;
        }
    }

    protected void clear()
    {
        Arrays.fill( heap, 0, size, null );
        size = 0;
    }

    @Override
    public void close()
    {
        if ( heap != null )
        {
            memoryTracker.releaseHeap( instanceSize() + trackedSize  );
            heap = null;
        }
    }

    Iterator<T> getIterator()
    {
        return Iterators.iterator( size, heap );
    }

    /**
     * Make a heap out of the backing array. O(n)
     */
    protected void heapify()
    {
        for ( int i = (size >>> 1) - 1; i >= 0; i-- )
        {
            siftDown( i, heap[i], size );
        }
    }

    /**
     * Sift down an element in the heap. O(log(n))
     *
     * @param k index to sift down from
     * @param x element to sift down
     * @param n the size of the heap
     */
    protected void siftDown( int k, T x, int n )
    {
        int half = n >>> 1;
        while ( k < half )
        {
            int child = (k << 1) + 1;
            T c = heap[child];
            int right = child + 1;
            if ( right < n && comparator.compare( c, heap[right] ) < 0 )
            {
                child = right;
                c = heap[child];
            }
            if ( comparator.compare( x, c ) >= 0 )
            {
                break;
            }
            heap[k] = c;
            k = child;
        }
        heap[k] = x;
    }

    private void siftUp( int k, T x )
    {
        while ( k > 0 )
        {
            int parent = (k - 1) >>> 1;
            T e = heap[parent];
            if ( comparator.compare( x, e ) <= 0 )
            {
                break;
            }
            heap[k] = e;
            k = parent;
        }
        heap[k] = x;
    }

    protected abstract void overflow( long maxSize );

    /**
     * Grow  heap.
     */
    protected void grow( long minimumCapacity )
    {
        int oldCapacity = heap.length;
        int newCapacity = oldCapacity + (oldCapacity >> 1) + 1; // Grow by 50%
        if ( newCapacity > MAX_ARRAY_SIZE || newCapacity < 0 ) // Check for overflow
        {
            if ( minimumCapacity > MAX_ARRAY_SIZE )
            {
                // Nothing left to do here. We have failed to prevent an overflow.
                overflow( MAX_ARRAY_SIZE );
            }
            newCapacity = MAX_ARRAY_SIZE;
        }

        long oldHeapUsage = trackedSize;
        trackedSize = shallowSizeOfObjectArray( newCapacity );
        memoryTracker.allocateHeap( trackedSize );
        T[] newHeap = (T[]) new Object[newCapacity];
        System.arraycopy( heap, 0, newHeap, 0, Math.min( size, newCapacity ) );
        heap = newHeap;
        memoryTracker.releaseHeap( oldHeapUsage );
    }
}
