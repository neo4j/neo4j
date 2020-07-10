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
import java.util.NoSuchElementException;

import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.AutoCloseablePlus;
import org.neo4j.internal.kernel.api.DefaultCloseListenable;
import org.neo4j.io.IOUtils;
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
abstract class MemoryTrackingHeap<T> extends DefaultCloseListenable implements AutoCloseablePlus
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
        memoryTracker.allocateHeap( shallowInstanceSize() + trackedSize );
        heap = (T[]) new Object[initialSize];
    }

    /**
     * The shallow size of an instance of the implementing class
     */
    protected abstract long shallowInstanceSize();

    /**
     * Insert a new element in the heap
     */
    protected boolean insert( T e )
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

    /**
     * Replace the top element of the heap
     */
    protected T replace( T e )
    {
        T head = heap[0];
        if ( comparator.compare( head, e ) > 0 )
        {
            heap[0] = e;
            siftDown( 0, e, size );
            return head;
        }
        return e;
    }

    /**
     * Sort the heapified backing array in-place
     */
    protected void sort()
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
    public void closeInternal()
    {
        if ( heap != null )
        {
            memoryTracker.releaseHeap( shallowInstanceSize() + trackedSize  );
            heap = null;
        }
    }

    @Override
    public boolean isClosed()
    {
        return false;
    }

    /**
     * Create a normal iterator.
     * To be used by sub-classes implementing iterator().
     */
    protected Iterator<T> getIterator()
    {
        return Iterators.iterator( size, heap );
    }

    /**
     * Create an iterator that will automatically call close() when it is exhausted.
     * To be used by sub-classes implementing autoClosingIterator().
     * The caller can also provide an optional closeable of its own that will also be closed.
     */
    protected Iterator<T> getAutoClosingIterator( AutoCloseable closeable )
    {
        return new Iterator<>()
        {
            int index;

            @Override
            public boolean hasNext()
            {
                if ( index >= size )
                {
                    close();
                    if ( closeable != null )
                    {
                        IOUtils.closeAllUnchecked( closeable );
                    }
                    return false;
                }
                return true;
            }

            @Override
            public T next()
            {
                if ( !hasNext() )
                {
                    throw new NoSuchElementException();
                }
                return heap[index++];
            }
        };
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

    /**
     * The implementing class gets to decide what to do in case growing results in an overflow
     */
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
