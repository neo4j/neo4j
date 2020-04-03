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
package org.neo4j.cypher.internal;

import java.util.Comparator;
import java.util.Iterator;

import org.neo4j.internal.helpers.collection.Iterators;

import static java.util.Objects.requireNonNull;
import static org.neo4j.util.Preconditions.checkArgument;

/**
 * The default implementation of a Top N table used by all runtimes
 *
 * It accepts tuples as boxed objects that implements Comparable
 *
 * Implements the following interface:
 * (since the code is generated it does not actually need to declare it with implements)
 *
 * public interface SortTable<T>
 * {
 *     boolean add( T e );
 *
 *     void sort();
 *
 *     Iterator<T> iterator();
 * }
 *
 * Uses a max heap to collect a maximum of {@code totalCount} tuples in reverse order.
 * When sort() is called, it does a heap sort in-place which will reverse the order
 * again, resulting in a fully sorted array which the iterator will go through.
 */
public class DefaultComparatorTopTable<T> implements Iterable<T> // implements SortTable<T>
{
    private final Comparator<T> comparator;
    private final int totalCount;
    private boolean heapified;
    private boolean isSorted;
    private int size;
    private T[] heap;

    @SuppressWarnings( "unchecked" )
    public DefaultComparatorTopTable( Comparator<T> comparator, int totalCount )
    {
        checkArgument( totalCount > 0, "Top table size must be greater than 0" );
        this.comparator = requireNonNull( comparator );
        this.totalCount = totalCount;

        heap = (T[]) new Object[totalCount];
    }

    public boolean add( T e )
    {
        if ( size < totalCount )
        {
            heap[size++] = e;
            return true;
        }
        else
        {
            if ( !heapified )
            {
                heapify();
            }
            T head = heap[0];
            if ( comparator.compare( head, e ) > 0 )
            {
                heap[0] = e;
                siftDown( 0, e, size );
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the top <code>totalCount</code> elements, but in no particular order
     */
    public Iterator<T> unorderedIterator()
    {
        return getIterator();
    }

    /**
     * Must call before calling <code>iterator()</code>.
     */
    public void sort()
    {
        if ( !heapified )
        {
            heapify();
        }

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
        isSorted = true;
    }

    /**
     * Must call after calling <code>sort()</code>.
     */
    @Override
    public Iterator<T> iterator()
    {
        if ( !isSorted )
        {
            // This should never happen in generated code but is here to simplify debugging if used incorrectly
            throw new IllegalStateException( "sort() needs to be called before requesting an iterator" );
        }
        return getIterator();
    }

    private Iterator<T> getIterator()
    {
        return Iterators.iterator( size, heap );
    }

    /**
     * Make a heap out of the backing array. O(n)
     */
    private void heapify()
    {
        for ( int i = (size >>> 1) - 1; i >= 0; i-- )
        {
            siftDown( i, heap[i], size );
        }
        heapified = true;
    }

    /**
     * Sift down an element in the heap. O(log(n))
     *
     * @param k index to sift down from
     * @param x element to sift down
     * @param n the size of the heap
     */
    private void siftDown( int k, T x, int n )
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
}
