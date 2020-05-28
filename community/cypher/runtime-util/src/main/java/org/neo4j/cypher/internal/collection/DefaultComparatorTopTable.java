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

import java.util.Comparator;
import java.util.Iterator;

import javax.annotation.Nonnull;

import org.neo4j.exceptions.CypherExecutionException;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;

import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;
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
public class DefaultComparatorTopTable<T> extends MemoryTrackingHeap<T> implements Iterable<T> // implements SortTable<T>
{
    private static final long SHALLOW_INSTANCE_SIZE = shallowSizeOfInstance( DefaultComparatorTopTable.class );

    private long totalCount;
    private boolean heapified;
    private boolean isSorted;

    public DefaultComparatorTopTable( Comparator<? super T> comparator, long totalCount )
    {
        this( comparator, totalCount, EmptyMemoryTracker.INSTANCE );
    }

    public DefaultComparatorTopTable( Comparator<? super T> comparator, long totalCount, MemoryTracker memoryTracker )
    {
        super( comparator, (int) Math.min( totalCount, 1024 ), memoryTracker );
        this.totalCount = totalCount;
    }

    @Override
    protected long shallowInstanceSize()
    {
        return SHALLOW_INSTANCE_SIZE;
    }

    public boolean add( T e )
    {
        T evicted = addAndGetEvicted( e );
        return e != evicted;
    }

    public T addAndGetEvicted( T e )
    {
        if ( size < totalCount )
        {
            if ( size >= heap.length )
            {
                grow( size + 1 );
            }
            heap[size++] = e;
            return null;
        }
        else
        {
            if ( !heapified )
            {
                heapify();
                heapified = true;
            }
            return super.replace( e );
        }
    }

    public int getSize()
    {
        return size;
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
    @Override
    public void sort()
    {
        if ( isSorted )
        {
            return;
        }

        if ( !heapified )
        {
            java.util.Arrays.sort( heap, 0, size, comparator );
        }
        else
        {
            // Heap sort the array
            super.sort();
        }
        isSorted = true;
    }

    public void reset( long newTotalCount )
    {
        checkArgument( newTotalCount > 0, "Top table size must be greater than 0" );
        totalCount = newTotalCount;

        clear();

        heapified = false;
        isSorted = false;
    }

    /**
     * Must call after calling <code>sort()</code>.
     */
    @Override
    @Nonnull
    public Iterator<T> iterator()
    {
        if ( !isSorted )
        {
            // This should never happen in generated code but is here to simplify debugging if used incorrectly
            throw new IllegalStateException( "sort() needs to be called before requesting an iterator" );
        }
        return getIterator();
    }

    /**
     * Create an iterator that will automatically call close() when it is exhausted.
     * The caller can also provide an optional closeable of its own that will also be closed.
     * Must call after calling <code>sort()</code>.
     */
    @Nonnull
    public Iterator<T> autoClosingIterator( AutoCloseable closeable )
    {
        if ( !isSorted )
        {
            // This should never happen in generated code but is here to simplify debugging if used incorrectly
            throw new IllegalStateException( "sort() needs to be called before requesting an iterator" );
        }
        return getAutoClosingIterator( closeable );
    }

    @Override
    protected void overflow( long maxSize )
    {
        throw new CypherExecutionException( "Top table cannot hold more than " + maxSize + " elements." );
    }
}
