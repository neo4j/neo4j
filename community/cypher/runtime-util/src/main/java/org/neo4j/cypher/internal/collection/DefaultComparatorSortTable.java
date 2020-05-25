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

import org.neo4j.exceptions.CypherExecutionException;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.VisibleForTesting;

import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

/**
 * Utility priority queue collection used to implement heap sort
 */
public class DefaultComparatorSortTable<T> extends MemoryTrackingHeap<T>
{
    private static final long SHALLOW_INSTANCE_SIZE = shallowSizeOfInstance( DefaultComparatorSortTable.class );

    public DefaultComparatorSortTable( Comparator<? super T> comparator, int initialSize )
    {
        this( comparator, initialSize, EmptyMemoryTracker.INSTANCE );
    }

    public DefaultComparatorSortTable( Comparator<? super T> comparator, int initialSize, MemoryTracker memoryTracker )
    {
        super( comparator.reversed(), initialSize, memoryTracker );
    }

    @Override
    protected long shallowInstanceSize()
    {
        return SHALLOW_INSTANCE_SIZE;
    }

    public int getSize()
    {
        return size;
    }

    public void reset()
    {
        clear();
    }

    /**
     * Returns all the elements in no particular order
     */
    @VisibleForTesting
    Iterator<T> unorderedIterator()
    {
        return getIterator();
    }

    public boolean add( T e )
    {
        return super.insert( e );
    }

    public T peek()
    {
        return heap[0];
    }

    public T poll()
    {
        var result = heap[0];
        if ( result != null )
        {
            int n = --size;
            // Take out the bottom/last element and sift down from the top/first
            var x = heap[n];
            heap[n] = null;
            if ( n > 0 )
            {
                siftDown( 0, x, n );
            }
        }
        return result;
    }

    public boolean isEmpty()
    {
        return size == 0;
    }

    /**
     * Sift down an element in the heap from the top. O(log(n))
     *
     * NOTE: Use only with an element that you know is already at the top,
     *       i.e. as was returned by peek()
     *
     * @param x element to sift down
     */
    public void siftDown( T x )
    {
        super.siftDown( 0, x, size );
    }

    @Override
    protected void overflow( long maxSize )
    {
        throw new CypherExecutionException( "Sort table cannot hold more than " + maxSize + " elements." );
    }
}
