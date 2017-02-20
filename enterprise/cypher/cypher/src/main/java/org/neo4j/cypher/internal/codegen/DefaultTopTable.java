/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.codegen;

import java.util.Collections;
import java.util.Iterator;
import java.util.PriorityQueue;

/**
 * The default implementation of a Top N table used by the generated code
 *
 * It accepts tuples as boxed objects that implements Comparable
 *
 * Implements the following interface:
 * (since the code is generated it does not actually need to declare it with implements)
 *
 * public interface SortTable<T extends Comparable<?>>
 * {
 *     boolean add( T e );
 *
 *     void sort();
 *
 *     Iterator<T> iterator();
 * }
 *
 * Uses a max heap (Java's standard PriorityQueue) to collect a maximum of totalCount tuples in reverse order.
 * When sort() is called it collects them in reverse sorted order into an array.
 * The iterator() then traverses this array backwards.
 */
public class DefaultTopTable<T extends Comparable<Object>> implements Iterable<T> // implements SortTable<T>
{
    private int totalCount;
    private int count = -1;
    private PriorityQueue<Object> heap;
    private Object[] array; // TODO: Use Guava's MinMaxPriorityQueue to avoid having this array

    public DefaultTopTable( int totalCount )
    {
        if ( totalCount <= 0 )
        {
            throw new IllegalArgumentException( "Top table size must be greater than 0" );
        }
        this.totalCount = totalCount;

        heap = new PriorityQueue<>( totalCount, Collections.reverseOrder() );
        array = new Object[ totalCount ];
    }

    @SuppressWarnings( "unchecked" )
    // Because of an issue with getting the same runtime-types with generics and byte code generation we use Object for now
    public boolean add( Object e )
    {
        if ( heap.size() < totalCount )
        {
            return heap.offer( e );
        }
        else
        {
            T head = (T) heap.peek();
            if ( head.compareTo( e ) > 0 )
            {
                heap.poll();
                return heap.offer( e );
            }
            else
            {
                return false;
            }
        }
    }

    public void sort()
    {
        count = heap.size();

        // We keep the values in reverse order so that we can write from start to end
        for ( int i = 0; i < count; i++ )
        {
            array[i] = heap.poll();
        }
    }

    @Override
    public Iterator<T> iterator()
    {
        if ( count == -1 )
        {
            // This should never happen in generated code but is here to simplify debugging if used incorrectly
            throw new IllegalStateException( "sort() needs to be called before requesting an iterator" );
        }
        return new Iterator<T>()
        {
            private int cursor = count;

            @Override
            public boolean hasNext()
            {
                return cursor > 0;
            }

            @Override
            @SuppressWarnings("unchecked")
            public T next()
            {
                return (T) array[--cursor];
            }
        };
    }
}
