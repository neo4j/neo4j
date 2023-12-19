/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.consensus.log.cache;

import static java.lang.Math.floorMod;
import static java.util.Arrays.fill;

/**
 * <pre>
 * Design
 *
 * S: start index
 * E: end index
 *
 * When S == E the buffer is empty.
 *
 * Examples:
 *
 *              S
 *              v
 * Empty   [ | | | | | | ]
 *              ^
 *              E
 *
 *
 *                S
 *                v
 * Size 2  [ | | | | | | ]
 *                    ^
 *                    E
 *
 *
 *                 S
 *                 v
 * Full   [ | | | | | | ]
 *               ^
 *               E
 *
 * New items are put at the current E, and then E is moved one step forward (circularly).
 * The item at E is never a valid item.
 *
 * If moving E one step forward moves it onto S
 * - then it knocks an element out
 * - and S is also moved one step forward
 *
 * The S element has index 0.
 * Removing an element moves S forward (circularly).
 *
 * @param <V> type of elements.
 */
public class CircularBuffer<V>
{
    private final int arraySize; // externally visible capacity is arraySize - 1
    private Object[] elementArr;

    private int S;
    private int E;

    CircularBuffer( int capacity )
    {
        if ( capacity <= 0 )
        {
            throw new IllegalArgumentException( "Capacity must be > 0." );
        }

        this.arraySize = capacity + 1; // 1 item as sentinel (can't hold entries)
        this.elementArr = new Object[arraySize];
    }

    /**
     * Clears the underlying buffer and fills the provided eviction array with all evicted elements.
     * The provided array must have the same capacity as the circular buffer.
     *
     * @param evictions Caller-provided array for evictions.
     */
    public void clear( V[] evictions )
    {
        if ( evictions.length != arraySize - 1 )
        {
            throw new IllegalArgumentException( "The eviction array must be of the same size as the capacity of the circular buffer." );
        }

        int i = 0;
        while ( S != E )
        {
            //noinspection unchecked
            evictions[i++] = (V) elementArr[S];
            S = pos( S, 1 );
        }

        S = 0;
        E = 0;

        fill( elementArr, null );
    }

    private int pos( int base, int delta )
    {
        return floorMod( base + delta, arraySize );
    }

    /**
     * Append to the end of the buffer, possibly overwriting the
     * oldest entry.
     *
     * @return any knocked out item, or null if nothing was knocked out.
     */
    public V append( V e )
    {
        elementArr[E] = e;
        E = pos( E, 1 );
        if ( E == S )
        {
            //noinspection unchecked
            V old = (V) elementArr[E];
            elementArr[E] = null;
            S = pos( S, 1 );
            return old;
        }
        else
        {
            return null;
        }
    }

    public V read( int idx )
    {
        //noinspection unchecked
        return (V) elementArr[pos( S, idx )];
    }

    public V remove()
    {
        if ( S == E )
        {
            return null;
        }
        //noinspection unchecked
        V e = (V) elementArr[S];
        elementArr[S] = null;
        S = pos( S, 1 );
        return e;
    }

    public V removeHead()
    {
        if ( S == E )
        {
            return null;
        }

        E = pos( E, -1 );
        //noinspection unchecked
        V e = (V) elementArr[E];
        elementArr[E] = null;
        return e;
    }

    public int size()
    {
        return floorMod( E - S, arraySize );
    }
}
