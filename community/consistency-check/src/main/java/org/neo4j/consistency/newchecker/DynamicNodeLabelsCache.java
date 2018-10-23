/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.consistency.newchecker;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.internal.batchimport.cache.IntArray;
import org.neo4j.internal.batchimport.cache.NumberArrayFactory;

import static java.lang.Math.toIntExact;

/**
 * Basically a dynamically growing int[] to store label ids in. Should only be used to store the dynamic node labels, not all the inlined
 * label ids too since it prioritizes simplicity over space efficiency.
 */
class DynamicNodeLabelsCache implements AutoCloseable
{
    private final IntArray cache = NumberArrayFactory.OFF_HEAP.newDynamicIntArray( 100_000, 0 );
    private final AtomicLong nextIndex = new AtomicLong();

    long put( long[] labels )
    {
        final long index = nextIndex.getAndAdd( labels.length + 1 );
        cache.set( index, labels.length );
        for ( int i = 0; i < labels.length; i++ )
        {
            cache.set( index + 1 + i, toIntExact( labels[i] ) );
        }
        return index;
    }

    long[] get( long index, long[] into )
    {
        int count = cache.get( index );
        if ( count > into.length )
        {
            into = Arrays.copyOf( into, count );
        }
        for ( int i = 0; i < count; i++ )
        {
            into[i] = cache.get( index + 1 + i );
        }
        if ( count < into.length )
        {
            // -1 terminate it
            into[count] = -1;
        }
        return into;
    }

    void clear()
    {
        cache.clear();
        nextIndex.set( 0 );
    }

    @Override
    public void close()
    {
        cache.close();
    }
}
