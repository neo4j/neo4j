/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.collection.primitive.hopscotch;

import static java.lang.Integer.highestOneBit;
import static java.lang.Math.max;
import static java.lang.String.format;

/**
 * Contains the basic table capacity- and size calculations. Always keeps the capacity a power of 2
 * for an efficient table mask.
 */
public abstract class PowerOfTwoQuantizedTable<VALUE> implements Table<VALUE>
{
    protected final int h;
    protected final int capacity;
    protected int tableMask;
    protected int size;

    protected PowerOfTwoQuantizedTable( int capacity, int h )
    {
        if ( h < 4 || h > 32 )
        {
            throw new IllegalArgumentException( "h needs to be 4 <= h <= 32, was " + h );
        }

        this.h = h;
        this.capacity = quantize( max( capacity, 2 ) );
        this.tableMask = highestOneBit( this.capacity )-1;
    }

    public static int baseCapacity( int h )
    {
        return h << 1;
    }

    protected static int quantize( int capacity )
    {
        int candidate = Integer.highestOneBit( capacity );
        return candidate == capacity ? candidate : candidate << 1;
    }

    @Override
    public int h()
    {
        return h;
    }

    @Override
    public int mask()
    {
        return tableMask;
    }

    @Override
    public int capacity()
    {
        return capacity;
    }

    @Override
    public boolean isEmpty()
    {
        return size == 0;
    }

    @Override
    public int size()
    {
        return size;
    }

    @Override
    public void clear()
    {
        size = 0;
    }

    @Override
    public int version()
    {   // Versioning not supported by default
        return 0;
    }

    @Override
    public int version( int index )
    {   // Versioning not supported by default
        return 0;
    }

    @Override
    public long nullKey()
    {
        return -1L;
    }

    @Override
    public Table<VALUE> grow()
    {
        return newInstance( capacity << 1 );
    }

    protected abstract Table<VALUE> newInstance( int newCapacity );

    @Override
    public String toString()
    {
        return format( "hopscotch-table[%s|capacity:%d, size:%d, usage:%f]", getClass().getSimpleName(),
                capacity, size, size/((double)capacity) );
    }

    @Override
    public void close()
    {   // Nothing to do by default
    }
}
