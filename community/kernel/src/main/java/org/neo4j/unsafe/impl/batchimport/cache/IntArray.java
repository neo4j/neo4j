/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.cache;

/**
 * Similar to {@link LongArray}, but has int values instead of long values. The values can be still be
 * addressed with long indexes.
 *
 * Implemented as a class wrapping a {@link LongArray} since DRY ftw. It works like this:
 *
 * consider the following 3 longs: [    ,    ] [    ,    ] [    ,    ]
 * long[] indexes these as:             0           1           2
 * int[] indexes these as:           0     1     2     3     4     5
 */
public class IntArray implements NumberArray
{
    private static final LongBitsManipulator LONG_BITS = new LongBitsManipulator( Integer.SIZE, Integer.SIZE );
    private final LongArray longs;

    public IntArray( LongArray longs )
    {
        this.longs = longs;
    }

    @Override
    public long length()
    {
        return longs.length()*2;
    }

    public int get( long index )
    {
        long longIndex = index >> 1;
        long longValue = longs.get( longIndex );
        return (int) LONG_BITS.get( longValue, (int)(index%2) );
    }

    public void set( long index, int value )
    {
        long longIndex = index >> 1;
        long longValue = longs.get( longIndex );
        int slot = (int)(index%2);
        longValue = LONG_BITS.set( longValue, slot, value );
        longs.set( longIndex, longValue );
    }

    public void setAll( int value )
    {
        long longValue = 0;
        longValue = LONG_BITS.set( longValue, 0, value );
        longValue = LONG_BITS.set( longValue, 1, value );
        longs.setAll( longValue );
    }

    @Override
    public void swap( long fromIndex, long toIndex, int numberOfEntries )
    {
        throw new UnsupportedOperationException( "Please implement" );
    }

    @Override
    public long highestSetIndex()
    {
        return (longs.highestSetIndex()+1) << 1; // off by one from time to time, but that's OK for the use cases at hand
    }

    public static IntArray intArray( LongArray longs )
    {
        return new IntArray( longs );
    }

    @Override
    public void visitMemoryStats( MemoryStatsVisitor visitor )
    {
        longs.visitMemoryStats( visitor );
    }
}
