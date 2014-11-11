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
    private final int defaultValue;
    private long highestSetIndex = -1;
    private long size;

    public IntArray( LongArrayFactory factory, long chunkSize, int defaultValue )
    {
        long defaultLongValue = 0;
        defaultLongValue = LONG_BITS.set( defaultLongValue, 0, defaultValue );
        defaultLongValue = LONG_BITS.set( defaultLongValue, 1, defaultValue );

        this.longs = factory.newDynamicLongArray( chunkSize, defaultLongValue );
        this.defaultValue = defaultValue;
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
        if ( LONG_BITS.get( longValue, slot ) == defaultValue )
        {
            size++;
        }
        longValue = LONG_BITS.set( longValue, slot, value );
        longs.set( longIndex, longValue );
        if ( index > highestSetIndex )
        {
            highestSetIndex = index;
        }
    }

    @Override
    public void clear()
    {
        longs.clear();
        highestSetIndex = -1;
        size = 0;
    }

    @Override
    public void swap( long fromIndex, long toIndex, int numberOfEntries )
    {
        throw new UnsupportedOperationException( "Please implement" );
    }

    @Override
    public long highestSetIndex()
    {
        return highestSetIndex;
    }

    @Override
    public long size()
    {
        return size;
    }

    @Override
    public void visitMemoryStats( MemoryStatsVisitor visitor )
    {
        longs.visitMemoryStats( visitor );
    }
}
