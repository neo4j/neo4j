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
package org.neo4j.unsafe.impl.batchimport.cache;

import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

/**
 * Off-heap version of {@link LongArray} using {@code sun.misc.Unsafe}. Supports arrays with length beyond
 * Integer.MAX_VALUE.
 */
public class OffHeapLongArray extends OffHeapNumberArray implements LongArray
{
    private final long defaultValue;

    public OffHeapLongArray( long length, long defaultValue )
    {
        super( length, 3 );
        this.defaultValue = defaultValue;
        clear();
    }

    @Override
    public long get( long index )
    {
        return UnsafeUtil.getLong( addressOf( index ) );
    }

    @Override
    public void set( long index, long value )
    {
        UnsafeUtil.putLong( addressOf( index ), value );
    }

    @Override
    public void clear()
    {
        if ( isByteUniform( defaultValue ) )
        {
            UnsafeUtil.setMemory( address, length << shift, (byte)defaultValue );
        }
        else
        {
            for ( long i = 0, adr = address; i < length; i++, adr += stride )
            {
                UnsafeUtil.putLong( adr, defaultValue );
            }
        }
    }

    @Override
    public void swap( long fromIndex, long toIndex, int numberOfEntries )
    {
        long fromAddress = addressOf( fromIndex );
        long toAddress = addressOf( toIndex );

        for ( int i = 0; i < numberOfEntries; i++, fromAddress += stride, toAddress += stride )
        {
            long fromValue = UnsafeUtil.getLong( fromAddress );
            UnsafeUtil.putLong( fromAddress, UnsafeUtil.getLong( toAddress ) );
            UnsafeUtil.putLong( toAddress, fromValue );
        }
    }

    @Override
    public LongArray fixate()
    {
        return this;
    }
}
