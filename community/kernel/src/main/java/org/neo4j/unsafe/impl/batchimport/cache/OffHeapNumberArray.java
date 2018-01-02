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
 * Base class for common functionality for any {@link NumberArray} where the data lives off-heap.
 */
abstract class OffHeapNumberArray implements NumberArray
{
    protected final long address;
    protected final long length;
    protected final int shift;
    protected final int stride;
    private boolean closed;

    protected OffHeapNumberArray( long length, int shift )
    {
        UnsafeUtil.assertHasUnsafe();
        this.length = length;
        this.shift = shift;
        this.stride = 1 << shift;
        this.address = UnsafeUtil.allocateMemory( length << shift );
    }

    @Override
    public long length()
    {
        return length;
    }

    protected long addressOf( long index )
    {
        if ( index < 0 || index >= length )
        {
            throw new ArrayIndexOutOfBoundsException( "Requested index " + index + ", but length is " + length );
        }
        return address + (index << shift);
    }

    protected boolean isByteUniform( long value )
    {
        byte any = 0; // assignment not really needed
        for ( int i = 0; i < stride; i++ )
        {
            byte test = (byte)(value >>> 8*i);
            if ( i == 0 )
            {
                any = test;
            }
            else if ( test != any )
            {
                return false;
            }
        }
        return true;
    }

    @Override
    public void acceptMemoryStatsVisitor( MemoryStatsVisitor visitor )
    {
        visitor.offHeapUsage( length * stride );
    }

    @Override
    public void close()
    {
        if ( !closed )
        {
            UnsafeUtil.free( address );
            closed = true;
        }
    }
}
