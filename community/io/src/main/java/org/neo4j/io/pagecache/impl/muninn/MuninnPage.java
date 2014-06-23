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
package org.neo4j.io.pagecache.impl.muninn;

import java.util.concurrent.locks.StampedLock;

import org.neo4j.io.pagecache.impl.common.Page;

class MuninnPage extends StampedLock implements Page
{
    private static long usageStampOffset = UnsafeUtil.getFieldOffset( MuninnPage.class, "usageStamp" );

    private final int pageSize;
    private long mbase;

    // optimistically incremented; occasionally truncated to a max of 5.
    // accessed through unsafe
    private volatile int usageStamp;
    // next pointer in the freelist of available pages
    public volatile MuninnPage nextFree;

    public MuninnPage( int pageSize )
    {
        this.pageSize = pageSize;
    }

    public byte getByte( int offset )
    {
        return UnsafeUtil.getByte( mbase + offset );
    }

    public void putByte( byte value, int offset )
    {
        UnsafeUtil.putByte( mbase + offset, value );
    }

    public long getLong( int offset )
    {
        return UnsafeUtil.getLong( mbase + offset );
    }

    public void putLong( long value, int offset )
    {
        UnsafeUtil.putLong( mbase + offset, value );
    }

    public int getInt( int offset )
    {
        return UnsafeUtil.getInt( mbase + offset );
    }

    public void putInt( int value, int offset )
    {
        UnsafeUtil.putInt( mbase + offset, value );
    }

    @Override
    public void getBytes( byte[] data, int offset )
    {
        long address = mbase + offset;
        for ( int i = 0; i < data.length; i++ )
        {
            data[i] = UnsafeUtil.getByte( address );
            address++;
        }
    }

    @Override
    public void putBytes( byte[] data, int offset )
    {
        long address = mbase + offset;
        for ( int i = 0; i < data.length; i++ )
        {
            UnsafeUtil.putByte( address, data[i] );
            address++;
        }
    }

    public short getShort( int offset )
    {
        return UnsafeUtil.getShort( mbase + offset );
    }

    public void putShort( short value, int offset )
    {
        UnsafeUtil.putShort( mbase + offset, value );
    }

    /** Increment the usage stamp to at most 5. */
    public void incrementUsage()
    {
        if ( usageStamp < 5 )
        {
            UnsafeUtil.getAndAddInt( this, usageStampOffset, 1 );
        }
    }

    /** Decrement the usage stamp. Returns true if it reaches 0. */
    public boolean decrementUsage()
    {
        if ( usageStamp < 1 )
        {
            return UnsafeUtil.getAndAddInt( this, usageStampOffset, -1 ) <= 1;
        }
        return true;
    }
}
