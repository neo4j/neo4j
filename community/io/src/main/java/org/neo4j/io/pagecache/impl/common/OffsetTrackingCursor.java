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
package org.neo4j.io.pagecache.impl.common;

import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.io.pagecache.PageCursor;

public class OffsetTrackingCursor implements PageCursor
{
    private static final AtomicInteger THREAD_ID_COUNTER = new AtomicInteger( -1 );

    private Page page;
    private int currentOffset;


    // each thread has a cursor and this id is used to find the right queue to talk to
    // when a srsp queue is used for synchronization
    private final int threadId;

    public OffsetTrackingCursor()
    {
        threadId = THREAD_ID_COUNTER.incrementAndGet();
    }

    public int getThreadId()
    {
        return threadId;
    }

    public byte getByte()
    {
        byte value = page.getByte( currentOffset );
        currentOffset += 1;
        return value;
    }

    public void putByte( byte value )
    {
        page.putByte( value, currentOffset );
        currentOffset += 1;
    }

    public long getLong()
    {
        long value = page.getLong( currentOffset );
        currentOffset += 8;
        return value;
    }

    public void putLong( long l )
    {
        page.putLong( l, currentOffset );
        currentOffset += 8;
    }

    public int getInt()
    {
        int value = page.getInt( currentOffset );
        currentOffset += 4;
        return value;
    }

    public long getUnsignedInt()
    {
        return getInt()&0xFFFFFFFFL;
    }

    public void putInt( int i )
    {
        page.putInt( i, currentOffset );
        currentOffset += 4;
    }

    public void getBytes( byte[] data )
    {
        page.getBytes( data, currentOffset );
        currentOffset += data.length;
    }

    public void putBytes( byte[] data )
    {
        page.putBytes( data, currentOffset );
        currentOffset += data.length;
    }

    public void setOffset( int offset )
    {
        currentOffset = offset;
    }

    public int getOffset()
    {
        return currentOffset;
    }

    public OffsetTrackingCursor reset( Page page )
    {
        this.page = page;
        currentOffset = 0;
        return this;
    }

    public Page getPage()
    {
        return page;
    }
}
