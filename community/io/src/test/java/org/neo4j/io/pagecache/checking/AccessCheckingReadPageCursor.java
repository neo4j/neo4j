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
package org.neo4j.io.pagecache.checking;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.impl.DelegatingPageCursor;

public class AccessCheckingReadPageCursor extends DelegatingPageCursor
{
    private boolean hasReadWithoutShouldRetry;

    public AccessCheckingReadPageCursor( PageCursor delegate )
    {
        super( delegate );
    }

    @Override
    public byte getByte()
    {
        markAsRead();
        return super.getByte();
    }

    @Override
    public void getBytes( byte[] data )
    {
        markAsRead();
        super.getBytes( data );
    }

    @Override
    public short getShort()
    {
        markAsRead();
        return super.getShort();
    }

    @Override
    public short getShort( int offset )
    {
        markAsRead();
        return super.getShort( offset );
    }

    @Override
    public long getLong()
    {
        markAsRead();
        return super.getLong();
    }

    @Override
    public long getLong( int offset )
    {
        markAsRead();
        return super.getLong( offset );
    }

    @Override
    public void getBytes( byte[] data, int arrayOffset, int length )
    {
        markAsRead();
        super.getBytes( data, arrayOffset, length );
    }

    @Override
    public int getInt( int offset )
    {
        markAsRead();
        return super.getInt( offset );
    }

    @Override
    public byte getByte( int offset )
    {
        markAsRead();
        return super.getByte( offset );
    }

    @Override
    public int getInt()
    {
        markAsRead();
        return super.getInt();
    }

    private void markAsRead()
    {
        hasReadWithoutShouldRetry = true;
    }

    @Override
    public boolean shouldRetry() throws IOException
    {
        hasReadWithoutShouldRetry = false;
        return super.shouldRetry();
    }

    @Override
    public boolean next() throws IOException
    {
        assertNoReadWithoutShouldRetry();
        return super.next();
    }

    @Override
    public void close()
    {
        assertNoReadWithoutShouldRetry();
        super.close();
    }

    @Override
    public boolean next( long pageId ) throws IOException
    {
        assertNoReadWithoutShouldRetry();
        return super.next( pageId );
    }

    private void assertNoReadWithoutShouldRetry()
    {
        if ( hasReadWithoutShouldRetry )
        {
            throw new AssertionError( "Performed read from a read cursor without shouldRetry" );
        }
    }
}
