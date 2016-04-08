/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.io.pagecache.impl;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;

/**
 * A {@link PageCursor} implementation that delegates all calls to a given delegate PageCursor.
 */
public class DelegatingPageCursor implements PageCursor
{
    private final PageCursor delegate;

    public byte getByte()
    {
        return delegate.getByte();
    }

    public int copyTo( int sourceOffset, PageCursor targetCursor, int targetOffset, int lengthInBytes )
    {
        return delegate.copyTo( sourceOffset, targetCursor, targetOffset, lengthInBytes );
    }

    public void putInt( int value )
    {
        delegate.putInt( value );
    }

    public void getBytes( byte[] data )
    {
        delegate.getBytes( data );
    }

    public boolean next() throws IOException
    {
        return delegate.next();
    }

    public void putBytes( byte[] data )
    {
        delegate.putBytes( data );
    }

    public short getShort()
    {
        return delegate.getShort();
    }

    public File getCurrentFile()
    {
        return delegate.getCurrentFile();
    }

    public void putShort( short value )
    {
        delegate.putShort( value );
    }

    public short getShort( int offset )
    {
        return delegate.getShort( offset );
    }

    public int getCurrentPageSize()
    {
        return delegate.getCurrentPageSize();
    }

    public long getLong()
    {
        return delegate.getLong();
    }

    public void putLong( long value )
    {
        delegate.putLong( value );
    }

    public int getOffset()
    {
        return delegate.getOffset();
    }

    public void close()
    {
        delegate.close();
    }

    public void putByte( int offset, byte value )
    {
        delegate.putByte( offset, value );
    }

    public void putInt( int offset, int value )
    {
        delegate.putInt( offset, value );
    }

    public void putBytes( byte[] data, int arrayOffset, int length )
    {
        delegate.putBytes( data, arrayOffset, length );
    }

    public void rewind()
    {
        delegate.rewind();
    }

    public void putByte( byte value )
    {
        delegate.putByte( value );
    }

    public boolean checkAndClearBoundsFlag()
    {
        return delegate.checkAndClearBoundsFlag();
    }

    @Override
    public void raiseOutOfBounds()
    {
        delegate.raiseOutOfBounds();
    }

    @Override
    public PageCursor openLinkedCursor( long pageId )
    {
        return delegate.openLinkedCursor( pageId );
    }

    public long getCurrentPageId()
    {
        return delegate.getCurrentPageId();
    }

    public void putShort( int offset, short value )
    {
        delegate.putShort( offset, value );
    }

    public boolean next( long pageId ) throws IOException
    {
        return delegate.next( pageId );
    }

    public void putLong( int offset, long value )
    {
        delegate.putLong( offset, value );
    }

    public long getLong( int offset )
    {
        return delegate.getLong( offset );
    }

    public void getBytes( byte[] data, int arrayOffset, int length )
    {
        delegate.getBytes( data, arrayOffset, length );
    }

    public int getInt( int offset )
    {
        return delegate.getInt( offset );
    }

    public void setOffset( int offset )
    {
        delegate.setOffset( offset );
    }

    public byte getByte( int offset )
    {
        return delegate.getByte( offset );
    }

    public int getInt()
    {
        return delegate.getInt();
    }

    public boolean shouldRetry() throws IOException
    {
        return delegate.shouldRetry();
    }

    public DelegatingPageCursor( PageCursor delegate )
    {
        this.delegate = delegate;
    }
}
