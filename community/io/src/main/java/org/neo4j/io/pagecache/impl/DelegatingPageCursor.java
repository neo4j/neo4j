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
package org.neo4j.io.pagecache.impl;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.io.pagecache.CursorException;
import org.neo4j.io.pagecache.PageCursor;

/**
 * A {@link PageCursor} implementation that delegates all calls to a given delegate PageCursor.
 */
public class DelegatingPageCursor extends PageCursor
{
    protected final PageCursor delegate;

    @Override
    public byte getByte()
    {
        return delegate.getByte();
    }

    @Override
    public int copyTo( int sourceOffset, PageCursor targetCursor, int targetOffset, int lengthInBytes )
    {
        return delegate.copyTo( sourceOffset, targetCursor, targetOffset, lengthInBytes );
    }

    @Override
    public int copyTo( int sourceOffset, ByteBuffer targetBuffer )
    {
        return delegate.copyTo( sourceOffset, targetBuffer );
    }

    @Override
    public void shiftBytes( int sourceOffset, int length, int shift )
    {
        delegate.shiftBytes( sourceOffset, length, shift );
    }

    @Override
    public void putInt( int value )
    {
        delegate.putInt( value );
    }

    @Override
    public void getBytes( byte[] data )
    {
        delegate.getBytes( data );
    }

    @Override
    public boolean next() throws IOException
    {
        return delegate.next();
    }

    @Override
    public void putBytes( byte[] data )
    {
        delegate.putBytes( data );
    }

    @Override
    public short getShort()
    {
        return delegate.getShort();
    }

    @Override
    public File getCurrentFile()
    {
        return delegate.getCurrentFile();
    }

    @Override
    public void putShort( short value )
    {
        delegate.putShort( value );
    }

    @Override
    public short getShort( int offset )
    {
        return delegate.getShort( offset );
    }

    @Override
    public int getCurrentPageSize()
    {
        return delegate.getCurrentPageSize();
    }

    @Override
    public long getLong()
    {
        return delegate.getLong();
    }

    @Override
    public void putLong( long value )
    {
        delegate.putLong( value );
    }

    @Override
    public int getOffset()
    {
        return delegate.getOffset();
    }

    @Override
    public void close()
    {
        delegate.close();
    }

    @Override
    public void putByte( int offset, byte value )
    {
        delegate.putByte( offset, value );
    }

    @Override
    public void putInt( int offset, int value )
    {
        delegate.putInt( offset, value );
    }

    @Override
    public void putBytes( byte[] data, int arrayOffset, int length )
    {
        delegate.putBytes( data, arrayOffset, length );
    }

    @Override
    public void putBytes( int bytes, byte value )
    {
        delegate.putBytes( bytes, value );
    }

    @Override
    public void rewind()
    {
        delegate.rewind();
    }

    @Override
    public void putByte( byte value )
    {
        delegate.putByte( value );
    }

    @Override
    public boolean checkAndClearBoundsFlag()
    {
        return delegate.checkAndClearBoundsFlag();
    }

    @Override
    public void checkAndClearCursorException() throws CursorException
    {
        delegate.checkAndClearCursorException();
    }

    @Override
    public void raiseOutOfBounds()
    {
        delegate.raiseOutOfBounds();
    }

    @Override
    public void setCursorException( String message )
    {
        delegate.setCursorException( message );
    }

    @Override
    public void clearCursorException()
    {
        delegate.clearCursorException();
    }

    @Override
    public PageCursor openLinkedCursor( long pageId ) throws IOException
    {
        return delegate.openLinkedCursor( pageId );
    }

    @Override
    public long getCurrentPageId()
    {
        return delegate.getCurrentPageId();
    }

    @Override
    public void putShort( int offset, short value )
    {
        delegate.putShort( offset, value );
    }

    @Override
    public boolean next( long pageId ) throws IOException
    {
        return delegate.next( pageId );
    }

    @Override
    public void putLong( int offset, long value )
    {
        delegate.putLong( offset, value );
    }

    @Override
    public long getLong( int offset )
    {
        return delegate.getLong( offset );
    }

    @Override
    public void getBytes( byte[] data, int arrayOffset, int length )
    {
        delegate.getBytes( data, arrayOffset, length );
    }

    @Override
    public int getInt( int offset )
    {
        return delegate.getInt( offset );
    }

    @Override
    public void setOffset( int offset )
    {
        delegate.setOffset( offset );
    }

    @Override
    public byte getByte( int offset )
    {
        return delegate.getByte( offset );
    }

    @Override
    public int getInt()
    {
        return delegate.getInt();
    }

    @Override
    public boolean shouldRetry() throws IOException
    {
        return delegate.shouldRetry();
    }

    @Override
    public void zapPage()
    {
        delegate.zapPage();
    }

    @Override
    public boolean isWriteLocked()
    {
        return delegate.isWriteLocked();
    }

    public PageCursor unwrap()
    {
        return delegate;
    }

    public DelegatingPageCursor( PageCursor delegate )
    {
        this.delegate = delegate;
    }
}
