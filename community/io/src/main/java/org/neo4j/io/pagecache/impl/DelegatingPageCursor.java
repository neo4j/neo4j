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

import org.neo4j.io.pagecache.CursorException;
import org.neo4j.io.pagecache.PageCursor;

/**
 * A {@link PageCursor} implementation that delegates all calls to a given delegate PageCursor.
 */
public class DelegatingPageCursor extends PageCursor
{
    private final PageCursor delegate;

    @Override
    public int copyTo( int sourceOffset, PageCursor targetCursor, int targetOffset, int lengthInBytes )
    {
        return delegate.copyTo( sourceOffset, targetCursor, targetOffset, lengthInBytes );
    }

    @Override
    public byte getByte()
    {
        return delegate.getByte();
    }

    @Override
    public long getLongBE()
    {
        return delegate.getLongBE();
    }

    @Override
    public long getLongLE()
    {
        return delegate.getLongLE();
    }

    @Override
    public long getLongBE( int offset )
    {
        return delegate.getLongBE( offset );
    }

    @Override
    public long getLongLE( int offset )
    {
        return delegate.getLongLE( offset );
    }

    @Override
    public void putLongBE( long value )
    {
        delegate.putLongBE( value );
    }

    @Override
    public void putLongLE( long value )
    {
        delegate.putLongLE( value );
    }

    @Override
    public void putLongBE( int offset, long value )
    {
        delegate.putLongBE( offset, value );
    }

    @Override
    public void putLongLE( int offset, long value )
    {
        delegate.putLongLE( offset, value );
    }

    @Override
    public int getIntBE()
    {
        return delegate.getIntBE();
    }

    @Override
    public int getIntLE()
    {
        return delegate.getIntLE();
    }

    @Override
    public int getIntBE( int offset )
    {
        return delegate.getIntBE( offset );
    }

    @Override
    public int getIntLE( int offset )
    {
        return delegate.getIntLE( offset );
    }

    @Override
    public void putIntBE( int value )
    {
        delegate.putIntBE( value );
    }

    @Override
    public void putIntLE( int value )
    {
        delegate.putIntLE( value );
    }

    @Override
    public void putIntBE( int offset, int value )
    {
        delegate.putIntBE( offset, value );
    }

    @Override
    public void putIntLE( int offset, int value )
    {
        delegate.putIntLE( offset, value );
    }

    @Override
    public short getShortBE()
    {
        return delegate.getShortBE();
    }

    @Override
    public short getShortLE()
    {
        return delegate.getShortLE();
    }

    @Override
    public short getShortBE( int offset )
    {
        return delegate.getShortBE( offset );
    }

    @Override
    public short getShortLE( int offset )
    {
        return delegate.getShortLE( offset );
    }

    @Override
    public void putShortBE( short value )
    {
        delegate.putShortBE( value );
    }

    @Override
    public void putShortLE( short value )
    {
        delegate.putShortLE( value );
    }

    @Override
    public void putShortBE( int offset, short value )
    {
        delegate.putShortBE( offset, value );
    }

    @Override
    public void putShortLE( int offset, short value )
    {
        delegate.putShortLE( offset, value );
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
    public File getCurrentFile()
    {
        return delegate.getCurrentFile();
    }

    @Override
    public int getCurrentPageSize()
    {
        return delegate.getCurrentPageSize();
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
    public void putBytes( byte[] data, int arrayOffset, int length )
    {
        delegate.putBytes( data, arrayOffset, length );
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
    public PageCursor openLinkedCursor( long pageId )
    {
        return delegate.openLinkedCursor( pageId );
    }

    @Override
    public long getCurrentPageId()
    {
        return delegate.getCurrentPageId();
    }

    @Override
    public boolean next( long pageId ) throws IOException
    {
        return delegate.next( pageId );
    }

    @Override
    public void getBytes( byte[] data, int arrayOffset, int length )
    {
        delegate.getBytes( data, arrayOffset, length );
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
    public boolean shouldRetry() throws IOException
    {
        return delegate.shouldRetry();
    }

    @Override
    public void zapPage()
    {
        delegate.zapPage();
    }

    public DelegatingPageCursor( PageCursor delegate )
    {
        this.delegate = delegate;
    }
}
