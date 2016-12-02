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
package org.neo4j.index.gbptree;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.pagecache.CursorException;
import org.neo4j.io.pagecache.PageCursor;

public class TestPageCursor extends PageCursor
{
    private final PageCursor actual;
    private boolean shouldRetry;

    public TestPageCursor( PageCursor actual )
    {
        this.actual = actual;
    }

    @Override
    public int hashCode()
    {
        return actual.hashCode();
    }

    @Override
    public byte getByte()
    {
        return actual.getByte();
    }

    @Override
    public byte getByte( int offset )
    {
        return actual.getByte( offset );
    }

    @Override
    public void putByte( byte value )
    {
        actual.putByte( value );
    }

    @Override
    public void putByte( int offset, byte value )
    {
        actual.putByte( offset, value );
    }

    @Override
    public long getLong()
    {
        return actual.getLong();
    }

    @Override
    public boolean equals( Object obj )
    {
        return actual.equals( obj );
    }

    @Override
    public long getLong( int offset )
    {
        return actual.getLong( offset );
    }

    @Override
    public void putLong( long value )
    {
        actual.putLong( value );
    }

    @Override
    public void putLong( int offset, long value )
    {
        actual.putLong( offset, value );
    }

    @Override
    public int getInt()
    {
        return actual.getInt();
    }

    @Override
    public int getInt( int offset )
    {
        return actual.getInt( offset );
    }

    @Override
    public void putInt( int value )
    {
        actual.putInt( value );
    }

    @Override
    public void putInt( int offset, int value )
    {
        actual.putInt( offset, value );
    }

    @Override
    public void getBytes( byte[] data )
    {
        actual.getBytes( data );
    }

    @Override
    public void getBytes( byte[] data, int arrayOffset, int length )
    {
        actual.getBytes( data, arrayOffset, length );
    }

    @Override
    public void putBytes( byte[] data )
    {
        actual.putBytes( data );
    }

    @Override
    public void putBytes( byte[] data, int arrayOffset, int length )
    {
        actual.putBytes( data, arrayOffset, length );
    }

    @Override
    public short getShort()
    {
        return actual.getShort();
    }

    @Override
    public short getShort( int offset )
    {
        return actual.getShort( offset );
    }

    @Override
    public void putShort( short value )
    {
        actual.putShort( value );
    }

    @Override
    public void putShort( int offset, short value )
    {
        actual.putShort( offset, value );
    }

    @Override
    public void setOffset( int offset )
    {
        actual.setOffset( offset );
    }

    @Override
    public int getOffset()
    {
        return actual.getOffset();
    }

    @Override
    public long getCurrentPageId()
    {
        return actual.getCurrentPageId();
    }

    @Override
    public int getCurrentPageSize()
    {
        return actual.getCurrentPageSize();
    }

    @Override
    public File getCurrentFile()
    {
        return actual.getCurrentFile();
    }

    @Override
    public void rewind()
    {
        actual.rewind();
    }

    @Override
    public boolean next() throws IOException
    {
        return actual.next();
    }

    @Override
    public String toString()
    {
        return actual.toString();
    }

    @Override
    public boolean next( long pageId ) throws IOException
    {
        return actual.next( pageId );
    }

    @Override
    public void close()
    {
        actual.close();
    }

    @Override
    public boolean shouldRetry() throws IOException
    {
        if ( shouldRetry )
        {
            shouldRetry = false;
            return true;
        }
        return actual.shouldRetry();
    }

    @Override
    public int copyTo( int sourceOffset, PageCursor targetCursor, int targetOffset, int lengthInBytes )
    {
        return actual.copyTo( sourceOffset, targetCursor, targetOffset, lengthInBytes );
    }

    @Override
    public boolean checkAndClearBoundsFlag()
    {
        return actual.checkAndClearBoundsFlag();
    }

    @Override
    public void checkAndClearCursorException() throws CursorException
    {
        actual.checkAndClearCursorException();
    }

    @Override
    public void raiseOutOfBounds()
    {
        actual.raiseOutOfBounds();
    }

    @Override
    public void setCursorException( String message )
    {
        actual.setCursorException( message );
    }

    @Override
    public void clearCursorException()
    {
        actual.clearCursorException();
    }

    @Override
    public PageCursor openLinkedCursor( long pageId )
    {
        return actual.openLinkedCursor( pageId );
    }

    @Override
    public void zapPage()
    {
        actual.zapPage();
    }

    public void changed()
    {
        shouldRetry = true;
    }
}
