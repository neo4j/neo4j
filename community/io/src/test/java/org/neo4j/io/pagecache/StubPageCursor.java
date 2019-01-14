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
package org.neo4j.io.pagecache;

import java.io.File;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Utility for testing code that depends on page cursors.
 */
public class StubPageCursor extends PageCursor
{
    private final long pageId;
    private final int pageSize;
    protected ByteBuffer page;
    private int currentOffset;
    private boolean observedOverflow;
    private String cursorErrorMessage;
    private boolean closed;
    private boolean needsRetry;
    protected StubPageCursor linkedCursor;
    private boolean writeLocked;

    public StubPageCursor( long initialPageId, int pageSize )
    {
        this( initialPageId, ByteBuffer.allocate( pageSize ) );
    }

    public StubPageCursor( long initialPageId, ByteBuffer buffer )
    {
        this.pageId = initialPageId;
        this.pageSize = buffer.capacity();
        this.page = buffer;
        this.writeLocked = true;
    }

    @Override
    public long getCurrentPageId()
    {
        return pageId;
    }

    @Override
    public int getCurrentPageSize()
    {
        return pageSize;
    }

    @Override
    public File getCurrentFile()
    {
        return new File( "" );
    }

    @Override
    public void rewind()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean next()
    {
        return true;
    }

    @Override
    public boolean next( long pageId )
    {
        return true;
    }

    @Override
    public void close()
    {
        closed = true;
        if ( linkedCursor != null )
        {
            linkedCursor.close();
            linkedCursor = null;
        }
    }

    public boolean isClosed()
    {
        return closed;
    }

    @Override
    public boolean shouldRetry()
    {
        if ( needsRetry )
        {
            checkAndClearBoundsFlag();
        }
        return needsRetry || (linkedCursor != null && linkedCursor.shouldRetry());
    }

    public void setNeedsRetry( boolean needsRetry )
    {
        this.needsRetry = needsRetry;
    }

    @Override
    public int copyTo( int sourceOffset, PageCursor targetCursor, int targetOffset, int lengthInBytes )
    {
        return 0;
    }

    @Override
    public int copyTo( int sourceOffset, ByteBuffer targetBuffer )
    {
        return 0;
    }

    @Override
    public void shiftBytes( int sourceOffset, int length, int shift )
    {
        throw new UnsupportedOperationException( "Stub cursor does not support this method... yet" );
    }

    @Override
    public boolean checkAndClearBoundsFlag()
    {
        boolean overflow = observedOverflow;
        observedOverflow = false;
        return overflow || (linkedCursor != null && linkedCursor.checkAndClearBoundsFlag());
    }

    @Override
    public void checkAndClearCursorException() throws CursorException
    {
        String message = this.cursorErrorMessage;
        if ( message != null )
        {
            throw new CursorException( message );
        }
    }

    @Override
    public void raiseOutOfBounds()
    {
        observedOverflow = true;
    }

    @Override
    public void setCursorException( String message )
    {
        this.cursorErrorMessage = message;
    }

    @Override
    public void clearCursorException()
    {
        this.cursorErrorMessage = null;
    }

    @Override
    public PageCursor openLinkedCursor( long pageId )
    {
        return linkedCursor = new StubPageCursor( pageId, pageSize );
    }

    @Override
    public byte getByte()
    {
        byte value = getByte( currentOffset );
        currentOffset += 1;
        return value;
    }

    @Override
    public byte getByte( int offset )
    {
        try
        {
            return page.get( offset );
        }
        catch ( IndexOutOfBoundsException | BufferOverflowException | BufferUnderflowException e )
        {
            return handleOverflow();
        }
    }

    private byte handleOverflow()
    {
        observedOverflow = true;
        return (byte) ThreadLocalRandom.current().nextInt();
    }

    @Override
    public void putByte( byte value )
    {
        putByte( currentOffset, value );
        currentOffset += 1;
    }

    @Override
    public void putByte( int offset, byte value )
    {
        try
        {
            page.put( offset, value );
        }
        catch ( IndexOutOfBoundsException | BufferOverflowException | BufferUnderflowException e )
        {
            handleOverflow();
        }
    }

    @Override
    public long getLong()
    {
        long value = getLong( currentOffset );
        currentOffset += 8;
        return value;
    }

    @Override
    public long getLong( int offset )
    {
        try
        {
            return page.getLong( offset );
        }
        catch ( IndexOutOfBoundsException | BufferOverflowException | BufferUnderflowException e )
        {
            return handleOverflow();
        }
    }

    @Override
    public void putLong( long value )
    {
        putLong( currentOffset, value );
        currentOffset += 8;
    }

    @Override
    public void putLong( int offset, long value )
    {
        try
        {
            page.putLong( offset, value );
        }
        catch ( IndexOutOfBoundsException | BufferOverflowException | BufferUnderflowException e )
        {
            handleOverflow();
        }
    }

    @Override
    public int getInt()
    {
        int value = getInt( currentOffset );
        currentOffset += 4;
        return value;
    }

    @Override
    public int getInt( int offset )
    {
        try
        {
            return page.getInt( offset );
        }
        catch ( IndexOutOfBoundsException | BufferOverflowException | BufferUnderflowException e )
        {
            return handleOverflow();
        }
    }

    @Override
    public void putInt( int value )
    {
        putInt( currentOffset, value );
        currentOffset += 4;
    }

    @Override
    public void putInt( int offset, int value )
    {
        try
        {
            page.putInt( offset, value );
        }
        catch ( IndexOutOfBoundsException | BufferOverflowException | BufferUnderflowException e )
        {
            handleOverflow();
        }
    }

    @Override
    public void getBytes( byte[] data )
    {
        getBytes( data, 0, data.length );
    }

    @Override
    public void getBytes( byte[] data, int arrayOffset, int length )
    {
        try
        {
            assert arrayOffset == 0 : "please implement support for arrayOffset";
            page.position( currentOffset );
            page.get( data, arrayOffset, length );
            currentOffset += length;
        }
        catch ( IndexOutOfBoundsException | BufferOverflowException | BufferUnderflowException e )
        {
            handleOverflow();
        }
    }

    @Override
    public void putBytes( byte[] data )
    {
        putBytes( data, 0, data.length );
    }

    @Override
    public void putBytes( byte[] data, int arrayOffset, int length )
    {
        try
        {
            assert arrayOffset == 0 : "please implement support for arrayOffset";
            page.position( currentOffset );
            page.put( data, arrayOffset, length );
            currentOffset += length;
        }
        catch ( IndexOutOfBoundsException | BufferOverflowException | BufferUnderflowException e )
        {
            handleOverflow();
        }
    }

    @Override
    public void putBytes( int bytes, byte value )
    {
        byte[] byteArray = new byte[bytes];
        Arrays.fill( byteArray, value );
        putBytes( byteArray, 0, bytes );
    }

    @Override
    public short getShort()
    {
        short value = getShort( currentOffset );
        currentOffset += 2;
        return value;
    }

    @Override
    public short getShort( int offset )
    {
        try
        {
            return page.getShort( offset );
        }
        catch ( IndexOutOfBoundsException | BufferOverflowException | BufferUnderflowException e )
        {
            return handleOverflow();
        }
    }

    @Override
    public void putShort( short value )
    {
        putShort( currentOffset, value );
        currentOffset += 2;
    }

    @Override
    public void putShort( int offset, short value )
    {
        try
        {
            page.putShort( offset, value );
        }
        catch ( IndexOutOfBoundsException | BufferOverflowException | BufferUnderflowException e )
        {
            handleOverflow();
        }
    }

    @Override
    public int getOffset()
    {
        return currentOffset;
    }

    @Override
    public void setOffset( int offset )
    {
        if ( offset < 0 )
        {
            throw new IndexOutOfBoundsException();
        }
        currentOffset = offset;
    }

    @Override
    public void zapPage()
    {
        for ( int i = 0; i < pageSize; i++ )
        {
            putByte( i, (byte) 0 );
        }
    }

    @Override
    public String toString()
    {
        return "PageCursor{" +
               "currentOffset=" + currentOffset +
               ", page=" + page +
               '}';
    }

    @Override
    public boolean isWriteLocked()
    {
        return writeLocked;
    }

    public void setWriteLocked( boolean writeLocked )
    {
        this.writeLocked = writeLocked;
    }
}
