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
package org.neo4j.index.internal.gbptree;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.io.pagecache.CursorException;
import org.neo4j.io.pagecache.PageCursor;

import static java.lang.String.format;
import static org.neo4j.io.pagecache.ByteArrayPageCursor.wrap;

class PageAwareByteArrayCursor extends PageCursor
{
    private final int pageSize;
    private final List<byte[]> pages;

    private PageCursor current;
    private long currentPageId = UNBOUND_PAGE_ID;
    private long nextPageId;
    private PageCursor linkedCursor;
    private boolean shouldRetry;

    PageAwareByteArrayCursor( int pageSize )
    {
        this( pageSize, 0 );
    }

    private PageAwareByteArrayCursor( int pageSize, long nextPageId )
    {
        this( new ArrayList<>(), pageSize, nextPageId );
    }

    private PageAwareByteArrayCursor( List<byte[]> pages, int pageSize, long nextPageId )
    {
        this.pages = pages;
        this.pageSize = pageSize;
        this.nextPageId = nextPageId;
        initialize();
    }

    private void initialize()
    {
        currentPageId = UNBOUND_PAGE_ID;
        current = null;
    }

    PageAwareByteArrayCursor duplicate()
    {
        return new PageAwareByteArrayCursor( pages, pageSize, currentPageId );
    }

    PageAwareByteArrayCursor duplicate( long nextPageId )
    {
        return new PageAwareByteArrayCursor( pages, pageSize, nextPageId );
    }

    void forceRetry()
    {
        shouldRetry = true;
    }

    @Override
    public long getCurrentPageId()
    {
        return currentPageId;
    }

    @Override
    public int getCurrentPageSize()
    {
        if ( currentPageId == UNBOUND_PAGE_ID )
        {
            return UNBOUND_PAGE_SIZE;
        }
        else
        {
            return page( currentPageId ).length;
        }
    }

    @Override
    public boolean next()
    {
        currentPageId = nextPageId;
        nextPageId++;
        assertPages();

        byte[] page = page( currentPageId );
        current = wrap( page, 0, page.length );
        return true;
    }

    @Override
    public boolean next( long pageId )
    {
        currentPageId = pageId;
        assertPages();

        byte[] page = page( currentPageId );
        current = wrap( page, 0, page.length );
        return true;
    }

    @Override
    public int copyTo( int sourceOffset, PageCursor targetCursor, int targetOffset, int lengthInBytes )
    {
        if ( sourceOffset < 0 || targetOffset < 0 || lengthInBytes < 0 )
        {
            throw new IllegalArgumentException( format( "sourceOffset=%d, targetOffset=%d, lengthInBytes=%d, currenPageId=%d",
                    sourceOffset, targetOffset, lengthInBytes, currentPageId ) );
        }
        int bytesToCopy = Math.min( lengthInBytes,
                Math.min( current.getCurrentPageSize() - sourceOffset,
                        targetCursor.getCurrentPageSize() - targetOffset ) );

        for ( int i = 0; i < bytesToCopy; i++ )
        {
            targetCursor.putByte( targetOffset + i, getByte( sourceOffset + i ) );
        }
        return bytesToCopy;
    }

    @Override
    public int copyTo( int sourceOffset, ByteBuffer buf )
    {
        int bytesToCopy = Math.min( buf.limit() - buf.position(), pageSize - sourceOffset );
        for ( int i = 0; i < bytesToCopy; i++ )
        {
            byte b = getByte( sourceOffset + i );
            buf.put( b );
        }
        return bytesToCopy;
    }

    @Override
    public void shiftBytes( int sourceOffset, int length, int shift )
    {
        current.shiftBytes( sourceOffset, length, shift );
    }

    private void assertPages()
    {
        if ( currentPageId >= pages.size() )
        {
            for ( int i = pages.size(); i <= currentPageId; i++ )
            {
                pages.add( new byte[pageSize] );
            }
        }
    }

    private byte[] page( long pageId )
    {
        return pages.get( (int) pageId );
    }

    /* DELEGATE METHODS */

    @Override
    public File getCurrentFile()
    {
        return current.getCurrentFile();
    }

    @Override
    public byte getByte()
    {
        return current.getByte();
    }

    @Override
    public byte getByte( int offset )
    {
        return current.getByte( offset );
    }

    @Override
    public void putByte( byte value )
    {
        current.putByte( value );
    }

    @Override
    public void putByte( int offset, byte value )
    {
        current.putByte( offset, value );
    }

    @Override
    public long getLong()
    {
        return current.getLong();
    }

    @Override
    public long getLong( int offset )
    {
        return current.getLong( offset );
    }

    @Override
    public void putLong( long value )
    {
        current.putLong( value );
    }

    @Override
    public void putLong( int offset, long value )
    {
        current.putLong( offset, value );
    }

    @Override
    public int getInt()
    {
        return current.getInt();
    }

    @Override
    public int getInt( int offset )
    {
        return current.getInt( offset );
    }

    @Override
    public void putInt( int value )
    {
        current.putInt( value );
    }

    @Override
    public void putInt( int offset, int value )
    {
        current.putInt( offset, value );
    }

    @Override
    public void getBytes( byte[] data )
    {
        current.getBytes( data );
    }

    @Override
    public void getBytes( byte[] data, int arrayOffset, int length )
    {
        current.getBytes( data, arrayOffset, length );
    }

    @Override
    public void putBytes( byte[] data )
    {
        current.putBytes( data );
    }

    @Override
    public void putBytes( byte[] data, int arrayOffset, int length )
    {
        current.putBytes( data, arrayOffset, length );
    }

    @Override
    public void putBytes( int bytes, byte value )
    {
        current.putBytes( bytes, value );
    }

    @Override
    public short getShort()
    {
        return current.getShort();
    }

    @Override
    public short getShort( int offset )
    {
        return current.getShort( offset );
    }

    @Override
    public void putShort( short value )
    {
        current.putShort( value );
    }

    @Override
    public void putShort( int offset, short value )
    {
        current.putShort( offset, value );
    }

    @Override
    public void setOffset( int offset )
    {
        current.setOffset( offset );
    }

    @Override
    public int getOffset()
    {
        return current.getOffset();
    }

    @Override
    public void rewind()
    {
        current.rewind();
    }

    @Override
    public void close()
    {
        if ( linkedCursor != null )
        {
            linkedCursor.close();
        }
        current.close();
    }

    @Override
    public boolean shouldRetry() throws IOException
    {
        if ( shouldRetry )
        {
            shouldRetry = false;

            // To reset shouldRetry for linked cursor as well
            if ( linkedCursor != null )
            {
                linkedCursor.shouldRetry();
            }
            return true;
        }
        return linkedCursor != null && linkedCursor.shouldRetry() || current.shouldRetry();
    }

    @Override
    public boolean checkAndClearBoundsFlag()
    {
        boolean result = false;
        if ( linkedCursor != null )
        {
            result = linkedCursor.checkAndClearBoundsFlag();
        }
        result |= current.checkAndClearBoundsFlag();
        return result;
    }

    @Override
    public void checkAndClearCursorException() throws CursorException
    {
        current.checkAndClearCursorException();
    }

    @Override
    public void raiseOutOfBounds()
    {
        current.raiseOutOfBounds();
    }

    @Override
    public void setCursorException( String message )
    {
        current.setCursorException( message );
    }

    @Override
    public void clearCursorException()
    {
        current.clearCursorException();
    }

    @Override
    public PageCursor openLinkedCursor( long pageId )
    {
        PageCursor toReturn = new PageAwareByteArrayCursor( pages, pageSize, pageId );
        if ( linkedCursor != null )
        {
            linkedCursor.close();
        }
        linkedCursor = toReturn;
        return toReturn;
    }

    @Override
    public void zapPage()
    {
        current.zapPage();
    }

    @Override
    public boolean isWriteLocked()
    {
        return current == null || current.isWriteLocked();
    }
}
