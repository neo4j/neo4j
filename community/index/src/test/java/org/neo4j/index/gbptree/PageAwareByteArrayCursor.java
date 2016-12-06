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
import java.util.ArrayList;
import java.util.List;

import org.neo4j.io.pagecache.CursorException;
import org.neo4j.io.pagecache.PageCursor;

import static org.neo4j.index.gbptree.ByteArrayPageCursor.*;

class PageAwareByteArrayCursor extends PageCursor
{
    private final int pageSize;
    private final List<byte[]> pages;

    private PageCursor current;
    private long currentPageId = UNBOUND_PAGE_ID;
    private long nextPageId;
    private PageCursor linkedCursor;

    PageAwareByteArrayCursor( int pageSize )
    {
        this( new ArrayList<>(), pageSize, 0 );
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
    public boolean next() throws IOException
    {
        currentPageId = nextPageId;
        nextPageId++;
        assertPages();

        byte[] page = page( currentPageId );
        current = wrap( page, 0, page.length );
        return true;
    }

    @Override
    public boolean next( long pageId ) throws IOException
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
        if ( sourceOffset < 0 || targetOffset < 0 || lengthInBytes < 1 )
        {
            throw new IllegalArgumentException();
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
    public long getLongBE()
    {
        return current.getLongBE();
    }

    @Override
    public long getLongLE()
    {
        return current.getLongLE();
    }

    @Override
    public long getLongBE( int offset )
    {
        return current.getLongBE( offset );
    }

    @Override
    public long getLongLE( int offset )
    {
        return current.getLongLE( offset );
    }

    @Override
    public void putLongBE( long value )
    {
        current.putLongBE( value );
    }

    @Override
    public void putLongLE( long value )
    {
        current.putLongLE( value );
    }

    @Override
    public void putLongBE( int offset, long value )
    {
        current.putLongBE( offset, value );
    }

    @Override
    public void putLongLE( int offset, long value )
    {
        current.putLongLE( offset, value );
    }

    @Override
    public int getIntBE()
    {
        return current.getIntBE();
    }

    @Override
    public int getIntLE()
    {
        return current.getIntLE();
    }

    @Override
    public int getIntBE( int offset )
    {
        return current.getIntBE( offset );
    }

    @Override
    public int getIntLE( int offset )
    {
        return current.getIntLE( offset );
    }

    @Override
    public void putIntBE( int value )
    {
        current.putIntBE( value );
    }

    @Override
    public void putIntLE( int value )
    {
        current.putIntLE( value );
    }

    @Override
    public void putIntBE( int offset, int value )
    {
        current.putIntBE( offset, value );
    }

    @Override
    public void putIntLE( int offset, int value )
    {
        current.putIntLE( offset, value );
    }

    @Override
    public short getShortBE()
    {
        return current.getShortBE();
    }

    @Override
    public short getShortLE()
    {
        return current.getShortLE();
    }

    @Override
    public short getShortBE( int offset )
    {
        return current.getShortBE( offset );
    }

    @Override
    public short getShortLE( int offset )
    {
        return current.getShortLE( offset );
    }

    @Override
    public void putShortBE( short value )
    {
        current.putShortBE( value );
    }

    @Override
    public void putShortLE( short value )
    {
        current.putShortLE( value );
    }

    @Override
    public void putShortBE( int offset, short value )
    {
        current.putShortBE( offset, value );
    }

    @Override
    public void putShortLE( int offset, short value )
    {
        current.putShortLE( offset, value );
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
}
