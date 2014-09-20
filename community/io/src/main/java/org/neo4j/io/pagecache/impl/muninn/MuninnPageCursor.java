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

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;

abstract class MuninnPageCursor implements PageCursor
{
    static final boolean monitorPinUnpin = Boolean.getBoolean(
            "org.neo4j.io.pagecache.impl.muninn.MuninnPageCursor.monitorPinUnpin" );

    protected MuninnPagedFile pagedFile;
    protected MuninnPage page;
    protected long pageId;
    protected int pf_flags;
    protected long currentPageId;
    protected long nextPageId;
    protected long lastPageId;
    protected long lockStamp;

    private boolean claimed;
    private int offset;

    public final void initialise( MuninnPagedFile pagedFile, long pageId, int pf_flags )
    {
        this.pagedFile = pagedFile;
        this.pageId = pageId;
        this.pf_flags = pf_flags;
    }

    public final void markAsClaimed()
    {
        claimed = true;
    }

    public final void assertUnclaimed()
    {
        if ( claimed )
        {
            throw new IllegalStateException(
                    "Cannot operate on more than one PageCursor at a time," +
                            " because it is prone to deadlocks" );
        }
    }

    @Override
    public final void rewind()
    {
        nextPageId = pageId;
        currentPageId = UNBOUND_PAGE_ID;
        lastPageId = pagedFile.getLastPageId();
    }

    public final void reset( MuninnPage page )
    {
        this.page = page;
        this.offset = 0;
    }

    @Override
    public final boolean next( long pageId ) throws IOException
    {
        nextPageId = pageId;
        return next();
    }

    @Override
    public final void close()
    {
        unpinCurrentPage();
        pagedFile = null;
        claimed = false;
    }

    @Override
    public final long getCurrentPageId()
    {
        return currentPageId;
    }

    protected abstract void unpinCurrentPage();

    // --- IO methods:

    @Override
    public final byte getByte()
    {
        byte b = page.getByte( offset );
        offset++;
        return b;
    }

    @Override
    public void putByte( byte value )
    {
        page.putByte( value, offset );
        offset++;
    }

    @Override
    public final long getLong()
    {
        long l = page.getLong( offset );
        offset += 8;
        return l;
    }

    @Override
    public void putLong( long value )
    {
        page.putLong( value, offset );
        offset += 8;
    }

    @Override
    public final int getInt()
    {
        int i = page.getInt( offset );
        offset += 4;
        return i;
    }

    @Override
    public void putInt( int value )
    {
        page.putInt( value, offset );
        offset += 4;
    }

    @Override
    public final long getUnsignedInt()
    {
        return getInt() & 0xFFFFFFFFL;
    }

    @Override
    public final void getBytes( byte[] data )
    {
        page.getBytes( data, offset );
        offset += data.length;
    }

    @Override
    public void putBytes( byte[] data )
    {
        page.putBytes( data, offset );
        offset += data.length;
    }

    @Override
    public final short getShort()
    {
        short s = page.getShort( offset );
        offset += 2;
        return s;
    }

    @Override
    public void putShort( short value )
    {
        page.putShort( value, offset );
        offset += 2;
    }

    @Override
    public final void setOffset( int offset )
    {
        if ( offset < 0 )
        {
            throw new IndexOutOfBoundsException();
        }
        this.offset = offset;
    }

    @Override
    public final int getOffset()
    {
        return offset;
    }
}
