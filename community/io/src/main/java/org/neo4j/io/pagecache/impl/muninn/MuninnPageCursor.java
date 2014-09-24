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
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageSwapper;

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

    /**
     * NOTE: Must be called while holding the right translationTableLock.writeLock
     * for the given translationTable!!!
     */
    void pageFault(
            long filePageId,
            PrimitiveLongObjectMap<MuninnPage> translationTable,
            AtomicReference<MuninnPage> freelist,
            PageSwapper swapper ) throws IOException
    {
        MuninnPage page;
        int iterationCount = 0;
        for (;;)
        {
            iterationCount++;
            page = freelist.get();
            if ( page == null )
            {
                page = pagedFile.unparkEvictor( iterationCount );
                if ( page != null )
                {
                    break;
                }
                continue;
            }
            if ( freelist.compareAndSet( page, page.nextFree ) )
            {
                break;
            }
        }

        // We got a free page, and we know that we have race-free access to it.
        // Well, it's not entirely race free, because other paged files might have
        // it in their translation tables, and try to pin it.
        // However, they will all fail because when they try to pin, the page will
        // either be 1) free, 2) bound to our file, or 3) the page is write locked.
        long stamp = page.writeLock();
        try
        {
            page.initBuffer();
            page.fault( swapper, filePageId );
        }
        catch ( Throwable throwable )
        {
            page.unlockWrite( stamp );
            throw throwable;
        }
        convertPageFaultLock( page, stamp );
        translationTable.put( filePageId, page );
        pinCursorToPage( page, filePageId, swapper );
        pagedFile.monitor.pageFaulted(filePageId, swapper);
    }

    protected abstract void unpinCurrentPage();

    protected abstract void convertPageFaultLock( MuninnPage page, long stamp );

    protected abstract void pinCursorToPage( MuninnPage page, long filePageId, PageSwapper swapper );

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
