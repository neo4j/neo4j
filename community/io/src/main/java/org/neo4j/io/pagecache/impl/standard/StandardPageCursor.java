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
package org.neo4j.io.pagecache.impl.standard;

import java.io.IOException;

import org.neo4j.io.pagecache.PageLock;
import org.neo4j.io.pagecache.impl.common.OffsetTrackingCursor;

import static org.neo4j.io.pagecache.PagedFile.PF_EXCLUSIVE_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_NO_GROW;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SINGLE_PAGE;

public class StandardPageCursor extends OffsetTrackingCursor
{
    private final CursorFreelist cursorFreelist;
    StandardPageCursor nextFree; // for the free-list chain

    private PageLock lockTypeHeld;
    private StandardPagedFile pagedFile;
    private long pageId;
    private long nextPageId;
    private long lastPageId;
    private int pf_flags;

    public StandardPageCursor( CursorFreelist cursorFreelist )
    {

        this.cursorFreelist = cursorFreelist;
    }

    public PinnablePage page()
    {
        return (PinnablePage)page;
    }

    /** The type of lock the cursor holds */
    public PageLock lockType()
    {
        return lockTypeHeld;
    }

    public void reset( PinnablePage page, PageLock lockTypeHeld )
    {
        this.lockTypeHeld = lockTypeHeld;
        super.reset( page );
    }

    public void assertNotInUse() throws IOException
    {
        if ( lockTypeHeld != null )
        {
            throw new IOException( "The cursor is already in use, you need to unpin the cursor before using it again." );
        }
    }

    @Override
    public long getCurrentPageId()
    {
        return page == null? StandardPinnablePage.UNBOUND_PAGE_ID : page().pageId();
    }

    @Override
    public void rewind() throws IOException
    {
        nextPageId = pageId;
        lastPageId = pagedFile.getLastPageId();
    }

    private static PageLock getLockType( int pf_flags ) throws IOException
    {
        // TODO this is an annoying conversion... we should use ints all the way down
        switch ( pf_flags & (PF_EXCLUSIVE_LOCK | PF_SHARED_LOCK) )
        {
            case PF_EXCLUSIVE_LOCK: return PageLock.EXCLUSIVE;
            case PF_SHARED_LOCK: return PageLock.SHARED;
            case PF_EXCLUSIVE_LOCK | PF_SHARED_LOCK: throw new IOException(
                    "Invalid flags: cannot ask to pin a page with both a shared and an exclusive lock" );
            default: throw new IOException(
                    "Invalid flags: must specify either shared or exclusive lock for page pinning" );
        }
    }

    @Override
    public boolean next() throws IOException
    {
        if ( page != null )
        {
            pagedFile.unpin( this );
        }

        if ( nextPageId >= lastPageId && (pf_flags & PF_NO_GROW) != 0 )
        {
            return false;
        }
        if ( (pf_flags & PF_SINGLE_PAGE) != 0 && nextPageId > pageId )
        {
            return false;
        }

        try
        {
            pagedFile.pin( this, getLockType( pf_flags ), nextPageId );
        }
        catch ( IOException e )
        {
            if ( page != null )
            {
                pagedFile.unpin( this );
            }
            throw e;
        }
        nextPageId++;
        return true;
    }

    @Override
    public void close()
    {
        if ( page != null )
        {
            pagedFile.unpin( this );
        }
        pagedFile = null;
        cursorFreelist.returnCursor( this );
    }

    public void initialise( StandardPagedFile pagedFile, long pageId, int pf_flags )
    {
        this.pagedFile = pagedFile;
        this.pageId = pageId;
        this.pf_flags = pf_flags;
    }
}
