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

import static org.neo4j.io.pagecache.PagedFile.PF_NO_GROW;

public class StandardPageCursor extends OffsetTrackingCursor
{
    private final CursorFreelist cursorFreelist;
    StandardPageCursor nextFree; // for the free-list chain

    private PageLock lockTypeHeld;
    private StandardPagedFile pagedFile;
    private long pageId;
    private long nextPageId;
    private long currentPageId;
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

    public int flags() { return pf_flags; }

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
    public boolean next() throws IOException
    {
        unpinCurrentPage();

        if ( checkNoGrow() )
        {
            pinNextPage();
            return true;
        }

        return false;
    }

    @Override
    public boolean next( long pageId ) throws IOException
    {
        unpinCurrentPage();
        nextPageId = pageId;

        if ( checkNoGrow() )
        {
            pinNextPage();
            return true;
        }

        return false;
    }

    private void unpinCurrentPage()
    {
        if ( page != null )
        {
            pagedFile.unpin( this );
        }
    }

    private boolean checkNoGrow()
    {
        if ( nextPageId > lastPageId )
        {
            if ( (pf_flags & PF_NO_GROW) != 0 )
            {
                return false;
            }
            else
            {
                lastPageId = pagedFile.increaseLastPageIdTo( nextPageId );
            }
        }
        return true;
    }

    private void pinNextPage() throws IOException
    {
        if ( pagedFile.getReferenceCount() == 0 )
        {
            throw new IllegalStateException( "File has been unmapped" );
        }
        currentPageId = nextPageId;
        nextPageId++;
        try
        {
            pagedFile.pin( this, pf_flags, currentPageId );
        }
        catch ( IOException e )
        {
            unpinCurrentPage();
            throw e;
        }
    }

    @Override
    public long getCurrentPageId()
    {
        return currentPageId;
    }

    @Override
    public void rewind() throws IOException
    {
        nextPageId = pageId;
        currentPageId = UNBOUND_PAGE_ID;
        lastPageId = pagedFile.getLastPageId();
    }

    @Override
    public boolean shouldRetry()
    {
        return false;
    }

    @Override
    public void close()
    {
        unpinCurrentPage();
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
