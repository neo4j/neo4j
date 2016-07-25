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
package org.neo4j.io.pagecache.impl.muninn;

import java.io.IOException;

import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.PagedFile;

final class MuninnWritePageCursor extends MuninnPageCursor
{
    private final CursorPool.CursorSets cursorSets;
    MuninnWritePageCursor nextCursor;

    MuninnWritePageCursor( CursorPool.CursorSets cursorSets, long victimPage )
    {
        super( victimPage );
        this.cursorSets = cursorSets;
    }

    @Override
    protected void unpinCurrentPage()
    {
        if ( page != null )
        {
            // Mark the page as dirty *after* our write access, to make sure it's dirty even if it was concurrently
            // flushed
            page.markAsDirty();
            pinEvent.done();
            unlockPage( page );
        }
        clearPageState();
    }

    @Override
    public boolean next() throws IOException
    {
        unpinCurrentPage();
        long lastPageId = assertPagedFileStillMappedAndGetIdOfLastPage();
        if ( nextPageId < 0 )
        {
            return false;
        }
        if ( nextPageId > lastPageId )
        {
            if ( (pf_flags & PagedFile.PF_NO_GROW) != 0 )
            {
                return false;
            }
            else
            {
                pagedFile.increaseLastPageIdTo( nextPageId );
            }
        }
        pin( nextPageId, true );
        currentPageId = nextPageId;
        nextPageId++;
        return true;
    }

    @Override
    protected boolean tryLockPage( MuninnPage page )
    {
        return page.tryWriteLock();
    }

    @Override
    protected void unlockPage( MuninnPage page )
    {
        page.unlockWrite();
    }

    @Override
    protected void pinCursorToPage( MuninnPage page, long filePageId, PageSwapper swapper )
    {
        reset( page );
        // Check if we've been racing with unmapping. We want to do this before
        // we make any changes to the contents of the page, because once all
        // files have been unmapped, the page cache can be closed. And when
        // that happens, dirty contents in memory will no longer have a chance
        // to get flushed. It is okay for this method to throw, because we are
        // after the reset() call, which means that if we throw, the cursor will
        // be closed and the page lock will be released.
        assertPagedFileStillMappedAndGetIdOfLastPage();
        page.incrementUsage();
    }

    @Override
    protected void convertPageFaultLock( MuninnPage page )
    {
        page.unlockExclusiveAndTakeWriteLock();
    }

    @Override
    protected void releaseCursor()
    {
        nextCursor = cursorSets.writeCursors;
        cursorSets.writeCursors = this;
    }

    @Override
    public final boolean shouldRetry()
    {
        // We take exclusive locks, so there's never a need to retry.
        return false;
    }
}
