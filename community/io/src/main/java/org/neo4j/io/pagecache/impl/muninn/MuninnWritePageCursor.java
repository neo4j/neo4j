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
package org.neo4j.io.pagecache.impl.muninn;

import java.io.IOException;

import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.impl.FileIsNotMappedException;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;

final class MuninnWritePageCursor extends MuninnPageCursor
{
    MuninnWritePageCursor( long victimPage, PageCursorTracer pageCursorTracer,
            VersionContextSupplier versionContextSupplier )
    {
        super( victimPage, pageCursorTracer, versionContextSupplier );
    }

    @Override
    protected void unpinCurrentPage()
    {
        if ( pinnedPageRef != 0 )
        {
            pinEvent.done();
            // Mark the page as dirty *after* our write access, to make sure it's dirty even if it was concurrently
            // flushed. Unlocking the write-locked page will mark it as dirty for us.
            if ( eagerFlush )
            {
                eagerlyFlushAndUnlockPage();
            }
            else
            {
                pagedFile.unlockWrite( pinnedPageRef );
            }
        }
        clearPageCursorState();
    }

    private void eagerlyFlushAndUnlockPage()
    {
        long flushStamp = pagedFile.unlockWriteAndTryTakeFlushLock( pinnedPageRef );
        if ( flushStamp != 0 )
        {
            boolean success = false;
            try
            {
                success = pagedFile.flushLockedPage( pinnedPageRef, currentPageId );
            }
            finally
            {
                pagedFile.unlockFlush( pinnedPageRef, flushStamp, success );
            }
        }
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
            if ( noGrow )
            {
                return false;
            }
            else
            {
                pagedFile.increaseLastPageIdTo( nextPageId );
            }
        }
        currentPageId = nextPageId;
        nextPageId++;
        pin( currentPageId, true );
        return true;
    }

    @Override
    protected boolean tryLockPage( long pageRef )
    {
        return pagedFile.tryWriteLock( pageRef );
    }

    @Override
    protected void unlockPage( long pageRef )
    {
        pagedFile.unlockWrite( pageRef );
    }

    @Override
    protected void pinCursorToPage( long pageRef, long filePageId, PageSwapper swapper ) throws FileIsNotMappedException
    {
        reset( pageRef );
        // Check if we've been racing with unmapping. We want to do this before
        // we make any changes to the contents of the page, because once all
        // files have been unmapped, the page cache can be closed. And when
        // that happens, dirty contents in memory will no longer have a chance
        // to get flushed. It is okay for this method to throw, because we are
        // after the reset() call, which means that if we throw, the cursor will
        // be closed and the page lock will be released.
        assertPagedFileStillMappedAndGetIdOfLastPage();
        pagedFile.incrementUsage( pageRef );
        pagedFile.setLastModifiedTxId( pageRef, versionContextSupplier.getVersionContext().committingTransactionId() );
    }

    @Override
    protected void convertPageFaultLock( long pageRef )
    {
        pagedFile.unlockExclusiveAndTakeWriteLock( pageRef );
    }

    @Override
    public boolean shouldRetry()
    {
        // We take exclusive locks, so there's never a need to retry.
        return false;
    }
}
