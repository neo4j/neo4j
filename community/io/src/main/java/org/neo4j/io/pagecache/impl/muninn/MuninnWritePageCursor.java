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
import java.util.concurrent.locks.StampedLock;

import org.neo4j.collection.primitive.PrimitiveLongIntMap;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.PagedFile;

class MuninnWritePageCursor extends MuninnPageCursor
{
    public MuninnWritePageCursor( MuninnCursorFreelist freelist )
    {
        super( freelist );
    }

    @Override
    protected void unpinCurrentPage()
    {
        if ( page != null )
        {
            assert page.isWriteLocked(): "page pinned for writing was not write locked: " + page;
            page.unlockWrite( lockStamp );
            page = null;
        }
        lockStamp = 0;
    }

    @Override
    public boolean next() throws IOException
    {
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
        unpinCurrentPage();
        pin( nextPageId );
        currentPageId = nextPageId;
        nextPageId++;
        return true;
    }

    void pin( long filePageId ) throws IOException
    {
        int stripe = (int) (filePageId & MuninnPagedFile.translationTableStripeMask);
        StampedLock translationTableLock = pagedFile.translationTableLocks[stripe];
        PrimitiveLongIntMap translationTable = pagedFile.translationTables[stripe];
        PageSwapper swapper = pagedFile.swapper;
        MuninnPage[] cachePages = pagedFile.cachePages;
        AtomicReference<MuninnPage> freelist = pagedFile.freelist;
        MuninnPage page;

        long stamp = translationTableLock.tryOptimisticRead();
        int cachePageId = translationTable.get( filePageId );
        if ( !translationTableLock.validate( stamp ) )
        {
            // The optimistic lock failed... Try again with a proper read lock.
            stamp = translationTableLock.readLock();
            try
            {
                cachePageId = translationTable.get( filePageId );
            }
            finally
            {
                translationTableLock.unlockRead( stamp );
            }
        }

        // The PrimitiveLongIntMap returns -1 for unmapped keys, so in that case we
        // know with high probability that we are going to page fault.
        // The only reason this might not happen, is that we are racing on the same
        // exact fault with another thread, and that thread ends up winning the race.
        if ( cachePageId == -1 )
        {
            // Because of the race, we have to check the table again once we have
            // the write lock.
            stamp = translationTableLock.writeLock();
            try
            {
                cachePageId = translationTable.get( filePageId );
                if ( cachePageId == -1 )
                {
                    // Our translation table is still outdated. Go ahead and page
                    // fault.
                    pageFault(
                            filePageId,
                            translationTable,
                            freelist,
                            swapper );
                    return;
                }
                // Another thread completed the page fault ahead of us.
                // Let's proceed like nothing happened.
            }
            finally
            {
                translationTableLock.unlockWrite( stamp );
            }
        }

        page = cachePages[cachePageId];
        lockStamp = page.writeLock();
        if ( page.pin( swapper, filePageId ) )
        {
            // Our translation table was also up to date, and the page is bound to
            // our file, and we could pin it since its not in the process of
            // eviction.
            pinCursorToPage( page );
            return;
        }
        page.unlockWrite( lockStamp );

        // Our translation table is outdated because the page has been bound to a
        // different file and/or filePageId.
        // We want to update our translationTable and do a page fault, but first,
        // we have to make sure that we are not racing with other threads that
        // might have the same exact idea.
        // To do this, we first have to lock the table, then check the table
        // again, since another thread might have already completed the page fault
        // for us.
        stamp = translationTableLock.writeLock();
        try
        {
            // Someone might have completed the page fault ahead of us, so we need
            // to check again if the translation table is still outdated.
            cachePageId = translationTable.get( filePageId );
            if ( cachePageId != -1 )
            {
                // Either someone completed the page fault, or eviction has not yet
                // cleared out our translation table entry.
                page = cachePages[cachePageId];
                // If we can pin the page now, someone already completed the page
                // fault ahead of us.
                lockStamp = page.writeLock();
                if ( page.pin( swapper, filePageId ) )
                {
                    pinCursorToPage( page );
                    return;
                }
                page.unlockWrite( lockStamp );
                // The translation table still contain a stale entry.
                // We'll overwrite it further down.
            }
            // The page is definitely no good, and our translation table is
            // definitely out of date.
            pageFault(
                    filePageId,
                    translationTable,
                    freelist,
                    swapper );
        }
        finally
        {
            translationTableLock.unlockWrite( stamp );
        }
    }

    private void pinCursorToPage( MuninnPage page )
    {
        reset( page );
        page.initBuffer();
        page.incrementUsage();
        page.markAsDirty();
    }

    /**
     * NOTE: Must be called while holding the right translationTableLock.writeLock
     * for the given translationTable!!!
     */
    void pageFault(
            long filePageId,
            PrimitiveLongIntMap translationTable,
            AtomicReference<MuninnPage> freelist,
            PageSwapper swapper ) throws IOException
    {
        MuninnPage page;
        for (;;)
        {
            page = freelist.get();
            if ( page == null )
            {
                pagedFile.pageCache.unparkEvictor();
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
        lockStamp = page.writeLock();
        page.initBuffer();
        page.fault( swapper, filePageId );
        translationTable.put( filePageId, page.cachePageId );
        pinCursorToPage( page );
        pagedFile.monitor.pageFault( filePageId, swapper );
    }

    @Override
    public final boolean retry()
    {
        // We take exclusive locks, so there's never a need to retry.
        return false;
    }
}
