/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.io.enterprise.pagecache.impl.muninn;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.enterprise.pagecache.impl.muninn.jsr166e.StampedLock;

class MuninnReadPageCursor extends MuninnPageCursor
{
    private boolean optimisticLock;

    public MuninnReadPageCursor( MuninnCursorFreelist freelist )
    {
        super( freelist );
    }

    @Override
    protected void unpinCurrentPage()
    {
        MuninnPage p = page;
        page = null;

        if ( p != null )
        {
            pagedFile.monitor.unpin( false, currentPageId, pagedFile.swapper );
            assert optimisticLock || p.isReadLocked() :
                    "pinned page wasn't really locked; not even optimistically: " + p;

            if ( !optimisticLock )
            {
                p.unlockRead( lockStamp );
            }
        }
        UnsafeUtil.retainReference( p );
        lockStamp = 0;
    }

    @Override
    public boolean next() throws IOException
    {
        if ( pagedFile.getRefCount() == 0 )
        {
            throw new IllegalStateException( "File has been unmapped" );
        }
        if ( nextPageId > lastPageId )
        {
            return false;
        }
        unpinCurrentPage();
        pin( nextPageId );
        currentPageId = nextPageId;
        nextPageId++;
        return true;
    }

    private void pin( long filePageId ) throws IOException
    {
        int stripe = (int) (filePageId & MuninnPagedFile.translationTableStripeMask);
        StampedLock translationTableLock = pagedFile.translationTableLocks[stripe];
        PrimitiveLongObjectMap<MuninnPage> translationTable = pagedFile.translationTables[stripe];
        PageSwapper swapper = pagedFile.swapper;
        AtomicReference<MuninnPage> freelist = pagedFile.freelist;
        MuninnPage page;

        long stamp = translationTableLock.tryOptimisticRead();
        page = translationTable.get( filePageId );
        if ( !translationTableLock.validate( stamp ) )
        {
            // The optimistic lock failed... Try again with a proper read lock.
            stamp = translationTableLock.readLock();
            try
            {
                page = translationTable.get( filePageId );
            }
            finally
            {
                translationTableLock.unlockRead( stamp );
            }
        }

        // The PrimitiveLongObjectMap returns null for unmapped keys, so in that case we
        // know with high probability that we are going to page fault.
        // The only reason this might not happen, is that we are racing on the same
        // exact fault with another thread, and that thread ends up winning the race.
        if ( page == null )
        {
            // Because of the race, we have to check the table again once we have
            // the write lock.
            stamp = translationTableLock.writeLock();
            try
            {
                page = translationTable.get( filePageId );
                if ( page == null )
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

        lockStamp = page.tryOptimisticRead();
        if ( page.pin( swapper, filePageId ) )
        {
            // Our translation table was also up to date, and the page is bound to
            // our file, and we could pin it since its not in the process of
            // eviction.
            pinCursorToPage( page, filePageId, swapper );
            optimisticLock = true;
            return;
        }

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
            page = translationTable.get( filePageId );
            if ( page != null )
            {
                // Either someone completed the page fault, or eviction has not yet
                // cleared out our translation table entry.
                // If we can pin the page now, someone already completed the page
                // fault ahead of us.
                lockStamp = page.readLock();
                if ( page.pin( swapper, filePageId ) )
                {
                    pinCursorToPage( page, filePageId, swapper );
                    optimisticLock = false;
                    return;
                }
                page.unlockRead( lockStamp );
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

    private void pinCursorToPage( MuninnPage page, long filePageId, PageSwapper swapper )
    {
        reset( page );
        // TODO we don't need to initBuffer here because page faulting does this for us:
        page.initBuffer(); // TODO looks like commenting this line out makes the writesFlushedFromPageFileMustBeExternallyObservable test fail predictably. WTF?!
        page.incrementUsage();
        pagedFile.monitor.pin( false, filePageId, swapper );
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
        for (;;)
        {
            page = freelist.get();
            if ( page == null )
            {
                pagedFile.unparkEvictor();
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
        long stamp = page.tryConvertToReadLock( lockStamp );
        assert stamp != 0: "Converting a write lock to a read lock should always succeed";
        lockStamp = stamp;
        optimisticLock = false; // We're using a pessimistic read lock
        translationTable.put( filePageId, page );
        pinCursorToPage( page, filePageId, swapper );
        page.incrementUsage(); // Add a second usage increment as a fault-bonus.
        pagedFile.monitor.pageFault( filePageId, swapper );
    }

    @Override
    public boolean retry() throws IOException
    {
        boolean needsRetry = optimisticLock && !page.validate( lockStamp );
        if ( needsRetry )
        {
            setOffset( 0 );
            optimisticLock = false;
            lockStamp = page.readLock();
            // We have a pessimistic read lock on the page now. This prevents
            // writes to the page, and it prevents the page from being evicted.
            // However, it might have been evicted while we held the optimistic
            // read lock, so we need to check with page.pin that this is still
            // the page we're actually interested in:
            if ( !page.pin( pagedFile.swapper, currentPageId ) )
            {
                // This is no longer the page we're interested in, so we have
                // to release our lock and redo the pinning.
                // This might in turn lead to a new optimistic lock on a
                // different page if someone else has taken the page fault for
                // us. If nobody has done that, we'll take the page fault
                // ourselves, and in that case we'll end up with first a write
                // lock during the faulting, and then a read lock once the
                // fault itself is over.
                page.unlockRead( lockStamp );
                pin( currentPageId );
            }
        }
        return needsRetry;
    }
}
