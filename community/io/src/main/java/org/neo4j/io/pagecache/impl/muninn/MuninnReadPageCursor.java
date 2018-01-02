/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

final class MuninnReadPageCursor extends MuninnPageCursor
{
    private boolean optimisticLock;

    @Override
    protected void unpinCurrentPage()
    {
        MuninnPage p = page;
        page = null;

        if ( p != null )
        {
            pinEvent.done();
            assert optimisticLock || p.isReadLocked() :
                    "pinned page wasn't really locked; not even optimistically: " + p;

            if ( !optimisticLock )
            {
                p.unlockRead( lockStamp );
            }
        }
        lockStamp = 0;
    }

    @Override
    public boolean next() throws IOException
    {
        unpinCurrentPage();
        assertPagedFileStillMapped();
        if ( nextPageId > lastPageId )
        {
            return false;
        }
        pin( nextPageId, false );
        currentPageId = nextPageId;
        nextPageId++;
        return true;
    }

    protected void lockPage( MuninnPage page )
    {
        lockStamp = page.tryOptimisticRead();
        optimisticLock = true;
    }

    protected void unlockPage( MuninnPage page )
    {
    }

    @Override
    protected void pinCursorToPage( MuninnPage page, long filePageId, PageSwapper swapper )
    {
        reset( page );
        page.incrementUsage();
    }

    @Override
    protected void convertPageFaultLock( MuninnPage page, long stamp )
    {
        stamp = page.tryConvertToReadLock( stamp );
        assert stamp != 0: "Converting a write lock to a read lock should always succeed";
        lockStamp = stamp;
        optimisticLock = false; // We're using a pessimistic read lock
    }

    @Override
    public boolean shouldRetry() throws IOException
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
            if ( !page.isBoundTo( pagedFile.swapper, currentPageId ) )
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
                // Forget about this page in case pin() throws and the cursor
                // is closed; we don't want unpinCurrentPage() to try unlocking
                // this page.
                page = null;
                pin( currentPageId, false );
            }
        }
        return needsRetry;
    }

    @Override
    public void putByte( byte value )
    {
        throw new IllegalStateException( "Cannot write to read-locked page" );
    }

    @Override
    public void putLong( long value )
    {
        throw new IllegalStateException( "Cannot write to read-locked page" );
    }

    @Override
    public void putInt( int value )
    {
        throw new IllegalStateException( "Cannot write to read-locked page" );
    }

    @Override
    public void putBytes( byte[] data, int arrayOffset, int length )
    {
        throw new IllegalStateException( "Cannot write to read-locked page" );
    }

    @Override
    public void putShort( short value )
    {
        throw new IllegalStateException( "Cannot write to read-locked page" );
    }
}
