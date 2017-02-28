/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

final class MuninnReadPageCursor extends MuninnPageCursor
{
    private final CursorPool.CursorSets cursorSets;
    private long lockStamp;
    MuninnReadPageCursor nextCursor;

    MuninnReadPageCursor( CursorPool.CursorSets cursorSets, long victimPage, PageCursorTracer pageCursorTracer )
    {
        super( victimPage, pageCursorTracer );
        this.cursorSets = cursorSets;
    }

    @Override
    protected void unpinCurrentPage()
    {
        MuninnPage p = page;

        if ( p != null )
        {
            pinEvent.done();
        }
        lockStamp = 0; // make sure not to accidentally keep a lock state around
        clearPageState();
    }

    @Override
    public boolean next() throws IOException
    {
        unpinCurrentPage();
        long lastPageId = assertPagedFileStillMappedAndGetIdOfLastPage();
        if ( nextPageId > lastPageId | nextPageId < 0 )
        {
            return false;
        }
        pin( nextPageId, false );
        currentPageId = nextPageId;
        nextPageId++;
        return true;
    }

    @Override
    protected boolean tryLockPage( MuninnPage page )
    {
        lockStamp = page.tryOptimisticReadLock();
        return true;
    }

    @Override
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
    protected void convertPageFaultLock( MuninnPage page )
    {
        lockStamp = page.unlockExclusive();
    }

    @Override
    protected void releaseCursor()
    {
        nextCursor = cursorSets.readCursors;
        cursorSets.readCursors = this;
    }

    @Override
    public boolean shouldRetry() throws IOException
    {
        MuninnPage p = page;
        boolean needsRetry = p != null && !p.validateReadLock( lockStamp );
        needsRetry |= linkedCursor != null && linkedCursor.shouldRetry();
        if ( needsRetry )
        {
            startRetry();
        }
        return needsRetry;
    }

    private void startRetry() throws IOException
    {
        setOffset( 0 );
        checkAndClearBoundsFlag();
        clearCursorException();
        lockStamp = page.tryOptimisticReadLock();
        // The page might have been evicted while we held the optimistic
        // read lock, so we need to check with page.pin that this is still
        // the page we're actually interested in:
        if ( !page.isBoundTo( pagedFile.swapper, currentPageId ) )
        {
            // This is no longer the page we're interested in, so we have
            // to redo the pinning.
            // This might in turn lead to a new optimistic lock on a
            // different page if someone else has taken the page fault for
            // us. If nobody has done that, we'll take the page fault
            // ourselves, and in that case we'll end up with first an exclusive
            // lock during the faulting, and then an optimistic read lock once the
            // fault itself is over.
            // First, forget about this page in case pin() throws and the cursor
            // is closed; we don't want unpinCurrentPage() to try unlocking
            // this page.
            page = null;
            // Then try pin again.
            pin( currentPageId, false );
        }
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

    @Override
    public void zapPage()
    {
        throw new IllegalStateException( "Cannot write to read-locked page" );
    }
}
