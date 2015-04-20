/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageEvictionCallback;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.FlushEventOpportunity;
import org.neo4j.io.pagecache.tracing.MajorFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageFaultEvent;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

final class MuninnPagedFile implements PagedFile
{
    private static final int translationTableChunkSizePower = Integer.getInteger(
            "org.neo4j.io.pagecache.impl.muninn.MuninnPagedFile.translationTableChunkSizePower", 12 );
    private static final int translationTableChunkSize = 1 << translationTableChunkSizePower;
    private static final int translationTableChunkSizeMask = translationTableChunkSize - 1;
    private static final int translationTableChunkArrayBase = UnsafeUtil.arrayBaseOffset( MuninnPage[].class );
    private static final int translationTableChunkArrayScale = UnsafeUtil.arrayIndexScale( MuninnPage[].class );

    private static final long referenceCounterOffset =
            UnsafeUtil.getFieldOffset( MuninnPagedFile.class, "referenceCounter" );
    private static final long lastPageIdOffset =
            UnsafeUtil.getFieldOffset( MuninnPagedFile.class, "lastPageId" );

    final MuninnPageCache pageCache;
    final int pageSize;
    final PageCacheTracer tracer;

    // This is the table where we translate file-page-ids to cache-page-ids. Only one thread can perform a resize at
    // a time, and we ensure this mutual exclusion using the monitor lock on this MuninnPagedFile object.
    volatile Object[][] translationTable;

    final PageSwapper swapper;
    private final CursorPool cursorPool;

    // Accessed via Unsafe
    private volatile int referenceCounter;
    private volatile long lastPageId;

    MuninnPagedFile(
            File file,
            MuninnPageCache pageCache,
            int pageSize,
            PageSwapperFactory swapperFactory,
            CursorPool cursorPool,
            PageCacheTracer tracer ) throws IOException
    {
        this.pageCache = pageCache;
        this.pageSize = pageSize;
        this.cursorPool = cursorPool;
        this.tracer = tracer;

        // The translation table is an array of arrays of references to either null, MuninnPage objects, or Latch
        // objects. The table only grows the outer array, and all the inner "chunks" all stay the same size. This
        // means that pages can be addressed with simple bit-wise operations on the filePageId. Eviction sets slots
        // to null with volatile writes. Page faults CAS's in a latch that will be opened after the page fault has
        // completed and written the final page reference to the slot. The initial CAS on a page fault is what
        // ensures that only a single thread will fault a page at a time. Look-ups use volatile reads of the slots.
        // If a look-up finds a latch, it awaits on it and retries the look-up. If a look-up finds a null reference,
        // it initiates a page fault. If a look-up finds that it is out of bounds of the translation table, it
        // resizes the table by first taking the resize lock, then verifying that the given filePageId is still out
        // of bounds, then creates a new and larger outer array, then copies over the existing inner arrays, fills
        // the remaining outer array slots with more inner arrays, and then finally assigns the new outer array to
        // the translationTable field and releases the resize lock.
        PageEvictionCallback onEviction = new MuninnPageEvictionCallback( this );
        swapper = swapperFactory.createPageSwapper( file, pageSize, onEviction );
        long lastPageId = swapper.getLastPageId();

        int initialChunks = 1 + computeChunkId( lastPageId );
        Object[][] tt = new Object[initialChunks][];
        for ( int i = 0; i < initialChunks; i++ )
        {
            tt[i] = new Object[translationTableChunkSize];
        }
        translationTable = tt;

        initialiseLastPageId( lastPageId );
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + swapper.file().getName() + "]";
    }

    @Override
    public PageCursor io( long pageId, int pf_flags )
    {
        if ( getRefCount() == 0 )
        {
            throw new IllegalStateException(
                    "File has been unmapped" );
        }

        int lockMask = PF_EXCLUSIVE_LOCK | PF_SHARED_LOCK;
        if ( (pf_flags & lockMask) == 0 )
        {
            throw new IllegalArgumentException(
                    "Must specify either PF_EXCLUSIVE_LOCK or PF_SHARED_LOCK" );
        }
        if ( (pf_flags & lockMask) == lockMask )
        {
            throw new IllegalArgumentException(
                    "Cannot specify both PF_EXCLUSIVE_LOCK and PF_SHARED_LOCK" );
        }
        MuninnPageCursor cursor;
        if ( (pf_flags & PF_SHARED_LOCK) == 0 )
        {
            cursor = cursorPool.takeWriteCursor();
        }
        else
        {
            cursor = cursorPool.takeReadCursor();
        }

        cursor.initialise( this, pageId, pf_flags );
        cursor.rewind();
        return cursor;
    }

    @Override
    public int pageSize()
    {
        return pageSize;
    }

    File file()
    {
        return swapper.file();
    }

    public void close() throws IOException
    {
        pageCache.unmap( this );
    }

    void closeSwapper() throws IOException
    {
        swapper.close();
    }

    @Override
    public void flushAndForce() throws IOException
    {
        try ( MajorFlushEvent flushEvent = tracer.beginFileFlush( swapper ) )
        {
            flushAndForceInternal( flushEvent.flushEventOpportunity() );
        }
    }

    void flushAndForceInternal( FlushEventOpportunity flushOpportunity ) throws IOException
    {
        pageCache.pauseBackgroundFlushTask();
        try
        {
            for ( Object[] chunk : translationTable )
            {
                for ( Object element : chunk )
                {
                    if ( element instanceof MuninnPage )
                    {
                        MuninnPage page = (MuninnPage) element;
                        long stamp = page.readLock();
                        try
                        {
                            page.flush( swapper, page.getFilePageId(), flushOpportunity );
                        }
                        finally
                        {
                            page.unlockRead( stamp );
                        }
                    }
                }
            }
            force();
        }
        finally
        {
            pageCache.unpauseBackgroundFlushTask();
        }
    }

    @Override
    public void force() throws IOException
    {
        swapper.force();
    }

    @Override
    public long getLastPageId()
    {
        return lastPageId;
    }

    private void initialiseLastPageId( long lastPageIdFromFile )
    {
        UnsafeUtil.putLong( this, lastPageIdOffset, lastPageIdFromFile );
    }

    /**
     * Make sure that the lastPageId is at least the given pageId
     */
    long increaseLastPageIdTo( long newLastPageId )
    {
        long current;
        do
        {
            current = lastPageId;
        }
        while ( current < newLastPageId
                && !UnsafeUtil.compareAndSwapLong( this, lastPageIdOffset, current, newLastPageId ) );
        return lastPageId;
    }

    /**
     * Atomically increment the reference count for this mapped file.
     */
    void incrementRefCount()
    {
        UnsafeUtil.getAndAddInt( this, referenceCounterOffset, 1 );
    }

    /**
     * Atomically decrement the reference count. Returns true if this was the
     * last reference.
     */
    boolean decrementRefCount()
    {
        // compares with 1 because getAndAdd returns the old value, and a 1
        // means the value is now 0.
        return UnsafeUtil.getAndAddInt( this, referenceCounterOffset, -1 ) <= 1;
    }

    /**
     * Get the current ref-count. Useful for checking if this PagedFile should
     * be considered unmapped.
     */
    int getRefCount()
    {
        return UnsafeUtil.getIntVolatile( this, referenceCounterOffset );
    }

    /**
     * Grab a free page for the purpose of page faulting. Possibly blocking if
     * none are immediately available.
     * @param faultEvent The trace event for the current page fault.
     */
    MuninnPage grabFreePage( PageFaultEvent faultEvent ) throws IOException
    {
        return pageCache.grabFreePage( faultEvent );
    }

    /**
     * Remove the mapping of the given filePageId from the translation table, and return the evicted page object.
     * @param filePageId The id of the file page to evict.
     * @return The page object of the evicted file page.
     */
    MuninnPage evictPage( long filePageId )
    {
        int chunkId = computeChunkId( filePageId );
        long chunkOffset = computeChunkOffset( filePageId );
        Object[] chunk = translationTable[chunkId];
        Object element = UnsafeUtil.getAndSetObject( chunk, chunkOffset, null );
        assert element instanceof MuninnPage: "Expected to evict a MuninnPage but found " + element;
        return (MuninnPage) element;
    }

    /**
     * Expand the translation table such that it can include at least the given chunkId.
     * @param maxChunkId The new translation table must be big enough to include at least this chunkId.
     * @return A reference to the expanded transaction table.
     */
    synchronized Object[][] expandCapacity( int maxChunkId )
    {
        Object[][] tt = translationTable;
        if ( tt.length <= maxChunkId )
        {
            int newLength = computeNewRootTableLength( maxChunkId );
            Object[][] ntt = new Object[newLength][];
            System.arraycopy( tt, 0, ntt, 0, tt.length );
            for ( int i = tt.length; i < ntt.length; i++ )
            {
                ntt[i] = new Object[translationTableChunkSize];
            }
            tt = ntt;
            translationTable = tt;
        }
        return tt;
    }

    private int computeNewRootTableLength( int maxChunkId )
    {
        // Grow by approx. 10% but always by at least one full chunk.
        return 1 + (int) (maxChunkId * 1.1);
    }

    int computeChunkId( long filePageId )
    {
        return (int) (filePageId >>> translationTableChunkSizePower);
    }

    long computeChunkOffset( long filePageId )
    {
        int index = (int) (filePageId & translationTableChunkSizeMask);
        return UnsafeUtil.arrayOffset( index, translationTableChunkArrayBase, translationTableChunkArrayScale );
    }
}
