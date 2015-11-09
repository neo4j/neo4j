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
import org.neo4j.io.pagecache.tracing.FlushEvent;
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
    private static final long translationTableChunkSizeMask = translationTableChunkSize - 1;
    private static final int translationTableChunkArrayBase = UnsafeUtil.arrayBaseOffset( MuninnPage[].class );
    private static final int translationTableChunkArrayScale = UnsafeUtil.arrayIndexScale( MuninnPage[].class );

    private static final long headerStateOffset =
            UnsafeUtil.getFieldOffset( MuninnPagedFile.class, "headerState" );
    private static final int headerStateRefCountShift = 48;
    private static final int headerStateRefCountMax = 0x7FFF;
    private static final long headerStateRefCountMask = 0x7FFF_0000_0000_0000L;
    private static final long headerStateLastPageIdMask = 0x8000_FFFF_FFFF_FFFFL;

    final MuninnPageCache pageCache;
    final int filePageSize;
    final PageCacheTracer tracer;

    // This is the table where we translate file-page-ids to cache-page-ids. Only one thread can perform a resize at
    // a time, and we ensure this mutual exclusion using the monitor lock on this MuninnPagedFile object.
    volatile Object[][] translationTable;

    final PageSwapper swapper;
    private final CursorPool cursorPool;

    /**
     * The header state includes both the reference count of the PagedFile – 15 bits – and the ID of the last page in
     * the file – 48 bits, plus an empty file marker bit. Because our pages are usually 2^13 bytes, this means that we
     * only loose 3 bits to the reference count, in terms of keeping large files byte addressable.
     *
     * The layout looks like this:
     *
     * ┏━ Empty file marker bit. When 1, the file is empty.
     * ┃    ┏━ Reference count, 15 bites.
     * ┃    ┃                ┏━ 48 bits for the last page id.
     * ┃┏━━━┻━━━━━━━━━━┓ ┏━━━┻━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
     * MRRRRRRR RRRRRRRR IIIIIIII IIIIIIII IIIIIIII IIIIIIII IIIIIIII IIIIIIII
     * 1        2        3        4        5        6        7        8        byte
     */
    @SuppressWarnings( "unused" ) // Accessed via Unsafe
    private volatile long headerState;

    MuninnPagedFile(
            File file,
            MuninnPageCache pageCache,
            int filePageSize,
            PageSwapperFactory swapperFactory,
            CursorPool cursorPool,
            PageCacheTracer tracer,
            boolean createIfNotExists,
            boolean truncateExisting ) throws IOException
    {
        this.pageCache = pageCache;
        this.filePageSize = filePageSize;
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
        swapper = swapperFactory.createPageSwapper( file, filePageSize, onEviction, createIfNotExists );
        if ( truncateExisting )
        {
            swapper.truncate();
        }
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
        return filePageSize;
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
            syncDevice();
        }
    }

    void flushAndForceInternal( FlushEventOpportunity flushOpportunity ) throws IOException
    {
        pageCache.pauseBackgroundFlushTask();
        long[] stamps = new long[translationTableChunkSize];
        MuninnPage[] pages = new MuninnPage[translationTableChunkSize];
        long filePageId = -1; // Start at -1 because we increment at the *start* of the chunk-loop iteration.
        try
        {
            for ( Object[] chunk : translationTable )
            {
                // TODO Look into if we can tolerate flushing a few clean pages if it means we can use larger vectors.
                // TODO The clean pages in question must still be loaded, though. Otherwise we'll end up writing
                // TODO garbage to the file.
                int pagesGrabbed = 0;
                for ( Object element : chunk )
                {
                    filePageId++;
                    if ( element instanceof MuninnPage )
                    {
                        MuninnPage page = (MuninnPage) element;
                        stamps[pagesGrabbed] = page.readLock();
                        if ( page.isBoundTo( swapper, filePageId ) && page.isDirty() )
                        {
                            // The page is still bound to the expected file and file page id after we locked it,
                            // so we didn't race with eviction and faulting, and the page is dirty.
                            // So we add it to our IO vector.
                            pages[pagesGrabbed] = page;
                            pagesGrabbed++;
                            continue;
                        }
                        else
                        {
                            page.unlockRead( stamps[pagesGrabbed] );
                        }
                    }
                    if ( pagesGrabbed > 0 )
                    {
                        pagesGrabbed = vectoredFlush( stamps, pages, pagesGrabbed, flushOpportunity );
                    }
                }
                if ( pagesGrabbed > 0 )
                {
                    vectoredFlush( stamps, pages, pagesGrabbed, flushOpportunity );
                }
            }

            swapper.force();
        }
        finally
        {
            pageCache.unpauseBackgroundFlushTask();
        }
    }

    private int vectoredFlush(
            long[] stamps, MuninnPage[] pages, int pagesGrabbed, FlushEventOpportunity flushOpportunity )
            throws IOException
    {
        FlushEvent flush = null;
        try
        {
            // Write the pages vector
            MuninnPage firstPage = pages[0];
            long startFilePageId = firstPage.getFilePageId();
            flush = flushOpportunity.beginFlush( startFilePageId, firstPage.getCachePageId(), swapper );
            long bytesWritten = swapper.write( startFilePageId, pages, 0, pagesGrabbed );

            // Update the flush event
            flush.addBytesWritten( bytesWritten );
            flush.addPagesFlushed( pagesGrabbed );
            flush.done();

            // Mark the flushed pages as clean
            for ( int j = 0; j < pagesGrabbed; j++ )
            {
                pages[j].markAsClean();
            }

            // There are now 0 'grabbed' pages
            return 0;
        }
        catch ( IOException ioe )
        {
            if ( flush != null )
            {
                flush.done( ioe );
            }
            throw ioe;
        }
        finally
        {
            // Always unlock all the pages in the vector
            for ( int j = 0; j < pagesGrabbed; j++ )
            {
                pages[j].unlockRead( stamps[j] );
            }
        }
    }

    private void syncDevice() throws IOException
    {
        pageCache.syncDevice();
    }

    @Override
    public long getLastPageId()
    {
        long state = getHeaderState();
        if ( refCountOf( state ) == 0 )
        {
            throw new IllegalStateException( "File has been unmapped: " + file().getPath() );
        }
        return state & headerStateLastPageIdMask;
    }

    private long getHeaderState()
    {
        return UnsafeUtil.getLongVolatile( this, headerStateOffset );
    }

    private long refCountOf( long state )
    {
        return (state & headerStateRefCountMask) >>> headerStateRefCountShift;
    }

    private void initialiseLastPageId( long lastPageIdFromFile )
    {
        if ( lastPageIdFromFile < 0 )
        {
            // MIN_VALUE only has the sign bit raised, and the rest of the bits are zeros.
            UnsafeUtil.putLongVolatile( this, headerStateOffset, Long.MIN_VALUE );
        }
        else
        {
            UnsafeUtil.putLongVolatile( this, headerStateOffset, lastPageIdFromFile );
        }
    }

    /**
     * Make sure that the lastPageId is at least the given pageId
     */
    void increaseLastPageIdTo( long newLastPageId )
    {
        long current, update, lastPageId;
        do
        {
            current = getHeaderState();
            update = newLastPageId + (current & headerStateRefCountMask);
            lastPageId = current & headerStateLastPageIdMask;
        }
        while ( lastPageId < newLastPageId
                && !UnsafeUtil.compareAndSwapLong( this, headerStateOffset, current, update ) );
    }

    /**
     * Atomically increment the reference count for this mapped file.
     */
    void incrementRefCount()
    {
        long current, update;
        do
        {
            current = getHeaderState();
            long count = refCountOf( current ) + 1;
            if ( count > headerStateRefCountMax )
            {
                throw new IllegalStateException( "Cannot map file because reference counter would overflow. " +
                                                 "Maximum reference count is " + headerStateRefCountMax + ". " +
                                                 "File is " + swapper.file().getAbsolutePath() );
            }
            update = (current & headerStateLastPageIdMask) + (count << headerStateRefCountShift);
        }
        while ( !UnsafeUtil.compareAndSwapLong( this, headerStateOffset, current, update ) );
    }

    /**
     * Atomically decrement the reference count. Returns true if this was the
     * last reference.
     */
    boolean decrementRefCount()
    {
        long current, update, count;
        do
        {
            current = getHeaderState();
            count = refCountOf( current ) - 1;
            if ( count < 0 )
            {
                throw new IllegalStateException( "File has already been closed and unmapped. " +
                                                 "It cannot be closed any further." );
            }
            update = (current & headerStateLastPageIdMask) + (count << headerStateRefCountShift);
        }
        while ( !UnsafeUtil.compareAndSwapLong( this, headerStateOffset, current, update ) );
        return count == 0;
    }

    /**
     * Get the current ref-count. Useful for checking if this PagedFile should
     * be considered unmapped.
     */
    int getRefCount()
    {
        return (int) refCountOf( getHeaderState() );
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

    static int computeChunkId( long filePageId )
    {
        return (int) (filePageId >>> translationTableChunkSizePower);
    }

    static long computeChunkOffset( long filePageId )
    {
        int index = (int) (filePageId & translationTableChunkSizeMask);
        return UnsafeUtil.arrayOffset( index, translationTableChunkArrayBase, translationTableChunkArrayScale );
    }
}
