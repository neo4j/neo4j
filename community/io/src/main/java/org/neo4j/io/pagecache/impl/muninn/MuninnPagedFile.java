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

import java.io.File;
import java.io.Flushable;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.neo4j.concurrent.BinaryLatch;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.Page;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageEvictionCallback;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.PagedReadableByteChannel;
import org.neo4j.io.pagecache.impl.PagedWritableByteChannel;
import org.neo4j.io.pagecache.tracing.FlushEvent;
import org.neo4j.io.pagecache.tracing.FlushEventOpportunity;
import org.neo4j.io.pagecache.tracing.MajorFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageFaultEvent;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

final class MuninnPagedFile implements PagedFile, Flushable
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
    final PageCacheTracer pageCacheTracer;

    // This is the table where we translate file-page-ids to cache-page-ids. Only one thread can perform a resize at
    // a time, and we ensure this mutual exclusion using the monitor lock on this MuninnPagedFile object.
    volatile Object[][] translationTable;

    final PageSwapper swapper;
    private final CursorPool cursorPool;

    // Guarded by the monitor lock on MuninnPageCache (map and unmap)
    private boolean deleteOnClose;

    // Used to trace the causes of any exceptions from getLastPageId.
    private volatile Exception closeStackTrace;

    /**
     * The header state includes both the reference count of the PagedFile – 15 bits – and the ID of the last page in
     * the file – 48 bits, plus an empty file marker bit. Because our pages are usually 2^13 bytes, this means that we
     * only loose 3 bits to the reference count, in terms of keeping large files byte addressable.
     *
     * The layout looks like this:
     *
     * ┏━ Empty file marker bit. When 1, the file is empty.
     * ┃    ┏━ Reference count, 15 bits.
     * ┃    ┃                ┏━ 48 bits for the last page id.
     * ┃┏━━━┻━━━━━━━━━━┓ ┏━━━┻━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
     * MRRRRRRR RRRRRRRR IIIIIIII IIIIIIII IIIIIIII IIIIIIII IIIIIIII IIIIIIII
     * 1        2        3        4        5        6        7        8        byte
     */
    @SuppressWarnings( "unused" ) // Accessed via Unsafe
    private volatile long headerState;

    /**
     * Create muninn page file
     * @param file original file
     * @param pageCache page cache
     * @param filePageSize file page size
     * @param swapperFactory page cache swapper factory
     * @param pageCacheTracer global page cache tracer
     * @param pageCursorTracerSupplier supplier of thread local (transaction local) page cursor tracer that will provide
     * thread local page cache statistics
     * @param createIfNotExists should create file if it does not exists
     * @param truncateExisting should truncate file if it exists
     * @throws IOException
     */
    MuninnPagedFile(
            File file,
            MuninnPageCache pageCache,
            int filePageSize,
            PageSwapperFactory swapperFactory,
            PageCacheTracer pageCacheTracer,
            PageCursorTracerSupplier pageCursorTracerSupplier,
            boolean createIfNotExists,
            boolean truncateExisting ) throws IOException
    {
        this.pageCache = pageCache;
        this.filePageSize = filePageSize;
        this.cursorPool = new CursorPool( this, pageCursorTracerSupplier, pageCacheTracer );
        this.pageCacheTracer = pageCacheTracer;

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
        return getClass().getSimpleName() + "[" + swapper.file() + "]";
    }

    @Override
    public PageCursor io( long pageId, int pf_flags )
    {
        int lockMask = PF_SHARED_WRITE_LOCK | PF_SHARED_READ_LOCK;
        if ( (pf_flags & lockMask) == 0 )
        {
            throw new IllegalArgumentException(
                    "Must specify either PF_SHARED_WRITE_LOCK or PF_SHARED_READ_LOCK" );
        }
        if ( (pf_flags & lockMask) == lockMask )
        {
            throw new IllegalArgumentException(
                    "Cannot specify both PF_SHARED_WRITE_LOCK and PF_SHARED_READ_LOCK" );
        }
        MuninnPageCursor cursor;
        if ( (pf_flags & PF_SHARED_READ_LOCK) == 0 )
        {
            cursor = cursorPool.takeWriteCursor( pageId, pf_flags );
        }
        else
        {
            cursor = cursorPool.takeReadCursor( pageId, pf_flags );
        }

        cursor.rewind();
        return cursor;
    }

    private static final int MAX_PAGES = 512;

    private final Object[][] chunkArrays = new Object[MAX_PAGES][];
    private final long[] chunkOffsets = new long[MAX_PAGES];
    private final long[] pageIds = new long[MAX_PAGES];
    private final MuninnPage[] pages = new MuninnPage[MAX_PAGES];

    @Override
    public int prefetch( long startPageId ) throws IOException
    {
        long maxPageId = Math.min( startPageId + MAX_PAGES - 1, getLastPageId() );
        int maxChunkId = MuninnPagedFile.computeChunkId( maxPageId );

        int maxCount = (int) (maxPageId - startPageId + 1);
        if ( maxCount < 1 )
        {
            return 0;
        }

        if ( translationTable.length <= maxChunkId )
        {
            expandCapacity( maxChunkId );
        }

        int swapCount = 0;
        int attemptCount = 0;

        try
        {
            for ( long pageId = startPageId; pageId <= maxPageId; pageId++ )
            {
                int chunkId = computeChunkId( pageId );
                long chunkOffset = computeChunkOffset( pageId );
                Object[] chunk = translationTable[chunkId];
                BinaryLatch latch = new BinaryLatch();
                if ( UnsafeUtil.compareAndSwapObject( chunk, chunkOffset, null, latch ) )
                {
                    chunkOffsets[swapCount] = chunkOffset;
                    chunkArrays[swapCount] = chunk;
                    pageIds[swapCount] = pageId;
                    swapCount++;
                    attemptCount++;
                }
                else if ( swapCount == 0 )
                {
                    // skip this page
                    startPageId = pageId + 1;
                    attemptCount++;
                }
                else
                {
                    // we have to break since there would be a gap otherwise, which the swapper doesn't support
                    break;
                }
            }
        }
        catch ( Throwable t )
        {
            for ( int i = 0; i < swapCount; i++ )
            {
                Object[] chunk = chunkArrays[i];
                long chunkOffset = chunkOffsets[i];
                BinaryLatch latch = (BinaryLatch) UnsafeUtil.getObjectVolatile( chunk, chunkOffset );
                UnsafeUtil.putObjectVolatile( chunk, chunkOffset, null );
                latch.release();
            }
            throw t; // todo: how to error handle?
        }

        if ( swapCount == 0 )
        {
            return attemptCount;
        }

        try
        {
            for ( int i = 0; i < swapCount; i++ )
            {
                pages[i] = grabFreeAndExclusivelyLockedPage( PageFaultEvent.NULL ); // todo: proper tracing event
            }

            assertPagedFileStillMapped();

            for ( int i = 0; i < swapCount; i++ )
            {
                pages[i].initBuffer();
                pages[i].markAsLoaded( pageIds[i] );
            }

            swapper.read( startPageId, pages, 0, swapCount );

            for ( int i = 0; i < swapCount; i++ )
            {
                pages[i].markAsBound( swapper );
            }
        }
        catch ( Throwable t )
        {
            for ( int i = 0; i < swapCount; i++ )
            {
                // todo: partial way through error handling
                pages[i].unlockExclusive();
                Object[] chunk = chunkArrays[i];
                long chunkOffset = chunkOffsets[i];
                BinaryLatch latch = (BinaryLatch) UnsafeUtil.getObjectVolatile( chunk, chunkOffset );
                UnsafeUtil.putObjectVolatile( chunk, chunkOffset, null );
                latch.release();
            }
            throw t; // todo: error handle
        }

        for ( int i = 0; i < swapCount; i++ )
        {
            Object[] chunk = chunkArrays[i];
            long chunkOffset = chunkOffsets[i];
            BinaryLatch latch = (BinaryLatch) UnsafeUtil.getObjectVolatile( chunk, chunkOffset );
            UnsafeUtil.putObjectVolatile( chunk, chunkOffset, pages[i] );
            pages[i].unlockExclusive();
            latch.release();
        }

        return attemptCount;
    }

    private void assertPagedFileStillMapped()
    {
        getLastPageId();
    }

    @Override
    public int pageSize()
    {
        return filePageSize;
    }

    @Override
    public long fileSize()
    {
        final long lastPageId = getLastPageId();
        if ( lastPageId < 0 )
        {
            return 0L;
        }
        return (lastPageId + 1) * pageSize();
    }

    File file()
    {
        return swapper.file();
    }

    public void close() throws IOException
    {
        pageCache.unmap( this );
    }

    @Override
    public ReadableByteChannel openReadableByteChannel() throws IOException
    {
        return new PagedReadableByteChannel( this );
    }

    @Override
    public WritableByteChannel openWritableByteChannel() throws IOException
    {
        return new PagedWritableByteChannel( this );
    }

    void closeSwapper() throws IOException
    {
        // We don't set closeStackTrace in close(), because the reference count may keep the file open.
        // But if we get here, to close the swapper, then we are definitely unmapping!
        closeStackTrace = new Exception( "tracing paged file closing" );

        if ( !deleteOnClose )
        {
            swapper.close();
        }
        else
        {
            swapper.closeAndDelete();
        }
    }

    @Override
    public void flushAndForce() throws IOException
    {
        flushAndForce( IOLimiter.unlimited() );
    }

    @Override
    public void flushAndForce( IOLimiter limiter ) throws IOException
    {
        if ( limiter == null )
        {
            throw new IllegalArgumentException( "IOPSLimiter cannot be null" );
        }
        try ( MajorFlushEvent flushEvent = pageCacheTracer.beginFileFlush( swapper ) )
        {
            flushAndForceInternal( flushEvent.flushEventOpportunity(), false, limiter );
            syncDevice();
        }
        pageCache.clearEvictorException();
    }

    void flushAndForceForClose() throws IOException
    {
        if ( deleteOnClose )
        {
            // No need to spend time flushing data to a file we're going to delete anyway.
            // However, we still have to mark the dirtied pages as clean since evicting would otherwise try to flush
            // these pages, and would fail because the file is closed, and we cannot allow that to happen.
            markAllDirtyPagesAsClean();
            return;
        }
        try ( MajorFlushEvent flushEvent = pageCacheTracer.beginFileFlush( swapper ) )
        {
            flushAndForceInternal( flushEvent.flushEventOpportunity(), true, IOLimiter.unlimited() );
            syncDevice();
        }
        pageCache.clearEvictorException();
    }

    private void markAllDirtyPagesAsClean()
    {
        long filePageId = -1; // Start at -1 because we increment at the *start* of the chunk-loop iteration.
        Object[][] tt = this.translationTable;
        for ( Object[] chunk : tt )
        {
            chunkLoop:
            for ( int i = 0; i < chunk.length; i++ )
            {
                filePageId++;
                long offset = computeChunkOffset( filePageId );

                // We might race with eviction, but we also mustn't miss a dirty page, so we loop until we succeed
                // in getting a lock on all available pages.
                for (;;)
                {
                    Object element = UnsafeUtil.getObjectVolatile( chunk, offset );
                    if ( element instanceof MuninnPage )
                    {
                        MuninnPage page = (MuninnPage) element;
                        long stamp = page.tryOptimisticReadLock();
                        if ( (!page.isDirty()) && page.validateReadLock( stamp ) )
                        {
                            // We got a valid read, and the page isn't dirty, so we skip it.
                            continue chunkLoop;
                        }

                        if ( !page.tryExclusiveLock() )
                        {
                            continue;
                        }
                        if ( page.isBoundTo( swapper, filePageId ) && page.isDirty() )
                        {
                            // The page is still bound to the expected file and file page id after we locked it,
                            // so we didn't race with eviction and faulting, and the page is dirty.
                            page.markAsClean();
                            page.unlockExclusive();
                            continue chunkLoop;
                        }
                    }
                    // There was no page at this entry in the table. Continue to the next entry.
                    continue chunkLoop;
                }
            }
        }
    }

    void flushAndForceInternal( FlushEventOpportunity flushOpportunity, boolean forClosing, IOLimiter limiter )
            throws IOException
    {
        // TODO it'd be awesome if, on Linux, we'd call sync_file_range(2) instead of fsync
        MuninnPage[] pages = new MuninnPage[translationTableChunkSize];
        long filePageId = -1; // Start at -1 because we increment at the *start* of the chunk-loop iteration.
        long limiterStamp = IOLimiter.INITIAL_STAMP;
        Object[][] tt = this.translationTable;
        for ( Object[] chunk : tt )
        {
            // TODO Look into if we can tolerate flushing a few clean pages if it means we can use larger vectors.
            // TODO The clean pages in question must still be loaded, though. Otherwise we'll end up writing
            // TODO garbage to the file.
            int pagesGrabbed = 0;
            chunkLoop:
            for ( int i = 0; i < chunk.length; i++ )
            {
                filePageId++;
                long offset = computeChunkOffset( filePageId );

                // We might race with eviction, but we also mustn't miss a dirty page, so we loop until we succeed
                // in getting a lock on all available pages.
                for (;;)
                {
                    Object element = UnsafeUtil.getObjectVolatile( chunk, offset );
                    if ( element instanceof MuninnPage )
                    {
                        MuninnPage page = (MuninnPage) element;
                        long stamp = page.tryOptimisticReadLock();
                        if ( (!page.isDirty()) && page.validateReadLock( stamp ) )
                        {
                            break;
                        }

                        if ( !(forClosing ? page.tryExclusiveLock() : page.tryFlushLock()) )
                        {
                            continue;
                        }
                        if ( page.isBoundTo( swapper, filePageId ) && page.isDirty() )
                        {
                            // The page is still bound to the expected file and file page id after we locked it,
                            // so we didn't race with eviction and faulting, and the page is dirty.
                            // So we add it to our IO vector.
                            pages[pagesGrabbed] = page;
                            pagesGrabbed++;
                            continue chunkLoop;
                        }
                        else if ( forClosing )
                        {
                            page.unlockExclusive();
                        }
                        else
                        {
                            page.unlockFlush();
                        }
                    }
                    break;
                }
                if ( pagesGrabbed > 0 )
                {
                    vectoredFlush( pages, pagesGrabbed, flushOpportunity, forClosing );
                    limiterStamp = limiter.maybeLimitIO( limiterStamp, pagesGrabbed, this );
                    pagesGrabbed = 0;
                }
            }
            if ( pagesGrabbed > 0 )
            {
                vectoredFlush( pages, pagesGrabbed, flushOpportunity, forClosing );
                limiterStamp = limiter.maybeLimitIO( limiterStamp, pagesGrabbed, this );
            }
        }

        swapper.force();
    }

    private void vectoredFlush(
            MuninnPage[] pages, int pagesGrabbed, FlushEventOpportunity flushOpportunity, boolean forClosing )
            throws IOException
    {
        FlushEvent flush = null;
        try
        {
            // Write the pages vector
            MuninnPage firstPage = pages[0];
            long startFilePageId = firstPage.getFilePageId();

            // Mark the flushed pages as clean before our flush, so concurrent page writes can mark it as dirty and
            // we'll be able to write those changes out on the next flush.
            for ( int j = 0; j < pagesGrabbed; j++ )
            {
                // If the flush fails, we'll undo this
                pages[j].markAsClean();
            }

            flush = flushOpportunity.beginFlush( startFilePageId, firstPage.getCachePageId(), swapper );
            long bytesWritten = swapper.write( startFilePageId, pages, 0, pagesGrabbed );

            // Update the flush event
            flush.addBytesWritten( bytesWritten );
            flush.addPagesFlushed( pagesGrabbed );
            flush.done();

            // There are now 0 'grabbed' pages
        }
        catch ( IOException ioe )
        {
            // Undo marking the pages as clean
            for ( int j = 0; j < pagesGrabbed; j++ )
            {
                pages[j].markAsDirty();
            }
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
                if ( forClosing )
                {
                    pages[j].unlockExclusive();
                }
                else
                {
                    pages[j].unlockFlush();
                }
            }
        }
    }

    private void syncDevice() throws IOException
    {
        pageCache.syncDevice();
    }

    @Override
    public void flush() throws IOException
    {
        swapper.force();
    }

    @Override
    public long getLastPageId()
    {
        long state = getHeaderState();
        if ( refCountOf( state ) == 0 )
        {
            String msg = "File has been unmapped: " + file().getPath();
            IllegalStateException exception = new IllegalStateException( msg );
            Exception closedBy = closeStackTrace;
            if ( closedBy != null )
            {
                exception.addSuppressed( closedBy );
            }
            throw exception;
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

    void markDeleteOnClose( boolean deleteOnClose )
    {
        this.deleteOnClose |= deleteOnClose;
    }

    /**
     * Grab a free page for the purpose of page faulting. Possibly blocking if
     * none are immediately available.
     * @param faultEvent The trace event for the current page fault.
     */
    MuninnPage grabFreeAndExclusivelyLockedPage( PageFaultEvent faultEvent ) throws IOException
    {
        return pageCache.grabFreeAndExclusivelyLockedPage( faultEvent );
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
        assert element instanceof MuninnPage : "Expected to evict a MuninnPage but found " + element;
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
