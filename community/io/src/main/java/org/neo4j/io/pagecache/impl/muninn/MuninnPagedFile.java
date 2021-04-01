/*
 * Copyright (c) "Neo4j"
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

import java.io.Flushable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Path;

import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageEvictionCallback;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.buffer.IOBufferFactory;
import org.neo4j.io.pagecache.buffer.NativeIOBuffer;
import org.neo4j.io.pagecache.impl.FileIsNotMappedException;
import org.neo4j.io.pagecache.tracing.FlushEvent;
import org.neo4j.io.pagecache.tracing.MajorFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageFaultEvent;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;

import static java.util.Arrays.fill;
import static java.util.Objects.requireNonNull;
import static org.neo4j.util.FeatureToggles.flag;
import static org.neo4j.util.FeatureToggles.getInteger;

final class MuninnPagedFile extends PageList implements PagedFile, Flushable
{
    static final int UNMAPPED_TTE = -1;
    private static final boolean mergePagesOnFlush = flag( MuninnPagedFile.class, "mergePagesOnFlush", true );
    private static final int maxChunkGrowth = getInteger( MuninnPagedFile.class, "maxChunkGrowth", 16 ); // One chunk is 32 MiB, by default.
    private static final int translationTableChunkSizePower = getInteger( MuninnPagedFile.class, "translationTableChunkSizePower", 12 );
    private static final int translationTableChunkSize = 1 << translationTableChunkSizePower;
    private static final long translationTableChunkSizeMask = translationTableChunkSize - 1;

    private static final int headerStateRefCountShift = 48;
    private static final int headerStateRefCountMax = 0x7FFF;
    private static final long headerStateRefCountMask = 0x7FFF_0000_0000_0000L;
    private static final long headerStateLastPageIdMask = 0x8000_FFFF_FFFF_FFFFL;
    private static final int PF_LOCK_MASK = PF_SHARED_WRITE_LOCK | PF_SHARED_READ_LOCK;

    final MuninnPageCache pageCache;
    final int filePageSize;
    private final PageCacheTracer pageCacheTracer;
    private final IOBufferFactory bufferFactory;
    final LatchMap pageFaultLatches;

    // This is the table where we translate file-page-ids to cache-page-ids. Only one thread can perform a resize at
    // a time, and we ensure this mutual exclusion using the monitor lock on this MuninnPagedFile object.
    static final VarHandle TRANSLATION_TABLE_ARRAY;
    volatile int[][] translationTable;

    final PageSwapper swapper;
    final int swapperId;
    private final CursorFactory cursorFactory;
    final String databaseName;
    private final IOController ioController;

    private volatile boolean deleteOnClose;

    // Used to trace the causes of any exceptions from getLastPageId.
    private volatile Exception closeStackTrace;

    // max modifier transaction id among evicted pages for this file
    @SuppressWarnings( "unused" ) // accessed with VarHandle
    private volatile long highestEvictedTransactionId;
    private static final VarHandle HIGHEST_EVICTED_TRANSACTION_ID;

    /**
     * The header state includes both the reference count of the PagedFile – 15 bits – and the ID of the last page in
     * the file – 48 bits, plus an empty file marker bit. Because our pages are usually 2^13 bytes, this means that we
     * only lose 3 bits to the reference count, in terms of keeping large files byte addressable.
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
    @SuppressWarnings( "unused" ) // accessed with VarHandle
    private volatile long headerState;
    private static final VarHandle HEADER_STATE;

    static
    {
        try
        {
            MethodHandles.Lookup l = MethodHandles.lookup();
            HEADER_STATE = l.findVarHandle( MuninnPagedFile.class, "headerState", long.class );
            HIGHEST_EVICTED_TRANSACTION_ID = l.findVarHandle( MuninnPagedFile.class, "highestEvictedTransactionId", long.class );
            TRANSLATION_TABLE_ARRAY = MethodHandles.arrayElementVarHandle( int[].class );
        }
        catch ( ReflectiveOperationException e )
        {
            throw new ExceptionInInitializerError( e );
        }
    }

    /**
     * Create muninn page file
     * @param path original file
     * @param pageCache page cache
     * @param filePageSize file page size
     * @param swapperFactory page cache swapper factory
     * @param pageCacheTracer global page cache tracer
     * @param versionContextSupplier supplier of thread local (transaction local) version context that will provide
     * access to thread local version context
     * @param createIfNotExists should create file if it does not exists
     * @param truncateExisting should truncate file if it exists
     * @param databaseName an optional name of the database this file belongs to. This option associates the mapped file with a database.
     * This information is currently used only for monitoring purposes.
     * @param ioController io controller to report page file io operations
     * @throws IOException If the {@link PageSwapper} could not be created.
     */
    MuninnPagedFile( Path path, MuninnPageCache pageCache, int filePageSize, PageSwapperFactory swapperFactory, PageCacheTracer pageCacheTracer,
            VersionContextSupplier versionContextSupplier, boolean createIfNotExists, boolean truncateExisting, boolean useDirectIo, String databaseName,
            int faultLockStriping, IOController ioController ) throws IOException
    {
        super( pageCache.pages );
        this.pageCache = pageCache;
        this.filePageSize = filePageSize;
        this.cursorFactory = new CursorFactory( this, versionContextSupplier );
        this.pageCacheTracer = pageCacheTracer;
        this.pageFaultLatches = new LatchMap( faultLockStriping );
        this.bufferFactory = pageCache.getBufferFactory();
        this.databaseName = requireNonNull( databaseName );
        this.ioController = requireNonNull( ioController );

        // The translation table is an array of arrays of integers that are either UNMAPPED_TTE, or the id of a page in
        // the page list. The table only grows the outer array, and all the inner "chunks" all stay the same size. This
        // means that pages can be addressed with simple bit-wise operations on the filePageId. Eviction sets slots
        // to UNMAPPED_TTE with volatile writes. Page faults guard their target entries via the LatchMap, and overwrites
        // the UNMAPPED_TTE value with the new page id, with a volatile write, and then finally releases their latch
        // from the LatchMap. The LatchMap will ensure that only a single thread will fault a page at a time. However,
        // after a latch has been taken, the thread must double-check the entry to make sure that it did not race with
        // another thread to fault in the page – this is called double-check locking. Look-ups use volatile reads of the
        // slots. If a look-up finds UNMAPPED_TTE, it will attempt to page fault. If the LatchMap returns null, then
        // someone else might already be faulting in that page. The LatchMap will wait for the existing latch to be
        // released, before returning null. Thus the thread can retry the lookup immediately. If a look-up finds that it
        // is out of bounds of the translation table, it resizes the table by first taking the resize lock, then
        // verifying that the given filePageId is still out of bounds, then creates a new and larger outer array, then
        // copies over the existing inner arrays, fills the remaining outer array slots with more inner arrays, in turn
        // filled with UNMAPPED_TTE values, and then finally assigns the new outer array to the translationTable field
        // and releases the resize lock.
        PageEvictionCallback onEviction = this::evictPage;
        swapper = swapperFactory.createPageSwapper( path, filePageSize, onEviction, createIfNotExists, useDirectIo, ioController, getSwappers() );
        if ( truncateExisting )
        {
            swapper.truncate();
        }
        long lastPageId = swapper.getLastPageId();

        int initialChunks = Math.max( 1 + computeChunkId( lastPageId ), 1 ); // At least one initial chunk. Always enough for the whole file.
        int[][] tt = new int[initialChunks][];
        for ( int i = 0; i < initialChunks; i++ )
        {
            tt[i] = newChunk();
        }
        translationTable = tt;

        initialiseLastPageId( lastPageId );
        this.swapperId = swapper.swapperId();
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + swapper.path() + ", reference count = " + getRefCount() + "]";
    }

    @Override
    public PageCursor io( long pageId, int pf_flags, PageCursorTracer tracer )
    {
        int lockFlags = pf_flags & PF_LOCK_MASK;
        MuninnPageCursor cursor;
        if ( lockFlags == PF_SHARED_READ_LOCK )
        {
            cursor = cursorFactory.takeReadCursor( pageId, pf_flags, tracer );
        }
        else if ( lockFlags == PF_SHARED_WRITE_LOCK )
        {
            cursor = cursorFactory.takeWriteCursor( pageId, pf_flags, tracer );
        }
        else
        {
            throw wrongLocksArgument( lockFlags );
        }

        cursor.rewind();
        if ( ( pf_flags & PF_READ_AHEAD ) == PF_READ_AHEAD && ( pf_flags & PF_NO_FAULT ) != PF_NO_FAULT )
        {
            pageCache.startPreFetching( cursor, cursorFactory );
        }
        return cursor;
    }

    private static IllegalArgumentException wrongLocksArgument( int lockFlags )
    {
        if ( lockFlags == 0 )
        {
            return new IllegalArgumentException(
                    "Must specify either PF_SHARED_WRITE_LOCK or PF_SHARED_READ_LOCK" );
        }
        else
        {
            return new IllegalArgumentException(
                    "Cannot specify both PF_SHARED_WRITE_LOCK and PF_SHARED_READ_LOCK" );
        }
    }

    @Override
    public int pageSize()
    {
        return filePageSize;
    }

    @Override
    public long fileSize() throws FileIsNotMappedException
    {
        final long lastPageId = getLastPageId();
        if ( lastPageId < 0 )
        {
            return 0L;
        }
        return (lastPageId + 1) * pageSize();
    }

    @Override
    public Path path()
    {
        return swapper.path();
    }

    @Override
    public void close()
    {
        pageCache.unmap( this );
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
        if ( getSwappers().free( swapperId ) )
        {
            // We need to do a vacuum of the cache, fully evicting all pages that have freed swapper ids.
            // We cannot reuse those swapper ids until there are no more pages using them.
            pageCache.vacuum( getSwappers() );
        }
        long filePageId = -1; // Start at -1 because we increment at the *start* of the chunk-loop iteration.
        int[][] tt = this.translationTable;
        for ( int[] chunk : tt )
        {
            for ( int i = 0; i < chunk.length; i++ )
            {
                filePageId++;
                TRANSLATION_TABLE_ARRAY.setVolatile( chunk, computeChunkIndex( filePageId ), UNMAPPED_TTE );
            }
        }
    }

    @Override
    public void flushAndForce() throws IOException
    {
        try ( MajorFlushEvent flushEvent = pageCacheTracer.beginFileFlush( swapper );
              var buffer = bufferFactory.createBuffer() )
        {
            flushAndForceInternal( flushEvent, false, ioController, buffer );
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
        try ( MajorFlushEvent flushEvent = pageCacheTracer.beginFileFlush( swapper );
              var buffer = bufferFactory.createBuffer() )
        {
            flushAndForceInternal( flushEvent, true, IOController.DISABLED, buffer );
        }
        pageCache.clearEvictorException();
    }

    private void markAllDirtyPagesAsClean()
    {
        long filePageId = -1; // Start at -1 because we increment at the *start* of the chunk-loop iteration.
        int[][] tt = this.translationTable;
        for ( int[] chunk : tt )
        {
            chunkLoop:
            for ( int i = 0; i < chunk.length; i++ )
            {
                filePageId++;
                int chunkIndex = computeChunkIndex( filePageId );

                // We might race with eviction, but we also mustn't miss a dirty page, so we loop until we succeed
                // in getting a lock on all available pages.
                for (;;)
                {
                    int pageId = (int) TRANSLATION_TABLE_ARRAY.getVolatile( chunk, chunkIndex );
                    if ( pageId != UNMAPPED_TTE )
                    {
                        long pageRef = deref( pageId );
                        long stamp = tryOptimisticReadLock( pageRef );
                        if ( (!isModified( pageRef )) && validateReadLock( pageRef, stamp ) )
                        {
                            // We got a valid read, and the page isn't dirty, so we skip it.
                            continue chunkLoop;
                        }

                        if ( !tryExclusiveLock( pageRef ) )
                        {
                            continue;
                        }
                        if ( isBoundTo( pageRef, swapperId, filePageId ) && isModified( pageRef ) )
                        {
                            // The page is still bound to the expected file and file page id after we locked it,
                            // so we didn't race with eviction and faulting, and the page is dirty.
                            explicitlyMarkPageUnmodifiedUnderExclusiveLock( pageRef );
                            unlockExclusive( pageRef );
                            continue chunkLoop;
                        }
                    }
                    // There was no page at this entry in the table. Continue to the next entry.
                    continue chunkLoop;
                }
            }
        }
    }

    void flushAndForceInternal( MajorFlushEvent flushEvent, boolean forClosing, IOController limiter, NativeIOBuffer ioBuffer )
            throws IOException
    {
        try
        {
            doFlushAndForceInternal( flushEvent, forClosing, limiter, ioBuffer );
        }
        catch ( ClosedChannelException e )
        {
            if ( getRefCount() > 0 )
            {
                // The file is not supposed to be closed, since we have a positive ref-count, yet we got a
                // ClosedChannelException anyway? It's an odd situation, so let's tell the outside world about
                // this failure.
                e.addSuppressed( closeStackTrace );
                throw e;
            }
            // Otherwise: The file was closed while we were trying to flush it. Since unmapping implies a flush
            // anyway, we can safely assume that this is not a problem. The file was flushed, and it doesn't
            // really matter how that happened. We'll ignore this exception.
        }
    }

    private void doFlushAndForceInternal( MajorFlushEvent flushes, boolean forClosing, IOController limiter, NativeIOBuffer ioBuffer )
            throws IOException
    {
        // TODO it'd be awesome if, on Linux, we'd call sync_file_range(2) instead of fsync
        long[] pages = new long[translationTableChunkSize];
        long[] flushStamps = forClosing ? null : new long[translationTableChunkSize];
        long[] bufferAddresses = new long[translationTableChunkSize];
        int[] bufferLengths = new int[translationTableChunkSize];
        long filePageId = -1; // Start at -1 because we increment at the *start* of the chunk-loop iteration.
        int[][] tt = this.translationTable;
        boolean useTemporaryBuffer = ioBuffer.isEnabled();

        flushes.startFlush( tt );

        for ( int[] chunk : tt )
        {
            var chunkEvent = flushes.startChunk( chunk );
            long notModifiedPages = 0;
            long flushPerChunk = 0;
            long buffersPerChunk = 0;
            long mergesPerChunk = 0;
            // TODO Look into if we can tolerate flushing a few clean pages if it means we can use larger vectors.
            // TODO The clean pages in question must still be loaded, though. Otherwise we'll end up writing
            // TODO garbage to the file.
            int pagesGrabbed = 0;
            long nextSequentialAddress = -1;
            int numberOfBuffers = 0;
            int lastBufferIndex = -1;
            int mergedPages = 0;

            boolean fillingDirtyBuffer = false;
            if ( useTemporaryBuffer )
            {
                // in case when we use temp intermediate buffer we have only buffer and its address and length are always stored in arrays with index 0
                bufferAddresses[0] = ioBuffer.getAddress();
                bufferLengths[0] = 0;
                buffersPerChunk = 1;
            }

            chunkLoop:
            for ( int i = 0; i < chunk.length; i++ )
            {
                filePageId++;
                int chunkIndex = computeChunkIndex( filePageId );

                // We might race with eviction, but we also mustn't miss a dirty page, so we loop until we succeed
                // in getting a lock on all available pages.
                for ( ; ; )
                {
                    int pageId = (int) TRANSLATION_TABLE_ARRAY.getVolatile( chunk, chunkIndex );
                    if ( pageId != UNMAPPED_TTE )
                    {
                        long pageRef = deref( pageId );
                        long stamp = tryOptimisticReadLock( pageRef );
                        if ( (!isModified( pageRef ) && !fillingDirtyBuffer) && validateReadLock( pageRef, stamp ) )
                        {
                            notModifiedPages++;
                            break; // not modified, continue with the chunk
                        }

                        long flushStamp = 0;
                        if ( !(forClosing ? tryExclusiveLock( pageRef ) : ((flushStamp = tryFlushLock( pageRef )) != 0)) )
                        {
                            continue; // retry lock
                        }
                        if ( isBoundTo( pageRef, swapperId, filePageId ) && (isModified( pageRef ) || fillingDirtyBuffer) )
                        {
                            // we should try to merge pages into buffer even if they are not modified only when we using intermediate temporary buffer
                            fillingDirtyBuffer = useTemporaryBuffer;
                            // The page is still bound to the expected file and file page id after we locked it,
                            // so we didn't race with eviction and faulting, and the page is dirty.
                            // So we add it to our IO vector.
                            pages[pagesGrabbed] = pageRef;
                            if ( !forClosing )
                            {
                                flushStamps[pagesGrabbed] = flushStamp;
                            }
                            pagesGrabbed++;
                            long address = getAddress( pageRef );
                            if ( useTemporaryBuffer )
                            {
                                // in case we use temp buffer to combine pages address and buffer lengths are located in corresponding arrays and have
                                // index 0.
                                // Reset of accumulated effective length of temp buffer happens after intermediate vectored flush if any
                                UnsafeUtil.copyMemory( address, bufferAddresses[0] + bufferLengths[0], filePageSize );
                                bufferLengths[0] += filePageSize;
                                numberOfBuffers = 1;
                                if ( !ioBuffer.hasMoreCapacity( bufferLengths[0], filePageSize ) )
                                {
                                    break; // continue to flush
                                }
                                else
                                {
                                    continue chunkLoop; // go to next page
                                }
                            }
                            else
                            {
                                if ( mergePagesOnFlush && nextSequentialAddress == address )
                                {
                                    // do not add new address, only bump length of previous buffer
                                    bufferLengths[lastBufferIndex] += filePageSize;
                                    mergedPages++;
                                    mergesPerChunk++;
                                }
                                else
                                {
                                    // add new address
                                    bufferAddresses[numberOfBuffers] = address;
                                    lastBufferIndex = numberOfBuffers;
                                    bufferLengths[numberOfBuffers] = filePageSize;
                                    numberOfBuffers++;
                                    buffersPerChunk++;
                                }
                                nextSequentialAddress = address + filePageSize;
                                continue chunkLoop; // go to next page
                            }
                        }
                        else
                        {
                            if ( forClosing )
                            {
                                unlockExclusive( pageRef );
                            }
                            else
                            {
                                unlockFlush( pageRef, flushStamp, false );
                            }
                            if ( useTemporaryBuffer && pagesGrabbed > 0 )
                            {
                                // flush previous grabbed region
                                break;
                            }
                        }
                    }
                    break;
                }
                if ( pagesGrabbed > 0 )
                {
                    vectoredFlush( pages, bufferAddresses, flushStamps, bufferLengths, numberOfBuffers, pagesGrabbed, mergedPages, flushes, forClosing );
                    limiter.maybeLimitIO( numberOfBuffers, this, flushes );
                    pagesGrabbed = 0;
                    nextSequentialAddress = -1;
                    numberOfBuffers = 0;
                    lastBufferIndex = -1;
                    mergedPages = 0;
                    fillingDirtyBuffer = false;
                    flushPerChunk++;
                    bufferLengths[0] = 0;
                }
            }
            if ( pagesGrabbed > 0 )
            {
                vectoredFlush( pages, bufferAddresses, flushStamps, bufferLengths, numberOfBuffers, pagesGrabbed, mergedPages, flushes, forClosing );
                limiter.maybeLimitIO( numberOfBuffers, this, flushes );
                flushPerChunk++;
            }
            chunkEvent.chunkFlushed( notModifiedPages, flushPerChunk, buffersPerChunk, mergesPerChunk );
        }

        swapper.force();
    }

    private void vectoredFlush(
            long[] pages, long[] bufferAddresses, long[] flushStamps, int[] bufferLengths, int numberOfBuffers, int pagesToFlush, int pagesMerged,
            MajorFlushEvent flushEvent, boolean forClosing ) throws IOException
    {
        FlushEvent flush = null;
        boolean successful = false;
        try
        {
            // Write the pages vector
            long firstPageRef = pages[0];
            long startFilePageId = getFilePageId( firstPageRef );
            flush = flushEvent.beginFlush( pages, swapper, this, pagesToFlush, pagesMerged );
            long bytesWritten = swapper.write( startFilePageId, bufferAddresses, bufferLengths, numberOfBuffers, pagesToFlush );

            // Update the flush event
            flush.addBytesWritten( bytesWritten );
            flush.addPagesFlushed( pagesToFlush );
            flush.addPagesMerged( pagesMerged );
            flush.done();
            successful = true;

            // There are now 0 'grabbed' pages
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
            if ( forClosing )
            {
                for ( int i = 0; i < pagesToFlush; i++ )
                {
                    long pageRef = pages[i];
                    if ( successful )
                    {
                        explicitlyMarkPageUnmodifiedUnderExclusiveLock( pageRef );
                    }
                    unlockExclusive( pageRef );
                }
            }
            else
            {
                for ( int i = 0; i < pagesToFlush; i++ )
                {
                    unlockFlush( pages[i], flushStamps[i], successful );
                }
            }
        }
    }

    boolean flushLockedPage( long pageRef, long filePageId )
    {
        boolean success = false;
        try ( MajorFlushEvent flushEvent = pageCacheTracer.beginFileFlush( swapper ) )
        {
            FlushEvent flush = flushEvent.beginFlush( pageRef, swapper, this );
            long address = getAddress( pageRef );
            try
            {
                long bytesWritten = swapper.write( filePageId, address );
                flush.addBytesWritten( bytesWritten );
                flush.addPagesFlushed( 1 );
                flush.done();
                success = true;
            }
            catch ( IOException e )
            {
                flush.done( e );
            }
        }
        return success;
    }

    @Override
    public void flush() throws IOException
    {
        swapper.force();
    }

    @Override
    public long getLastPageId() throws FileIsNotMappedException
    {
        long state = getHeaderState();
        if ( refCountOf( state ) == 0 )
        {
            throw fileIsNotMappedException();
        }
        return state & headerStateLastPageIdMask;
    }

    private FileIsNotMappedException fileIsNotMappedException()
    {
        FileIsNotMappedException exception = new FileIsNotMappedException( path() );
        Exception closedBy = closeStackTrace;
        if ( closedBy != null )
        {
            exception.addSuppressed( closedBy );
        }
        return exception;
    }

    private long getHeaderState()
    {
        return (long) HEADER_STATE.getVolatile( this );
    }

    private static long refCountOf( long state )
    {
        return (state & headerStateRefCountMask) >>> headerStateRefCountShift;
    }

    private void initialiseLastPageId( long lastPageIdFromFile )
    {
        if ( lastPageIdFromFile < 0 )
        {
            // MIN_VALUE only has the sign bit raised, and the rest of the bits are zeros.
            HEADER_STATE.setVolatile( this, Long.MIN_VALUE );
        }
        else
        {
            HEADER_STATE.setVolatile( this, lastPageIdFromFile );
        }
    }

    /**
     * Make sure that the lastPageId is at least the given pageId
     */
    void increaseLastPageIdTo( long newLastPageId )
    {
        long current;
        long update;
        long lastPageId;
        do
        {
            current = getHeaderState();
            update = newLastPageId + (current & headerStateRefCountMask);
            lastPageId = current & headerStateLastPageIdMask;
        }
        while ( lastPageId < newLastPageId && !HEADER_STATE.weakCompareAndSet( this, current, update ) );
    }

    /**
     * Atomically increment the reference count for this mapped file.
     */
    void incrementRefCount()
    {
        long current;
        long update;
        do
        {
            current = getHeaderState();
            long count = refCountOf( current ) + 1;
            if ( count > headerStateRefCountMax )
            {
                throw new IllegalStateException( "Cannot map file because reference counter would overflow. " +
                                                 "Maximum reference count is " + headerStateRefCountMax + ". " +
                                                 "File is " + swapper.path().toAbsolutePath() );
            }
            update = (current & headerStateLastPageIdMask) + (count << headerStateRefCountShift);
        }
        while ( !HEADER_STATE.weakCompareAndSet( this, current, update ) );
    }

    /**
     * Atomically decrement the reference count. Returns true if this was the
     * last reference.
     */
    boolean decrementRefCount()
    {
        long current;
        long update;
        long count;
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
        while ( !HEADER_STATE.weakCompareAndSet( this, current, update ) );
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

    @Override
    public void setDeleteOnClose( boolean deleteOnClose )
    {
        this.deleteOnClose = deleteOnClose;
    }

    @Override
    public boolean isDeleteOnClose()
    {
        return deleteOnClose;
    }

    @Override
    public String getDatabaseName()
    {
        return databaseName;
    }

    /**
     * Grab a free page for the purpose of page faulting. Possibly blocking if
     * none are immediately available.
     * @param faultEvent The trace event for the current page fault.
     */
    long grabFreeAndExclusivelyLockedPage( PageFaultEvent faultEvent ) throws IOException
    {
        return pageCache.grabFreeAndExclusivelyLockedPage( faultEvent );
    }

    /**
     * Remove the mapping of the given filePageId from the translation table, and return the evicted page object.
     * @param filePageId The id of the file page to evict.
     */
    private void evictPage( long filePageId )
    {
        int chunkId = computeChunkId( filePageId );
        int chunkIndex = computeChunkIndex( filePageId );
        int[] chunk = translationTable[chunkId];

        int mappedPageId = (int) TRANSLATION_TABLE_ARRAY.getVolatile( chunk, chunkIndex );
        long pageRef = deref( mappedPageId );
        setHighestEvictedTransactionId( getAndResetLastModifiedTransactionId( pageRef ) );
        TRANSLATION_TABLE_ARRAY.setVolatile( chunk, chunkIndex, UNMAPPED_TTE );
    }

    private void setHighestEvictedTransactionId( long modifiedTransactionId )
    {
        long current;
        do
        {
            current = (long) HIGHEST_EVICTED_TRANSACTION_ID.getVolatile( this );
            if ( current >= modifiedTransactionId )
            {
                return;
            }
        } while ( !HIGHEST_EVICTED_TRANSACTION_ID.weakCompareAndSet( this, current, modifiedTransactionId ) );
    }

    long getHighestEvictedTransactionId()
    {
        return (long) HIGHEST_EVICTED_TRANSACTION_ID.getVolatile( this );
    }

    /**
     * Expand the translation table such that it can include at least the given chunkId.
     * @param maxChunkId The new translation table must be big enough to include at least this chunkId.
     * @return A reference to the expanded transaction table.
     */
    synchronized int[][] expandCapacity( int maxChunkId ) throws IOException
    {
        int[][] tt = translationTable;
        if ( tt.length <= maxChunkId )
        {
            int newLength = computeNewRootTableLength( maxChunkId );
            int[][] ntt = new int[newLength][];
            System.arraycopy( tt, 0, ntt, 0, tt.length );
            for ( int i = tt.length; i < ntt.length; i++ )
            {
                ntt[i] = newChunk();
            }
            tt = ntt;
            if ( swapper.canAllocate() )
            {
                // Hint to the file system that we've grown our file.
                // This should reduce our tendency to fragment files.
                long newFileSize = tt.length; // New number of chunks.
                newFileSize *= translationTableChunkSize; // Pages per chunk.
                newFileSize *= filePageSize; // Bytes per page.
                swapper.allocate( newFileSize );
            }
        }
        // It is important to publish the extended table only
        // after the new region of the file has been allocated.
        // Allocating a new region of a file and using
        // the region is not thread safe on all file systems,
        // so if the new extended table is published before
        // the allocation has finished, other threads might start
        // using pages in the region, which might not be safe.
        translationTable = tt;
        return tt;
    }

    private static int[] newChunk()
    {
        int[] chunk = new int[translationTableChunkSize];
        fill( chunk, UNMAPPED_TTE );
        return chunk;
    }

    private int computeNewRootTableLength( int maxChunkId )
    {
        // Grow by approximate 10% but always by at least one full chunk, and no more than maxChunkGrowth (16 by default, equivalent to 512 MiB).
        int next = 1 + (int) (maxChunkId * 1.1);
        return Math.min( next, maxChunkId + maxChunkGrowth );
    }

    static int computeChunkId( long filePageId )
    {
        return (int) (filePageId >>> translationTableChunkSizePower);
    }

    static int computeChunkIndex( long filePageId )
    {
        return (int) (filePageId & translationTableChunkSizeMask);
    }
}
