/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.io.pagecache.impl.muninn;

import static java.util.Arrays.fill;
import static java.util.Objects.requireNonNull;
import static org.neo4j.util.FeatureToggles.flag;
import static org.neo4j.util.FeatureToggles.getInteger;
import static org.neo4j.util.FeatureToggles.getLong;

import java.io.Flushable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageEvictionCallback;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.buffer.IOBufferFactory;
import org.neo4j.io.pagecache.buffer.NativeIOBuffer;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.impl.FileIsNotMappedException;
import org.neo4j.io.pagecache.monitoring.PageFileCounters;
import org.neo4j.io.pagecache.tracing.EvictionRunEvent;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageFaultEvent;
import org.neo4j.io.pagecache.tracing.VectoredPageFaultEvent;
import org.neo4j.io.pagecache.tracing.version.FileTruncateEvent;
import org.neo4j.time.Stopwatch;

final class MuninnPagedFile extends PageList implements PagedFile, Flushable {
    static final int UNMAPPED_TTE = -1;
    private static final long TIME_LIMIT_ON_FILE_UNMAP_SECONDS =
            getLong(MuninnPagedFile.class, "TIME_LIMIT_ON_FILE_UNMAP_SECONDS", 0L);
    private static final boolean TRACE_FILE_CLOSE = flag(MuninnPagedFile.class, "TRACE_FILE_CLOSE", true);

    @SuppressWarnings("ThrowableInstanceNeverThrown")
    private static final Exception PLACEHOLDER_CLOSE_EXCEPTION = new Exception("Enable "
            + MuninnPagedFile.class.getName() + ".TRACE_FILE_CLOSE flag to enable tracing paged file closing");

    private static final boolean USE_VECTORIZED_TOUCH = flag(MuninnPagedFile.class, "USE_VECTORIZED_TOUCH", true);
    private static final boolean MERGE_PAGES_ON_FLUSH = flag(MuninnPagedFile.class, "mergePagesOnFlush", true);
    private static final int MAX_CHUNK_GROWTH =
            getInteger(MuninnPagedFile.class, "maxChunkGrowth", 16); // One chunk is 32 MiB, by default.
    private static final int TRANSLATION_TABLE_CHUNK_SIZE_POWER =
            getInteger(MuninnPagedFile.class, "translationTableChunkSizePower", 12);
    private static final int TRANSLATION_TABLE_CHUNK_SIZE = 1 << TRANSLATION_TABLE_CHUNK_SIZE_POWER;
    private static final long TRANSLATION_TABLE_CHUNK_SIZE_MASK = TRANSLATION_TABLE_CHUNK_SIZE - 1;

    private static final int HEADER_STATE_REF_COUNT_SHIFT = 48;
    private static final int HEADER_STATE_REF_COUNT_MAX = 0x7FFF;
    private static final long HEADER_STATE_REF_COUNT_MASK = 0x7FFF_0000_0000_0000L;
    private static final long HEADER_STATE_LAST_PAGE_ID_MASK = 0x8000_FFFF_FFFF_FFFFL;
    private static final long EMPTY_STATE_HEADER = 0x8000_0000_0000_0000L;
    private static final int PF_LOCK_MASK = PF_SHARED_WRITE_LOCK | PF_SHARED_READ_LOCK;

    final MuninnPageCache pageCache;
    final int filePageSize;
    final int fileReservedPageBytes;
    final VersionStorage versionStorage;
    final boolean multiVersioned;
    final boolean contextVersionUpdates;
    final boolean littleEndian;
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
    // If store files should be automatically pre-allocated,
    // this flag does not influence explicit preAllocate() operation.
    private final boolean preallocateFile;

    private volatile boolean deleteOnClose;

    // Used to trace the causes of any exceptions from getLastPageId.
    private volatile Exception closeStackTrace;

    // max modifier transaction id among evicted pages for this file
    @SuppressWarnings("unused") // accessed with VarHandle
    private volatile long highestEvictedTransactionId;

    private static final VarHandle HIGHEST_EVICTED_TRANSACTION_ID;

    /**
     * The header state includes both the reference count of the PagedFile – 15 bits – and the ID of the last page in
     * the file – 48 bits, plus an empty file marker bit. Because our pages are usually 2^13 bytes, this means that we
     * only lose 3 bits to the reference count, in terms of keeping large files byte addressable.
     * The layout looks like this:
     * ┏━ Empty file marker bit. When 1, the file is empty.
     * ┃    ┏━ Reference count, 15 bits.
     * ┃    ┃                ┏━ 48 bits for the last page id.
     * ┃┏━━━┻━━━━━━━━━━┓ ┏━━━┻━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
     * MRRRRRRR RRRRRRRR IIIIIIII IIIIIIII IIIIIIII IIIIIIII IIIIIIII IIIIIIII
     * 1        2        3        4        5        6        7        8        byte
     */
    @SuppressWarnings("unused") // accessed with VarHandle
    private volatile long headerState;

    private static final VarHandle HEADER_STATE;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            HEADER_STATE = l.findVarHandle(MuninnPagedFile.class, "headerState", long.class);
            HIGHEST_EVICTED_TRANSACTION_ID =
                    l.findVarHandle(MuninnPagedFile.class, "highestEvictedTransactionId", long.class);
            TRANSLATION_TABLE_ARRAY = MethodHandles.arrayElementVarHandle(int[].class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Create muninn page file
     * @param path original file
     * @param pageCache page cache
     * @param filePageSize file page size
     * @param swapperFactory page cache swapper factory
     * @param pageCacheTracer global page cache tracer
     * @param createIfNotExists should create file if it does not exists
     * @param truncateExisting should truncate file if it exists
     * @param useDirectIo use direct io for page file operations
     * @param preallocateFile try to preallocate store files when they grow on supported platforms
     * @param databaseName an optional name of the database this file belongs to. This option associates the mapped file with a database.
     * This information is currently used only for monitoring purposes.
     * @param ioController io controller to report page file io operations
     * @param multiVersioned if file is mutli versioned
     * @param versionStorage page file old versioned pages storage
     * @param littleEndian page file endianess
     * @throws IOException If the {@link PageSwapper} could not be created.
     */
    MuninnPagedFile(
            Path path,
            MuninnPageCache pageCache,
            int filePageSize,
            PageSwapperFactory swapperFactory,
            PageCacheTracer pageCacheTracer,
            boolean createIfNotExists,
            boolean truncateExisting,
            boolean useDirectIo,
            boolean preallocateFile,
            String databaseName,
            int faultLockStriping,
            IOController ioController,
            EvictionBouncer evictionBouncer,
            boolean multiVersioned,
            boolean contextVersionUpdates,
            int reservedBytes,
            VersionStorage versionStorage,
            boolean littleEndian)
            throws IOException {
        super(pageCache.pages);
        this.pageCache = pageCache;
        this.filePageSize = filePageSize;
        this.fileReservedPageBytes = reservedBytes;
        this.versionStorage = versionStorage;
        this.multiVersioned = multiVersioned;
        this.contextVersionUpdates = contextVersionUpdates;
        this.littleEndian = littleEndian;
        this.cursorFactory = new CursorFactory(this);
        this.pageCacheTracer = pageCacheTracer;
        this.pageFaultLatches = new LatchMap(faultLockStriping);
        this.bufferFactory = pageCache.getBufferFactory();
        this.databaseName = requireNonNull(databaseName);
        this.ioController = requireNonNull(ioController);
        this.preallocateFile = preallocateFile;

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
        swapper = swapperFactory.createPageSwapper(
                path,
                filePageSize,
                onEviction,
                createIfNotExists,
                useDirectIo,
                ioController,
                evictionBouncer,
                getSwappers());
        if (truncateExisting) {
            swapper.truncate();
        }
        long lastPageId = swapper.getLastPageId();

        int initialChunks = Math.max(
                1 + computeChunkId(lastPageId), 1); // At least one initial chunk. Always enough for the whole file.
        int[][] tt = new int[initialChunks][];
        for (int i = 0; i < initialChunks; i++) {
            tt[i] = newChunk();
        }
        translationTable = tt;

        initialiseLastPageId(lastPageId);
        this.swapperId = swapper.swapperId();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + swapper.path() + ", reference count = " + getRefCount() + "]";
    }

    @Override
    public PageCursor io(long pageId, int pf_flags, CursorContext context) {
        int lockFlags = pf_flags & PF_LOCK_MASK;
        MuninnPageCursor cursor;
        if (lockFlags == PF_SHARED_READ_LOCK) {
            cursor = cursorFactory.takeReadCursor(pageId, pf_flags, context);
        } else if (lockFlags == PF_SHARED_WRITE_LOCK) {
            cursor = cursorFactory.takeWriteCursor(pageId, pf_flags, context);
        } else {
            throw wrongLocksArgument(lockFlags);
        }

        if ((pf_flags & PF_READ_AHEAD) == PF_READ_AHEAD && (pf_flags & PF_NO_FAULT) != PF_NO_FAULT) {
            pageCache.startPreFetching(cursor, cursorFactory);
        }
        return cursor;
    }

    private static IllegalArgumentException wrongLocksArgument(int lockFlags) {
        if (lockFlags == 0) {
            return new IllegalArgumentException("Must specify either PF_SHARED_WRITE_LOCK or PF_SHARED_READ_LOCK");
        } else {
            return new IllegalArgumentException("Cannot specify both PF_SHARED_WRITE_LOCK and PF_SHARED_READ_LOCK");
        }
    }

    @Override
    public int pageSize() {
        return filePageSize;
    }

    @Override
    public int payloadSize() {
        return filePageSize - fileReservedPageBytes;
    }

    @Override
    public int pageReservedBytes() {
        return fileReservedPageBytes;
    }

    @Override
    public long fileSize() throws FileIsNotMappedException {
        final long lastPageId = getLastPageId();
        if (lastPageId < 0) {
            return 0L;
        }
        return (lastPageId + 1) * pageSize();
    }

    @Override
    public synchronized void truncate(long pagesToKeep, FileTruncateEvent truncateEvent) throws IOException {
        long lastPageId = getLastPageId();
        if (lastPageId < pagesToKeep) {
            return;
        }
        // header state update
        setLastPageIdTo(pagesToKeep - 1);
        // update translation table
        truncateCapacity(pagesToKeep);
        // truncate file
        swapper.truncate(pagesToKeep * filePageSize);
        truncateEvent.truncatedBytes(lastPageId, pagesToKeep, filePageSize);
    }

    @Override
    public Path path() {
        return swapper.path();
    }

    @Override
    public void close() {
        pageCache.unmap(this);
    }

    void closeSwapper() throws IOException {
        // We don't set closeStackTrace in close(), because the reference count may keep the file open.
        // But if we get here, to close the swapper, then we are definitely unmapping!
        if (TRACE_FILE_CLOSE) {
            closeStackTrace = new Exception("tracing paged file closing");
        } else {
            closeStackTrace = PLACEHOLDER_CLOSE_EXCEPTION;
        }

        evictPages();
        if (!deleteOnClose) {
            swapper.close();
        } else {
            swapper.closeAndDelete();
        }
        pageCache.sweep(getSwappers());
    }

    private void evictPages() throws IOException {
        long totalPages = 0;
        long evictedPages = 0;
        PageList pages = pageCache.pages;
        try (EvictionRunEvent evictionEvent = pageCacheTracer.beginEviction()) {
            long filePageId = -1; // Start at -1 because we increment at the *start* of the chunk-loop iteration.
            int[][] tt = this.translationTable;
            for (int[] chunk : tt) {
                for (int i = 0; i < chunk.length; i++) {
                    filePageId++;
                    int chunkIndex = computeChunkIndex(filePageId);

                    int pageId = translationTableGetVolatile(chunk, chunkIndex);
                    if (pageId != UNMAPPED_TTE) {
                        long pageRef = deref(pageId);
                        // try to evict page, but we can fail if there is a race or if we still have cursor open for
                        // some page
                        // in this case we will deal with this page later on postponed sweep or eviction will do its
                        // business
                        if (PageList.isLoaded(pageRef)) {
                            if (pages.tryEvict(pageRef, evictionEvent)) {
                                pageCache.addFreePageToFreelist(pageRef, evictionEvent);
                                evictedPages++;
                            }
                            totalPages++;
                        }
                    }
                }
            }
        }
        SwapperSet swappers = getSwappers();
        if (totalPages == evictedPages) {
            swappers.free(swapperId);
        } else {
            swappers.postponedFree(swapperId);
        }
    }

    @Override
    public void flushAndForce(FileFlushEvent flushEvent) throws IOException {
        try (var buffer = bufferFactory.createBuffer()) {
            flushAndForceInternal(flushEvent, false, ioController, buffer, true);
        }
        pageCache.clearEvictorException();
    }

    void flushAndForceForClose() throws IOException {
        if (deleteOnClose) {
            // No need to spend time flushing data to a file we're going to delete anyway.
            // However, we still have to mark the dirtied pages as clean since evicting would otherwise try to flush
            // these pages, and would fail because the file is closed, and we cannot allow that to happen.
            markAllDirtyPagesAsClean();
            return;
        }
        try (FileFlushEvent flushEvent = pageCacheTracer.beginFileFlush(swapper);
                var buffer = bufferFactory.createBuffer()) {
            flushAndForceInternal(flushEvent, true, ioController, buffer, true);
        }
        pageCache.clearEvictorException();
    }

    private void markAllDirtyPagesAsClean() {
        int[][] tt = this.translationTable;
        markAllDirtyPagesAsClean(tt);
    }

    private void markAllDirtyPagesAsClean(int[][] tt) {
        long filePageId = -1; // Start at -1 because we increment at the *start* of the chunk-loop iteration.
        Stopwatch stopwatch = Stopwatch.start();
        for (int[] chunk : tt) {
            chunkLoop:
            for (int i = 0; i < chunk.length; i++) {
                filePageId++;
                int chunkIndex = computeChunkIndex(filePageId);

                // We might race with eviction, but we also mustn't miss a dirty page, so we loop until we succeed
                // in getting a lock on all available pages.
                for (; ; ) {
                    int pageId = translationTableGetVolatile(chunk, chunkIndex);
                    if (pageId != UNMAPPED_TTE) {
                        long pageRef = deref(pageId);
                        long stamp = tryOptimisticReadLock(pageRef);
                        if ((!isModified(pageRef)) && validateReadLock(pageRef, stamp)) {
                            // We got a valid read, and the page isn't dirty, so we skip it.
                            continue chunkLoop;
                        }
                        pageCacheTracer.beforePageExclusiveLock();
                        if (!tryExclusiveLock(pageRef)) {
                            if (TIME_LIMIT_ON_FILE_UNMAP_SECONDS != 0L) {
                                if (stopwatch.hasTimedOut(TIME_LIMIT_ON_FILE_UNMAP_SECONDS, TimeUnit.SECONDS)) {
                                    String message = unmapTimeoutMessage("markAllDirtyPagesAsClean", pageId, pageRef);
                                    pageCacheTracer.failedUnmap(message);
                                    throw new RuntimeException(message);
                                }
                            }
                            continue;
                        }
                        if (isBoundTo(pageRef, swapperId, filePageId)) {
                            // The page is still bound to the expected file and file page id after we locked it,
                            // so we didn't race with eviction and faulting, and the page maybe dirty.
                            explicitlyMarkPageUnmodifiedUnderExclusiveLock(pageRef);
                            unlockExclusive(pageRef);
                            continue chunkLoop;
                        }
                        unlockExclusive(pageRef);
                        // race with eviction, retry to make sure that pageId is unmapped
                        continue;
                    }
                    // There was no page at this entry in the table. Continue to the next entry.
                    continue chunkLoop;
                }
            }
        }
    }

    private String unmapTimeoutMessage(String method, int pageId, long pageRef) {
        return "Timeout on file unmap in " + method + ". " + "file: "
                + swapper.path() + " swapperId: " + swapperId + " pageId: " + pageId
                + " page metadata:\n" + pageMetadata(pageRef);
    }

    private void markPagesAsFree(int[][] table, int initialChunkIndex, int initialChunkOffset, long initialFilePageId) {
        // Start at index -1 because we increment at the *start* of the chunk-loop iteration.
        long filePageId = initialFilePageId - 1;
        int chunkOffset = initialChunkOffset;
        for (int j = initialChunkIndex; j < table.length; j++) {
            int[] chunk = table[j];
            chunkLoop:
            for (int i = chunkOffset; i < chunk.length; i++) {
                filePageId++;
                int chunkIndex = computeChunkIndex(filePageId);

                // We might race with eviction, but we also mustn't miss a dirty page, so we loop until we succeed
                // in getting a lock on all available pages.
                for (; ; ) {
                    int pageId = translationTableGetVolatile(chunk, chunkIndex);
                    if (pageId != UNMAPPED_TTE) {
                        long pageRef = deref(pageId);
                        pageCacheTracer.beforePageExclusiveLock();
                        if (!tryExclusiveLock(pageRef)) {
                            continue;
                        }
                        if (isBoundTo(pageRef, swapperId, filePageId)) {
                            // The page is still bound to the expected file and file page id after we locked it,
                            // so we didn't race with eviction and faulting, and the page is dirty.
                            explicitlyMarkPageUnmodifiedUnderExclusiveLock(pageRef);
                            // here we are doing a shortcut and add truncated pages directly to free list
                            // by doing mass targeted evictions of affected pages that we know are affected
                            // and page in a free list. Page should be locked exclusively in the free list.
                            // see MunningPageCache#grabFreeAndExclusivelyLockedPage
                            // see MuninnPageCursor#pageFault
                            translationTableSetVolatile(chunk, chunkIndex, UNMAPPED_TTE);
                            clearBinding(pageRef);
                            pageCache.addFreePageToFreelist(pageRef, EvictionRunEvent.NULL);
                            continue chunkLoop;
                        }
                        unlockExclusive(pageRef);
                        // race with eviction, retry to make sure that pageId is unmapped
                        continue;
                    }
                    // There was no page at this entry in the table. Continue to the next entry.
                    continue chunkLoop;
                }
            }
            chunkOffset = 0;
        }
    }

    void flushAndForceInternal(
            FileFlushEvent flushEvent, boolean forClosing, IOController limiter, NativeIOBuffer ioBuffer, boolean force)
            throws IOException {
        try {
            doFlushAndForceInternal(flushEvent, forClosing, limiter, ioBuffer, force);
        } catch (ClosedChannelException e) {
            if (getRefCount() > 0) {
                // The file is not supposed to be closed, since we have a positive ref-count, yet we got a
                // ClosedChannelException anyway? It's an odd situation, so let's tell the outside world about
                // this failure.
                e.addSuppressed(closeStackTrace);
                throw e;
            }
            // Otherwise: The file was closed while we were trying to flush it. Since unmapping implies a flush
            // anyway, we can safely assume that this is not a problem. The file was flushed, and it doesn't
            // really matter how that happened. We'll ignore this exception.
        }
    }

    private void doFlushAndForceInternal(
            FileFlushEvent flushes, boolean forClosing, IOController limiter, NativeIOBuffer ioBuffer, boolean force)
            throws IOException {
        // TODO it'd be awesome if, on Linux, we'd call sync_file_range(2) instead of fsync
        long[] pages = new long[TRANSLATION_TABLE_CHUNK_SIZE];
        long[] flushStamps = forClosing ? null : new long[TRANSLATION_TABLE_CHUNK_SIZE];
        long[] bufferAddresses = new long[TRANSLATION_TABLE_CHUNK_SIZE];
        int[] bufferLengths = new int[TRANSLATION_TABLE_CHUNK_SIZE];
        long filePageId = -1; // Start at -1 because we increment at the *start* of the chunk-loop iteration.
        int[][] tt = this.translationTable;
        boolean useTemporaryBuffer = ioBuffer.isEnabled();

        flushes.startFlush(tt);
        Stopwatch stopwatch = Stopwatch.start();
        for (int[] chunk : tt) {
            var chunkEvent = flushes.startChunk(chunk);
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
            if (useTemporaryBuffer) {
                // in case when we use temp intermediate buffer we have only buffer and its address and length are
                // always stored in arrays with index 0
                bufferAddresses[0] = ioBuffer.getAddress();
                bufferLengths[0] = 0;
                buffersPerChunk = 1;
            }

            chunkLoop:
            for (int i = 0; i < chunk.length; i++) {
                filePageId++;
                int chunkIndex = computeChunkIndex(filePageId);

                // We might race with eviction, but we also mustn't miss a dirty page, so we loop until we succeed
                // in getting a lock on all available pages.
                for (; ; ) {
                    int pageId = translationTableGetVolatile(chunk, chunkIndex);
                    if (pageId != UNMAPPED_TTE) {
                        long pageRef = deref(pageId);
                        long stamp = tryOptimisticReadLock(pageRef);
                        if ((!isModified(pageRef) && !fillingDirtyBuffer) && validateReadLock(pageRef, stamp)) {
                            notModifiedPages++;
                            break; // not modified, continue with the chunk
                        }

                        long flushStamp = 0;
                        if (!(forClosing ? tryExclusiveLock(pageRef) : ((flushStamp = tryFlushLock(pageRef)) != 0))) {
                            if (TIME_LIMIT_ON_FILE_UNMAP_SECONDS != 0L) {
                                if (stopwatch.hasTimedOut(TIME_LIMIT_ON_FILE_UNMAP_SECONDS, TimeUnit.SECONDS)) {
                                    String message = unmapTimeoutMessage("doFlushAndForceInternal", pageId, pageRef);
                                    pageCacheTracer.failedUnmap(message);
                                    throw new RuntimeException(message);
                                }
                            }
                            continue; // retry lock
                        }
                        if (isBoundTo(pageRef, swapperId, filePageId) && (isModified(pageRef) || fillingDirtyBuffer)) {
                            // we should try to merge pages into buffer even if they are not modified only when we using
                            // intermediate temporary buffer
                            fillingDirtyBuffer = useTemporaryBuffer;
                            // The page is still bound to the expected file and file page id after we locked it,
                            // so we didn't race with eviction and faulting, and the page is dirty.
                            // So we add it to our IO vector.
                            pages[pagesGrabbed] = pageRef;
                            if (!forClosing) {
                                flushStamps[pagesGrabbed] = flushStamp;
                            }
                            pagesGrabbed++;
                            long address = getAddress(pageRef);
                            if (useTemporaryBuffer) {
                                // in case we use temp buffer to combine pages address and buffer lengths are located in
                                // corresponding arrays and have
                                // index 0.
                                // Reset of accumulated effective length of temp buffer happens after intermediate
                                // vectored flush if any
                                UnsafeUtil.copyMemory(address, bufferAddresses[0] + bufferLengths[0], filePageSize);
                                bufferLengths[0] += filePageSize;
                                numberOfBuffers = 1;
                                if (!ioBuffer.hasMoreCapacity(bufferLengths[0], filePageSize)) {
                                    break; // continue to flush
                                } else {
                                    continue chunkLoop; // go to next page
                                }
                            } else {
                                if (MERGE_PAGES_ON_FLUSH && nextSequentialAddress == address) {
                                    // do not add new address, only bump length of previous buffer
                                    bufferLengths[lastBufferIndex] += filePageSize;
                                    mergedPages++;
                                    mergesPerChunk++;
                                } else {
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
                        } else {
                            if (forClosing) {
                                unlockExclusive(pageRef);
                            } else {
                                unlockFlush(pageRef, flushStamp, false);
                            }
                            if (useTemporaryBuffer && pagesGrabbed > 0) {
                                // flush previous grabbed region
                                break;
                            }
                        }
                    }
                    break;
                }
                if (pagesGrabbed > 0) {
                    vectoredFlush(
                            pages,
                            bufferAddresses,
                            flushStamps,
                            bufferLengths,
                            numberOfBuffers,
                            pagesGrabbed,
                            mergedPages,
                            flushes,
                            forClosing);
                    flushes.reportIO(numberOfBuffers);
                    limiter.maybeLimitIO(numberOfBuffers, flushes);
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
            if (pagesGrabbed > 0) {
                vectoredFlush(
                        pages,
                        bufferAddresses,
                        flushStamps,
                        bufferLengths,
                        numberOfBuffers,
                        pagesGrabbed,
                        mergedPages,
                        flushes,
                        forClosing);
                flushes.reportIO(numberOfBuffers);
                limiter.maybeLimitIO(numberOfBuffers, flushes);
                flushPerChunk++;
            }
            chunkEvent.chunkFlushed(notModifiedPages, flushPerChunk, buffersPerChunk, mergesPerChunk);
        }

        if (force) {
            swapper.force();
        }
    }

    private void vectoredFlush(
            long[] pages,
            long[] bufferAddresses,
            long[] flushStamps,
            int[] bufferLengths,
            int numberOfBuffers,
            int pagesToFlush,
            int pagesMerged,
            FileFlushEvent flushEvent,
            boolean forClosing)
            throws IOException {
        try (var flush = flushEvent.beginFlush(pages, swapper, this, pagesToFlush, pagesMerged)) {
            boolean successful = false;
            try {
                // Write the pages vector
                long firstPageRef = pages[0];
                long startFilePageId = getFilePageId(firstPageRef);
                long bytesWritten =
                        swapper.write(startFilePageId, bufferAddresses, bufferLengths, numberOfBuffers, pagesToFlush);

                // Update the flush event
                flush.addBytesWritten(bytesWritten);
                flush.addPagesFlushed(pagesToFlush);
                flush.addPagesMerged(pagesMerged);
                successful = true;

                // There are now 0 'grabbed' pages
            } catch (IOException ioe) {
                flush.setException(ioe);
                throw ioe;
            } finally {
                // Always unlock all the pages in the vector
                if (forClosing) {
                    for (int i = 0; i < pagesToFlush; i++) {
                        long pageRef = pages[i];
                        if (successful) {
                            explicitlyMarkPageUnmodifiedUnderExclusiveLock(pageRef);
                        }
                        unlockExclusive(pageRef);
                    }
                } else {
                    for (int i = 0; i < pagesToFlush; i++) {
                        unlockFlush(pages[i], flushStamps[i], successful);
                    }
                }
            }
        }
    }

    boolean flushLockedPage(long pageRef, long filePageId) {
        boolean success = false;
        try (var majorFlushEvent = pageCacheTracer.beginFileFlush(swapper);
                var flushEvent = majorFlushEvent.beginFlush(pageRef, swapper, this)) {
            long address = getAddress(pageRef);
            try {
                long bytesWritten = swapper.write(filePageId, address);
                flushEvent.addBytesWritten(bytesWritten);
                flushEvent.addPagesFlushed(1);
                success = true;
            } catch (IOException e) {
                flushEvent.setException(e);
            }
        }
        return success;
    }

    @Override
    public void flush() throws IOException {
        swapper.force();
    }

    @Override
    public long getLastPageId() throws FileIsNotMappedException {
        long state = getHeaderState();
        if (refCountOf(state) == 0) {
            throw fileIsNotMappedException();
        }
        return state & HEADER_STATE_LAST_PAGE_ID_MASK;
    }

    private FileIsNotMappedException fileIsNotMappedException() {
        FileIsNotMappedException exception = new FileIsNotMappedException(path());
        Exception closedBy = closeStackTrace;
        if (closedBy != null) {
            exception.addSuppressed(closedBy);
        }
        return exception;
    }

    private long getHeaderState() {
        return (long) HEADER_STATE.getVolatile(this);
    }

    private static long refCountOf(long state) {
        return (state & HEADER_STATE_REF_COUNT_MASK) >>> HEADER_STATE_REF_COUNT_SHIFT;
    }

    private void initialiseLastPageId(long lastPageIdFromFile) {
        if (lastPageIdFromFile < 0) {
            // MIN_VALUE only has the sign bit raised, and the rest of the bits are zeros.
            HEADER_STATE.setVolatile(this, Long.MIN_VALUE);
        } else {
            HEADER_STATE.setVolatile(this, lastPageIdFromFile);
        }
    }

    @Override
    public void increaseLastPageIdTo(long newLastPageId) {
        long current;
        long update;
        long lastPageId;
        do {
            current = getHeaderState();
            update = newLastPageId + (current & HEADER_STATE_REF_COUNT_MASK);
            lastPageId = current & HEADER_STATE_LAST_PAGE_ID_MASK;
        } while (lastPageId < newLastPageId && !HEADER_STATE.weakCompareAndSet(this, current, update));
    }

    private void setLastPageIdTo(long newLastPageId) {
        long current;
        long update;
        do {
            current = getHeaderState();
            long state = newLastPageId < 0 ? EMPTY_STATE_HEADER : newLastPageId;
            update = state + (current & HEADER_STATE_REF_COUNT_MASK);
        } while (!HEADER_STATE.weakCompareAndSet(this, current, update));
    }

    /**
     * Atomically increment the reference count for this mapped file.
     */
    void incrementRefCount() {
        long current;
        long update;
        do {
            current = getHeaderState();
            long count = refCountOf(current) + 1;
            if (count > HEADER_STATE_REF_COUNT_MAX) {
                throw new IllegalStateException(
                        "Cannot map file because reference counter would overflow. " + "Maximum reference count is "
                                + HEADER_STATE_REF_COUNT_MAX + ". " + "File is "
                                + swapper.path().toAbsolutePath());
            }
            update = (current & HEADER_STATE_LAST_PAGE_ID_MASK) + (count << HEADER_STATE_REF_COUNT_SHIFT);
        } while (!HEADER_STATE.weakCompareAndSet(this, current, update));
    }

    /**
     * Atomically decrement the reference count. Returns true if this was the
     * last reference.
     */
    boolean decrementRefCount() {
        long current;
        long update;
        long count;
        do {
            current = getHeaderState();
            count = refCountOf(current) - 1;
            if (count < 0) {
                throw new IllegalStateException(
                        "File has already been closed and unmapped. " + "It cannot be closed any further.");
            }
            update = (current & HEADER_STATE_LAST_PAGE_ID_MASK) + (count << HEADER_STATE_REF_COUNT_SHIFT);
        } while (!HEADER_STATE.weakCompareAndSet(this, current, update));
        return count == 0;
    }

    /**
     * Get the current ref-count. Useful for checking if this PagedFile should
     * be considered unmapped.
     */
    int getRefCount() {
        return (int) refCountOf(getHeaderState());
    }

    @Override
    public void setDeleteOnClose(boolean deleteOnClose) {
        this.deleteOnClose = deleteOnClose;
    }

    @Override
    public boolean isDeleteOnClose() {
        return deleteOnClose;
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public PageFileCounters pageFileCounters() {
        return swapper.fileSwapperTracer();
    }

    /**
     * Grab a free page for the purpose of page faulting. Possibly blocking if
     * none are immediately available.
     *
     * @param faultEvent The trace event for the current page fault.
     */
    long grabFreeAndExclusivelyLockedPage(PageFaultEvent faultEvent) throws IOException {
        return pageCache.grabFreeAndExclusivelyLockedPage(faultEvent);
    }

    /**
     * Remove the mapping of the given filePageId from the translation table, and return the evicted page object.
     *
     * @param filePageId The id of the file page to evict.
     */
    private void evictPage(long filePageId) {
        int chunkId = computeChunkId(filePageId);
        int chunkIndex = computeChunkIndex(filePageId);
        int[] chunk = translationTable[chunkId];

        int mappedPageId = translationTableGetVolatile(chunk, chunkIndex);
        long pageRef = deref(mappedPageId);
        if (!multiVersioned && contextVersionUpdates) {
            setHighestEvictedTransactionId(getAndResetLastModifiedTransactionId(pageRef));
        }
        translationTableSetVolatile(chunk, chunkIndex, UNMAPPED_TTE);
    }

    private void setHighestEvictedTransactionId(long modifiedTransactionId) {
        long current;
        do {
            current = (long) HIGHEST_EVICTED_TRANSACTION_ID.getVolatile(this);
            if (current >= modifiedTransactionId) {
                return;
            }
        } while (!HIGHEST_EVICTED_TRANSACTION_ID.weakCompareAndSet(this, current, modifiedTransactionId));
    }

    long getHighestEvictedTransactionId() {
        return (long) HIGHEST_EVICTED_TRANSACTION_ID.getVolatile(this);
    }

    /**
     * Expand the translation table such that it can include at least the given chunkId.
     *
     * @param maxChunkId The new translation table must be big enough to include at least this chunkId.
     * @return A reference to the expanded transaction table.
     */
    synchronized int[][] expandCapacity(int maxChunkId) throws IOException {
        int[][] tt = translationTable;
        if (tt.length <= maxChunkId) {
            int newLength = computeNewRootTableLength(maxChunkId);
            int[][] ntt = new int[newLength][];
            System.arraycopy(tt, 0, ntt, 0, tt.length);
            for (int i = tt.length; i < ntt.length; i++) {
                ntt[i] = newChunk();
            }
            tt = ntt;
            if (preallocateFile && swapper.canAllocate()) {
                // Hint to the file system that we've grown our file.
                // This should reduce our tendency to fragment files.
                long newFileSize = tt.length; // New number of chunks.
                newFileSize *= TRANSLATION_TABLE_CHUNK_SIZE; // Pages per chunk.
                newFileSize *= filePageSize; // Bytes per page.
                swapper.allocate(newFileSize);
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

    synchronized void truncateCapacity(long pagesToKeep) {
        int chunkId = MuninnPagedFile.computeChunkId(pagesToKeep);

        int[][] tt = translationTable;
        if (tt.length > chunkId) {
            markPagesAsFree(tt, chunkId, MuninnPagedFile.computeChunkIndex(pagesToKeep), pagesToKeep);
            int newLength = computeNewRootTableLength(chunkId);
            int[][] ntt = new int[newLength][];
            System.arraycopy(tt, 0, ntt, 0, ntt.length);
            tt = ntt;
        }
        translationTable = tt;
    }

    private static int[] newChunk() {
        int[] chunk = new int[TRANSLATION_TABLE_CHUNK_SIZE];
        fill(chunk, UNMAPPED_TTE);
        return chunk;
    }

    private static int computeNewRootTableLength(int maxChunkId) {
        // Grow by approximate 10% but always by at least one full chunk, and no more than maxChunkGrowth (16 by
        // default, equivalent to 512 MiB).
        int next = 1 + (int) (maxChunkId * 1.1);
        return Math.min(next, maxChunkId + MAX_CHUNK_GROWTH);
    }

    static int computeChunkId(long filePageId) {
        return (int) (filePageId >>> TRANSLATION_TABLE_CHUNK_SIZE_POWER);
    }

    static int computeChunkIndex(long filePageId) {
        return (int) (filePageId & TRANSLATION_TABLE_CHUNK_SIZE_MASK);
    }

    @Override
    public boolean isMultiVersioned() {
        return multiVersioned;
    }

    public boolean isPreallocateFile() {
        return preallocateFile;
    }

    /**
     * Grab page fault latches for unmapped pages starting from pageId up to count, latches will be put into the latches array.
     * Returns number of grabbed latches
     */
    private int grabPageFaultLatches(long pageId, int count, LatchMap.Latch[] latches) {
        var latchesToGrab = Math.min(count, pageFaultLatches.size());
        int index = 0;
        while (index < latchesToGrab) {
            int chunkId = computeChunkId(pageId + index);
            int chunkIndex = computeChunkIndex(pageId + index);
            int[] chunk = translationTable[chunkId];

            for (; ; ) {
                int mappedPageId = translationTableGetVolatile(chunk, chunkIndex);
                if (mappedPageId == UNMAPPED_TTE) {
                    var latch = pageFaultLatches.takeOrAwaitLatch(pageId + index);
                    if (latch != null) {
                        // double check that no race with other page fault
                        if (translationTableGetVolatile(chunk, chunkIndex) == UNMAPPED_TTE) {
                            // great, remember the latch
                            latches[index] = latch;
                            break;
                        } else {
                            // found page that already mapped, release latch and exit
                            latch.release();
                            return index;
                        }
                    }
                } else {
                    return index;
                }
            }
            index++;
        }
        return index;
    }

    private static int translationTableGetVolatile(int[] chunk, int chunkIndex) {
        return (int) TRANSLATION_TABLE_ARRAY.getVolatile(chunk, chunkIndex);
    }

    private static void translationTableSetVolatile(int[] chunk, int chunkIndex, int value) {
        TRANSLATION_TABLE_ARRAY.setVolatile(chunk, chunkIndex, value);
    }

    @Override
    public int touch(long pageId, int count, CursorContext cursorContext) throws IOException {
        var lastPageId = getLastPageId();
        if (pageId < 0 || pageId > lastPageId) {
            return 0;
        }
        count = Math.min(count, (int) (lastPageId - pageId + 1));
        int touched = 0;
        if (USE_VECTORIZED_TOUCH) {
            try (var faultEvent = cursorContext.getCursorTracer().beginVectoredPageFault(swapper)) {
                touched = vectoredPageFault(pageId, count, faultEvent);
            }
        }
        if (touched < count) {
            // touch the rest with the cursor
            try (var cursor = io(pageId + touched, PF_SHARED_READ_LOCK, cursorContext)) {
                while (touched < count && cursor.next()) {
                    touched++;
                }
            }
        }
        return touched;
    }

    @Override
    public boolean preAllocateSupported() {
        return swapper.canAllocate();
    }

    @Override
    public synchronized void preAllocate(long newFileSizeInPages) throws IOException {
        if (getLastPageId() >= newFileSizeInPages || !preAllocateSupported()) {
            return;
        }

        int chunkId = MuninnPagedFile.computeChunkId(newFileSizeInPages - 1);
        // If automatic pre-allocation is enabled, the file is automatically pre-allocated
        // with each translation table extension.
        // Let's use this mechanism as it will be used anyway when the data is actually
        // written to the pre-allocated pages and there is no point pre-allocating x pages
        // if this mechanism will pre-allocate y later.
        if (preallocateFile && chunkId != 0) {
            if (translationTable.length <= chunkId) {
                expandCapacity(chunkId);
            }

            return;
        }

        swapper.allocate(newFileSizeInPages * filePageSize);
    }

    private int vectoredPageFault(long filePageId, int count, VectoredPageFaultEvent faultEvent) throws IOException {
        var latches = new LatchMap.Latch[count];
        int numberOfPages = grabPageFaultLatches(filePageId, count, latches);
        long[] pageRefs = new long[numberOfPages];
        try {
            // Note: It is important that we assign the filePageId after we grabbed it.
            // If the swapping fails, the page will be considered
            // loaded for the purpose of eviction, and will eventually return to
            // the freelist. However, because we don't assign the swapper until the
            // swapping-in has succeeded, the page will not be considered bound to
            // the file page, so any subsequent thread that finds the page in their
            // translation table will re-do the page fault.
            for (int i = 0; i < numberOfPages; i++) {
                pageRefs[i] = grabFreeAndExclusivelyLockedPage(faultEvent);
                PageList.validatePageRefAndSetFilePageId(pageRefs[i], swapper, swapperId, filePageId + i);
            }

            // Check if we're racing with unmapping. We have the page lock
            // here, so the unmapping would have already happened. We do this
            // check before page.fault(), because that would otherwise reopen
            // the file channel.
            getLastPageId();

            long[] bufferAddresses = new long[numberOfPages];
            int[] bufferLengths = new int[numberOfPages];
            for (int i = 0; i < numberOfPages; i++) {
                bufferAddresses[i] = initBuffer(pageRefs[i]);
                bufferLengths[i] = filePageSize;
            }

            long bytesRead = swapper.read(filePageId, bufferAddresses, bufferLengths, numberOfPages);
            faultEvent.addBytesRead(bytesRead);
            for (int i = 0; i < numberOfPages; i++) {
                setSwapperId(pageRefs[i], swapperId); // Page now considered isBoundTo( swapper, filePageId )
                // Put the page in the translation table before we undo the exclusive lock, as we could otherwise race
                // with
                // eviction, and the onEvict callback expects to find a MuninnPage object in the table.
                int pageCachePageId = toId(pageRefs[i]);
                int chunkId = computeChunkId(filePageId + i);
                int chunkIndex = computeChunkIndex(filePageId + i);
                translationTableSetVolatile(translationTable[chunkId], chunkIndex, pageCachePageId);
            }
            faultEvent.addPagesFaulted(numberOfPages, pageRefs, this);
        } catch (Throwable throwable) {
            faultEvent.setException(throwable);
            // mark pages as unmapped
            for (int i = 0; i < numberOfPages; i++) {
                int chunkId = computeChunkId(filePageId + i);
                int chunkIndex = computeChunkIndex(filePageId + i);
                translationTableSetVolatile(translationTable[chunkId], chunkIndex, UNMAPPED_TTE);
            }
            throw throwable;
        } finally {
            for (int i = 0; i < numberOfPages; i++) {
                if (pageRefs[i] != 0) {
                    PageList.unlockExclusive(pageRefs[i]);
                }
            }
            for (int i = 0; i < numberOfPages; i++) {
                if (latches[i] != null) {
                    latches[i].release();
                }
            }
        }
        return numberOfPages;
    }
}
