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

import static java.lang.String.format;
import static org.neo4j.util.FeatureToggles.flag;

import java.io.IOException;
import java.lang.invoke.VarHandle;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.mem.MemoryAllocator;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.tracing.EvictionEvent;
import org.neo4j.io.pagecache.tracing.EvictionEventOpportunity;
import org.neo4j.io.pagecache.tracing.PageReferenceTranslator;
import org.neo4j.io.pagecache.tracing.PinPageFaultEvent;

/**
 * The PageList maintains the off-heap meta-data for the individual memory pages.
 * <p>
 * The meta-data for each page is the following:
 *
 * <table>
 * <tr><th>Bytes</th><th>Use</th></tr>
 * <tr><td>8</td><td>Sequence lock word.</td></tr>
 * <tr><td>8</td><td>Pointer to the memory page.</td></tr>
 * <tr><td>8</td><td>Last modified transaction id.</td></tr>
 * <tr><td>8</td><td>Page binding. The first 40 bits (5 bytes) are the file page id.
 * The following (low order) 21 bits (2 bytes and 5 bits) are the swapper id.
 * The last (lowest order) 3 bits are the page usage counter.</td></tr>
 * </table>
 */
class PageList implements PageReferenceTranslator {
    private static final boolean forceSlowMemoryClear = flag(PageList.class, "forceSlowMemoryClear", false);

    static final int META_DATA_BYTES_PER_PAGE = 32;
    static final long MAX_PAGES = Integer.MAX_VALUE;

    private static final long UNBOUND_LAST_MODIFIED_TX_ID = 0;
    private static final long MAX_USAGE_COUNT = 4;
    private static final int SHIFT_FILE_PAGE_ID = 24;
    private static final int SHIFT_SWAPPER_ID = 3;
    private static final int SHIFT_PARTIAL_FILE_PAGE_ID = SHIFT_FILE_PAGE_ID - SHIFT_SWAPPER_ID;
    private static final long MASK_USAGE_COUNT = (1L << SHIFT_SWAPPER_ID) - 1L;
    private static final long MASK_NOT_FILE_PAGE_ID = (1L << SHIFT_FILE_PAGE_ID) - 1L;
    private static final long MASK_SHIFTED_SWAPPER_ID = MASK_NOT_FILE_PAGE_ID >>> SHIFT_SWAPPER_ID;
    private static final long MASK_NOT_SWAPPER_ID = ~(MASK_SHIFTED_SWAPPER_ID << SHIFT_SWAPPER_ID);
    private static final long UNBOUND_PAGE_BINDING = PageCursor.UNBOUND_PAGE_ID << SHIFT_FILE_PAGE_ID;

    // 40 bits for file page id
    private static final long MAX_FILE_PAGE_ID = (1L << Long.SIZE - SHIFT_FILE_PAGE_ID) - 1L;

    private static final int OFFSET_LOCK_WORD = 0; // 8 bytes.
    private static final int OFFSET_ADDRESS = 8; // 8 bytes.
    private static final int OFFSET_LAST_TX_ID = 16; // 8 bytes.
    // we use the same bytes to store previous chain version as
    // single version OFFSET_LAST_TX_ID, since those should never work together
    private static final int OFFSET_PREVIOUS_CHAIN_TX_ID = OFFSET_LAST_TX_ID;
    // The high 5 bytes of the page binding are the file page id.
    // The 21 following lower bits are the swapper id.
    // And the last 3 low bits are the usage counter.
    private static final int OFFSET_PAGE_BINDING = 24; // 8 bytes.
    // UNKNOWN value of previous chain modifier. Page with this modifier is always flushable.
    private static final int UNKNOWN_CHAIN_MODIFIER = 0;

    private final int pageCount;
    private final int cachePageSize;
    private final MemoryAllocator memoryAllocator;
    private final SwapperSet swappers;
    private final long victimPageAddress;
    private final long baseAddress;
    private final long bufferAlignment;

    PageList(
            int pageCount,
            int cachePageSize,
            MemoryAllocator memoryAllocator,
            SwapperSet swappers,
            long victimPageAddress,
            long bufferAlignment) {
        this.pageCount = pageCount;
        this.cachePageSize = cachePageSize;
        this.memoryAllocator = memoryAllocator;
        this.swappers = swappers;
        this.victimPageAddress = victimPageAddress;
        long bytes = ((long) pageCount) * META_DATA_BYTES_PER_PAGE;
        this.baseAddress = memoryAllocator.allocateAligned(bytes, Long.BYTES);
        this.bufferAlignment = bufferAlignment;
        clearMemory(baseAddress, pageCount);
    }

    /**
     * This copy-constructor is useful for classes that want to extend the {@code PageList} class to inline its fields.
     * All data and state will be shared between this and the given {@code PageList}. This means that changing the page
     * list state through one has the same effect as changing it through the other – they are both effectively the same
     * object.
     *
     * @param pageList The {@code PageList} instance whose state to copy.
     */
    PageList(PageList pageList) {
        this.pageCount = pageList.pageCount;
        this.cachePageSize = pageList.cachePageSize;
        this.memoryAllocator = pageList.memoryAllocator;
        this.swappers = pageList.swappers;
        this.victimPageAddress = pageList.victimPageAddress;
        this.baseAddress = pageList.baseAddress;
        this.bufferAlignment = pageList.bufferAlignment;
    }

    private static void clearMemory(long baseAddress, long pageCount) {
        long memcpyChunkSize = UnsafeUtil.pageSize();
        long metaDataEntriesPerChunk = memcpyChunkSize / META_DATA_BYTES_PER_PAGE;
        if (pageCount < metaDataEntriesPerChunk || forceSlowMemoryClear) {
            clearMemorySimple(baseAddress, pageCount);
        } else {
            clearMemoryFast(baseAddress, pageCount, memcpyChunkSize, metaDataEntriesPerChunk);
        }
        VarHandle.fullFence(); // Guarantee the visibility of the cleared memory.
    }

    private static void clearMemorySimple(long baseAddress, long pageCount) {
        long address = baseAddress - Long.BYTES;
        long initialLockWord = OffHeapPageLock.initialLockWordWithExclusiveLock();
        for (long i = 0; i < pageCount; i++) {
            UnsafeUtil.putLong(address += Long.BYTES, initialLockWord); // lock word
            UnsafeUtil.putLong(address += Long.BYTES, 0); // pointer
            UnsafeUtil.putLong(address += Long.BYTES, 0); // last tx id
            UnsafeUtil.putLong(address += Long.BYTES, UNBOUND_PAGE_BINDING);
        }
    }

    private static void clearMemoryFast(
            long baseAddress, long pageCount, long memcpyChunkSize, long metaDataEntriesPerChunk) {
        // Initialise one chunk worth of data.
        clearMemorySimple(baseAddress, metaDataEntriesPerChunk);
        // Since all entries contain the same data, we can now copy this chunk over and over.
        long chunkCopies = pageCount / metaDataEntriesPerChunk - 1;
        long address = baseAddress + memcpyChunkSize;
        for (int i = 0; i < chunkCopies; i++) {
            UnsafeUtil.copyMemory(baseAddress, address, memcpyChunkSize);
            address += memcpyChunkSize;
        }
        // Finally fill in the tail.
        long tailCount = pageCount % metaDataEntriesPerChunk;
        clearMemorySimple(address, tailCount);
    }

    /**
     * @return The capacity of the page list.
     */
    int getPageCount() {
        return pageCount;
    }

    SwapperSet getSwappers() {
        return swappers;
    }

    /**
     * Turn a {@code pageId} into a {@code pageRef} that can be used for accessing and manipulating the given page
     * using the other methods in this class.
     *
     * @param pageId The {@code pageId} to turn into a {@code pageRef}.
     * @return A {@code pageRef} which is an opaque, internal and direct pointer to the meta-data of the given memory
     * page.
     */
    long deref(int pageId) {
        assert pageId >= 0 && pageId < pageCount : "PageId out of range: " + pageId + ". PageCount: " + pageCount;
        //noinspection UnnecessaryLocalVariable
        long id = pageId; // convert to long to avoid int multiplication
        return baseAddress + (id * META_DATA_BYTES_PER_PAGE);
    }

    @Override
    public int toId(long pageRef) {
        // >> 5 is equivalent to dividing by 32, META_DATA_BYTES_PER_PAGE.
        return (int) ((pageRef - baseAddress) >> 5);
    }

    private static long offLastModifiedTransactionId(long pageRef) {
        return pageRef + OFFSET_LAST_TX_ID;
    }

    private static long offPageHorizon(long pageRef) {
        return pageRef + OFFSET_PREVIOUS_CHAIN_TX_ID;
    }

    private static long offLock(long pageRef) {
        return pageRef + OFFSET_LOCK_WORD;
    }

    private static long offAddress(long pageRef) {
        return pageRef + OFFSET_ADDRESS;
    }

    private static long offPageBinding(long pageRef) {
        return pageRef + OFFSET_PAGE_BINDING;
    }

    static long tryOptimisticReadLock(long pageRef) {
        return OffHeapPageLock.tryOptimisticReadLock(offLock(pageRef));
    }

    static boolean validateReadLock(long pageRef, long stamp) {
        return OffHeapPageLock.validateReadLock(offLock(pageRef), stamp);
    }

    static boolean isModified(long pageRef) {
        return OffHeapPageLock.isModified(offLock(pageRef));
    }

    static boolean isExclusivelyLocked(long pageRef) {
        return OffHeapPageLock.isExclusivelyLocked(offLock(pageRef));
    }

    static boolean isWriteLocked(long pageRef) {
        return OffHeapPageLock.isWriteLocked(offLock(pageRef));
    }

    static boolean tryWriteLock(long pageRef, boolean multiVersioned) {
        return OffHeapPageLock.tryWriteLock(offLock(pageRef), multiVersioned);
    }

    static void unlockWrite(long pageRef) {
        OffHeapPageLock.unlockWrite(offLock(pageRef));
    }

    static long unlockWriteAndTryTakeFlushLock(long pageRef) {
        return OffHeapPageLock.unlockWriteAndTryTakeFlushLock(offLock(pageRef));
    }

    static boolean tryExclusiveLock(long pageRef) {
        return OffHeapPageLock.tryExclusiveLock(offLock(pageRef));
    }

    static long unlockExclusive(long pageRef) {
        return OffHeapPageLock.unlockExclusive(offLock(pageRef));
    }

    static void unlockExclusiveAndTakeWriteLock(long pageRef) {
        OffHeapPageLock.unlockExclusiveAndTakeWriteLock(offLock(pageRef));
    }

    static long tryFlushLock(long pageRef) {
        return OffHeapPageLock.tryFlushLock(offLock(pageRef));
    }

    static void unlockFlush(long pageRef, long stamp, boolean success) {
        OffHeapPageLock.unlockFlush(offLock(pageRef), stamp, success);
    }

    static void explicitlyMarkPageUnmodifiedUnderExclusiveLock(long pageRef) {
        OffHeapPageLock.explicitlyMarkPageUnmodifiedUnderExclusiveLock(offLock(pageRef));
    }

    int getCachePageSize() {
        return cachePageSize;
    }

    static long getAddress(long pageRef) {
        return UnsafeUtil.getLong(offAddress(pageRef));
    }

    long initBuffer(long pageRef) {
        var address = getAddress(pageRef);
        if (address == 0L) {
            address = memoryAllocator.allocateAligned(getCachePageSize(), bufferAlignment);
            UnsafeUtil.putLong(offAddress(pageRef), address);
        }
        return address;
    }

    /**
     * Increment the usage stamp to at most 4.
     **/
    static void incrementUsage(long pageRef) {
        // This is intentionally left benignly racy for performance.
        long address = offPageBinding(pageRef);
        long value = UnsafeUtil.getLongVolatile(address);
        long usage = value & MASK_USAGE_COUNT;
        if (usage < MAX_USAGE_COUNT) // avoid cache sloshing by not doing a write if counter is already maxed out
        {
            long update = value + 1;
            // Use compareAndSwapLong to only actually store the updated count if nothing else changed
            // in this word-line. The word-line is shared with the file page id, and the swapper id.
            // Those fields are updated under guard of the exclusive lock, but we *might* race with
            // that here, and in that case we would never want a usage counter update to clobber a page
            // binding update.
            UnsafeUtil.compareAndSwapLong(null, address, value, update);
        }
    }

    /**
     * Decrement the usage stamp. Returns true if it reaches 0.
     **/
    static boolean decrementUsage(long pageRef) {
        // This is intentionally left benignly racy for performance.
        long address = offPageBinding(pageRef);
        long value = UnsafeUtil.getLongVolatile(address);
        long usage = value & MASK_USAGE_COUNT;
        if (usage > 0) {
            long update = value - 1;
            // See `incrementUsage` about why we use `compareAndSwapLong`.
            UnsafeUtil.compareAndSwapLong(null, address, value, update);
        }
        return usage <= 1;
    }

    static long getUsage(long pageRef) {
        return UnsafeUtil.getLongVolatile(offPageBinding(pageRef)) & MASK_USAGE_COUNT;
    }

    static long getFilePageId(long pageRef) {
        long filePageId = UnsafeUtil.getLong(offPageBinding(pageRef)) >>> SHIFT_FILE_PAGE_ID;
        return filePageId == MAX_FILE_PAGE_ID ? PageCursor.UNBOUND_PAGE_ID : filePageId;
    }

    static void setFilePageId(long pageRef, long filePageId) {
        if (filePageId > MAX_FILE_PAGE_ID) {
            throw new IllegalArgumentException(
                    format("File page id: %s is bigger then max supported value %s.", filePageId, MAX_FILE_PAGE_ID));
        }
        long address = offPageBinding(pageRef);
        long v = UnsafeUtil.getLong(address);
        filePageId = (filePageId << SHIFT_FILE_PAGE_ID) + (v & MASK_NOT_FILE_PAGE_ID);
        UnsafeUtil.putLong(address, filePageId);
    }

    static long getLastModifiedTxId(long pageRef) {
        return UnsafeUtil.getLongVolatile(offLastModifiedTransactionId(pageRef));
    }

    /**
     * @return return last modifier transaction id and resets it to {@link #UNBOUND_LAST_MODIFIED_TX_ID}
     */
    static long getAndResetLastModifiedTransactionId(long pageRef) {
        return UnsafeUtil.getAndSetLong(null, offLastModifiedTransactionId(pageRef), UNBOUND_LAST_MODIFIED_TX_ID);
    }

    static void setLastModifiedTxId(long pageRef, long modifierTxId) {
        UnsafeUtil.compareAndSetMaxLong(null, offLastModifiedTransactionId(pageRef), modifierTxId);
    }

    static long getAndResetPageHorizon(long pageRef) {
        return UnsafeUtil.getAndSetLong(null, offPageHorizon(pageRef), UNKNOWN_CHAIN_MODIFIER);
    }

    static long getPageHorizon(long pageRef) {
        return UnsafeUtil.getLongVolatile(offPageHorizon(pageRef));
    }

    static void setPageHorizon(long pageRef, long horizon) {
        UnsafeUtil.putLong(offPageHorizon(pageRef), horizon);
    }

    static int getSwapperId(long pageRef) {
        long v = UnsafeUtil.getLong(offPageBinding(pageRef)) >>> SHIFT_SWAPPER_ID;
        return (int) (v & MASK_SHIFTED_SWAPPER_ID); // 21 bits.
    }

    static void setSwapperId(long pageRef, int swapperId) {
        swapperId = swapperId << SHIFT_SWAPPER_ID;
        long address = offPageBinding(pageRef);
        long v = UnsafeUtil.getLong(address) & MASK_NOT_SWAPPER_ID;
        UnsafeUtil.putLong(address, v + swapperId);
    }

    static boolean isLoaded(long pageRef) {
        return getFilePageId(pageRef) != PageCursor.UNBOUND_PAGE_ID;
    }

    static boolean isBoundTo(long pageRef, int swapperId, long filePageId) {
        long address = offPageBinding(pageRef);
        long expectedBinding = (filePageId << SHIFT_PARTIAL_FILE_PAGE_ID) + swapperId;
        long actualBinding = UnsafeUtil.getLong(address) >>> SHIFT_SWAPPER_ID;
        return expectedBinding == actualBinding;
    }

    static void validatePageRefAndSetFilePageId(long pageRef, PageSwapper swapper, int swapperId, long filePageId) {
        assert swapper != null;
        assert filePageId != PageCursor.UNBOUND_PAGE_ID;
        long currentFilePageId = getFilePageId(pageRef);
        int currentSwapper = getSwapperId(pageRef);
        if (currentFilePageId != PageCursor.UNBOUND_PAGE_ID) {
            throw cannotFaultException(pageRef, swapper, swapperId, filePageId, currentSwapper, currentFilePageId);
        }
        // Note: It is important that we assign the filePageId right after it's grabbed and before we swap
        // the page in. If the swapping fails, the page will be considered
        // loaded for the purpose of eviction, and will eventually return to
        // the freelist. However, because we don't assign the swapper until the
        // swapping-in has succeeded, the page will not be considered bound to
        // the file page, so any subsequent thread that finds the page in their
        // translation table will re-do the page fault.
        setFilePageId(pageRef, filePageId); // Page now considered isLoaded()

        if (!isExclusivelyLocked(pageRef) || currentSwapper != 0) {
            throw cannotFaultException(pageRef, swapper, swapperId, filePageId, currentSwapper, currentFilePageId);
        }
    }

    static void fault(long pageRef, PageSwapper swapper, int swapperId, long filePageId, PinPageFaultEvent event)
            throws IOException {
        long bytesRead = swapper.read(filePageId, getAddress(pageRef));
        event.addBytesRead(bytesRead);
        setSwapperId(pageRef, swapperId); // Page now considered isBoundTo( swapper, filePageId )
    }

    static IllegalStateException cannotFaultException(
            long pageRef,
            PageSwapper swapper,
            int swapperId,
            long filePageId,
            int currentSwapper,
            long currentFilePageId) {
        String msg = format(
                "Cannot fault page {filePageId = %s, swapper = %s (swapper id = %s)} into "
                        + "cache page %s. Already bound to {filePageId = "
                        + "%s, swapper id = %s}.",
                filePageId, swapper, swapperId, pageRef, currentFilePageId, currentSwapper);
        return new IllegalStateException(msg);
    }

    boolean tryEvict(long pageRef, EvictionEventOpportunity evictionOpportunity) throws IOException {
        if (tryExclusiveLock(pageRef)) {
            if (isLoaded(pageRef)) {
                try (var evictionEvent = evictionOpportunity.beginEviction(toId(pageRef))) {
                    evict(pageRef, evictionEvent);
                    return true;
                }
            }
            unlockExclusive(pageRef);
        }
        return false;
    }

    private void evict(long pageRef, EvictionEvent evictionEvent) throws IOException {
        long filePageId = getFilePageId(pageRef);
        evictionEvent.setFilePageId(filePageId);
        int swapperId = getSwapperId(pageRef);
        if (swapperId != 0) {
            // If the swapper id is non-zero, then the page was not only loaded, but also bound, and possibly modified.
            SwapperSet.SwapperMapping swapperMapping = swappers.getAllocation(swapperId);
            if (swapperMapping != null) {
                // The allocation can be null if the file has been unmapped, but there are still pages
                // lingering in the cache that were bound to file page in that file.
                PageSwapper swapper = swapperMapping.swapper;
                evictionEvent.setSwapper(swapper);

                if (isModified(pageRef)) {
                    if (swapper.isPageFlushable(pageRef)) {
                        flushModifiedPage(pageRef, evictionEvent, filePageId, swapper, this);
                    } else {
                        explicitlyMarkPageUnmodifiedUnderExclusiveLock(pageRef);
                    }
                }
                swapper.evicted(filePageId);
            }
        }
        clearBinding(pageRef);
    }

    private static void flushModifiedPage(
            long pageRef,
            EvictionEvent evictionEvent,
            long filePageId,
            PageSwapper swapper,
            PageList pageReferenceTranslator)
            throws IOException {
        try (var flushEvent = evictionEvent.beginFlush(pageRef, swapper, pageReferenceTranslator)) {
            try {
                long address = getAddress(pageRef);
                long bytesWritten = swapper.write(filePageId, address);
                explicitlyMarkPageUnmodifiedUnderExclusiveLock(pageRef);
                flushEvent.addBytesWritten(bytesWritten);
                flushEvent.addEvictionFlushedPages(1);
            } catch (IOException e) {
                unlockExclusive(pageRef);
                flushEvent.setException(e);
                evictionEvent.setException(e);
                throw e;
            }
        }
    }

    static void clearBinding(long pageRef) {
        PageList.getAndResetPageHorizon(pageRef);
        UnsafeUtil.putLong(offPageBinding(pageRef), UNBOUND_PAGE_BINDING);
    }

    static String pageMetadata(long pageRef) {
        return "Lock word: " + Long.toHexString(UnsafeUtil.getLong(offLock(pageRef))) + "\nAddress: "
                + Long.toHexString(UnsafeUtil.getLong(offAddress(pageRef))) + "\nPrevious/Last TxId: "
                + Long.toHexString(UnsafeUtil.getLong(offPageHorizon(pageRef))) + "\nBinding: "
                + Long.toHexString(UnsafeUtil.getLong(offPageBinding(pageRef)));
    }
}
