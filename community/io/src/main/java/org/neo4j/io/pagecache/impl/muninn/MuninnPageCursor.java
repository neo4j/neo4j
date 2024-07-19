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

import static org.neo4j.io.pagecache.PagedFile.PF_EAGER_FLUSH;
import static org.neo4j.io.pagecache.PagedFile.PF_NO_CHAIN_FOLLOW;
import static org.neo4j.io.pagecache.PagedFile.PF_NO_FAULT;
import static org.neo4j.io.pagecache.PagedFile.PF_NO_LOAD;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_WRITE_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_TRANSIENT;
import static org.neo4j.io.pagecache.impl.muninn.MuninnPagedFile.UNMAPPED_TTE;
import static org.neo4j.io.pagecache.impl.muninn.PageList.setSwapperId;
import static org.neo4j.io.pagecache.impl.muninn.PageList.validatePageRefAndSetFilePageId;
import static org.neo4j.util.FeatureToggles.flag;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Objects;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.pagecache.CursorException;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.VersionContext;
import org.neo4j.io.pagecache.impl.FileIsNotMappedException;
import org.neo4j.io.pagecache.tracing.PinEvent;
import org.neo4j.io.pagecache.tracing.PinPageFaultEvent;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.util.Preconditions;
import org.neo4j.util.VisibleForTesting;

public abstract class MuninnPageCursor extends PageCursor {
    private static final boolean usePreciseCursorErrorStackTraces =
            flag(MuninnPageCursor.class, "usePreciseCursorErrorStackTraces", false);

    private static final boolean boundsCheck = flag(MuninnPageCursor.class, "boundsCheck", true);

    private static final int BYTE_ARRAY_BASE_OFFSET = UnsafeUtil.arrayBaseOffset(byte[].class);
    private static final int BYTE_ARRAY_INDEX_SCALE = UnsafeUtil.arrayIndexScale(byte[].class);

    // Size of the respective primitive types in bytes.
    private static final int SIZE_OF_BYTE = Byte.BYTES;
    private static final int SIZE_OF_SHORT = Short.BYTES;
    private static final int SIZE_OF_INT = Integer.BYTES;
    private static final int SIZE_OF_LONG = Long.BYTES;

    protected final PageCursorTracer tracer;
    protected final VersionContext versionContext;
    protected final CursorContext cursorContext;

    protected final MuninnPagedFile pagedFile;
    protected final PageSwapper swapper;
    final VersionStorage versionStorage;
    protected VersionState versionState;
    private final long victimPage;
    protected final int swapperId;
    private final int filePageSize;
    private final int pageReservedBytes;
    private final int filePayloadSize;
    private final int pf_flags;
    protected final boolean eagerFlush;
    private final boolean noFault;
    protected final boolean chainFollow;
    protected final boolean noLoad;
    protected final boolean noGrow;
    private final boolean updateUsage;
    protected final boolean multiVersioned;
    protected final boolean contextVersionUpdates;
    protected final boolean littleEndian;

    @SuppressWarnings("unused") // accessed via VarHandle.
    private long currentPageId;

    protected long pinnedPageRef;
    protected long nextPageId;
    protected long pointer;
    protected long version;
    private int pageSize;
    private int payloadSize;
    private int offset;
    private int mark;
    private boolean outOfBounds;
    private boolean markOutOfBounds;
    protected boolean closed;

    protected MuninnPageCursor linkedCursor;
    protected MuninnPageCursor backLinkedCursor;
    protected JobHandle<?> preFetcher;

    // This is a String with the exception message if usePreciseCursorErrorStackTraces is false, otherwise it is a
    // CursorExceptionWithPreciseStackTrace with the message and stack trace pointing more or less directly at the
    // offending code.
    private Object cursorException;

    private static final VarHandle CURRENT_PAGE_ID;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            CURRENT_PAGE_ID = l.findVarHandle(MuninnPageCursor.class, "currentPageId", long.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    MuninnPageCursor(
            MuninnPagedFile pagedFile, int pf_flags, long victimPage, CursorContext cursorContext, long pageId) {
        this.pagedFile = pagedFile;
        this.swapper = pagedFile.swapper;
        this.swapperId = pagedFile.swapperId;
        this.filePageSize = pagedFile.filePageSize;
        this.pageReservedBytes = pagedFile.pageReservedBytes();
        this.versionStorage = pagedFile.versionStorage;
        this.multiVersioned = pagedFile.multiVersioned;
        this.contextVersionUpdates = pagedFile.contextVersionUpdates;
        this.littleEndian = pagedFile.littleEndian;
        this.filePayloadSize = filePageSize - pageReservedBytes;
        this.pf_flags = pf_flags;
        this.eagerFlush = isFlagRaised(pf_flags, PF_EAGER_FLUSH);
        this.updateUsage = !isFlagRaised(pf_flags, PF_TRANSIENT);
        this.noFault = isFlagRaised(pf_flags, PF_NO_FAULT);
        this.chainFollow = !isFlagRaised(pf_flags, PF_NO_CHAIN_FOLLOW);
        this.noLoad = isFlagRaised(pf_flags, PF_NO_LOAD);
        this.noGrow = noFault || isFlagRaised(pf_flags, PagedFile.PF_NO_GROW);
        this.victimPage = victimPage;
        this.tracer = cursorContext.getCursorTracer();
        this.versionContext = cursorContext.getVersionContext();
        this.cursorContext = cursorContext;

        openCursor(pageId);
    }

    private void openCursor(long pageId) {
        nextPageId = pageId;
        offset = pageReservedBytes;
        pointer = victimPage;
        tracer.openCursor();
        storeCurrentPageId(UNBOUND_PAGE_ID);
        closed = false;
    }

    private static boolean isFlagRaised(int flagSet, int flag) {
        return (flagSet & flag) == flag;
    }

    protected long loadPlainCurrentPageId() {
        return currentPageId;
    }

    long loadVolatileCurrentPageId() {
        return (long) CURRENT_PAGE_ID.getVolatile(this);
    }

    protected void storeCurrentPageId(long pageId) {
        CURRENT_PAGE_ID.setRelease(this, pageId);
    }

    public final void init(PinEvent pinEvent, long pageRef) {
        this.pinnedPageRef = pageRef;
        this.offset = pageReservedBytes;
        this.pageSize = filePageSize;
        this.payloadSize = filePayloadSize;
        this.pointer = PageList.getAddress(pageRef);
        pinEvent.setCachePageId(pagedFile.toId(pageRef));
        if (updateUsage) {
            PageList.incrementUsage(pageRef);
        }
    }

    @Override
    public final boolean next(long pageId) throws IOException {
        if (loadPlainCurrentPageId() == pageId) {
            verifyContext();
            return true;
        }
        nextPageId = pageId;
        return next();
    }

    void verifyContext() {
        if (multiVersioned || !contextVersionUpdates) {
            return;
        }
        long lastClosedTransactionId = versionContext.lastClosedTransactionId();
        if (lastClosedTransactionId == Long.MAX_VALUE) {
            return;
        }
        if (isPotentiallyReadingDirtyData(lastClosedTransactionId)) {
            versionContext.markAsDirty();
        }
    }

    /**
     * When reading potentially dirty data in case if our page last modification version is higher than
     * requested lastClosedTransactionId; or for this page file we already evict some page with version that is higher
     * than requested lastClosedTransactionId. In this case we can't be sure that the data of the current page is satisfying
     * the visibility requirements, and we pessimistically will assume that we are reading dirty data.
     * @param lastClosedTransactionId last closed transaction id
     * @return true in case if we are reading potentially dirty data for requested lastClosedTransactionId.
     */
    private boolean isPotentiallyReadingDirtyData(long lastClosedTransactionId) {
        long pageRef = pinnedPageRef;
        return pageRef != 0
                && (PageList.getLastModifiedTxId(pageRef) > lastClosedTransactionId
                        || pagedFile.getHighestEvictedTransactionId() > lastClosedTransactionId);
    }

    @Override
    public final void close() {
        if (closed) {
            return;
        }
        closeLinks(this);
    }

    private void closeLinks(MuninnPageCursor cursor) {
        while (cursor != null && !cursor.closed) {
            cursor.unpin();
            cursor.closed = true;
            // Signal to any pre-fetchers that the cursor is closed.
            cursor.storeCurrentPageId(UNBOUND_PAGE_ID);
            if (preFetcher != null) {
                preFetcher.cancel();
                preFetcher = null;
            }
            tracer.closeCursor();
            cursor = cursor.linkedCursor;
        }
    }

    @Override
    public PageCursor openLinkedCursor(long pageId) {
        if (closed) {
            // This cursor has been closed
            throw new IllegalStateException("Cannot open linked cursor on closed page cursor");
        }
        if (linkedCursor != null) {
            if (!linkedCursor.closed) {
                throw new IllegalStateException("Previously created linked cursor still in use");
            }
            linkedCursor.openCursor(pageId);
        } else {
            linkedCursor = (MuninnPageCursor) pagedFile.io(pageId, pf_flags, cursorContext);
            linkedCursor.backLinkedCursor = this;
        }
        return linkedCursor;
    }

    /**
     * Must be called by {@link #unpin()}.
     */
    protected void clearPageCursorState() {
        // We don't need to clear the pointer field, because setting the page size to 0 will make all future accesses
        // go out of bounds, which in turn imply that they will always end up accessing the victim page anyway.
        clearPageReference();
        cursorException = null;
    }

    protected void clearPageReference() {
        // Make all future bounds checks fail, and send future accesses to the victim page.
        pageSize = 0;
        payloadSize = 0;
        version = 0;
        // Decouple us from the memory page, so we avoid messing with the page meta-data.
        pinnedPageRef = 0;
        versionState = null;
    }

    @Override
    public final long getCurrentPageId() {
        return loadPlainCurrentPageId();
    }

    @Override
    public Path getRawCurrentFile() {
        return closed ? null : pagedFile.path();
    }

    @Override
    public final Path getCurrentFile() {
        return loadPlainCurrentPageId() == UNBOUND_PAGE_ID ? null : getRawCurrentFile();
    }

    @Override
    public PagedFile getPagedFile() {
        return pagedFile;
    }

    /**
     * Pin the desired file page to this cursor, page faulting it into memory if it isn't there already.
     *
     * @param pinEvent - ongoing page pining event
     * @param filePageId The file page id we want to pin this cursor to.
     * @throws IOException if anything goes wrong with the pin, most likely during a page fault.
     */
    protected void pin(PinEvent pinEvent, long filePageId) throws IOException {
        int chunkId = MuninnPagedFile.computeChunkId(filePageId);
        // The chunkOffset is the addressing offset into the chunk array object for the relevant array slot. Using
        // this, we can access the array slot with Unsafe.
        int chunkIndex = MuninnPagedFile.computeChunkIndex(filePageId);
        int[][] tt = pagedFile.translationTable;
        if (tt.length <= chunkId) {
            tt = pagedFile.expandCapacity(chunkId);
        }
        int[] chunk = tt[chunkId];

        // Now, if the reference in the chunk slot is a latch, we wait on it and look up again (in a loop, since the
        // page might get evicted right after the page fault completes). If we find a page, we lock it and check its
        // binding (since it might get evicted and faulted into something else in the time between our look up and
        // our locking of the page). If the reference is null or it referred to a page that had wrong bindings, we CAS
        // in a latch. If that CAS succeeds, we page fault, set the slot to the faulted in page and open the latch.
        // If the CAS failed, we retry the look up and start over from the top.
        for (; ; ) {
            int mappedPageId = (int) MuninnPagedFile.TRANSLATION_TABLE_ARRAY.getVolatile(chunk, chunkIndex);
            if (mappedPageId != UNMAPPED_TTE) {
                // We got *a* page, but we might be racing with eviction. To cope with that, we have to take some
                // kind of lock on the page, and check that it is indeed bound to what we expect. If not, then it has
                // been evicted, and possibly even page faulted into something else. In this case, we discard the
                // item and try again, as the eviction thread would have set the chunk array slot to null.
                long pageRef = pagedFile.deref(mappedPageId);
                boolean locked = tryLockPage(pageRef);
                if (locked && PageList.isBoundTo(pageRef, swapperId, filePageId)) {
                    pinCursorToPage(pinEvent, pageRef, filePageId, swapper);
                    pinEvent.hit();
                    return;
                }
                if (locked) {
                    unlockPage(pageRef);
                }
            } else {
                if (uncommonPin(pinEvent, filePageId, chunkIndex, chunk)) {
                    return;
                }
            }
            // Assert that file still mapped before another attempt.
            // When file and swapper are closed, swapper forgets the reference to the eviction callback, and evictor
            // is unable to clean translation table entry. This leaves record of the pagecache page id
            // that is not bounded to the current swapper and filePageId, making pin loop indefinitely unless we fail
            // here.
            assertCursorOpenFileMappedAndGetIdOfLastPage();
        }
    }

    private boolean uncommonPin(PinEvent pinEvent, long filePageId, int chunkIndex, int[] chunk) throws IOException {
        if (noFault) {
            // The only page state that needs to be cleared is the currentPageId, since it was set prior to pin.
            storeCurrentPageId(UNBOUND_PAGE_ID);
            pinEvent.noFault();
            return true;
        }
        // Looks like there's no mapping, so we'd like to do a page fault.
        LatchMap.Latch latch = pagedFile.pageFaultLatches.takeOrAwaitLatch(filePageId);
        if (latch != null) {
            // We managed to inject our latch, so we now own the right to perform the page fault. We also
            // have a duty to eventually release and remove the latch, no matter what happens now.
            // However, we first have to double-check that a page fault did not complete in-between our initial
            // check in the translation table, and us getting a latch.
            if ((int) MuninnPagedFile.TRANSLATION_TABLE_ARRAY.getVolatile(chunk, chunkIndex) == UNMAPPED_TTE) {
                // Sweet, we didn't race with any other fault on this translation table entry.
                long pageRef = pageFault(pinEvent, filePageId, swapper, chunkIndex, chunk, latch);
                pinCursorToPage(pinEvent, pageRef, filePageId, swapper);
                return true;
            }
            // Oops, looks like we raced with another page fault on this file page.
            // Let's release our latch and retry the pin.
            latch.release();
        }
        // We found a latch, so someone else is already doing a page fault for this page.
        // The `takeOrAwaitLatch` already waited for this latch to be released on our behalf,
        // so now we just have to do another iteration of the loop to see what's in the translation table now.
        return false;
    }

    private long pageFault(
            PinEvent pinEvent, long filePageId, PageSwapper swapper, int chunkIndex, int[] chunk, LatchMap.Latch latch)
            throws IOException {
        // We are page faulting. This is a critical time, because we currently have the given latch in the chunk array
        // slot that we are faulting into. We MUST make sure to release that latch, and remove it from the chunk, no
        // matter what happens. Otherwise other threads will get stuck waiting forever for our page fault to finish.
        // If we manage to get a free page to fault into, then we will also be taking a write lock on that page, to
        // protect it against concurrent eviction as we assigning a binding to the page. If anything goes wrong, then
        // we must make sure to release that write lock as well.
        try (var faultEvent = pinEvent.beginPageFault(filePageId, swapper)) {
            long pageRef;
            int pageId;
            try {
                // The grabFreePage method might throw.
                pageRef = pagedFile.grabFreeAndExclusivelyLockedPage(faultEvent);
                // We got a free page, and we know that we have race-free access to it. Well, it's not entirely race
                // free, because other paged files might have it in their translation tables (or rather, their reads of
                // their translation tables might race with eviction) and try to pin it.
                // However, they will all fail because when they try to pin, because the page will be exclusively locked
                // and possibly bound to our page.
            } catch (Throwable throwable) {
                abortPageFault(throwable, chunk, chunkIndex, faultEvent);
                throw throwable;
            }
            try {
                validatePageRefAndSetFilePageId(pageRef, swapper, swapperId, filePageId);
                // Check if we're racing with unmapping. We have the page lock
                // here, so the unmapping would have already happened. We do this
                // check before page.fault(), because that would otherwise reopen
                // the file channel.
                assertCursorOpenFileMappedAndGetIdOfLastPage();
                pagedFile.initBuffer(pageRef);
                if (noLoad) {
                    setSwapperId(pageRef, swapperId); // Page now considered isBoundTo( swapper, filePageId )
                } else {
                    PageList.fault(pageRef, swapper, pagedFile.swapperId, filePageId, faultEvent);
                }
            } catch (Throwable throwable) {
                try {
                    // Make sure to unlock the page, so the eviction thread can pick up our trash.
                    PageList.unlockExclusive(pageRef);
                } finally {
                    abortPageFault(throwable, chunk, chunkIndex, faultEvent);
                }
                throw throwable;
            }
            // Put the page in the translation table before we undo the exclusive lock, as we could otherwise race with
            // eviction, and the onEvict callback expects to find a MuninnPage object in the table.
            pageId = pagedFile.toId(pageRef);
            faultEvent.setCachePageId(pageId);
            MuninnPagedFile.TRANSLATION_TABLE_ARRAY.setVolatile(chunk, chunkIndex, pageId);
            // Once we page has been published to the translation table, we can convert our exclusive lock to whatever
            // we
            // need for the page cursor.
            convertPageFaultLock(pageRef);
            return pageRef;
        } finally {
            latch.release();
        }
    }

    private static void abortPageFault(Throwable throwable, int[] chunk, int chunkIndex, PinPageFaultEvent faultEvent) {
        MuninnPagedFile.TRANSLATION_TABLE_ARRAY.setVolatile(chunk, chunkIndex, UNMAPPED_TTE);
        faultEvent.setException(throwable);
    }

    protected long assertCursorOpenFileMappedAndGetIdOfLastPage() throws FileIsNotMappedException {
        if (closed) {
            throw new IllegalStateException("This cursor is closed");
        }
        return pagedFile.getLastPageId();
    }

    protected abstract void convertPageFaultLock(long pageRef);

    protected abstract void pinCursorToPage(PinEvent pinEvent, long pageRef, long filePageId, PageSwapper swapper)
            throws FileIsNotMappedException;

    protected abstract boolean tryLockPage(long pageRef);

    protected abstract void unlockPage(long pageRef);

    /**
     * Returns true if the page has entered an inconsistent state since the last call to next() or shouldRetry().
     * This method must be equialent of shouldRetry without triggering retry and changing cursor state
     */
    public abstract boolean retrySnapshot();

    // --- IO methods:

    /**
     * Compute a pointer that guarantees (assuming {@code size} is less than or equal to {@link #pageSize}) that the
     * page access will be within the bounds of the page.
     * This might mean that the pointer won't point to where one might naively expect, but will instead be
     * truncated to point within the page. In this case, an overflow has happened and the {@link #outOfBounds}
     * flag will be raised.
     */
    private long getBoundedPointer(int offset, int size) {
        long p = pointer;
        long can = p + offset + pageReservedBytes;
        if (boundsCheck) {
            if (can + size > p + pageSize || can < p + pageReservedBytes) {
                outOfBounds = true;
                // Return the victim page when we are out of bounds, since at this point we can't tell if the pointer
                // will be used for reading or writing.
                return victimPage;
            }
        }
        return can;
    }

    /**
     * Compute a pointer that guarantees (assuming {@code size} is less than or equal to {@link #pageSize}) that the
     * page access will be within the bounds of the page.
     * This works just like {@link #getBoundedPointer(int, int)}, except in terms of the current {@link #offset}.
     * This version is faster when applicable, because it can ignore the <em>page underflow</em> case.
     */
    private long nextBoundedPointer(int size) {
        int offset = this.offset;
        long can = pointer + offset;
        if (boundsCheck) {
            if (offset + size > pageSize) {
                outOfBounds = true;
                // Return the victim page when we are out of bounds, since at this point we can't tell if the pointer
                // will be used for reading or writing.
                return victimPage;
            }
        }
        return can;
    }

    @Override
    public final byte getByte() {
        long p = nextBoundedPointer(SIZE_OF_BYTE);
        byte b = UnsafeUtil.getByte(p);
        offset++;
        return b;
    }

    @Override
    public byte getByte(int offset) {
        long p = getBoundedPointer(offset, SIZE_OF_BYTE);
        return UnsafeUtil.getByte(p);
    }

    @Override
    public void putByte(byte value) {
        long p = nextBoundedPointer(SIZE_OF_BYTE);
        UnsafeUtil.putByte(p, value);
        offset++;
    }

    @Override
    public void putByte(int offset, byte value) {
        long p = getBoundedPointer(offset, SIZE_OF_BYTE);
        UnsafeUtil.putByte(p, value);
    }

    @Override
    public long getLong() {
        long p = nextBoundedPointer(SIZE_OF_LONG);
        long value = getLongAt(p, littleEndian);
        offset += SIZE_OF_LONG;
        return value;
    }

    @Override
    public long getLong(int offset) {
        long p = getBoundedPointer(offset, SIZE_OF_LONG);
        return getLongAt(p, littleEndian);
    }

    static long getLongAt(long p, boolean littleEndian) {
        if (UnsafeUtil.allowUnalignedMemoryAccess) {
            var value = UnsafeUtil.getLong(p);
            if (UnsafeUtil.nativeByteOrderIsLittleEndian == littleEndian) {
                return value;
            }
            return Long.reverseBytes(value);
        }
        return getLongUnaligned(p, littleEndian);
    }

    private static long getLongUnaligned(long p, boolean littleEndian) {
        long a = UnsafeUtil.getByte(p) & 0xFF;
        long b = UnsafeUtil.getByte(p + 1) & 0xFF;
        long c = UnsafeUtil.getByte(p + 2) & 0xFF;
        long d = UnsafeUtil.getByte(p + 3) & 0xFF;
        long e = UnsafeUtil.getByte(p + 4) & 0xFF;
        long f = UnsafeUtil.getByte(p + 5) & 0xFF;
        long g = UnsafeUtil.getByte(p + 6) & 0xFF;
        long h = UnsafeUtil.getByte(p + 7) & 0xFF;
        if (littleEndian) {
            return (h << 56) | (g << 48) | (f << 40) | (e << 32) | (d << 24) | (c << 16) | (b << 8) | a;
        }
        return (a << 56) | (b << 48) | (c << 40) | (d << 32) | (e << 24) | (f << 16) | (g << 8) | h;
    }

    @Override
    public void putLong(long value) {
        long p = nextBoundedPointer(SIZE_OF_LONG);
        putLongAt(p, value, littleEndian);
        offset += SIZE_OF_LONG;
    }

    @Override
    public void putLong(int offset, long value) {
        long p = getBoundedPointer(offset, SIZE_OF_LONG);
        putLongAt(p, value, littleEndian);
    }

    static void putLongAt(long p, long value, boolean littleEndian) {
        if (UnsafeUtil.allowUnalignedMemoryAccess) {
            UnsafeUtil.putLong(
                    p, UnsafeUtil.nativeByteOrderIsLittleEndian == littleEndian ? value : Long.reverseBytes(value));
        } else {
            putLongUnaligned(value, p, littleEndian);
        }
    }

    private static void putLongUnaligned(long value, long p, boolean littleEndian) {
        if (littleEndian) {
            UnsafeUtil.putByte(p, (byte) value);
            UnsafeUtil.putByte(p + 1, (byte) (value >> 8));
            UnsafeUtil.putByte(p + 2, (byte) (value >> 16));
            UnsafeUtil.putByte(p + 3, (byte) (value >> 24));
            UnsafeUtil.putByte(p + 4, (byte) (value >> 32));
            UnsafeUtil.putByte(p + 5, (byte) (value >> 40));
            UnsafeUtil.putByte(p + 6, (byte) (value >> 48));
            UnsafeUtil.putByte(p + 7, (byte) (value >> 56));
        } else {
            UnsafeUtil.putByte(p, (byte) (value >> 56));
            UnsafeUtil.putByte(p + 1, (byte) (value >> 48));
            UnsafeUtil.putByte(p + 2, (byte) (value >> 40));
            UnsafeUtil.putByte(p + 3, (byte) (value >> 32));
            UnsafeUtil.putByte(p + 4, (byte) (value >> 24));
            UnsafeUtil.putByte(p + 5, (byte) (value >> 16));
            UnsafeUtil.putByte(p + 6, (byte) (value >> 8));
            UnsafeUtil.putByte(p + 7, (byte) value);
        }
    }

    @Override
    public int getInt() {
        long p = nextBoundedPointer(SIZE_OF_INT);
        int i = getIntAt(p, littleEndian);
        offset += SIZE_OF_INT;
        return i;
    }

    @Override
    public int getInt(int offset) {
        long p = getBoundedPointer(offset, SIZE_OF_INT);
        return getIntAt(p, littleEndian);
    }

    private static int getIntAt(long p, boolean littleEndian) {
        if (UnsafeUtil.allowUnalignedMemoryAccess) {
            int x = UnsafeUtil.getInt(p);
            return UnsafeUtil.nativeByteOrderIsLittleEndian == littleEndian ? x : Integer.reverseBytes(x);
        }
        return getIntUnaligned(p, littleEndian);
    }

    private static int getIntUnaligned(long p, boolean littleEndian) {
        int a = UnsafeUtil.getByte(p) & 0xFF;
        int b = UnsafeUtil.getByte(p + 1) & 0xFF;
        int c = UnsafeUtil.getByte(p + 2) & 0xFF;
        int d = UnsafeUtil.getByte(p + 3) & 0xFF;
        if (littleEndian) {
            return (d << 24) | (c << 16) | (b << 8) | a;
        }
        return (a << 24) | (b << 16) | (c << 8) | d;
    }

    @Override
    public void putInt(int value) {
        long p = nextBoundedPointer(SIZE_OF_INT);
        putIntAt(p, value, littleEndian);
        offset += SIZE_OF_INT;
    }

    @Override
    public void putInt(int offset, int value) {
        long p = getBoundedPointer(offset, SIZE_OF_INT);
        putIntAt(p, value, littleEndian);
    }

    private static void putIntAt(long p, int value, boolean littleEndian) {
        if (UnsafeUtil.allowUnalignedMemoryAccess) {
            UnsafeUtil.putInt(
                    p, UnsafeUtil.nativeByteOrderIsLittleEndian == littleEndian ? value : Integer.reverseBytes(value));
        } else {
            putIntUnaligned(value, p, littleEndian);
        }
    }

    private static void putIntUnaligned(int value, long p, boolean littleEndian) {
        if (littleEndian) {
            UnsafeUtil.putByte(p, (byte) value);
            UnsafeUtil.putByte(p + 1, (byte) (value >> 8));
            UnsafeUtil.putByte(p + 2, (byte) (value >> 16));
            UnsafeUtil.putByte(p + 3, (byte) (value >> 24));
        } else {
            UnsafeUtil.putByte(p, (byte) (value >> 24));
            UnsafeUtil.putByte(p + 1, (byte) (value >> 16));
            UnsafeUtil.putByte(p + 2, (byte) (value >> 8));
            UnsafeUtil.putByte(p + 3, (byte) value);
        }
    }

    @Override
    public void getBytes(byte[] data) {
        getBytes(data, 0, data.length);
    }

    @Override
    public void getBytes(byte[] data, int arrayOffset, int length) {
        if (arrayOffset + length > data.length) {
            throw new ArrayIndexOutOfBoundsException();
        }
        long p = nextBoundedPointer(length);
        if (!outOfBounds) {
            int inset = UnsafeUtil.arrayOffset(arrayOffset, BYTE_ARRAY_BASE_OFFSET, BYTE_ARRAY_INDEX_SCALE);
            if (length < 16) {
                for (int i = 0; i < length; i++) {
                    UnsafeUtil.putByte(data, inset + i, UnsafeUtil.getByte(p + i));
                }
            } else {
                UnsafeUtil.copyMemory(null, p, data, inset, length);
            }
        }
        offset += length;
    }

    @Override
    public final void putBytes(byte[] data) {
        putBytes(data, 0, data.length);
    }

    @Override
    public void putBytes(byte[] data, int arrayOffset, int length) {
        if (arrayOffset + length > data.length) {
            throw new ArrayIndexOutOfBoundsException();
        }
        long p = nextBoundedPointer(length);
        if (!outOfBounds) {
            int inset = UnsafeUtil.arrayOffset(arrayOffset, BYTE_ARRAY_BASE_OFFSET, BYTE_ARRAY_INDEX_SCALE);
            if (length < 16) {
                for (int i = 0; i < length; i++) {
                    UnsafeUtil.putByte(p + i, UnsafeUtil.getByte(data, inset + i));
                }
            } else {
                UnsafeUtil.copyMemory(data, inset, null, p, length);
            }
        }
        offset += length;
    }

    @Override
    public void putBytes(int bytes, byte value) {
        long p = nextBoundedPointer(bytes);
        if (!outOfBounds) {
            UnsafeUtil.setMemory(p, bytes, value);
        }
        offset += bytes;
    }

    @Override
    public final short getShort() {
        long p = nextBoundedPointer(SIZE_OF_SHORT);
        short s = getShortAt(p, littleEndian);
        offset += SIZE_OF_SHORT;
        return s;
    }

    @Override
    public short getShort(int offset) {
        long p = getBoundedPointer(offset, SIZE_OF_SHORT);
        return getShortAt(p, littleEndian);
    }

    private static short getShortAt(long p, boolean littleEndian) {
        if (UnsafeUtil.allowUnalignedMemoryAccess) {
            short x = UnsafeUtil.getShort(p);
            return UnsafeUtil.nativeByteOrderIsLittleEndian == littleEndian ? x : Short.reverseBytes(x);
        }
        return getShortUnaligned(p, littleEndian);
    }

    private static short getShortUnaligned(long p, boolean littleEndian) {
        short a = (short) (UnsafeUtil.getByte(p) & 0xFF);
        short b = (short) (UnsafeUtil.getByte(p + 1) & 0xFF);
        if (littleEndian) {
            return (short) ((b << 8) | a);
        }
        return (short) ((a << 8) | b);
    }

    @Override
    public void putShort(short value) {
        long p = nextBoundedPointer(SIZE_OF_SHORT);
        putShortAt(p, value, littleEndian);
        offset += SIZE_OF_SHORT;
    }

    @Override
    public void putShort(int offset, short value) {
        long p = getBoundedPointer(offset, SIZE_OF_SHORT);
        putShortAt(p, value, littleEndian);
    }

    private static void putShortAt(long p, short value, boolean littleEndian) {
        if (UnsafeUtil.allowUnalignedMemoryAccess) {
            UnsafeUtil.putShort(
                    p, UnsafeUtil.nativeByteOrderIsLittleEndian == littleEndian ? value : Short.reverseBytes(value));
        } else {
            putShortUnaligned(value, p, littleEndian);
        }
    }

    private static void putShortUnaligned(short value, long p, boolean littleEndian) {
        if (littleEndian) {
            UnsafeUtil.putByte(p, (byte) value);
            UnsafeUtil.putByte(p + 1, (byte) (value >> 8));
        } else {
            UnsafeUtil.putByte(p, (byte) (value >> 8));
            UnsafeUtil.putByte(p + 1, (byte) value);
        }
    }

    @Override
    public void copyPage(PageCursor targetCursor) {
        if (targetCursor.getClass() != MuninnWritePageCursor.class) {
            throw new IllegalArgumentException("Target cursor must be writable");
        }
        MuninnPageCursor target = (MuninnPageCursor) targetCursor;
        if (pageSize != target.pageSize) {
            throw new IllegalArgumentException("Target cursor page size: " + target.pageSize
                    + " is not equal to source cursor page size: " + pageSize);
        }
        UnsafeUtil.copyMemory(pointer, target.pointer, target.pageSize);
    }

    @Override
    public int copyTo(int sourceOffset, PageCursor targetCursor, int targetOffset, int lengthInBytes) {
        if (targetCursor.getClass() != MuninnWritePageCursor.class) {
            throw new IllegalArgumentException("Target cursor must be writable");
        }
        MuninnPageCursor cursor = (MuninnPageCursor) targetCursor;
        int source = sourceOffset + pageReservedBytes;
        int target = targetOffset + pageReservedBytes;
        int sourcePageSize = pageSize;
        int targetPageSize = cursor.pageSize;
        if (source >= pageReservedBytes
                && target >= pageReservedBytes
                && source < sourcePageSize
                && target < targetPageSize
                && lengthInBytes >= 0) {
            int remainingSource = sourcePageSize - source;
            int remainingTarget = targetPageSize - target;
            int bytes = Math.min(lengthInBytes, Math.min(remainingSource, remainingTarget));
            UnsafeUtil.copyMemory(pointer + source, cursor.pointer + target, bytes);
            return bytes;
        }
        outOfBounds = true;
        return 0;
    }

    @Override
    public int copyTo(int sourceOffset, ByteBuffer buf) {
        if (buf.getClass() == UnsafeUtil.DIRECT_BYTE_BUFFER_CLASS
                && buf.isDirect()
                && !buf.isReadOnly()
                && UnsafeUtil.unsafeByteBufferAccessAvailable()) {
            // We expect that the mutable direct byte buffer is implemented with a class that is distinct from the
            // non-mutable (read-only) and non-direct (on-heap) byte buffers. By comparing class object instances,
            // we also implicitly assume that the classes are loaded by the same class loader, which should be
            // trivially true in almost all practical cases.
            // If our expectations are not met, then the additional isDirect and !isReadOnly checks will send all
            // calls to the byte-wise-copy fallback.
            return copyToDirectByteBuffer(sourceOffset, buf);
        }
        return copyToByteBufferByteWise(sourceOffset, buf);
    }

    private int copyToDirectByteBuffer(int sourceOffset, ByteBuffer buf) {
        int pos = buf.position();
        int bytesToCopy = Math.min(buf.limit() - pos, payloadSize - sourceOffset);
        long source = pointer + sourceOffset + pageReservedBytes;
        if (sourceOffset < payloadSize && sourceOffset >= 0) {
            long target = UnsafeUtil.getDirectByteBufferAddress(buf);
            UnsafeUtil.copyMemory(source, target + pos, bytesToCopy);
            buf.position(pos + bytesToCopy);
        } else {
            outOfBounds = true;
        }
        return bytesToCopy;
    }

    private int copyToByteBufferByteWise(int sourceOffset, ByteBuffer buf) {
        int bytesToCopy = Math.min(buf.limit() - buf.position(), payloadSize - sourceOffset);
        for (int i = 0; i < bytesToCopy; i++) {
            byte b = getByte(sourceOffset + i);
            buf.put(b);
        }
        return bytesToCopy;
    }

    @Override
    public int copyFrom(ByteBuffer sourceBuffer, int targetOffset) {
        int bytesToCopy = Math.min(sourceBuffer.limit() - sourceBuffer.position(), payloadSize - targetOffset);
        for (int i = 0; i < bytesToCopy; i++) {
            byte b = sourceBuffer.get();
            putByte(targetOffset + i, b);
        }
        return bytesToCopy;
    }

    @Override
    public void shiftBytes(int sourceOffset, int length, int shift) {
        int offset = sourceOffset + pageReservedBytes;
        int sourceEnd = offset + length;
        int targetStart = offset + shift;
        int targetEnd = offset + length + shift;
        if (offset < pageReservedBytes
                || sourceEnd > filePageSize
                || targetStart < pageReservedBytes
                || targetEnd > filePageSize
                || length < 0) {
            outOfBounds = true;
            return;
        }

        if (length < 16) {
            if (shift < 0) {
                unsafeShiftLeft(offset, sourceEnd, length, shift);
            } else {
                unsafeShiftRight(sourceEnd, offset, length, shift);
            }
        } else {
            UnsafeUtil.copyMemory(pointer + offset, pointer + targetStart, length);
        }
    }

    private void unsafeShiftLeft(int fromPos, int toPos, int length, int shift) {
        int longSteps = length >> 3;
        if (UnsafeUtil.allowUnalignedMemoryAccess && longSteps > 0) {
            for (int i = 0; i < longSteps; i++) {
                long x = UnsafeUtil.getLong(pointer + fromPos);
                UnsafeUtil.putLong(pointer + fromPos + shift, x);
                fromPos += Long.BYTES;
            }
        }

        while (fromPos < toPos) {
            byte b = UnsafeUtil.getByte(pointer + fromPos);
            UnsafeUtil.putByte(pointer + fromPos + shift, b);
            fromPos++;
        }
    }

    private void unsafeShiftRight(int fromPos, int toPos, int length, int shift) {
        int longSteps = length >> 3;
        if (UnsafeUtil.allowUnalignedMemoryAccess && longSteps > 0) {
            for (int i = 0; i < longSteps; i++) {
                fromPos -= Long.BYTES;
                long x = UnsafeUtil.getLong(pointer + fromPos);
                UnsafeUtil.putLong(pointer + fromPos + shift, x);
            }
        }

        while (fromPos > toPos) {
            fromPos--;
            byte b = UnsafeUtil.getByte(pointer + fromPos);
            UnsafeUtil.putByte(pointer + fromPos + shift, b);
        }
    }

    @Override
    public void setOffset(int logicalOffset) {
        this.offset = logicalOffset + pageReservedBytes;
        if (offset < pageReservedBytes || offset > filePageSize) {
            this.offset = pageReservedBytes;
            outOfBounds = true;
        }
    }

    @Override
    public final int getOffset() {
        return offset - pageReservedBytes;
    }

    @Override
    public void mark() {
        this.mark = offset;
        this.markOutOfBounds = outOfBounds;
    }

    @Override
    public void setOffsetToMark() {
        this.offset = mark;
        this.outOfBounds = markOutOfBounds;
    }

    @Override
    public boolean checkAndClearBoundsFlag() {
        MuninnPageCursor cursor = this;
        boolean result = false;
        do {
            result |= cursor.outOfBounds;
            cursor.outOfBounds = false;
            cursor = cursor.linkedCursor;
        } while (cursor != null);
        return result;
    }

    @Override
    public void checkAndClearCursorException() throws CursorException {
        MuninnPageCursor cursor = this;
        do {
            Object error = cursor.cursorException;
            if (error != null) {
                clearCursorError(cursor);
                if (usePreciseCursorErrorStackTraces) {
                    throw (CursorExceptionWithPreciseStackTrace) error;
                } else {
                    throw new CursorException((String) error);
                }
            }
            cursor = cursor.linkedCursor;
        } while (cursor != null);
    }

    @Override
    public void clearCursorException() {
        clearCursorError(this);
    }

    private static void clearCursorError(MuninnPageCursor cursor) {
        while (cursor != null) {
            cursor.cursorException = null;
            cursor = cursor.linkedCursor;
        }
    }

    @Override
    public void setCursorException(String message) {
        Objects.requireNonNull(message);
        if (usePreciseCursorErrorStackTraces) {
            this.cursorException = new CursorExceptionWithPreciseStackTrace(message);
        } else {
            this.cursorException = message;
        }
    }

    @Override
    public void zapPage() {
        if (pageSize == 0) {
            // if this page has been closed then pageSize == 0 and we must adhere to making writes
            // trigger outOfBounds when closed
            outOfBounds = true;
        } else {
            UnsafeUtil.setMemory(pointer, pageSize, (byte) 0);
        }
    }

    @Override
    public boolean isWriteLocked() {
        return isFlagRaised(pf_flags, PF_SHARED_WRITE_LOCK);
    }

    @VisibleForTesting
    public long lastTxModifierId() {
        long pageRef = pinnedPageRef;
        Preconditions.checkState(pageRef != 0, "Cursor is closed.");
        return PageList.getLastModifiedTxId(pageRef);
    }

    abstract long lockStamp();

    public void unmapSnapshot() {
        var remappedState = resetSnapshot();
        if (remappedState != null) {
            remappedState.close();
        }
    }

    public VersionState resetSnapshot() {
        var remappedState = versionState;
        if (remappedState != null) {
            restoreState(remappedState);
            versionState = null;
        }
        return remappedState;
    }

    protected void restoreState(VersionState remappedState) {
        this.pinnedPageRef = remappedState.pinnedPageRef;
        this.version = remappedState.version;
        this.pointer = remappedState.pointer;
    }

    public void remapSnapshot(MuninnPageCursor cursor) {
        // unmap any previous state that we can have in the middle of should retry loops, we only need to close it
        // we do not need to close state here since the only way we're replacing some state is inside retry loop
        resetSnapshot();
        versionState = new VersionState(pinnedPageRef, version, pointer, lockStamp(), cursor);
        pinnedPageRef = cursor.pinnedPageRef;
        version = cursor.version;
        pointer = cursor.pointer;
    }

    record VersionState(long pinnedPageRef, long version, long pointer, long lockStamp, MuninnPageCursor cursor)
            implements AutoCloseable {
        @Override
        public void close() {
            cursor.close();
        }
    }

    @VisibleForTesting
    public int getPageSize() {
        return pageSize;
    }

    @VisibleForTesting
    public int getPayloadSize() {
        return payloadSize;
    }

    @Override
    public ByteOrder getByteOrder() {
        return littleEndian ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
    }
}
