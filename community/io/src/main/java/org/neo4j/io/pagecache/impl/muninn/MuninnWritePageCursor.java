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

import static org.neo4j.util.FeatureToggles.flag;

import java.io.IOException;
import org.eclipse.collections.api.map.primitive.MutableLongLongMap;
import org.eclipse.collections.impl.factory.primitive.LongLongMaps;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.impl.FileIsNotMappedException;
import org.neo4j.io.pagecache.tracing.PinEvent;

final class MuninnWritePageCursor extends MuninnPageCursor {

    private static final MutableLongLongMap LOCKED_PAGES = flag(MuninnWritePageCursor.class, "CHECK_WRITE_LOCKS", false)
            ? LongLongMaps.mutable.empty().asSynchronized()
            : null;

    MuninnWritePageCursor(long victimPage, CursorContext cursorContext) {
        super(victimPage, cursorContext);
    }

    @Override
    protected void unpinCurrentPage() {
        long pageRef = pinnedPageRef;
        if (pageRef != 0) {
            tracer.unpin(loadPlainCurrentPageId(), swapper);

            if (multiVersioned) {
                updateChain(pageRef);
            }

            // Mark the page as dirty *after* our write access, to make sure it's dirty even if it was concurrently
            // flushed. Unlocking the write-locked page will mark it as dirty for us.
            if (eagerFlush) {
                eagerlyFlushAndUnlockPage(pageRef);
            } else {
                unlockPage(pageRef);
            }
        }
        clearPageCursorState();
    }

    private void eagerlyFlushAndUnlockPage(long pageRef) {
        if (LOCKED_PAGES != null && multiVersioned) {
            var locker = LOCKED_PAGES.removeKeyIfAbsent(pageRef, -1);
            if (locker != Thread.currentThread().getId()) {
                throw new IllegalStateException("oops");
            }
        }
        long flushStamp = PageList.unlockWriteAndTryTakeFlushLock(pageRef);
        if (flushStamp != 0) {
            boolean success = false;
            try {
                success = pagedFile.flushLockedPage(pageRef, loadPlainCurrentPageId());
            } finally {
                PageList.unlockFlush(pageRef, flushStamp, success);
            }
        }
    }

    @Override
    public boolean next() throws IOException {
        unpinCurrentPage();
        long lastPageId = assertPagedFileStillMappedAndGetIdOfLastPage();
        if (nextPageId < 0) {
            storeCurrentPageId(UNBOUND_PAGE_ID);
            return false;
        }
        if (nextPageId > lastPageId) {
            if (noGrow) {
                storeCurrentPageId(UNBOUND_PAGE_ID);
                return false;
            } else {
                pagedFile.increaseLastPageIdTo(nextPageId);
            }
        }
        storeCurrentPageId(nextPageId);
        nextPageId++;
        long filePageId = loadPlainCurrentPageId();
        try (var pinEvent = tracer.beginPin(true, filePageId, swapper)) {
            pin(pinEvent, filePageId);
        }
        return true;
    }

    @Override
    protected boolean tryLockPage(long pageRef) {
        if (LOCKED_PAGES != null && multiVersioned) {
            // you see we are not atomic or synchronized here, this is ok, because we care about *current* thread
            // already being successful in taking write lock on this page
            var locker = LOCKED_PAGES.getIfAbsent(pageRef, -1);
            var threadId = Thread.currentThread().getId();
            if (locker == threadId) {
                throw new IllegalStateException("Multiversioned page locks are not reentrant. Thread " + threadId
                        + " already holds write lock on page " + pageRef);
            }
        }
        var writeLock = PageList.tryWriteLock(pageRef, multiVersioned);
        if (LOCKED_PAGES != null && multiVersioned && writeLock) {
            LOCKED_PAGES.put(pageRef, Thread.currentThread().getId());
        }
        return writeLock;
    }

    @Override
    protected void unlockPage(long pageRef) {
        if (LOCKED_PAGES != null && multiVersioned) {
            // remove before unlock to avoid clearing others lock
            var locker = LOCKED_PAGES.removeKeyIfAbsent(pageRef, -1);
            if (locker != Thread.currentThread().getId()) {
                throw new IllegalStateException("oops");
            }
        }
        PageList.unlockWrite(pageRef);
    }

    @Override
    protected void pinCursorToPage(PinEvent pinEvent, long pageRef, long filePageId, PageSwapper swapper)
            throws FileIsNotMappedException {
        init(pinEvent, pageRef);
        // Check if we've been racing with unmapping. We want to do this before
        // we make any changes to the contents of the page, because once all
        // files have been unmapped, the page cache can be closed. And when
        // that happens, dirty contents in memory will no longer have a chance
        // to get flushed. It is okay for this method to throw, because we are
        // after the reset() call, which means that if we throw, the cursor will
        // be closed and the page lock will be released.
        assertPagedFileStillMappedAndGetIdOfLastPage();
        if (updateUsage) {
            PageList.incrementUsage(pageRef);
        }
        if (multiVersioned) {
            copyPage(pageRef);
        } else {
            PageList.setLastModifiedTxId(pageRef, versionContext.committingTransactionId());
        }
    }

    @Override
    protected void convertPageFaultLock(long pageRef) {
        PageList.unlockExclusiveAndTakeWriteLock(pageRef);
        if (LOCKED_PAGES != null && multiVersioned) {
            LOCKED_PAGES.put(pageRef, Thread.currentThread().getId());
        }
    }

    @Override
    public boolean shouldRetry() {
        // We take exclusive locks, so there's never a need to retry.
        return false;
    }
}
