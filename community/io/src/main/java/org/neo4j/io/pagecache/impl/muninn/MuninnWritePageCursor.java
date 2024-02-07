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

import static org.neo4j.io.pagecache.impl.muninn.VersionStorage.NEXT_REFERENCE_OFFSET;
import static org.neo4j.util.FeatureToggles.flag;

import java.io.IOException;
import org.eclipse.collections.api.map.primitive.MutableLongLongMap;
import org.eclipse.collections.impl.factory.primitive.LongLongMaps;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.VersionContext;
import org.neo4j.io.pagecache.impl.FileIsNotMappedException;
import org.neo4j.io.pagecache.tracing.PinEvent;

final class MuninnWritePageCursor extends MuninnPageCursor {

    private static final long UNKNOWN_STAMP = -1;

    private static final MutableLongLongMap LOCKED_PAGES = flag(MuninnWritePageCursor.class, "CHECK_WRITE_LOCKS", false)
            ? LongLongMaps.mutable.empty().asSynchronized()
            : null;

    MuninnWritePageCursor(
            MuninnPagedFile pagedFile, int pf_flags, long victimPage, CursorContext cursorContext, long pageId) {
        super(pagedFile, pf_flags, victimPage, cursorContext, pageId);
    }

    @Override
    public void unpin() {
        long pageRef = pinnedPageRef;
        if (pageRef != 0) {
            tracer.unpin(loadPlainCurrentPageId(), swapper);

            // Mark the page as dirty *after* our write access, to make sure it's dirty even if it was concurrently
            // flushed. Unlocking the write-locked page will mark it as dirty for us.
            if (eagerFlush) {
                eagerlyFlushAndUnlockPage(pageRef);
            } else {
                unlockPage(pageRef);
            }
        }
        clearPageCursorState();
        storeCurrentPageId(UNBOUND_PAGE_ID);
    }

    private void eagerlyFlushAndUnlockPage(long pageRef) {
        long flushStamp = 0;
        if (multiVersioned) {
            // in multiversion case check if we last of the linked cursors who pin that page
            if (!isPinnedByLinkedFriends(pageRef)) {
                if (LOCKED_PAGES != null) {
                    // remove before unlock to avoid clearing others lock
                    var locker = LOCKED_PAGES.removeKeyIfAbsent(pageRef, -1);
                    var currentThread = Thread.currentThread().getId();
                    if (locker != currentThread) {
                        throw new IllegalStateException("Recorded locker of the page is " + locker
                                + " doesn't match current thread id " + currentThread);
                    }
                }
                flushStamp = PageList.unlockWriteAndTryTakeFlushLock(pageRef);
            }
        } else {
            flushStamp = PageList.unlockWriteAndTryTakeFlushLock(pageRef);
        }
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
        unpin();
        long lastPageId = assertCursorOpenFileMappedAndGetIdOfLastPage();
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
        if (multiVersioned) {
            if (isPinnedByLinkedFriends(pageRef)) {
                return true;
            }
            if (LOCKED_PAGES != null) {
                // you see we are not atomic or synchronized here, this is ok, because we care about *current* thread
                // already being successful in taking write lock on this page
                var locker = LOCKED_PAGES.getIfAbsent(pageRef, -1);
                var threadId = Thread.currentThread().getId();
                if (locker == threadId) {
                    throw new IllegalStateException(
                            "Multiversioned page locks are not reentrant unless it's from linked cursors. Other thread "
                                    + threadId + " already holds write lock on page " + pageRef);
                }
            }
            var writeLock = PageList.tryWriteLock(pageRef, true);
            if (LOCKED_PAGES != null && writeLock) {
                LOCKED_PAGES.put(pageRef, Thread.currentThread().getId());
            }
            return writeLock;
        }
        return PageList.tryWriteLock(pageRef, false);
    }

    private boolean isPinnedByLinkedFriends(long pageRef) {
        var backwardCursor = backLinkedCursor;
        while (backwardCursor != null) {
            if (backwardCursor.pinnedPageRef == pageRef) {
                return true;
            }
            backwardCursor = backwardCursor.backLinkedCursor;
        }
        var forwardCursor = linkedCursor;
        while (forwardCursor != null) {
            if (forwardCursor.pinnedPageRef == pageRef) {
                return true;
            }
            forwardCursor = forwardCursor.linkedCursor;
        }
        return false;
    }

    @Override
    protected void unlockPage(long pageRef) {
        if (multiVersioned) {
            // in multiversion case check if we last of the linked cursors who pin that page
            if (!isPinnedByLinkedFriends(pageRef)) {
                if (LOCKED_PAGES != null) {
                    // remove before unlock to avoid clearing others lock
                    var locker = LOCKED_PAGES.removeKeyIfAbsent(pageRef, -1);
                    var currentThread = Thread.currentThread().getId();
                    if (locker != currentThread) {
                        throw new IllegalStateException("Recorded locker of the page is " + locker
                                + " doesn't match current thread id " + currentThread);
                    }
                }
                PageList.unlockWrite(pageRef);
            }
        } else {
            PageList.unlockWrite(pageRef);
        }
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
        assertCursorOpenFileMappedAndGetIdOfLastPage();
        if (multiVersioned) {
            long pagePointer = pointer;
            long headVersion = getLongAt(pagePointer, littleEndian);
            if (isOldHead(versionContext, headVersion)) {
                long copyPageReference = versionStorage.createPageSnapshot(this, versionContext, headVersion, pinEvent);

                // update from current to next copied page
                putLongAt(pagePointer + NEXT_REFERENCE_OFFSET, copyPageReference, littleEndian);
                putLongAt(pagePointer, versionContext.committingTransactionId(), littleEndian);
            }
        } else if (contextVersionUpdates) {
            PageList.setLastModifiedTxId(pageRef, versionContext.committingTransactionId());
        }
    }

    private boolean isOldHead(VersionContext versionContext, long headVersion) {
        return headVersion != versionContext.committingTransactionId();
    }

    @Override
    long lockStamp() {
        return UNKNOWN_STAMP;
    }

    @Override
    protected void convertPageFaultLock(long pageRef) {
        PageList.unlockExclusiveAndTakeWriteLock(pageRef);
        if (LOCKED_PAGES != null && multiVersioned) {
            LOCKED_PAGES.put(pageRef, Thread.currentThread().getId());
        }
    }

    @Override
    public void setPageHorizon(long horizon) {
        if (multiVersioned && pinnedPageRef != 0) {
            PageList.setPageHorizon(pinnedPageRef, horizon);
        }
    }

    @Override
    public boolean shouldRetry() {
        // We take exclusive locks, so there's never a need to retry.
        return false;
    }

    @Override
    public boolean retrySnapshot() {
        return false;
    }
}
