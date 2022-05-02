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

import java.io.IOException;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.impl.FileIsNotMappedException;
import org.neo4j.io.pagecache.tracing.PinEvent;

final class MuninnWritePageCursor extends MuninnPageCursor {
    MuninnWritePageCursor(long victimPage, CursorContext cursorContext) {
        super(victimPage, cursorContext);
    }

    @Override
    protected void unpinCurrentPage() {
        long pageRef = pinnedPageRef;
        if (pageRef != 0) {
            tracer.unpin(loadPlainCurrentPageId(), swapper);
            // Mark the page as dirty *after* our write access, to make sure it's dirty even if it was concurrently
            // flushed. Unlocking the write-locked page will mark it as dirty for us.
            if (eagerFlush) {
                eagerlyFlushAndUnlockPage(pageRef);
            } else {
                PageList.unlockWrite(pageRef);
            }
        }
        clearPageCursorState();
    }

    private void eagerlyFlushAndUnlockPage(long pageRef) {
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
        return PageList.tryWriteLock(pageRef, versioned);
    }

    @Override
    protected void unlockPage(long pageRef) {
        PageList.unlockWrite(pageRef);
    }

    @Override
    protected void pinCursorToPage(PinEvent pinEvent, long pageRef, long filePageId, PageSwapper swapper)
            throws FileIsNotMappedException {
        reset(pinEvent, pageRef);
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
        PageList.setLastModifiedTxId(pageRef, versionContext.committingTransactionId());
    }

    @Override
    protected void convertPageFaultLock(long pageRef) {
        PageList.unlockExclusiveAndTakeWriteLock(pageRef);
    }

    @Override
    public boolean shouldRetry() {
        // We take exclusive locks, so there's never a need to retry.
        return false;
    }
}
