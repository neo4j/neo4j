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

import static org.neo4j.io.pagecache.context.TransactionIdSnapshot.isNotVisible;

import java.io.IOException;
import org.neo4j.io.pagecache.PageSwapper;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PinEvent;

final class MuninnReadPageCursor extends MuninnPageCursor {
    long lockStamp;

    MuninnReadPageCursor(
            MuninnPagedFile pagedFile, int pf_flags, long victimPage, CursorContext cursorContext, long pageId) {
        super(pagedFile, pf_flags, victimPage, cursorContext, pageId);
    }

    @Override
    public void unpin() {
        if (versionState != null) {
            unmapSnapshot();
        }
        if (pinnedPageRef != 0) {
            tracer.unpin(loadPlainCurrentPageId(), swapper);
        }
        lockStamp = 0; // make sure not to accidentally keep a lock state around
        clearPageCursorState();
        storeCurrentPageId(UNBOUND_PAGE_ID);
    }

    @Override
    public boolean next() throws IOException {
        unpin();
        long lastPageId = assertCursorOpenFileMappedAndGetIdOfLastPage();
        if (nextPageId > lastPageId || nextPageId < 0) {
            storeCurrentPageId(UNBOUND_PAGE_ID);
            return false;
        }
        storeCurrentPageId(nextPageId);
        nextPageId++;
        long filePageId = loadPlainCurrentPageId();
        try (var pinEvent = tracer.beginPin(false, filePageId, swapper)) {
            pin(pinEvent, filePageId);
        }
        verifyContext();
        return true;
    }

    @Override
    protected boolean tryLockPage(long pageRef) {
        lockStamp = PageList.tryOptimisticReadLock(pageRef);
        return true;
    }

    @Override
    protected void unlockPage(long pageRef) {}

    @Override
    protected void pinCursorToPage(PinEvent pinEvent, long pageRef, long filePageId, PageSwapper swapper) {
        init(pinEvent, pageRef);
        if (multiVersioned) {
            long pagePointer = pointer;
            version = getLongAt(pagePointer, littleEndian);
            versionContext.observedChainHead(version);
            if (shouldLoadSnapshot(version)) {
                versionContext.markHeadInvisible();
                if (chainFollow) {
                    versionStorage.loadReadSnapshot(this, versionContext, pinEvent);
                }
            }
        }
    }

    private boolean shouldLoadSnapshot(long pageVersion) {
        return pageVersion != versionContext.committingTransactionId()
                && (pageVersion > versionContext.highestClosed()
                        || isNotVisible(versionContext.notVisibleTransactionIds(), pageVersion));
    }

    @Override
    protected void convertPageFaultLock(long pageRef) {
        lockStamp = PageList.unlockExclusive(pageRef);
    }

    @Override
    public void remapSnapshot(MuninnPageCursor cursor) {
        super.remapSnapshot(cursor);
        lockStamp = cursor.lockStamp();
    }

    @Override
    protected void restoreState(VersionState remappedState) {
        super.restoreState(remappedState);
        lockStamp = remappedState.lockStamp();
    }

    @Override
    public boolean shouldRetry() throws IOException {
        MuninnReadPageCursor cursor = this;
        do {
            long pageRef = cursor.pinnedPageRef;
            if (pageRef != 0 && !PageList.validateReadLock(pageRef, cursor.lockStamp)) {
                assertCursorOpenFileMappedAndGetIdOfLastPage();
                startRetryLinkedChain();
                return true;
            }
            cursor = (MuninnReadPageCursor) cursor.linkedCursor;
        } while (cursor != null);
        return false;
    }

    @Override
    public boolean retrySnapshot() {
        MuninnReadPageCursor cursor = this;
        do {
            long pageRef = cursor.pinnedPageRef;
            if (pageRef != 0 && !PageList.validateReadLock(pageRef, cursor.lockStamp)) {
                return true;
            }
            cursor = (MuninnReadPageCursor) cursor.linkedCursor;
        } while (cursor != null);
        return false;
    }

    private void startRetryLinkedChain() throws IOException {
        MuninnReadPageCursor cursor = this;
        do {
            cursor.unmapSnapshot();
            long pageRef = cursor.pinnedPageRef;
            if (pageRef != 0) {
                cursor.startRetry(pageRef);
            }
            cursor = (MuninnReadPageCursor) cursor.linkedCursor;
        } while (cursor != null);
    }

    private void startRetry(long pageRef) throws IOException {
        setOffset(0);
        checkAndClearBoundsFlag();
        clearCursorException();
        lockStamp = PageList.tryOptimisticReadLock(pageRef);
        // The page might have been evicted while we held the optimistic
        // read lock, so we need to check with page.pin that this is still
        // the page we're actually interested in:
        var filePageId = loadPlainCurrentPageId();
        if (!PageList.isBoundTo(pageRef, swapperId, filePageId) || multiVersioned) {
            // This is no longer the page we're interested in, so we have
            // to redo the pinning.
            // This might in turn lead to a new optimistic lock on a
            // different page if someone else has taken the page fault for
            // us. If nobody has done that, we'll take the page fault
            // ourselves, and in that case we'll end up with first an exclusive
            // lock during the faulting, and then an optimistic read lock once the
            // fault itself is over.
            // First, forget about this page in case pin() throws and the cursor
            // is closed, or in case we have PF_NO_FAULT and the page is no longer
            // in memory.
            clearPageReference();
            // trace unpin before trying pin again
            tracer.unpin(filePageId, swapper);
            // Then try pin again.
            try (var pinEvent = tracer.beginPin(false, filePageId, swapper)) {
                pin(pinEvent, filePageId);
            }
        }
    }

    @Override
    public void putByte(byte value) {
        throw new IllegalStateException("Cannot write to read-locked page");
    }

    @Override
    public void putLong(long value) {
        throw new IllegalStateException("Cannot write to read-locked page");
    }

    @Override
    public void putInt(int value) {
        throw new IllegalStateException("Cannot write to read-locked page");
    }

    @Override
    public void putBytes(byte[] data, int arrayOffset, int length) {
        throw new IllegalStateException("Cannot write to read-locked page");
    }

    @Override
    public void putShort(short value) {
        throw new IllegalStateException("Cannot write to read-locked page");
    }

    @Override
    public void shiftBytes(int sourceStart, int length, int shift) {
        throw new IllegalStateException("Cannot write to read-locked page");
    }

    @Override
    long lockStamp() {
        return lockStamp;
    }

    @Override
    public void zapPage() {
        throw new IllegalStateException("Cannot write to read-locked page");
    }

    @Override
    public void setPageHorizon(long horizon) {
        throw new IllegalStateException("Cannot mark read only page");
    }
}
