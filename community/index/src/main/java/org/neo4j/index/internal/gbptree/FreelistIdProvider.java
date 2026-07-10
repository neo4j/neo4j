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
package org.neo4j.index.internal.gbptree;

import static org.neo4j.index.internal.gbptree.PointerChecking.checkOutOfBounds;
import static org.neo4j.io.pagecache.PageCursorUtil.goTo;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;
import org.eclipse.collections.api.factory.primitive.LongLists;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageCursorUtil;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContext;

class FreelistIdProvider implements IdProvider {
    private static final int CACHE_SIZE = 30;

    interface Monitor {
        /**
         * Called when a page id was acquired for storing released ids into.
         *
         * @param freelistPageId page id of the acquired page.
         */
        default void acquiredFreelistPageId(long freelistPageId) { // Empty by default
        }

        /**
         * Called when a free-list page was released due to all its ids being acquired.
         * A released free-list page ends up in the free-list itself.
         *
         * @param freelistPageId page if of the released page.
         */
        default void releasedFreelistPageId(long freelistPageId) { // Empty by default
        }
    }

    static final FreelistIdProvider.Monitor NO_MONITOR = new FreelistIdProvider.Monitor() { // Empty
            };

    /**
     * {@link FreelistNode} governs physical layout of a free-list.
     */
    private final FreelistNode freelistNode;

    /**
     * There's one free-list which both stable and unstable state (the state pages A/B) shares.
     * Each free list page links to a potential next free-list page, by using the last entry containing
     * page id to the next.
     * <p>
     * Each entry in the free list consist of a page id and the generation in which it was freed.
     * <p>
     * Read pointer cannot go beyond entries belonging to stable generation.
     * About the free-list id/offset variables below:
     * <pre>
     * Every cell in picture contains generation, page id is omitted for briefness.
     * StableGeneration   = 1
     * UnstableGeneration = 2
     *
     *        readMetaData.pos                writeMetaData.pos
     *        v                               v
     *  ┌───┬───┬───┬───┬───┬───┐   ┌───┬───┬───┬───┬───┬───┐
     *  │ 1 │ 1 │ 1 │ 2 │ 2 │ 2 │-->│ 2 │ 2 │   │   │   │   │
     *  └───┴───┴───┴───┴───┴───┘   └───┴───┴───┴───┴───┴───┘
     *  ^                           ^
     *  readMetaData.pageId          writeMetaData.pageId}
     * </pre>
     */
    private volatile ListHeadMetaData readMetaData;

    private volatile ListHeadMetaData writeMetaData;

    /**
     * Last allocated page id, used for allocating new ids as more data gets inserted into the tree.
     */
    private final AtomicLong lastId = new AtomicLong();

    private final PagedFile pagedFile;

    /**
     * For monitoring internal free-list activity.
     */
    private final Monitor monitor;

    /**
     * A small cache containing zero or more freed IDs, i.e. IDs that have been released and check-pointed.
     * The read cache will never need to interact with the write cache because of the generations where an entry written in one generation
     * cannot be used within that same generation anyway.
     * <p>
     * Reading released entries into the cache moves the readMetaData.pos, like so:
     * <pre>
     *
     *  Before cached:
     *
     *     readMetaData.pos
     *     v
     *  ┌───┬───┬───┬───┬───┬───┐
     *  │ 1 │ 1 │ 1 │ 2 │ 2 │ 2 │
     *  └───┴───┴───┴───┴───┴───┘
     *
     *  After cached:
     *
     *                readMetaData.pos
     *                v
     *  ┌───┬───┬───┬───┬───┬───┐
     *  │ 1 │ 1 │ 1 │ 2 │ 2 │ 2 │
     *  └───┴───┴───┴───┴───┴───┘
     *   ^    ^   ^
     *   └────┴───┴── > cached
     * <pre/>
     */
    private final ConcurrentLinkedDeque<FreelistEntry> acquireCache = new ConcurrentLinkedDeque<>();

    private final ConcurrentLinkedDeque<Long> releaseCache = new ConcurrentLinkedDeque<>();
    private volatile boolean mayBeMoreToReadIntoCache;
    private final AtomicBoolean exclusiveAccessMode = new AtomicBoolean();

    FreelistIdProvider(PagedFile pagedFile) {
        this(pagedFile, NO_MONITOR);
    }

    FreelistIdProvider(PagedFile pagedFile, Monitor monitor) {
        this.pagedFile = pagedFile;
        this.monitor = monitor;
        this.freelistNode = new FreelistNode(pagedFile.payloadSize());
    }

    void initialize(long lastId, long writePageId, long readPageId, int writePos, int readPos) {
        this.lastId.set(lastId);
        this.writeMetaData = new ListHeadMetaData(writePageId, writePos);
        this.readMetaData = new ListHeadMetaData(readPageId, readPos);
        this.mayBeMoreToReadIntoCache = true;
    }

    void initializeAfterCreation(CursorCreator cursorCreator, long lastId) throws IOException {
        // Allocate a new free-list page id and set both write/read free-list page id to it.
        this.lastId.set(lastId);
        initializeNewFreelist(cursorCreator, lastId);
    }

    private void initializeNewFreelist(CursorCreator cursorCreator, long lastId) throws IOException {
        writeMetaData = new ListHeadMetaData(lastId, 0);
        readMetaData = new ListHeadMetaData(lastId, 0);
        mayBeMoreToReadIntoCache = false;

        try (var cursor = cursorCreator.create()) {
            goTo(cursor, "free-list", lastId);
            FreelistNode.initialize(cursor);
            checkOutOfBounds(cursor);
        }
    }

    @Override
    public long acquireNewId(long stableGeneration, CursorCreator cursorCreator, CursorContext cursorContext)
            throws IOException {
        long acquiredId = acquireNewIdFromFreelistOrEnd(stableGeneration, cursorCreator);
        try (var cursor =
                pagedFile.io(acquiredId, PagedFile.PF_SHARED_WRITE_LOCK | PagedFile.PF_NO_LOAD, cursorContext)) {
            zapPage(acquiredId, cursor);
        }
        return acquiredId;
    }

    private static void zapPage(long acquiredId, PageCursor cursor) throws IOException {
        // Zap the page, i.e. set all bytes to zero
        goTo(cursor, "newly allocated free-list page", acquiredId);
        cursor.zapPage();
    }

    private void fillAcquireCache(long stableGeneration, CursorCreator cursorCreator) throws IOException {
        if (!mayBeMoreToReadIntoCache) {
            return;
        }

        boolean moreAfterThis = false;
        ListHeadMetaData writeMetaDataSnapshot = this.writeMetaData;
        long readPageId = readMetaData.pageId;
        int readPos = readMetaData.pos;
        try (var cursor = cursorCreator.create()) {
            while (readPageId != writeMetaDataSnapshot.pageId || readPos < writeMetaDataSnapshot.pos) {
                // It looks like reader isn't even caught up to the writer page-wise,
                // or the read pos is < write pos so check if we can grab the next id (generation could still mismatch).
                goTo(cursor, "Free-list read page ", readPageId);
                PointerWithGeneration resultPageId = freelistNode.read(cursor, stableGeneration, readPos);
                if (resultPageId.pointer() != FreelistNode.NO_PAGE_ID) {
                    FreelistEntry entry =
                            new FreelistEntry(readPageId, readPos, resultPageId.pointer(), resultPageId.generation());

                    // FreelistNode compares generation and so this means that we have an available
                    // id in the free list which we can acquire from a stable generation. Increment readPos
                    readPos++;
                    if (readPos >= freelistNode.maxEntries()) {
                        // The current reader page is exhausted, go to the next free-list page.
                        readPos = 0;
                        readPageId = FreelistNode.next(cursor);
                    }
                    acquireCache.addLast(entry);
                    if (acquireCache.size() >= CACHE_SIZE) {
                        moreAfterThis = true;
                        break;
                    }
                } else {
                    break;
                }
            }
        }
        this.readMetaData = new ListHeadMetaData(readPageId, readPos);
        mayBeMoreToReadIntoCache = moreAfterThis;
    }

    private synchronized long acquireNewIdFromFreelistOrEnd(long stableGeneration, CursorCreator cursorCreator)
            throws IOException {
        do {
            FreelistEntry entry = acquireCache.poll();
            if (entry != null) {
                if (entry.pos == freelistNode.maxEntries() - 1) {
                    queueReleasedId(entry.freelistPageId);
                }
                if (entry.id <= lastId.get()) {
                    return entry.id;
                }
            }
            fillAcquireCache(stableGeneration, cursorCreator);
        } while (mayBeMoreToReadIntoCache || !acquireCache.isEmpty());
        return nextLastId();
    }

    private long nextLastId() {
        return lastId.incrementAndGet();
    }

    @Override
    public void releaseId(long stableGeneration, long unstableGeneration, long id, CursorCreator cursorCreator)
            throws IOException {
        queueReleasedId(id);
        if (releaseCache.size() >= CACHE_SIZE && !exclusiveAccessMode.get()) {
            flush(stableGeneration, unstableGeneration, cursorCreator);
        }
    }

    private void queueReleasedId(long id) {
        releaseCache.addLast(id);
        monitor.releasedFreelistPageId(id);
    }

    private synchronized void flushReleaseCache(
            Deque<Long> releaseCache, long unstableGeneration, CursorCreator cursorCreator, IdSupplier idSupplier)
            throws IOException {
        if (releaseCache.isEmpty()) {
            return;
        }

        long writePageId = writeMetaData.pageId;
        int writePos = writeMetaData.pos;
        long lastId = this.lastId.get();
        try (var cursor = cursorCreator.create()) {
            Long id;
            while ((id = releaseCache.poll()) != null) {
                if (id > lastId) {
                    continue;
                }

                PageCursorUtil.goTo(cursor, "free-list write page", writePageId);
                freelistNode.write(cursor, unstableGeneration, id, writePos);
                writePos++;

                if (writePos >= freelistNode.maxEntries()) {
                    // Current free-list write page is full, allocate a new one.
                    long nextFreelistPage = idSupplier.get(cursor);
                    FreelistNode.setNext(cursor, nextFreelistPage);
                    PageCursorUtil.goTo(cursor, "free-list write page", nextFreelistPage);
                    FreelistNode.initialize(cursor);
                    // Link previous --> new writer page
                    writePageId = nextFreelistPage;
                    writePos = 0;
                    monitor.acquiredFreelistPageId(nextFreelistPage);
                }
            }
        }
        // Install the new write meta-data, both of those fields atomically, to potential concurrent readers
        writeMetaData = new ListHeadMetaData(writePageId, writePos);
        mayBeMoreToReadIntoCache = true;
    }

    void flush(long stableGeneration, long unstableGeneration, CursorCreator cursorCreator) throws IOException {
        flush(stableGeneration, unstableGeneration, cursorCreator, ignored -> {});
    }

    void flush(
            long stableGeneration,
            long unstableGeneration,
            CursorCreator cursorCreator,
            LongConsumer acquiredIdsMonitor)
            throws IOException {
        flushReleaseCache(releaseCache, unstableGeneration, cursorCreator, cursor -> {
            long id = acquireNewId(stableGeneration, CursorCreator.bind(cursor), CursorContext.NULL_CONTEXT);
            acquiredIdsMonitor.accept(id);
            return id;
        });
    }

    @Override
    public void visitFreelist(IdProviderVisitor visitor, CursorCreator cursorCreator) throws IOException {
        ListHeadMetaData readMetaData = this.readMetaData;
        long pageId = readMetaData.pageId;
        int pos = readMetaData.pos;
        FreelistEntry cachedEntry = acquireCache.peek();
        if (cachedEntry != null) {
            pageId = cachedEntry.freelistPageId;
            pos = cachedEntry.pos;
        }

        if (pageId == FreelistNode.NO_PAGE_ID) {
            return;
        }

        try (var cursor = cursorCreator.create()) {
            long prevPage;
            ListHeadMetaData writeMetaDataSnapshot = this.writeMetaData;
            do {
                PageCursorUtil.goTo(cursor, "free-list", pageId);
                visitor.beginFreelistPage(pageId);
                int targetPos =
                        pageId == writeMetaDataSnapshot.pageId ? writeMetaDataSnapshot.pos : freelistNode.maxEntries();
                while (pos < targetPos) {
                    // Read next un-acquired id
                    PointerWithGeneration unacquiredId;
                    do {
                        unacquiredId = freelistNode.read(cursor, Long.MAX_VALUE, pos);
                    } while (cursor.shouldRetry());
                    if (unacquiredId.pointer() <= lastId.get()) {
                        // Since the file can shrink, just ignore freed ids that are beyond the last id.
                        visitor.freelistEntry(unacquiredId.pointer(), unacquiredId.generation(), pos);
                    }
                    pos++;
                }
                visitor.endFreelistPage(pageId);

                prevPage = pageId;
                pos = 0;
                do {
                    pageId = FreelistNode.next(cursor);
                } while (cursor.shouldRetry());
            } while (prevPage != writeMetaDataSnapshot.pageId);
        }

        // Include the release cache
        releaseCache.forEach(visitor::freelistEntryFromReleaseCache);
    }

    @Override
    public long lastId() {
        return lastId.get();
    }

    FreelistMetaData metaData() {
        // Note: this can return write meta-data for unwritten released ids. The caller is supposed to handle flushing
        // vs. calling this method
        // for various purposes, e.g. writing meta-data state page etc.
        long lastId = this.lastId.get();
        long writePageId = writeMetaData.pageId;
        long readPageId = readMetaData.pageId;
        int writePos = writeMetaData.pos;
        int readPos = readMetaData.pos;

        FreelistEntry acquireCacheEntry = acquireCache.peek();
        if (acquireCacheEntry != null) {
            readPageId = acquireCacheEntry.freelistPageId;
            readPos = acquireCacheEntry.pos;
        }

        return new FreelistMetaData(lastId, writePageId, readPageId, writePos, readPos);
    }

    @Override
    public ExclusiveAccessMode exclusiveAccess() {
        if (!exclusiveAccessMode.compareAndSet(false, true)) {
            throw new IllegalStateException("Already in exclusive-access mode");
        }

        return new ExclusiveAccessMode() {
            @Override
            public void shrink(long numberOfPages) {
                releaseAcquireCache();
                lastId.addAndGet(-numberOfPages);
            }

            @Override
            public RewriteResult rewrite(
                    CursorCreator cursorCreator,
                    long belowId,
                    long stableGeneration,
                    long unstableGeneration,
                    CursorContext cursorContext)
                    throws IOException {
                releaseAcquireCache();
                flush(stableGeneration, unstableGeneration, cursorCreator);

                CheckAvailabilityVisitor checkAvailabilityVisitor =
                        new CheckAvailabilityVisitor(belowId, stableGeneration);
                visitFreelist(checkAvailabilityVisitor, cursorCreator);
                long numNewFreelistPagesRequired = calculateNumRequiredFreelistPages(
                        checkAvailabilityVisitor.numEntriesInFreelist, checkAvailabilityVisitor.freelistPageIds.size());
                if (checkAvailabilityVisitor.numAvailableIdsBelowId < numNewFreelistPagesRequired) {
                    // There's no point in rewriting the freelist since the rewritten freelist wouldn't be able
                    // to satisfy the constraint of residing below the given id.
                    return null;
                }

                RewriteVisitor rewriteVisitor = new RewriteVisitor(
                        stableGeneration,
                        unstableGeneration,
                        belowId,
                        numNewFreelistPagesRequired,
                        cursorCreator,
                        cursorContext,
                        checkAvailabilityVisitor.freelistPageIds);
                visitFreelist(rewriteVisitor, cursorCreator);
                rewriteVisitor.flush();
                return new RewriteResult(
                        checkAvailabilityVisitor.freelistPageIds.toArray(),
                        rewriteVisitor.idsForNewFreelistPages.toArray());
            }

            @Override
            public void close() {
                exclusiveAccessMode.set(false);
            }
        };
    }

    private long calculateNumRequiredFreelistPages(long numEntriesInFreelist, long numFreelistPageIds) {
        // We need space for all the entries plus the to-be-old freelist page IDs
        long numEntries = numEntriesInFreelist + numFreelistPageIds;

        // This padding exists because of the way that freelist pages themselves are allocated from the freelist.
        // So if there's exactly mod maxEntries+1 (1 for the freelist page and maxEntries for the items)
        // then writing the last entry would trigger a new freelist page to be allocated
        // (which we wouldn't have had set aside a page for) - otoh if we set aside one more that means that
        // there's now one less id to write and therefor the new freelist page allocation will not happen.
        // So, we set aside one additional page which will be freed back into the new freelist in the end.
        int padding = numEntries > 0 && (numEntries % (freelistNode.maxEntries() + 1)) == 0 ? 1 : 0;

        // Though some of these IDs will be used for freelist pages, so subtract those
        return (numEntries - 1) / (freelistNode.maxEntries() + 1) + 1 + padding;
    }

    private void releaseAcquireCache() {
        FreelistEntry first = acquireCache.poll();
        if (first != null) {
            this.readMetaData = new ListHeadMetaData(first.freelistPageId, first.pos);
        }
        acquireCache.clear();
    }

    // test-access method
    int entriesPerPage() {
        return freelistNode.maxEntries();
    }

    record FreelistMetaData(long lastId, long writePageId, long readPageId, int writePos, int readPos) {}

    private record ListHeadMetaData(long pageId, int pos) {}

    private interface IdSupplier {
        long get(PageCursor cursor) throws IOException;
    }

    private static class CheckAvailabilityVisitor extends IdProviderVisitor.Adaptor {
        private final MutableLongList freelistPageIds = LongLists.mutable.empty();
        private final long belowId;
        private final long stableGeneration;
        private long numAvailableIdsBelowId;
        private long numEntriesInFreelist;

        public CheckAvailabilityVisitor(long belowId, long stableGeneration) {
            this.belowId = belowId;
            this.stableGeneration = stableGeneration;
        }

        @Override
        public void beginFreelistPage(long pageId) {
            freelistPageIds.add(pageId);
        }

        @Override
        public void freelistEntry(long pageId, long generation, int pos) {
            numEntriesInFreelist++;
            if (pageId < belowId && generation <= stableGeneration) {
                numAvailableIdsBelowId++;
            }
        }
    }

    private class RewriteVisitor extends IdProviderVisitor.Adaptor {
        private final MutableLongList idsForNewFreelistPages;
        private final LinkedList<FreelistEntry> tempEntries = new LinkedList<>();
        private final LinkedList<Long> batchOfIdsToRelease = new LinkedList<>();
        private final long stableGeneration;
        private final long unstableGeneration;
        private final long belowId;
        private final long numNewFreelistPagesRequired;
        private final CursorCreator cursorCreator;
        private final CursorContext cursorContext;
        private final MutableLongList freelistPageIds;
        private LongIterator idsForNewFreelistPagesIterator;
        private boolean freelistInitialized;

        public RewriteVisitor(
                long stableGeneration,
                long unstableGeneration,
                long belowId,
                long numNewFreelistPagesRequired,
                CursorCreator cursorCreator,
                CursorContext cursorContext,
                MutableLongList freelistPageIds) {
            this.stableGeneration = stableGeneration;
            this.unstableGeneration = unstableGeneration;
            this.belowId = belowId;
            this.numNewFreelistPagesRequired = numNewFreelistPagesRequired;
            this.cursorCreator = cursorCreator;
            this.cursorContext = cursorContext;
            this.freelistPageIds = freelistPageIds;
            this.idsForNewFreelistPages = LongLists.mutable.empty();
        }

        @Override
        public void freelistEntry(long pageId, long generation, int pos) {
            assert generation <= stableGeneration;
            try {
                if (!freelistInitialized) {
                    if (pageId < belowId) {
                        idsForNewFreelistPages.add(pageId);
                        if (idsForNewFreelistPages.size() == numNewFreelistPagesRequired) {
                            idsForNewFreelistPagesIterator = idsForNewFreelistPages.longIterator();
                            initializeNewFreelist(cursorCreator, idsForNewFreelistPagesIterator.next());
                            while (!tempEntries.isEmpty()) {
                                FreelistEntry entry = tempEntries.poll();
                                writeFreeId(entry.id, entry.generation);
                            }
                            freelistInitialized = true;
                        }
                    } else {
                        tempEntries.add(new FreelistEntry(-1, pos, pageId, generation));
                    }
                } else {
                    assert tempEntries.isEmpty();
                    writeFreeId(pageId, generation);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        void flush() throws IOException {
            // Write the remaining free IDs with the correct generation
            batchOfIdsToRelease.sort(Long::compareTo);
            flushReleaseCache(
                    batchOfIdsToRelease, stableGeneration, cursorCreator, c -> idsForNewFreelistPagesIterator.next());
            batchOfIdsToRelease.clear();

            // Write the IDs of the old freelist pages
            MutableLongIterator oldIds = freelistPageIds.longIterator();
            while (oldIds.hasNext()) {
                batchOfIdsToRelease.add(oldIds.next());
            }
            flushReleaseCache(
                    batchOfIdsToRelease, unstableGeneration, cursorCreator, c -> idsForNewFreelistPagesIterator.next());
            batchOfIdsToRelease.clear();

            // Write any remaining IDs that we previously reserved in anticipation of them being used for new
            // freelist pages. This happens rarely and basically comes from a one-off scenario occurring because
            // of how free IDs are also used for freelist pages to hold other free IDs.
            while (idsForNewFreelistPagesIterator.hasNext()) {
                batchOfIdsToRelease.add(idsForNewFreelistPagesIterator.next());
            }
            flushReleaseCache(batchOfIdsToRelease, unstableGeneration, cursorCreator, cursor -> {
                long freelistPageId = acquireNewId(stableGeneration, CursorCreator.bind(cursor), cursorContext);
                idsForNewFreelistPages.add(freelistPageId);
                return freelistPageId;
            });
        }

        private void writeFreeId(long id, long generation) throws IOException {
            // Here we can take the opportunity to sort the free IDs, and actually there's no real need to keep
            // the exact generation for generations <= stable
            boolean isUnstableGeneration = generation > stableGeneration;
            if (batchOfIdsToRelease.size() > 100_000) {
                batchOfIdsToRelease.sort(Long::compareTo);
                flushReleaseCache(
                        batchOfIdsToRelease,
                        stableGeneration,
                        cursorCreator,
                        c -> idsForNewFreelistPagesIterator.next());
                batchOfIdsToRelease.clear();
            }
            batchOfIdsToRelease.add(id);
        }
    }
}
