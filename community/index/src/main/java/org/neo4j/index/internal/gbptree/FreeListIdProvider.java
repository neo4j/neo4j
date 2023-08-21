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
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageCursorUtil;

class FreeListIdProvider implements IdProvider {
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

    static final Monitor NO_MONITOR = new Monitor() { // Empty
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
     * Each entry in the the free list consist of a page id and the generation in which it was freed.
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

    FreeListIdProvider(int payloadSize) {
        this(payloadSize, NO_MONITOR);
    }

    FreeListIdProvider(int payloadSize, Monitor monitor) {
        this.monitor = monitor;
        this.freelistNode = new FreelistNode(payloadSize);
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
        writeMetaData = new ListHeadMetaData(lastId, 0);
        long pageId = writeMetaData.pageId;
        readMetaData = new ListHeadMetaData(pageId, 0);
        mayBeMoreToReadIntoCache = false;

        try (var cursor = cursorCreator.create()) {
            goTo(cursor, "free-list", pageId);
            FreelistNode.initialize(cursor);
            checkOutOfBounds(cursor);
        }
    }

    @Override
    public long acquireNewId(long stableGeneration, long unstableGeneration, CursorCreator cursorCreator)
            throws IOException {
        try (var cursor = cursorCreator.create()) {
            long acquiredId = acquireNewIdFromFreelistOrEnd(stableGeneration, cursor);
            zapPage(acquiredId, cursor);
            return acquiredId;
        }
    }

    private static void zapPage(long acquiredId, PageCursor cursor) throws IOException {
        // Zap the page, i.e. set all bytes to zero
        goTo(cursor, "newly allocated free-list page", acquiredId);
        cursor.zapPage();
    }

    private synchronized void fillAcquireCache(long stableGeneration, PageCursor cursor) throws IOException {
        if (!mayBeMoreToReadIntoCache) {
            return;
        }

        boolean moreAfterThis = false;
        GenerationKeeper generationKeeper = new GenerationKeeper();
        ListHeadMetaData writeMetaDataSnapshot = this.writeMetaData;
        long readPageId = readMetaData.pageId;
        int readPos = readMetaData.pos;
        while (readPageId != writeMetaDataSnapshot.pageId || readPos < writeMetaDataSnapshot.pos) {
            // It looks like reader isn't even caught up to the writer page-wise,
            // or the read pos is < write pos so check if we can grab the next id (generation could still mismatch).
            goTo(cursor, "Free-list read page ", readPageId);
            long resultPageId = freelistNode.read(cursor, stableGeneration, readPos, generationKeeper);
            if (resultPageId != FreelistNode.NO_PAGE_ID) {
                FreelistEntry entry = new FreelistEntry(readPageId, readPos, resultPageId, generationKeeper.generation);

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
        this.readMetaData = new ListHeadMetaData(readPageId, readPos);
        mayBeMoreToReadIntoCache = moreAfterThis;
    }

    private long acquireNewIdFromFreelistOrEnd(long stableGeneration, PageCursor cursor) throws IOException {
        do {
            FreelistEntry entry = acquireCache.poll();
            if (entry != null) {
                if (entry.pos == freelistNode.maxEntries() - 1) {
                    queueReleasedId(entry.freelistPageId);
                }
                return entry.id;
            }
            fillAcquireCache(stableGeneration, cursor);
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
        if (releaseCache.size() >= CACHE_SIZE) {
            flushReleaseCache(stableGeneration, unstableGeneration, cursorCreator);
        }
    }

    private void queueReleasedId(long id) {
        releaseCache.addLast(id);
        monitor.releasedFreelistPageId(id);
    }

    private synchronized void flushReleaseCache(
            long stableGeneration, long unstableGeneration, CursorCreator cursorCreator) throws IOException {
        if (releaseCache.isEmpty()) {
            return;
        }

        long writePageId = writeMetaData.pageId;
        int writePos = writeMetaData.pos;
        try (var cursor = cursorCreator.create()) {
            Long id;
            while ((id = releaseCache.poll()) != null) {
                PageCursorUtil.goTo(cursor, "free-list write page", writePageId);
                freelistNode.write(cursor, unstableGeneration, id, writePos);
                writePos++;

                if (writePos >= freelistNode.maxEntries()) {
                    // Current free-list write page is full, allocate a new one.
                    long nextFreelistPage =
                            acquireNewId(stableGeneration, unstableGeneration, CursorCreator.bind(cursor));
                    PageCursorUtil.goTo(cursor, "free-list write page", writePageId);
                    FreelistNode.initialize(cursor);
                    // Link previous --> new writer page
                    FreelistNode.setNext(cursor, nextFreelistPage);
                    writePageId = nextFreelistPage;
                    writePos = 0;
                    monitor.acquiredFreelistPageId(nextFreelistPage);
                }
            }
        }
        // Install the new write meta data, both of those fields atomically, to potential concurrent readers
        writeMetaData = new ListHeadMetaData(writePageId, writePos);
        mayBeMoreToReadIntoCache = true;
    }

    void flush(long stableGeneration, long unstableGeneration, CursorCreator cursorCreator) throws IOException {
        flushReleaseCache(stableGeneration, unstableGeneration, cursorCreator);
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
            GenerationKeeper generation = new GenerationKeeper();
            long prevPage;
            ListHeadMetaData writeMetaDataSnapshot;
            do {
                PageCursorUtil.goTo(cursor, "free-list", pageId);
                visitor.beginFreelistPage(pageId);
                writeMetaDataSnapshot = this.writeMetaData;
                int targetPos =
                        pageId == writeMetaDataSnapshot.pageId ? writeMetaDataSnapshot.pos : freelistNode.maxEntries();
                while (pos < targetPos) {
                    // Read next un-acquired id
                    long unacquiredId;
                    do {
                        unacquiredId = freelistNode.read(cursor, Long.MAX_VALUE, pos, generation);
                    } while (cursor.shouldRetry());
                    visitor.freelistEntry(unacquiredId, generation.generation, pos);
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
        // Note: this can return write meta data for unwritten released ids. The caller is supposed to handle flushing
        // vs. calling this method
        // for various purposes, e.g. writing meta data state page etc.
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

    // test-access method
    int entriesPerPage() {
        return freelistNode.maxEntries();
    }

    record FreelistMetaData(long lastId, long writePageId, long readPageId, int writePos, int readPos) {}

    private record ListHeadMetaData(long pageId, int pos) {}
}
