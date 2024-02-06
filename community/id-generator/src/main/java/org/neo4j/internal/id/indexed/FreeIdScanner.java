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
package org.neo4j.internal.id.indexed;

import static org.neo4j.internal.id.IdUtils.combinedIdAndNumberOfIds;
import static org.neo4j.internal.id.IdUtils.idFromCombinedId;
import static org.neo4j.internal.id.IdUtils.numberOfIdsFromCombinedId;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.mutable.MutableInt;
import org.eclipse.collections.api.factory.primitive.LongLists;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.io.pagecache.context.CursorContext;

/**
 * Responsible for starting and managing scans of a {@link GBPTree}, populating a cache with free ids that gets discovered in the scan.
 * Ids which are placed into cache are also marked as reserved, i.e. not free anymore. This way those ids that were found in one "round"
 * of a scan will not be found in upcoming rounds and reserving ids becomes more of a batch operation.
 */
class FreeIdScanner {
    /**
     * Used as low bound of a new scan. Continuing a scan makes use of {@link #ongoingScanRangeIndex}.
     */
    private static final IdRangeKey LOW_KEY = new IdRangeKey(0);
    /**
     * Used as high bound for all scans.
     */
    private static final IdRangeKey HIGH_KEY = new IdRangeKey(Long.MAX_VALUE);

    static final int MAX_SLOT_SIZE = 128;

    private final int idsPerEntry;
    private final GBPTree<IdRangeKey, IdRange> tree;
    private final IdRangeLayout layout;
    private final IdCache cache;
    private final AtomicInteger freeIdsNotifier;
    private final AtomicInteger seenFreeIdsNotification = new AtomicInteger();
    private final MarkerProvider markerProvider;
    private final long generation;
    private final ScanLock lock;
    private final IndexedIdGenerator.Monitor monitor;
    /**
     * Manages IDs (ranges of IDs, really) that gets skipped when allocations are made from high ID.
     * This happens because one ID range cannot cross a page boundary. Allocators will populate this queue
     * and the thread getting the scan lock will consume it at a later point.
     */
    private final ConcurrentLinkedQueue<Long> queuedSkippedHighIds = new ConcurrentLinkedQueue<>();
    /**
     * Manages IDs (range of IDs, really) that sometimes gets temporarily wasted when an allocation is made
     * from the cache where the ID range size is somewhere between two slot sizes. The remainder of the ID range
     * will be queued in this queue and consumed by the thread getting the scan lock at a later point.
     */
    private final ConcurrentLinkedQueue<Long> queuedWastedCachedIds = new ConcurrentLinkedQueue<>();
    /**
     * State for whether there's an ongoing scan, and if so where it should begin from. This is used in
     * {@link #findSomeIdsToCache(MutableLongList, MutableInt, CursorContext)}  both to know where to initiate a scan from and to
     * set it, if the cache got full before scan completed, or set it to null of the scan ended. The actual {@link Seeker} itself is local to the scan method.
     */
    private volatile Long ongoingScanRangeIndex;

    private final AtomicLong numQueuedIds = new AtomicLong();
    /**
     * Keeps the state of {@link IdGenerator#allocationEnabled()}. It lives in here because this is the class that mutates it under lock.
     */
    private volatile boolean allocationEnabled;

    private final boolean useDirectToCache;

    FreeIdScanner(
            int idsPerEntry,
            GBPTree<IdRangeKey, IdRange> tree,
            IdRangeLayout layout,
            IdCache cache,
            AtomicInteger freeIdsNotifier,
            MarkerProvider markerProvider,
            long generation,
            boolean strictlyPrioritizeFreelistOverHighId,
            IndexedIdGenerator.Monitor monitor,
            boolean allocationEnabled,
            boolean useDirectToCache) {
        this.idsPerEntry = idsPerEntry;
        this.tree = tree;
        this.layout = layout;
        this.cache = cache;
        this.freeIdsNotifier = freeIdsNotifier;
        this.markerProvider = markerProvider;
        this.generation = generation;
        this.lock = strictlyPrioritizeFreelistOverHighId
                ? ScanLock.lockyAndPessimistic()
                : ScanLock.lockFreeAndOptimistic();
        this.monitor = monitor;
        this.allocationEnabled = allocationEnabled;
        this.useDirectToCache = useDirectToCache;
    }

    /**
     * Do a batch of scanning, either start a new scan from the beginning if none is active, or continue where a previous scan
     * paused. In this call free ids can be discovered and placed into the ID cache. IDs are marked as reserved before placed into cache.
     */
    void tryLoadFreeIdsIntoCache(boolean blocking, boolean maintenance, CursorContext cursorContext) {
        if (!hasMoreFreeIds(maintenance)) {
            // If no scan is in progress and if we have no reason to expect finding any free id from a scan then don't
            // do it.
            return;
        }

        if (scanLock(blocking)) {
            try {
                if (!allocationEnabled) {
                    return;
                }
                handleQueuedIds(cursorContext);
                if (shouldFindFreeIdsByScan()) {
                    var availableSpaceById = new MutableInt(cache.availableSpaceById());
                    if (availableSpaceById.intValue() > 0) {
                        var pendingIdQueue = LongLists.mutable.empty();
                        if (findSomeIdsToCache(pendingIdQueue, availableSpaceById, cursorContext)) {
                            // Get a writer and mark the found ids as reserved
                            reserveAndOfferToCache(pendingIdQueue, cursorContext);
                        }
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                lock.unlock();
            }
        }
    }

    private void handleQueuedIds(CursorContext cursorContext) {
        if (!queuedSkippedHighIds.isEmpty() || !queuedWastedCachedIds.isEmpty()) {
            try (var marker = markerProvider.getMarker(cursorContext)) {
                handleQueuedIds(marker);
            }
        }
    }

    private void handleQueuedIds(IdGenerator.ContextualMarker marker) {
        consumeQueuedIds(queuedSkippedHighIds, marker, IdGenerator.ContextualMarker::markFree);
        consumeQueuedIds(queuedWastedCachedIds, marker, (mark, id, size) -> {
            int accepted = cache.offer(id, size, monitor);
            if (accepted < size) {
                // A part of or the whole ID will not make it to the cache. Take the long route and
                // insert marks so that they may enter the cache via a scan later on.
                // Mark as free and unreserved because an ID in cache can have two free/reserved states:
                // - free:1, reserved:1 (if it couldn't take the short-cut into cache when freed)
                // - free:0, reserved:0 (if it took the short-cut into cache when freed)
                mark.markUncached(id + accepted, size - accepted);
            }
        });
    }

    private void consumeQueuedIds(
            ConcurrentLinkedQueue<Long> queue, IdGenerator.ContextualMarker marker, QueueConsumer consumer) {
        if (!queue.isEmpty()) {
            // There may be a race here which will result in ids that gets queued right when we flip missed here, but
            // they will be picked
            // up on the next restart. It should be rare. And to introduce locking or synchronization to prevent it may
            // not be worth it.
            Long idAndSize;
            int numConsumedIds = 0;
            while ((idAndSize = queue.poll()) != null) {
                long id = idFromCombinedId(idAndSize);
                int size = numberOfIdsFromCombinedId(idAndSize);
                consumer.accept(marker, id, size);
                numConsumedIds++;
            }
            numQueuedIds.addAndGet(-numConsumedIds);
        }
    }

    boolean hasMoreFreeIds(boolean maintenance) {
        if (!allocationEnabled) {
            return false;
        }

        // For the case when this is a tx allocating IDs we don't want to force a scan for every little added ID,
        // so add a little lee-way so that there has to be a at least a bunch of these "skipped" IDs to make it worth
        // wile.
        int numQueuedIdsThreshold = maintenance ? 1 : 1_000;
        return shouldFindFreeIdsByScan() || numQueuedIds.get() >= numQueuedIdsThreshold;
    }

    private boolean shouldFindFreeIdsByScan() {
        return ongoingScanRangeIndex != null || seenFreeIdsNotification.get() != freeIdsNotifier.get();
    }

    private boolean scanLock(boolean blocking) {
        if (blocking) {
            lock.lock();
            return true;
        }
        return lock.tryLock();
    }

    void clearCache(boolean allocationWillBeEnabled, CursorContext cursorContext) {
        lock.lock();
        try {
            // Restart scan from the beginning after cache is cleared
            ongoingScanRangeIndex = null;

            if (allocationEnabled) {
                // Since placing an id into the cache marks it as reserved, here when taking the ids out from the cache
                // revert that by marking them as unreserved
                try (var marker = markerProvider.getMarker(cursorContext)) {
                    handleQueuedIds(marker);
                    cache.drain(marker::markUncached);
                }
                freeIdsNotifier.incrementAndGet();
            } else {
                handleQueuedIds(IndexedIdGenerator.NOOP_MARKER);
                cache.drain((id, size) -> {});
            }
            allocationEnabled = allocationWillBeEnabled;
        } finally {
            lock.unlock();
        }
    }

    void queueSkippedHighId(long id, int numberOfIds) {
        queuedSkippedHighIds.offer(combinedIdAndNumberOfIds(id, numberOfIds, false));
        numQueuedIds.incrementAndGet();
    }

    void queueWastedCachedId(long id, int numberOfIds) {
        queuedWastedCachedIds.offer(combinedIdAndNumberOfIds(id, numberOfIds, false));
        numQueuedIds.incrementAndGet();
    }

    private void reserveAndOfferToCache(MutableLongList pendingIdQueue, CursorContext cursorContext) {
        try (var marker = markerProvider.getMarker(cursorContext)) {
            var iterator = pendingIdQueue.longIterator();
            while (iterator.hasNext()) {
                var combinedId = iterator.next();
                var id = idFromCombinedId(combinedId);
                var numberOfIds = numberOfIdsFromCombinedId(combinedId);
                // Mark as reserved before placing into cache. This prevents a race which could otherwise allow
                // the ID to be allocated, used and (again) deleted and freed before marked as reserved here,
                // and therefore "lost" until next restart.
                marker.markReserved(id, numberOfIds);
                var accepted = cache.offer(id, numberOfIds, monitor);
                if (accepted < numberOfIds) {
                    long idToUndo = id + accepted;
                    int numberOfIdsToUndo = numberOfIds - accepted;
                    if (useDirectToCache) {
                        marker.markUncached(idToUndo, numberOfIdsToUndo);
                    } else {
                        marker.markUnreserved(idToUndo, numberOfIdsToUndo);
                    }
                }
            }
        }
    }

    private boolean findSomeIdsToCache(
            MutableLongList pendingIdQueue, MutableInt availableSpaceById, CursorContext cursorContext)
            throws IOException {
        boolean startedNow = ongoingScanRangeIndex == null;
        IdRangeKey from = ongoingScanRangeIndex == null ? LOW_KEY : new IdRangeKey(ongoingScanRangeIndex);
        boolean seekerExhausted = false;
        int freeIdsNotificationBeforeScan = freeIdsNotifier.get();
        IdRange.FreeIdVisitor visitor =
                (id, numberOfIds) -> queueId(pendingIdQueue, availableSpaceById, id, numberOfIds);

        try (Seeker<IdRangeKey, IdRange> scanner = tree.seek(from, HIGH_KEY, cursorContext)) {
            // Continue scanning until the cache is full or there's nothing more to scan
            while (availableSpaceById.intValue() > 0) {
                if (!scanner.next()) {
                    seekerExhausted = true;
                    break;
                }

                var baseId = scanner.key().getIdRangeIdx() * idsPerEntry;
                scanner.value().visitFreeIds(baseId, generation, visitor);
            }
            // If there's more left to scan "this round" then make a note of it so that we start from this place the
            // next time
            ongoingScanRangeIndex = seekerExhausted ? null : scanner.key().getIdRangeIdx();
        }

        boolean somethingWasCached = !pendingIdQueue.isEmpty();
        if (seekerExhausted) {
            if (!somethingWasCached && startedNow) {
                // chill a bit until at least one id gets freed
                seenFreeIdsNotification.set(freeIdsNotificationBeforeScan);
            }
        }
        return somethingWasCached;
    }

    private boolean queueId(MutableLongList pendingIdQueue, MutableInt availableSpaceById, long id, int numberOfIds) {
        assert layout.idRangeIndex(id) == layout.idRangeIndex(id + numberOfIds - 1);
        pendingIdQueue.add(combinedIdAndNumberOfIds(id, numberOfIds, false));
        return availableSpaceById.addAndGet(-numberOfIds) > 0;
    }

    boolean allocationEnabled() {
        return allocationEnabled;
    }

    private interface QueueConsumer {
        void accept(IdGenerator.ContextualMarker marker, long id, int size);
    }
}
