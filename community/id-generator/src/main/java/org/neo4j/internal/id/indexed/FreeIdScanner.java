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

import static org.neo4j.internal.id.IdGenerator.NO_ID;
import static org.neo4j.internal.id.IdUtils.combinedIdAndNumberOfIds;
import static org.neo4j.internal.id.IdUtils.idFromCombinedId;
import static org.neo4j.internal.id.IdUtils.numberOfIdsFromCombinedId;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.eclipse.collections.api.factory.primitive.LongLists;
import org.eclipse.collections.api.iterator.LongIterator;
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
     * Used as low bound of a partitioning scan.
     */
    private static final IdRangeKey LOW_KEY = new IdRangeKey(0);
    /**
     * Used as high bound for all scans.
     */
    private static final IdRangeKey HIGH_KEY = new IdRangeKey(Long.MAX_VALUE);

    private static final int FAR_SCAN_THRESHOLD = 1_000;
    private static final int TOO_FAR_SCAN_THRESHOLD = 10_000;
    private static final int GOOD_ENOUGH_FOUND_THRESHOLD = 10;
    private static final int DESIRED_NUMBER_OF_PARTITIONS = Runtime.getRuntime().availableProcessors();

    private final int idsPerEntry;
    private final GBPTree<IdRangeKey, IdRange> tree;
    private final IdRangeLayout layout;
    private final IdCache cache;
    private final FreeIdFindState freeIdFindState;
    private final MarkerProvider markerProvider;
    private final long generation;
    private final ReentrantReadWriteLock clearCacheLock = new ReentrantReadWriteLock();
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

    private final AtomicLong numQueuedIds = new AtomicLong();
    /**
     * Keeps the state of {@link IdGenerator#allocationEnabled()}. It lives in here because this is the class that mutates it under lock.
     */
    private volatile boolean allocationEnabled;

    private final boolean useDirectToCache;

    private final ScanLock partitionsLock;
    private volatile Partitions partitions;

    FreeIdScanner(
            int idsPerEntry,
            GBPTree<IdRangeKey, IdRange> tree,
            IdRangeLayout layout,
            IdCache cache,
            FreeIdFindState freeIdFindState,
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
        this.freeIdFindState = freeIdFindState;
        this.markerProvider = markerProvider;
        this.generation = generation;
        this.partitionsLock = strictlyPrioritizeFreelistOverHighId
                ? ScanLock.lockyAndPessimistic()
                : ScanLock.lockFreeAndOptimistic();
        this.monitor = monitor;
        this.allocationEnabled = allocationEnabled;
        this.useDirectToCache = useDirectToCache;
    }

    /**
     * Do a batch of scanning, either start a new scan from the beginning if none is active, or continue where a previous scan
     * paused. In this call free ids can be discovered and placed into the ID cache. IDs are marked as reserved before placed into cache.
     * @param blocking whether to await the partition lock. If {@code false} then this method will return immediately w/o
     * doing a scan if the lock is not available. The partition lock is only held by a thread either when
     * selecting a partition to scan, or when creating new partitions - not actual scanning.
     * @param maintenance whether this is a maintenance scan. Maintenance scan has lower thresholds in the
     * condition checks for starting a new scan.
     * @param requestedNumberOfIds the number of IDs that the requester wants to find as a side effect of this scan.
     * If this value is larger than 0 and an ID of this size is found, then it will be returned from this method
     * instead of being placed into cache. This is so that it's guaranteed that the requestor won't get starved
     * in a highly concurrent allocation scenario.
     * @return the first matching ID of size {@code requestedNumberOfIds} found in the scan,
     * or {@code NO_ID} if none was found or if no scan was done.
     */
    long tryLoadFreeIdsIntoCache(
            boolean blocking, boolean maintenance, int requestedNumberOfIds, CursorContext cursorContext) {
        if (!hasMoreFreeIds(maintenance, requestedNumberOfIds)) {
            // If no scan is in progress and if we have no reason to expect finding any free id from a scan then don't
            // do it.
            return NO_ID;
        }

        clearCacheLock.readLock().lock();
        try {
            if (!allocationEnabled) {
                return NO_ID;
            }
            handleQueuedIds(cursorContext);
            Partition partition = acquirePartition(blocking, requestedNumberOfIds, cursorContext);

            if (partition != null) {
                boolean somethingWasCached = false;
                Partition remainingPartition = null;
                try {
                    MutableLongList pendingIdQueue = LongLists.mutable.empty();
                    AvailableSpace availableSpace = new AvailableSpace(
                            cache, requestedNumberOfIds, maintenance ? 1 : partitions.numNonExhausted());
                    remainingPartition = findSomeIdsToCache(pendingIdQueue, availableSpace, partition, cursorContext);
                    somethingWasCached = !pendingIdQueue.isEmpty();
                    if (somethingWasCached) {
                        // Get a writer and mark the found ids as reserved
                        long foundId = reserveAndOfferToCache(pendingIdQueue, requestedNumberOfIds, cursorContext);
                        if (foundId != NO_ID && !maintenance) {
                            return foundId;
                        }
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                } finally {
                    partitions.leave(remainingPartition, somethingWasCached);
                }
            }

            return NO_ID;
        } finally {
            clearCacheLock.readLock().unlock();
        }
    }

    private Partition acquirePartition(boolean blocking, int requestedNumberOfIds, CursorContext cursorContext) {
        // Optimistically try to grab a partition if there's on in the queue
        Partitions partitions = this.partitions;
        if (partitions != null) {
            Partition partition = partitions.next(requestedNumberOfIds);
            if (partition != null) {
                return partition;
            }
        }

        // Optimistically try to grab the lock
        if (!partitionsLock.tryLock()) {
            if (!blocking) {
                return null; // If we don't allow blocking we have to wait for the next iteration
            }
            partitionsLock.lock();
        }
        try {
            // We must check this allocationEnabled when having the lock, since clearCache() is the method
            // that can change allocationEnabled and that method does so having this same lock.
            if (!allocationEnabled) {
                return null;
            }

            if (!tryBeginNextScanRound(cursorContext)) {
                return null; // No observed frees, no need to scan
            }
            return this.partitions.next(requestedNumberOfIds);
        } finally {
            partitionsLock.unlock();
        }
    }

    /**
     * Prepares a fresh set of scan partitions if the current set has been fully consumed,
     * and signals whether scanning should proceed.
     * <p>
     * When all partitions from the previous round came up empty, this method checks whether
     * any IDs have been freed since that round began. If not, there is no point starting another scan and
     * {@code false} is returned so the caller can bail out early.
     * <p>
     * Must be called while holding the partition lock.
     *
     * @return {@code true} if scanning should proceed (partitions are ready),
     *         {@code false} if the evidence suggests a new scan would find nothing.
     */
    private boolean tryBeginNextScanRound(CursorContext cursorContext) {
        if (partitions == null || partitions.isExhausted()) {
            if (partitions != null && partitions.allCameUpEmpty()) {
                freeIdFindState.setAcknowledgedNotificationCount(partitions.lowestNotificationCountAtScanStart());
                if (!freeIdFindState.hasPendingFrees()) {
                    // All partitions came up empty, and we have not seen more free IDs since the last scan
                    // started, so a new scan won't find anything.
                    partitions = null;
                    return false;
                }
            }
            List<Partition> newPartitions = partitionIdSpace(tree, cursorContext);
            partitions = new Partitions(freeIdFindState, newPartitions);
            freeIdFindState.beginNewScanRound();
            monitor.scanPartitionsCreated(newPartitions.size());
        }
        return true;
    }

    private static List<Partition> partitionIdSpace(GBPTree<IdRangeKey, IdRange> tree, CursorContext cursorContext) {
        try {
            List<IdRangeKey> idRangeKeys =
                    tree.partitionedSeek(LOW_KEY, HIGH_KEY, DESIRED_NUMBER_OF_PARTITIONS, cursorContext);
            List<Partition> partitions = new ArrayList<>(idRangeKeys.size() - 1);
            for (int i = 0; i < idRangeKeys.size() - 1; i++) {
                partitions.add(new Partition(
                        idRangeKeys.get(i).getIdRangeIdx(),
                        idRangeKeys.get(i + 1).getIdRangeIdx()));
            }
            return partitions;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
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

    boolean hasMoreFreeIds(boolean maintenance, int requestedNumberOfIds) {
        if (!allocationEnabled) {
            return false;
        }

        // For the case when this is a tx allocating IDs we don't want to force a scan for every little added ID,
        // so add a little lee-way so that there has to be at least a bunch of these "skipped" IDs to make it worth
        // wile.
        int numQueuedIdsThreshold = maintenance ? 1 : 1_000;
        return shouldFindFreeIdsByScan(requestedNumberOfIds) || numQueuedIds.get() >= numQueuedIdsThreshold;
    }

    private boolean shouldFindFreeIdsByScan(int requestedNumberOfIds) {
        FreeIdFindState.Snapshot snapshot = freeIdFindState.snapshot();
        if (snapshot.hasObservedFrees() && requestedNumberOfIds > snapshot.largestObservedSlotSize()) {
            return false;
        }
        Partitions partitions = this.partitions;
        boolean canIContinueScan = partitions != null && !partitions.isExhausted();
        return canIContinueScan || freeIdFindState.hasPendingFrees();
    }

    void clearCache(boolean allocationWillBeEnabled, CursorContext cursorContext) {
        clearCacheLock.writeLock().lock();
        try {
            partitionsLock.lock();
            try {
                // Restart scan from the beginning after cache is cleared
                partitions = null;

                if (allocationEnabled) {
                    // Since placing an id into the cache marks it as reserved, here when taking the ids out from the
                    // cache revert that by marking them as unreserved
                    try (var marker = markerProvider.getMarker(cursorContext)) {
                        handleQueuedIds(marker);
                        cache.drain(marker::markUncached);
                    }

                    freeIdFindState.beginNewScanRound();
                    freeIdFindState.bumpNotificationCount();
                } else {
                    handleQueuedIds(IndexedIdGenerator.NOOP_MARKER);
                    cache.drain((id, size) -> {});
                }
                allocationEnabled = allocationWillBeEnabled;
            } finally {
                partitionsLock.unlock();
            }
        } finally {
            clearCacheLock.writeLock().unlock();
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

    private long reserveAndOfferToCache(
            MutableLongList pendingIdQueue, int requestedNumberOfIds, CursorContext cursorContext) {
        long pocketedId = NO_ID;
        try (var marker = markerProvider.getMarker(cursorContext)) {
            LongIterator iterator = pendingIdQueue.longIterator();
            while (iterator.hasNext()) {
                long combinedId = iterator.next();
                long id = idFromCombinedId(combinedId);
                int numberOfIds = numberOfIdsFromCombinedId(combinedId);

                // Mark as reserved before placing into cache. This prevents a race which could otherwise allow
                // the ID to be allocated, used and (again) deleted and freed before marked as reserved here,
                // and therefore "lost" until next restart.
                marker.markReserved(id, numberOfIds);
            }

            marker.flush();

            iterator = pendingIdQueue.longIterator();
            while (iterator.hasNext()) {
                long combinedId = iterator.next();
                long id = idFromCombinedId(combinedId);
                int numberOfIds = numberOfIdsFromCombinedId(combinedId);
                if (requestedNumberOfIds > 0 && numberOfIds >= requestedNumberOfIds && pocketedId == NO_ID) {
                    pocketedId = id;
                    id += requestedNumberOfIds;
                    numberOfIds -= requestedNumberOfIds;
                    if (numberOfIds == 0) {
                        continue;
                    }
                }
                int accepted = cache.offer(id, numberOfIds, monitor);
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
        return pocketedId;
    }

    /**
     * Scans a partition until it meets an end condition.
     * @param pendingIdQueue to add found IDs into.
     * @param availableSpace amount of cache space available to find IDs for.
     * @param partition partition boundaries to scan.
     * @param cursorContext cursor context.
     * @return if the scan didn't fully exhaust the partition then a new {@link Partition} instance is returned
     * with the {@link Partition#fromRangeIdx()} where this scan stopped, and w/ same {@link Partition#toRangeIdx().
     * If exhausted then {@code null} is returned.
     * @throws IOException on I/O error.
     */
    private Partition findSomeIdsToCache(
            MutableLongList pendingIdQueue,
            AvailableSpace availableSpace,
            Partition partition,
            CursorContext cursorContext)
            throws IOException {
        IdRangeKey startingPoint = partition.from();
        monitor.scanStart();

        IdRange.FreeIdVisitor visitor = (id, numberOfIds) -> {
            assert layout.idRangeIndex(id) == layout.idRangeIndex(id + numberOfIds - 1);
            if (availableSpace.trackSpaceUsageOfQueuedId(numberOfIds)) {
                pendingIdQueue.add(combinedIdAndNumberOfIds(id, numberOfIds, false));
                return true;
            }
            return false;
        };

        long currentRangeIdx;
        try (Seeker<IdRangeKey, IdRange> scanner = tree.seek(startingPoint, partition.to(), cursorContext)) {
            // Continue scanning until the cache is full or there's nothing more to scan
            int numEntriesVisited = 0;
            ScanEndCondition scanEndCondition;
            do {
                if (!scanner.next()) {
                    currentRangeIdx = NO_ID;
                    scanEndCondition = ScanEndCondition.EXHAUSTED;
                    break;
                }

                numEntriesVisited++;
                currentRangeIdx = scanner.key().getIdRangeIdx();
                long baseId = scanner.key().getIdRangeIdx() * idsPerEntry;
                scanner.value().visitFreeIds(baseId, generation, visitor);
            } while ((scanEndCondition = availableSpace.scanEndConditionMet(numEntriesVisited)) == null);

            monitor.scanEnd(numEntriesVisited, availableSpace.foundIds(), scanEndCondition);
        }
        return currentRangeIdx != NO_ID ? partition.subPartition(currentRangeIdx) : null;
    }

    boolean allocationEnabled() {
        return allocationEnabled;
    }

    private interface QueueConsumer {
        void accept(IdGenerator.ContextualMarker marker, long id, int size);
    }

    private static class AvailableSpace {
        private final IdCache cache;
        private final int requestedNumberOfIds;
        private final int[] availableSpace;
        private final int[] initialAvailableSpace;
        private final int findThreshold;

        private int numFoundSuitableIds;

        AvailableSpace(IdCache cache, int requestedNumberOfIds, int desiredNumPartitions) {
            this.availableSpace = cache.availableSpaceBySlotIndex(desiredNumPartitions);
            this.initialAvailableSpace = availableSpace.clone();
            this.cache = cache;
            this.requestedNumberOfIds = requestedNumberOfIds;
            this.findThreshold = availableSpace[cache.lowestSlotIndexCapableOf(requestedNumberOfIds)];
        }

        /**
         * Given the {@code availableSpace} array with available space per slot, check if an ID of size {@code numberOfIds}
         * fits, either partly or entirely. Also, the {@code availableSpace} values will decrease equivalent to the size of
         * {@code numberOfIds} and which slot(s) it fits into.
         *
         * @param numberOfIds the size of the ID to try and fit into the cache.
         * @return {@code true} if there's space for an ID of size {@code numberOfIds}, or at least parts of it and
         * otherwise {@code false}.
         */
        boolean trackSpaceUsageOfQueuedId(int numberOfIds) {
            int trackedNumberOfIds = numberOfIds;
            int slotIndex = cache.largestSlotIndex(trackedNumberOfIds);
            while (trackedNumberOfIds > 0) {
                if (availableSpace[slotIndex] > 0) {
                    if (slotIndex == 0) {
                        // We're at the lowest slot (assumed to be 1, right?). We can't continue in this loop
                        // so just add all remaining IDs as individual IDs and be done with.
                        int prev = availableSpace[slotIndex];
                        availableSpace[slotIndex] = Math.max(0, prev - trackedNumberOfIds);
                        if (requestedNumberOfIds == 1) {
                            numFoundSuitableIds += prev - availableSpace[slotIndex];
                        }
                        trackedNumberOfIds = 0;
                    } else {
                        availableSpace[slotIndex]--;
                        int slotSize = cache.slotSizeSlotIndex(slotIndex);
                        if (slotSize >= requestedNumberOfIds) {
                            numFoundSuitableIds++;
                        }
                        trackedNumberOfIds -= slotSize;
                    }
                    if (trackedNumberOfIds > 0) {
                        slotIndex = cache.largestSlotIndex(trackedNumberOfIds);
                    }
                } else if (slotIndex > 0) {
                    slotIndex--;
                } else {
                    break;
                }
            }
            return trackedNumberOfIds < numberOfIds;
        }

        int[] foundIds() {
            int[] counts = initialAvailableSpace.clone();
            for (int i = 0; i < availableSpace.length; i++) {
                counts[i] -= availableSpace[i];
            }
            return counts;
        }

        ScanEndCondition scanEndConditionMet(int numEntriesVisited) {
            if (numFoundSuitableIds >= findThreshold) {
                return ScanEndCondition.QUOTA_FOUND;
            }
            if (numEntriesVisited >= FAR_SCAN_THRESHOLD && numFoundSuitableIds >= GOOD_ENOUGH_FOUND_THRESHOLD) {
                return ScanEndCondition.ENOUGH_FOUND;
            }
            if (numEntriesVisited >= TOO_FAR_SCAN_THRESHOLD) {
                return numFoundSuitableIds == 0 ? ScanEndCondition.NOTHING_FOUND : ScanEndCondition.ENOUGH_FOUND;
            }
            return null;
        }
    }

    private static class Partitions {
        private final Deque<Partition> partitions;
        private final FreeIdFindState freeIdFindState;
        private final AtomicInteger numNonExhausted;
        private final AtomicBoolean somethingWasCached = new AtomicBoolean();
        private final int notificationCountAtScanStart;

        Partitions(FreeIdFindState freeIdFindState, List<Partition> partitions) {
            this.freeIdFindState = freeIdFindState;
            this.partitions = new ConcurrentLinkedDeque<>((partitions));
            this.numNonExhausted = new AtomicInteger(partitions.size());
            this.notificationCountAtScanStart = freeIdFindState.snapshot().notificationCount();
        }

        boolean isExhausted() {
            return numNonExhausted.get() == 0;
        }

        int numNonExhausted() {
            return numNonExhausted.get();
        }

        Partition next(int requestedNumberOfIds) {
            FreeIdFindState.Snapshot snapshot = freeIdFindState.snapshot();
            if (requestedNumberOfIds > 0
                    && snapshot.hasObservedFrees()
                    && requestedNumberOfIds > snapshot.largestObservedSlotSize()) {
                return null;
            }
            return partitions.poll();
        }

        void leave(Partition partition, boolean somethingWasCached) {
            if (somethingWasCached) {
                this.somethingWasCached.set(true);
            }
            if (partition != null) {
                partitions.offer(partition);
            } else {
                numNonExhausted.decrementAndGet();
            }
        }

        boolean allCameUpEmpty() {
            return !somethingWasCached.get();
        }

        int lowestNotificationCountAtScanStart() {
            return notificationCountAtScanStart;
        }
    }

    private record Partition(long fromRangeIdx, long toRangeIdx) {
        IdRangeKey from() {
            return new IdRangeKey(fromRangeIdx);
        }

        IdRangeKey to() {
            return new IdRangeKey(toRangeIdx);
        }

        Partition subPartition(long fromRangeIdx) {
            return new Partition(fromRangeIdx, toRangeIdx);
        }
    }
}
