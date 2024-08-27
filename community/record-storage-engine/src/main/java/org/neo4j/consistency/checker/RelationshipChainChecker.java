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
package org.neo4j.consistency.checker;

import static java.lang.Math.max;
import static org.neo4j.consistency.checker.RelationshipLink.SOURCE_NEXT;
import static org.neo4j.consistency.checker.RelationshipLink.SOURCE_PREV;
import static org.neo4j.consistency.checker.RelationshipLink.TARGET_NEXT;
import static org.neo4j.consistency.checker.RelationshipLink.TARGET_PREV;
import static org.neo4j.internal.helpers.Format.duration;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.RELATIONSHIP_CURSOR;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.neo4j.consistency.checker.ParallelExecution.ThrowingRunnable;
import org.neo4j.consistency.checking.ConsistencyFlags;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.cache.CacheSlots;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.time.Stopwatch;

/**
 * Checks relationship chains, i.e. their internal pointers between relationship records.
 */
class RelationshipChainChecker implements Checker {
    private static final String RELATIONSHIP_CONSISTENCY_CHECKER_TAG = "relationshipConsistencyChecker";
    private static final String SINGLE_RELATIONSHIP_CONSISTENCY_CHECKER_TAG =
            "simpleChainsRelationshipConsistencyChecker";
    private final ConsistencyReport.Reporter reporter;
    private final CheckerContext context;
    private final int numberOfChainCheckers;
    private final CacheAccess cacheAccess;
    private final RecordLoading recordLoader;
    private final ProgressListener progress;

    RelationshipChainChecker(CheckerContext context) {
        this.context = context;
        this.reporter = context.reporter;
        // There will be two threads in addition to the relationship checkers:
        // - Relationship store scanner
        // - (Internal) thread helping pre-fetching the relationship store pages
        this.numberOfChainCheckers = max(1, context.execution.getNumberOfThreads() - 2);
        this.cacheAccess = context.cacheAccess;
        this.recordLoader = context.recordLoader;
        this.progress = context.progressReporter(
                this,
                "Relationship chains",
                context.neoStores.getRelationshipStore().getIdGenerator().getHighId() * 2);
    }

    @Override
    public void check(LongRange nodeIdRange, boolean firstRange, boolean lastRange, MemoryTracker memoryTracker)
            throws Exception {
        // Forward scan (cache prev pointers)
        checkDirection(nodeIdRange, ScanDirection.FORWARD);

        // Backward scan (cache next pointers)
        context.paddedDebug(
                "%s moving over to backwards relationship chain checking",
                getClass().getSimpleName());
        checkDirection(nodeIdRange, ScanDirection.BACKWARD);
    }

    private void checkDirection(LongRange nodeIdRange, ScanDirection direction) throws Exception {
        RelationshipStore relationshipStore = context.neoStores.getRelationshipStore();
        long highId = relationshipStore.getIdGenerator().getHighId();
        AtomicBoolean end = new AtomicBoolean();
        int numberOfThreads = numberOfChainCheckers + 1;
        ThrowingRunnable[] workers = new ThrowingRunnable[numberOfThreads];
        ArrayBlockingQueue<BatchedRelationshipRecords>[] threadQueues = new ArrayBlockingQueue[numberOfChainCheckers];
        BatchedRelationshipRecords[] threadBatches = new BatchedRelationshipRecords[numberOfChainCheckers];
        for (int i = 0; i < numberOfChainCheckers; i++) {
            threadQueues[i] = new ArrayBlockingQueue<>(20);
            threadBatches[i] = new BatchedRelationshipRecords();
            workers[i] = relationshipVsRelationshipChecker(
                    nodeIdRange, direction, relationshipStore, threadQueues[i], end, i);
        }

        // Record reader
        workers[workers.length - 1] = () -> {
            RelationshipRecord relationship = relationshipStore.newRecord();
            try (var cursorContext = context.contextFactory.create(RELATIONSHIP_CONSISTENCY_CHECKER_TAG);
                    var cursor = relationshipStore.openPageCursorForReadingWithPrefetching(0, cursorContext);
                    var localProgress = progress.threadLocalReporter()) {
                int recordsPerPage = relationshipStore.getRecordsPerPage();
                long id = direction.startingId(highId);
                while (id >= 0 && id < highId && !context.isCancelled()) {
                    for (int i = 0; i < recordsPerPage && id >= 0 && id < highId; i++, id = direction.nextId(id)) {
                        relationshipStore.getRecordByCursor(id, relationship, FORCE, cursor, context.memoryTracker);
                        localProgress.add(1);
                        if (relationship.inUse()) {
                            queueRelationshipCheck(threadQueues, threadBatches, relationship);
                        }
                    }
                }
                processLastRelationshipChecks(threadQueues, threadBatches, end);
            }
        };

        Stopwatch stopwatch = Stopwatch.start();
        cacheAccess.clearCache();
        context.execution.runAll(getClass().getSimpleName() + "-" + direction.name(), workers);
        detectSingleRelationshipChainInconsistencies(nodeIdRange);
        context.paddedDebug("%s %s took %s", this, direction, duration(stopwatch.elapsed(TimeUnit.MILLISECONDS)));
    }

    @Override
    public boolean shouldBeChecked(ConsistencyFlags flags) {
        return flags.checkGraph();
    }

    private void detectSingleRelationshipChainInconsistencies(LongRange nodeIdRange) {
        CacheAccess.Client client = cacheAccess.client();
        RelationshipStore relationshipStore = context.neoStores.getRelationshipStore();
        try (var cursorContext = context.contextFactory.create(SINGLE_RELATIONSHIP_CONSISTENCY_CHECKER_TAG);
                var relationshipCursor = relationshipStore.openPageCursorForReading(0, cursorContext)) {
            for (long nodeId = nodeIdRange.from(); nodeId < nodeIdRange.to(); nodeId++) {
                boolean inUse = client.getBooleanFromCache(nodeId, CacheSlots.RelationshipLink.SLOT_IN_USE);
                boolean hasMultipleRelationships =
                        client.getBooleanFromCache(nodeId, CacheSlots.RelationshipLink.SLOT_HAS_MULTIPLE_RELATIONSHIPS);
                if (inUse && !hasMultipleRelationships) {
                    long reference = client.getFromCache(nodeId, CacheSlots.RelationshipLink.SLOT_REFERENCE);
                    long relationshipId = client.getFromCache(nodeId, CacheSlots.RelationshipLink.SLOT_RELATIONSHIP_ID);
                    long sourceOrTarget =
                            client.getFromCache(nodeId, CacheSlots.RelationshipLink.SLOT_SOURCE_OR_TARGET);
                    long prevOrNext = client.getFromCache(nodeId, CacheSlots.RelationshipLink.SLOT_PREV_OR_NEXT);
                    boolean isFirstInChain =
                            client.getBooleanFromCache(nodeId, CacheSlots.RelationshipLink.SLOT_FIRST_IN_CHAIN);

                    boolean consistent;
                    if (prevOrNext == CacheSlots.RelationshipLink.PREV) {
                        // we don't know here if this chain belongs to a group and has external degrees, because if so
                        // it could have any value here
                        consistent = isFirstInChain;
                    } else {
                        consistent = NULL_REFERENCE.is(reference);
                    }

                    if (!consistent) {
                        RelationshipRecord relationship = relationshipStore.getRecordByCursor(
                                relationshipId,
                                relationshipStore.newRecord(),
                                FORCE,
                                relationshipCursor,
                                context.memoryTracker);
                        RelationshipRecord referenceRelationship = relationshipStore.getRecordByCursor(
                                reference,
                                relationshipStore.newRecord(),
                                FORCE,
                                relationshipCursor,
                                context.memoryTracker);
                        linkOf(
                                        sourceOrTarget == CacheSlots.RelationshipLink.SOURCE,
                                        prevOrNext == CacheSlots.RelationshipLink.PREV)
                                .reportDoesNotReferenceBack(reporter, relationship, referenceRelationship);
                    }
                }
            }
        }
    }

    private static RelationshipLink linkOf(boolean source, boolean prev) {
        if (source) {
            return prev ? SOURCE_PREV : SOURCE_NEXT;
        }
        return prev ? TARGET_PREV : TARGET_NEXT;
    }

    private ThrowingRunnable relationshipVsRelationshipChecker(
            LongRange nodeIdRange,
            ScanDirection direction,
            RelationshipStore store,
            ArrayBlockingQueue<BatchedRelationshipRecords> queue,
            AtomicBoolean end,
            int threadId) {
        final RelationshipRecord relationship = store.newRecord();
        final RelationshipRecord otherRelationship = store.newRecord();
        final CacheAccess.Client client = cacheAccess.client();
        final RelationshipLink sourceCachePointer = direction.sourceLink;
        final RelationshipLink targetCachePointer = direction.targetLink;
        final long prevOrNext = direction.cacheSlot;
        return () -> {
            try (var cursorContext = context.contextFactory.create(RELATIONSHIP_CONSISTENCY_CHECKER_TAG);
                    var storeCursors = new CachedStoreCursors(context.neoStores, cursorContext)) {
                while ((!end.get() || !queue.isEmpty()) && !context.isCancelled()) {
                    BatchedRelationshipRecords batch = queue.poll(100, TimeUnit.MILLISECONDS);
                    if (batch != null) {
                        while (batch.fillNext(relationship) && !context.isCancelled()) {
                            long firstNode = relationship.getFirstNode();
                            long secondNode = relationship.getSecondNode();
                            // Intentionally not checking nodes outside highId of node store because RelationshipChecker
                            // will spot this inconsistency
                            boolean processStartNode = Math.abs(firstNode % numberOfChainCheckers) == threadId
                                    && nodeIdRange.isWithinRangeExclusiveTo(firstNode);
                            boolean processEndNode = Math.abs(secondNode % numberOfChainCheckers) == threadId
                                    && nodeIdRange.isWithinRangeExclusiveTo(secondNode);
                            if (processStartNode) {
                                checkRelationshipLink(
                                        direction,
                                        SOURCE_PREV,
                                        relationship,
                                        client,
                                        otherRelationship,
                                        store,
                                        storeCursors);
                                checkRelationshipLink(
                                        direction,
                                        SOURCE_NEXT,
                                        relationship,
                                        client,
                                        otherRelationship,
                                        store,
                                        storeCursors);
                            }
                            if (processEndNode) {
                                checkRelationshipLink(
                                        direction,
                                        TARGET_PREV,
                                        relationship,
                                        client,
                                        otherRelationship,
                                        store,
                                        storeCursors);
                                checkRelationshipLink(
                                        direction,
                                        TARGET_NEXT,
                                        relationship,
                                        client,
                                        otherRelationship,
                                        store,
                                        storeCursors);
                            }
                            if (processStartNode) {
                                boolean wasInUse =
                                        client.getBooleanFromCache(firstNode, CacheSlots.RelationshipLink.SLOT_IN_USE);
                                long link = sourceCachePointer.link(relationship);
                                if (link < NULL_REFERENCE.longValue()) {
                                    sourceCachePointer.reportDoesNotReferenceBack(
                                            reporter, relationship, otherRelationship);
                                } else {
                                    client.putToCache(
                                            firstNode,
                                            relationship.getId(),
                                            link,
                                            CacheSlots.RelationshipLink.SOURCE,
                                            prevOrNext,
                                            1,
                                            CacheSlots.longOf(wasInUse),
                                            CacheSlots.longOf(relationship.isFirstInFirstChain()));
                                }
                            }
                            if (processEndNode) {
                                boolean wasInUse =
                                        client.getBooleanFromCache(secondNode, CacheSlots.RelationshipLink.SLOT_IN_USE);

                                long link = targetCachePointer.link(relationship);
                                if (link < NULL_REFERENCE.longValue()) {
                                    targetCachePointer.reportDoesNotReferenceBack(
                                            reporter, relationship, otherRelationship);
                                } else {
                                    client.putToCache(
                                            secondNode,
                                            relationship.getId(),
                                            link,
                                            CacheSlots.RelationshipLink.TARGET,
                                            prevOrNext,
                                            1,
                                            CacheSlots.longOf(wasInUse),
                                            CacheSlots.longOf(relationship.isFirstInSecondChain()));
                                }
                            }
                        }
                    }
                }
            }
        };
    }

    private void checkRelationshipLink(
            ScanDirection direction,
            RelationshipLink link,
            RelationshipRecord relationshipCursor,
            CacheAccess.Client client,
            RelationshipRecord otherRelationship,
            RelationshipStore store,
            StoreCursors storeCursors) {
        long relationshipId = relationshipCursor.getId();
        long nodeId = link.node(relationshipCursor);
        long linkId = link.link(relationshipCursor);
        long fromCache = client.getFromCache(nodeId, CacheSlots.RelationshipLink.SLOT_RELATIONSHIP_ID);
        boolean cachedLinkInUse = client.getBooleanFromCache(nodeId, CacheSlots.RelationshipLink.SLOT_IN_USE);
        if (!link.endOfChain(relationshipCursor) && cachedLinkInUse) {
            if (fromCache != linkId) {
                // We can't use the cache since it doesn't contain the relationship right before us in this chain
                if (direction.exclude(relationshipId, linkId)) {
                    return;
                } else if (!NULL_REFERENCE.is(fromCache)) {
                    // Load it from store
                    store.getRecordByCursor(
                            linkId,
                            otherRelationship,
                            FORCE,
                            storeCursors.readCursor(RELATIONSHIP_CURSOR),
                            context.memoryTracker);
                } else {
                    otherRelationship.clear();
                    link.reportDoesNotReferenceBack(
                            reporter,
                            recordLoader.relationship(relationshipCursor.getId(), storeCursors, context.memoryTracker),
                            otherRelationship);
                }
            } else {
                // OK good we can use the cached values representing a relationship right before us in this chain
                otherRelationship.clear();
                otherRelationship.setId(linkId);
                long other = client.getFromCache(nodeId, CacheSlots.RelationshipLink.SLOT_REFERENCE);
                NodeLink nodeLink = client.getFromCache(nodeId, CacheSlots.RelationshipLink.SLOT_SOURCE_OR_TARGET)
                                == CacheSlots.RelationshipLink.SOURCE
                        ? NodeLink.SOURCE
                        : NodeLink.TARGET;
                nodeLink.setNode(otherRelationship, nodeId);
                link.setOther(otherRelationship, nodeLink, other);
                otherRelationship.setInUse(client.getBooleanFromCache(nodeId, CacheSlots.RelationshipLink.SLOT_IN_USE));
                otherRelationship.setCreated();
            }
            checkRelationshipLink(direction, link, otherRelationship, relationshipId, nodeId, linkId, storeCursors);
        }
    }

    private void checkRelationshipLink(
            ScanDirection direction,
            RelationshipLink thing,
            RelationshipRecord otherRelationship,
            long relationshipId,
            long nodeId,
            long linkId,
            StoreCursors storeCursors) {
        // Perform the checks
        NodeLink nodeLink = NodeLink.select(otherRelationship, nodeId);
        if (nodeLink == null) {
            thing.reportOtherNode(
                    reporter,
                    recordLoader.relationship(relationshipId, storeCursors, context.memoryTracker),
                    recordLoader.relationship(linkId, storeCursors, context.memoryTracker));
        } else {
            if (thing.other(otherRelationship, nodeLink) != relationshipId) {
                // Read the relationship from store and do the check on that actual record instead, should happen rarely
                // anyway
                if (otherRelationship.isCreated()) {
                    recordLoader.relationship(
                            otherRelationship, otherRelationship.getId(), storeCursors, context.memoryTracker);
                    // Call this method one more time, now with !created
                    checkRelationshipLink(
                            direction, thing, otherRelationship, relationshipId, nodeId, linkId, storeCursors);
                    return;
                }

                thing.reportDoesNotReferenceBack(
                        reporter,
                        recordLoader.relationship(relationshipId, storeCursors, context.memoryTracker),
                        recordLoader.relationship(linkId, storeCursors, context.memoryTracker));
            } else {
                if (!direction.exclude(relationshipId, linkId) && !otherRelationship.inUse()) {
                    thing.reportNotUsedRelationshipReferencedInChain(
                            reporter,
                            recordLoader.relationship(relationshipId, storeCursors, context.memoryTracker),
                            recordLoader.relationship(linkId, storeCursors, context.memoryTracker));
                }
            }
        }
    }

    private void queueRelationshipCheck(
            ArrayBlockingQueue<BatchedRelationshipRecords>[] threadQueues,
            BatchedRelationshipRecords[] threadBatches,
            RelationshipRecord relationshipCursor)
            throws InterruptedException {
        int sourceThread = (int) Math.abs(relationshipCursor.getFirstNode() % numberOfChainCheckers);
        queueRelationshipCheck(threadQueues, threadBatches, relationshipCursor, sourceThread);
        int targetThread = (int) Math.abs(relationshipCursor.getSecondNode() % numberOfChainCheckers);
        if (targetThread != sourceThread) {
            queueRelationshipCheck(threadQueues, threadBatches, relationshipCursor, targetThread);
        }
    }

    private static void queueRelationshipCheck(
            ArrayBlockingQueue<BatchedRelationshipRecords>[] threadQueues,
            BatchedRelationshipRecords[] threadBatches,
            RelationshipRecord relationshipCursor,
            int thread)
            throws InterruptedException {
        if (!threadBatches[thread].hasMoreSpace()) {
            threadQueues[thread].put(threadBatches[thread]);
            threadBatches[thread] = new BatchedRelationshipRecords();
        }
        threadBatches[thread].add(relationshipCursor);
    }

    private static void processLastRelationshipChecks(
            ArrayBlockingQueue<BatchedRelationshipRecords>[] threadQueues,
            BatchedRelationshipRecords[] threadBatches,
            AtomicBoolean end)
            throws Exception {
        for (int i = 0; i < threadBatches.length; i++) {
            if (threadBatches[i].numberOfRelationships() > 0) {
                threadQueues[i].put(threadBatches[i]);
            }
        }
        end.set(true);
    }

    @Override
    public String toString() {
        var relGroupStore = context.neoStores.getRelationshipStore();
        return String.format(
                "%s[highId:%d]",
                getClass().getSimpleName(), relGroupStore.getIdGenerator().getHighId());
    }

    private enum ScanDirection {
        FORWARD(SOURCE_PREV, TARGET_PREV, CacheSlots.RelationshipLink.PREV) {
            @Override
            boolean exclude(long id, long reference) {
                return !NULL_REFERENCE.is(reference) && reference > id;
            }

            @Override
            long nextId(long id) {
                return id + 1;
            }

            @Override
            long startingId(long highId) {
                return 0;
            }
        },
        BACKWARD(SOURCE_NEXT, TARGET_NEXT, CacheSlots.RelationshipLink.NEXT) {
            @Override
            boolean exclude(long id, long reference) {
                return !NULL_REFERENCE.is(reference) && reference < id;
            }

            @Override
            long nextId(long id) {
                return id - 1;
            }

            @Override
            long startingId(long highId) {
                return highId - 1;
            }
        };

        final RelationshipLink sourceLink;
        final RelationshipLink targetLink;
        final long cacheSlot;

        ScanDirection(RelationshipLink sourceLink, RelationshipLink targetLink, long cacheSlot) {
            this.sourceLink = sourceLink;
            this.targetLink = targetLink;
            this.cacheSlot = cacheSlot;
        }

        abstract boolean exclude(long id, long reference);

        abstract long nextId(long id);

        abstract long startingId(long highId);
    }
}
