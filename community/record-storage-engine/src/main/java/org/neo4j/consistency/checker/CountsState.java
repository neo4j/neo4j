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

import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.neo4j.internal.counts.GBPTreeCountsStore.nodeKey;
import static org.neo4j.internal.counts.GBPTreeCountsStore.relationshipKey;
import static org.neo4j.internal.recordstorage.RelationshipCounter.labelsCountsLength;
import static org.neo4j.internal.recordstorage.RelationshipCounter.wildcardCountsLength;
import static org.neo4j.kernel.impl.store.InlineNodeLabels.parseInlined;
import static org.neo4j.token.api.TokenConstants.ANY_LABEL;
import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.cache.CacheSlots;
import org.neo4j.consistency.report.ConsistencyReporter;
import org.neo4j.consistency.store.synthetic.CountsEntry;
import org.neo4j.counts.CountsVisitor;
import org.neo4j.internal.batchimport.cache.LongArray;
import org.neo4j.internal.batchimport.cache.NumberArrayFactories;
import org.neo4j.internal.batchimport.cache.OffHeapLongArray;
import org.neo4j.internal.counts.CountsKey;
import org.neo4j.internal.recordstorage.RelationshipCounter;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.memory.MemoryTracker;

/**
 * Keeps observed counts when checking nodes and relationships. This counts state can then be checked against the counts store.
 */
class CountsState implements AutoCloseable {
    private static final long COUNT_VISITED_MARK = 0x40000000_00000000L;
    private final int highLabelId;
    private final int highRelationshipTypeId;
    private final long highNodeId;
    private final CacheAccess cacheAccess;
    private final OffHeapLongArray nodeCounts;
    private final ConcurrentMap<CountsKey, AtomicLong> nodeCountsStray = new ConcurrentHashMap<>();
    private final LongArray relationshipLabelCounts;
    private final LongArray relationshipWildcardCounts;
    private final ConcurrentMap<CountsKey, AtomicLong> relationshipCountsStray = new ConcurrentHashMap<>();
    private final DynamicNodeLabelsCache dynamicNodeLabelsCache;

    CountsState(NeoStores neoStores, CacheAccess cacheAccess, MemoryTracker memoryTracker) {
        this(
                (int) neoStores.getLabelTokenStore().getIdGenerator().getHighId(),
                (int) neoStores.getRelationshipTypeTokenStore().getIdGenerator().getHighId(),
                neoStores.getNodeStore().getIdGenerator().getHighId(),
                cacheAccess,
                memoryTracker);
    }

    CountsState(
            int highLabelId,
            int highRelationshipTypeId,
            long highNodeId,
            CacheAccess cacheAccess,
            MemoryTracker memoryTracker) {
        this.highLabelId = highLabelId;
        this.highRelationshipTypeId = highRelationshipTypeId;
        this.highNodeId = highNodeId;
        this.cacheAccess = cacheAccess;
        var arrayFactory = NumberArrayFactories.OFF_HEAP;
        this.nodeCounts = (OffHeapLongArray) arrayFactory.newLongArray(highLabelId + 1, 0, memoryTracker);
        this.relationshipLabelCounts =
                arrayFactory.newLongArray(labelsCountsLength(highLabelId, highRelationshipTypeId), 0, memoryTracker);
        this.relationshipWildcardCounts =
                arrayFactory.newLongArray(wildcardCountsLength(highRelationshipTypeId), 0, memoryTracker);
        this.dynamicNodeLabelsCache = new DynamicNodeLabelsCache(memoryTracker);
    }

    @Override
    public void close() {
        nodeCounts.close();
        relationshipLabelCounts.close();
        relationshipWildcardCounts.close();
        dynamicNodeLabelsCache.close();
    }

    RelationshipCounter instantiateRelationshipCounter() {
        return new RelationshipCounter(
                nodeLabelsLookup(),
                highLabelId,
                highRelationshipTypeId,
                relationshipWildcardCounts,
                relationshipLabelCounts,
                (array, index) -> ((OffHeapLongArray) array).getAndAdd(index, 1));
    }

    long cacheDynamicNodeLabels(int[] labelIds) {
        return dynamicNodeLabelsCache.put(labelIds);
    }

    void clearDynamicNodeLabelsCache() {
        dynamicNodeLabelsCache.clear();
    }

    RelationshipCounter.NodeLabelsLookup nodeLabelsLookup() {
        return new RelationshipCounter.NodeLabelsLookup() {
            private final CacheAccess.Client cacheAccessClient = cacheAccess.client();
            private final int[] labelsHolder = new int[20]; // should be big enough for most cases, right?

            @Override
            public int[] nodeLabels(long nodeId) {
                if (nodeId >= highNodeId) {
                    return EMPTY_INT_ARRAY;
                }
                boolean hasSingleLabel =
                        cacheAccessClient.getBooleanFromCache(nodeId, CacheSlots.NodeLink.SLOT_HAS_SINGLE_LABEL);
                long labelField = cacheAccessClient.getFromCache(nodeId, CacheSlots.NodeLink.SLOT_LABELS);
                if (hasSingleLabel) {
                    // Just grab the field, which represents a single label and then terminate the array with -1
                    labelsHolder[0] = (int) labelField;
                    labelsHolder[1] = -1;
                    return labelsHolder;
                } else {
                    boolean hasInlinedLabels =
                            cacheAccessClient.getBooleanFromCache(nodeId, CacheSlots.NodeLink.SLOT_HAS_INLINED_LABELS);
                    return hasInlinedLabels
                            ? parseInlined(labelField)
                            : dynamicNodeLabelsCache.get(labelField, labelsHolder);
                }
            }
        };
    }

    void incrementNodeLabel(int label, long increment) {
        if (isValidLabelId(label)) {
            nodeCounts.getAndAdd(labelIdArrayPos(label), increment);
        } else {
            nodeCountsStray
                    .computeIfAbsent(nodeKey(label), k -> new AtomicLong())
                    .addAndGet(increment);
        }
    }

    private boolean isValidLabelId(int label) {
        return label == ANY_LABEL || (label >= 0 && label < highLabelId);
    }

    /**
     * Increments counts for the given {@code relationship}, counts that revolve around the relationship type, not its nodes or node labels.
     * @param counter {@link RelationshipCounter} used for incrementing.
     * @param relationship {@link RelationshipRecord} containing type information.
     */
    void incrementRelationshipTypeCounts(RelationshipCounter counter, RelationshipRecord relationship) {
        counter.processRelationshipTypeCounts(relationship, (s, t, e) -> relationshipCountsStray
                .computeIfAbsent(relationshipKey(s, t, e), k -> new AtomicLong())
                .incrementAndGet());
    }

    /**
     * Increments counts for the given {@code relationship}, counts that revolve around the nodes and node labels.
     * @param counter {@link RelationshipCounter} used for incrementing.
     * @param relationship {@link RelationshipRecord} containing type information.
     * @param processStartNode whether or not to increment counts for the start node.
     * @param processEndNode whether or not to increment counts for the end node.
     */
    void incrementRelationshipNodeCounts(
            RelationshipCounter counter,
            RelationshipRecord relationship,
            boolean processStartNode,
            boolean processEndNode) {
        counter.processRelationshipNodeCounts(
                relationship,
                (s, t, e) -> relationshipCountsStray
                        .computeIfAbsent(relationshipKey(s, t, e), k -> new AtomicLong())
                        .incrementAndGet(),
                processStartNode,
                processEndNode);
    }

    private static boolean hasVisitedCountMark(long countValue) {
        return (countValue & COUNT_VISITED_MARK) != 0;
    }

    private static long markCountVisited(long countValue) {
        return countValue | COUNT_VISITED_MARK;
    }

    private static long unmarkCountVisited(long countValue) {
        return countValue & ~COUNT_VISITED_MARK;
    }

    CountsChecker checker(ConsistencyReporter reporter) {
        return new CountsChecker() {
            final RelationshipCounter relationshipCounter = instantiateRelationshipCounter();

            @Override
            public void visitNodeCount(int labelId, long count) {
                if (isValidLabelId(labelId)) {
                    long pos = labelIdArrayPos(labelId);
                    long expected = unmarkCountVisited(nodeCounts.get(pos));
                    if (expected != count) {
                        reporter.forCounts(new CountsEntry(nodeKey(labelId), count))
                                .inconsistentNodeCount(expected);
                    }
                    nodeCounts.set(pos, markCountVisited(expected));
                } else {
                    AtomicLong expected = nodeCountsStray.remove(nodeKey(labelId));
                    if (expected != null) {
                        if (expected.longValue() != count) {
                            reporter.forCounts(new CountsEntry(nodeKey(labelId), count))
                                    .inconsistentNodeCount(expected.longValue());
                        }
                    } else {
                        reporter.forCounts(new CountsEntry(nodeKey(labelId), count))
                                .inconsistentNodeCount(0);
                    }
                }
            }

            @Override
            public void visitRelationshipCount(int startLabelId, int relTypeId, int endLabelId, long count) {
                CountsKey countsKey = relationshipKey(startLabelId, relTypeId, endLabelId);
                if (relationshipCounter.isValid(startLabelId, relTypeId, endLabelId)) {
                    long expected = unmarkCountVisited(relationshipCounter.get(startLabelId, relTypeId, endLabelId));
                    if (expected != count) {
                        reporter.forCounts(new CountsEntry(countsKey, count)).inconsistentRelationshipCount(expected);
                    }
                    relationshipCounter.set(startLabelId, relTypeId, endLabelId, markCountVisited(expected));
                } else {
                    AtomicLong expected = relationshipCountsStray.remove(countsKey);
                    if (expected != null) {
                        if (expected.longValue() != count) {
                            reporter.forCounts(new CountsEntry(countsKey, count))
                                    .inconsistentRelationshipCount(expected.longValue());
                        }
                    } else {
                        reporter.forCounts(new CountsEntry(countsKey, count)).inconsistentRelationshipCount(0);
                    }
                }
            }

            @Override
            public void close() {
                // Report counts that have been collected in this consistency check, but aren't in the existing store
                for (int labelId = 0; labelId < highLabelId; labelId++) {
                    long count = nodeCounts.get(labelId);
                    if (!hasVisitedCountMark(count) && count > 0) {
                        reporter.forCounts(new CountsEntry(nodeKey(labelId), 0)).inconsistentNodeCount(count);
                    }
                }

                for (int label = ANY_LABEL; label < highLabelId; label++) {
                    for (int type = ANY_RELATIONSHIP_TYPE; type < highRelationshipTypeId; type++) {
                        // we only keep counts for where at least one of start/end is ANY
                        reportRelationshipIfNeeded(ANY_LABEL, type, label);
                        reportRelationshipIfNeeded(label, type, ANY_LABEL);
                    }
                }

                // Entities with invalid tokens that are missing from the counts store
                nodeCountsStray.forEach(
                        (countsKey, count) -> reporter.forCounts(new CountsEntry(countsKey, count.get()))
                                .inconsistentNodeCount(0));
                relationshipCountsStray.forEach(
                        (countsKey, count) -> reporter.forCounts(new CountsEntry(countsKey, count.get()))
                                .inconsistentRelationshipCount(0));
            }

            private void reportRelationshipIfNeeded(int labelStart, int relType, int labelEnd) {
                long count = relationshipCounter.get(labelStart, relType, labelEnd);
                if (!hasVisitedCountMark(count) && count > 0) {
                    reporter.forCounts(new CountsEntry(relationshipKey(labelStart, relType, labelEnd), 0))
                            .inconsistentRelationshipCount(count);
                }
            }
        };
    }

    /**
     * Valid label ids are [0,highLabelId) AND -1 (ANY_LABEL). -1 will be placed at highLabelId.
     */
    private long labelIdArrayPos(int labelId) {
        return labelId == ANY_LABEL ? highLabelId : labelId;
    }

    interface CountsChecker extends CountsVisitor, AutoCloseable {
        @Override
        void close();
    }
}
