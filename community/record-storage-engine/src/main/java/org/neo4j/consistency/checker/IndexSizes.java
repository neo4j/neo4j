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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.neo4j.common.EntityType;
import org.neo4j.consistency.checker.ParallelExecution.ThrowingRunnable;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.internal.schema.FulltextSchemaDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.api.index.IndexAccessor;

/**
 * Calculates index sizes in parallel and caches the sizes
 */
class IndexSizes {
    private static final String SIZE_CALCULATOR_TAG = "sizeCalculator";
    private static final double SMALL_INDEX_FACTOR_THRESHOLD = 0.05;
    private static final int INEFFICIENT_AMOUNT_OF_RELATIONSHIP_ROUNDS = 3;

    private final ParallelExecution execution;
    private final IndexAccessors indexAccessors;
    private final ConcurrentMap<IndexDescriptor, Long> nodeIndexSizes = new ConcurrentHashMap<>();
    private final ConcurrentMap<IndexDescriptor, Long> relationshipIndexSizes = new ConcurrentHashMap<>();
    private final long highNodeId;
    private final long highRelationshipId;
    private final CursorContextFactory contextFactory;
    private final boolean shouldLetRelationshipIndexesBeLarge;

    IndexSizes(
            ParallelExecution execution,
            IndexAccessors indexAccessors,
            long highNodeId,
            long highRelationshipId,
            CursorContextFactory contextFactory,
            EntityBasedMemoryLimiter limiter) {
        this.execution = execution;
        this.indexAccessors = indexAccessors;
        this.highNodeId = highNodeId;
        this.highRelationshipId = highRelationshipId;
        this.contextFactory = contextFactory;
        this.shouldLetRelationshipIndexesBeLarge =
                limiter.numberOfRelationshipRanges() / Long.max(1, limiter.numberOfNodeRanges())
                        <= INEFFICIENT_AMOUNT_OF_RELATIONSHIP_ROUNDS;
    }

    void initialize() throws Exception {
        calculateSizes(EntityType.NODE, nodeIndexSizes);
        calculateSizes(EntityType.RELATIONSHIP, relationshipIndexSizes);
    }

    private void calculateSizes(EntityType entityType, ConcurrentMap<IndexDescriptor, Long> indexSizes)
            throws Exception {
        List<IndexDescriptor> indexes = indexAccessors.onlineRules(entityType);
        execution.run(
                "Estimate index sizes",
                indexes.stream()
                        .map(index -> (ThrowingRunnable) () -> {
                            try (var cursorContext = contextFactory.create(SIZE_CALCULATOR_TAG)) {
                                IndexAccessor accessor = indexAccessors.accessorFor(index);
                                indexSizes.put(index, accessor.estimateNumberOfEntries(cursorContext));
                            }
                        })
                        .toArray(ThrowingRunnable[]::new));
    }

    private List<IndexDescriptor> getAllIndexes(EntityType entityType) {
        return new ArrayList<>(indexAccessors.onlineRules(entityType));
    }

    List<IndexDescriptor> smallIndexes(EntityType entityType) {
        List<IndexDescriptor> smallIndexes = getAllIndexes(entityType);
        smallIndexes.removeAll(largeIndexes(entityType));
        return smallIndexes;
    }

    List<IndexDescriptor> largeIndexes(EntityType entityType) {
        if (entityType == EntityType.RELATIONSHIP && !shouldLetRelationshipIndexesBeLarge) {
            // Having "large" relationship indexes checked by the IndexChecker would be inefficient
            // due to doing too many rounds, so instead check them using the normal RelationshipChecker
            return Collections.emptyList();
        }

        List<IndexDescriptor> indexes = getAllIndexes(entityType);
        indexes.sort(Comparator.comparingLong(this::getEstimatedIndexSize).reversed());
        int threshold = 0;
        for (IndexDescriptor index : indexes) {
            if (!index.schema().isSchemaDescriptorType(FulltextSchemaDescriptor.class) && !hasValues(index)) {
                // Skip those that we cannot read values from. They should not be checked by the IndexChecker,
                // but the "inefficient" way of doing a lookup per node/index in NodeChecker instead
                continue;
            }

            if (getSizeFactor(index, entityType) > SMALL_INDEX_FACTOR_THRESHOLD
                    || threshold % IndexChecker.NUM_INDEXES_IN_CACHE != 0) {
                threshold++;
            }
        }
        return indexes.subList(0, threshold);
    }

    static boolean hasValues(IndexDescriptor index) {
        return index.getCapability().supportsReturningValues()
                && !index.schema().isSchemaDescriptorType(FulltextSchemaDescriptor.class);
    }

    private double getSizeFactor(IndexDescriptor index, EntityType entityType) {
        if (entityType == EntityType.RELATIONSHIP) {
            return (double) getEstimatedIndexSize(index) / highRelationshipId;
        }
        return (double) getEstimatedIndexSize(index) / highNodeId;
    }

    long getEstimatedIndexSize(IndexDescriptor index) {
        EntityType entityType = index.schema().entityType();
        ConcurrentMap<IndexDescriptor, Long> map =
                entityType == EntityType.NODE ? nodeIndexSizes : relationshipIndexSizes;
        return map.get(index);
    }
}
