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

import static org.neo4j.common.EntityType.RELATIONSHIP;
import static org.neo4j.consistency.checker.NodeChecker.compareTwoSortedIntArrays;
import static org.neo4j.consistency.checker.RecordLoading.checkValidToken;
import static org.neo4j.consistency.checker.RecordLoading.lightReplace;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;

import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.neo4j.consistency.checking.ConsistencyFlags;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.cache.CacheSlots;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.synthetic.TokenScanDocument;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.recordstorage.RelationshipCounter;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaPatternMatchingType;
import org.neo4j.internal.schema.constraints.PropertyTypeSet;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.index.schema.EntityTokenRange;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.storable.Value;

/**
 * Checks relationships and their properties, type and schema indexes.
 */
class RelationshipChecker implements Checker {
    private static final String UNUSED_RELATIONSHIP_CHECKER_TAG = "unusedRelationshipChecker";
    private static final String RELATIONSHIP_RANGE_CHECKER_TAG = "relationshipRangeChecker";
    private final NeoStores neoStores;
    private final ParallelExecution execution;
    private final ConsistencyReport.Reporter reporter;
    private final CacheAccess cacheAccess;
    private final TokenHolders tokenHolders;
    private final RecordLoading recordLoader;
    private final CountsState observedCounts;
    private final CheckerContext context;
    private final IntObjectMap<? extends IntSet> mandatoryProperties;
    private final IntObjectMap<? extends IntObjectMap<PropertyTypeSet>> allowedTypes;
    private final List<IndexDescriptor> indexes;
    private final ProgressListener progress;

    RelationshipChecker(
            CheckerContext context,
            IntObjectMap<? extends IntSet> mandatoryProperties,
            IntObjectMap<? extends IntObjectMap<PropertyTypeSet>> allowedTypes) {
        this.context = context;
        this.neoStores = context.neoStores;
        this.execution = context.execution;
        this.reporter = context.reporter;
        this.cacheAccess = context.cacheAccess;
        this.tokenHolders = context.tokenHolders;
        this.recordLoader = context.recordLoader;
        this.observedCounts = context.observedCounts;
        this.mandatoryProperties = mandatoryProperties;
        this.allowedTypes = allowedTypes;
        this.indexes = context.indexSizes.smallIndexes(RELATIONSHIP);
        this.progress = context.progressReporter(
                this,
                "Relationships",
                neoStores.getRelationshipStore().getIdGenerator().getHighId());
    }

    @Override
    public boolean shouldBeChecked(ConsistencyFlags flags) {
        return flags.checkGraph() || !indexes.isEmpty() && flags.checkIndexes();
    }

    @Override
    public void check(LongRange nodeIdRange, boolean firstRange, boolean lastRange) throws Exception {
        execution.run(
                getClass().getSimpleName() + "-relationships",
                execution.partition(
                        neoStores.getRelationshipStore(),
                        (from, to, last) -> () -> check(nodeIdRange, firstRange, from, to, firstRange && last)));
        // Let's not report progress for this since it's so much faster than store checks, it's just scanning the cache
        execution.run(
                getClass().getSimpleName() + "-unusedRelationships",
                execution.partition(
                        nodeIdRange,
                        (from, to, last) ->
                                () -> checkNodesReferencingUnusedRelationships(from, to, context.contextFactory)));
    }

    private void check(
            LongRange nodeIdRange,
            boolean firstRound,
            long fromRelationshipId,
            long toRelationshipId,
            boolean checkToEndOfIndex)
            throws Exception {
        RelationshipCounter counter = observedCounts.instantiateRelationshipCounter();
        int[] typeHolder = new int[1];
        try (var cursorContext = context.contextFactory.create(RELATIONSHIP_RANGE_CHECKER_TAG);
                var storeCursors = new CachedStoreCursors(this.context.neoStores, cursorContext);
                RecordReader<RelationshipRecord> relationshipReader =
                        new RecordReader<>(context.neoStores.getRelationshipStore(), true, cursorContext);
                BoundedIterable<EntityTokenRange> relationshipTypeReader = getRelationshipTypeIndexReader(
                        fromRelationshipId, toRelationshipId, checkToEndOfIndex, cursorContext);
                SafePropertyChainReader property = new SafePropertyChainReader(context, cursorContext);
                SchemaComplianceChecker schemaComplianceChecker = new SchemaComplianceChecker(
                        context, mandatoryProperties, allowedTypes, indexes, cursorContext, storeCursors);
                var localProgress = progress.threadLocalReporter();
                var freeIdsIterator = firstRound
                        ? context.neoStores
                                .getRelationshipStore()
                                .getIdGenerator()
                                .notUsedIdsIterator(fromRelationshipId, toRelationshipId)
                        : null) {
            CacheAccess.Client client = cacheAccess.client();
            IntObjectHashMap<Value> propertyValues = new IntObjectHashMap<>();
            Iterator<EntityTokenRange> relationshipTypeRangeIterator = relationshipTypeReader.iterator();
            EntityTokenIndexCheckState typeIndexState = new EntityTokenIndexCheckState(null, fromRelationshipId - 1);
            long nextFreeId = NULL_REFERENCE.longValue();

            for (long relationshipId = fromRelationshipId;
                    relationshipId < toRelationshipId && !context.isCancelled();
                    relationshipId++) {
                localProgress.add(1);
                RelationshipRecord relationshipRecord = relationshipReader.read(relationshipId);
                if (firstRound) {
                    while (nextFreeId < relationshipId && freeIdsIterator.hasNext()) {
                        nextFreeId = freeIdsIterator.next();
                    }
                }
                if (!relationshipRecord.inUse()) {
                    if (firstRound) {
                        if (relationshipId != nextFreeId) {
                            reporter.forRelationship(relationshipRecord).idIsNotFreed();
                        }
                    }
                    continue;
                }

                // Start/end nodes
                long startNode = relationshipRecord.getFirstNode();
                boolean startNodeIsWithinRange = nodeIdRange.isWithinRangeExclusiveTo(startNode);
                boolean startNodeIsNegativeOnFirstRound = startNode < 0 && firstRound;
                if (startNodeIsWithinRange || startNodeIsNegativeOnFirstRound) {
                    checkRelationshipVsNode(
                            client,
                            relationshipRecord,
                            startNode,
                            relationshipRecord.isFirstInFirstChain(),
                            (relationship, node) ->
                                    reporter.forRelationship(relationship).sourceNodeNotInUse(node),
                            (relationship, node) ->
                                    reporter.forRelationship(relationship).sourceNodeDoesNotReferenceBack(node),
                            (relationship, node) ->
                                    reporter.forNode(node).relationshipNotFirstInSourceChain(relationship),
                            (relationship, node) ->
                                    reporter.forRelationship(relationship).sourceNodeHasNoRelationships(node),
                            relationship ->
                                    reporter.forRelationship(relationship).illegalSourceNode(),
                            storeCursors);
                }
                long endNode = relationshipRecord.getSecondNode();
                boolean endNodeIsWithinRange = nodeIdRange.isWithinRangeExclusiveTo(endNode);
                boolean endNodeIsNegativeOnFirstRound = endNode < 0 && firstRound;
                if (endNodeIsWithinRange || endNodeIsNegativeOnFirstRound) {
                    checkRelationshipVsNode(
                            client,
                            relationshipRecord,
                            endNode,
                            relationshipRecord.isFirstInSecondChain(),
                            (relationship, node) ->
                                    reporter.forRelationship(relationship).targetNodeNotInUse(node),
                            (relationship, node) ->
                                    reporter.forRelationship(relationship).targetNodeDoesNotReferenceBack(node),
                            (relationship, node) ->
                                    reporter.forNode(node).relationshipNotFirstInTargetChain(relationship),
                            (relationship, node) ->
                                    reporter.forRelationship(relationship).targetNodeHasNoRelationships(node),
                            relationship ->
                                    reporter.forRelationship(relationship).illegalTargetNode(),
                            storeCursors);
                }

                if (firstRound) {
                    if (relationshipId == nextFreeId) {
                        reporter.forRelationship(relationshipRecord).idIsFreed();
                    }
                    if (startNode >= context.highNodeId) {
                        reporter.forRelationship(relationshipRecord)
                                .sourceNodeNotInUse(context.recordLoader.node(startNode, storeCursors));
                    }

                    if (endNode >= context.highNodeId) {
                        reporter.forRelationship(relationshipRecord)
                                .targetNodeNotInUse(context.recordLoader.node(endNode, storeCursors));
                    }

                    // Properties
                    typeHolder[0] = relationshipRecord.getType();
                    propertyValues = lightReplace(propertyValues);
                    boolean propertyChainIsOk =
                            property.read(propertyValues, relationshipRecord, reporter::forRelationship, storeCursors);
                    if (propertyChainIsOk) {
                        schemaComplianceChecker.checkExistenceAndTypeConstraints(
                                relationshipRecord, typeHolder, propertyValues, reporter::forRelationship);
                        // Here only the very small indexes (or indexes that we can't read the values from, like
                        // fulltext indexes)
                        // gets checked this way, larger indexes will be checked in IndexChecker
                        if (context.consistencyFlags.checkIndexes()) {
                            schemaComplianceChecker.checkCorrectlyIndexed(
                                    relationshipRecord, typeHolder, propertyValues, reporter::forRelationship);
                        }
                    }

                    // Type and count
                    checkValidToken(
                            relationshipRecord,
                            relationshipRecord.getType(),
                            tokenHolders.relationshipTypeTokens(),
                            neoStores.getRelationshipTypeTokenStore(),
                            (rel, token) -> reporter.forRelationship(rel).illegalRelationshipType(),
                            (rel, token) -> reporter.forRelationship(rel).relationshipTypeNotInUse(token),
                            storeCursors);
                    observedCounts.incrementRelationshipTypeCounts(counter, relationshipRecord);

                    // Relationship type index
                    if (relationshipTypeReader.maxCount() != 0) {
                        checkRelationshipVsRelationshipTypeIndex(
                                relationshipRecord,
                                relationshipTypeRangeIterator,
                                typeIndexState,
                                relationshipId,
                                relationshipRecord.getType(),
                                fromRelationshipId,
                                storeCursors);
                    }
                }
                observedCounts.incrementRelationshipNodeCounts(
                        counter, relationshipRecord, startNodeIsWithinRange, endNodeIsWithinRange);
            }
            if (firstRound && !context.isCancelled() && relationshipTypeReader.maxCount() != 0) {
                reportRemainingRelationshipTypeIndexEntries(
                        relationshipTypeRangeIterator,
                        typeIndexState,
                        checkToEndOfIndex ? Long.MAX_VALUE : toRelationshipId,
                        storeCursors);
            }
        }
    }

    private BoundedIterable<EntityTokenRange> getRelationshipTypeIndexReader(
            long fromRelationshipId, long toRelationshipId, boolean last, CursorContext cursorContext) {
        if (context.relationshipTypeIndex != null) {
            return context.relationshipTypeIndex.newAllEntriesTokenReader(
                    fromRelationshipId, last ? Long.MAX_VALUE : toRelationshipId, cursorContext);
        }
        return BoundedIterable.empty();
    }

    private void checkRelationshipVsRelationshipTypeIndex(
            RelationshipRecord relationshipRecord,
            Iterator<EntityTokenRange> relationshipTypeRangeIterator,
            EntityTokenIndexCheckState relationshipTypeIndexState,
            long relationshipId,
            int type,
            long fromRelationshipId,
            StoreCursors storeCursors) {
        // Detect relationship-type combinations that exists in the relationship type index, but not in the store
        while (relationshipTypeIndexState.needToMoveRangeForwardToReachEntity(relationshipId)
                && !context.isCancelled()) {
            if (relationshipTypeRangeIterator.hasNext()) {
                if (relationshipTypeIndexState.currentRange != null) {
                    for (long relationshipIdMissingFromStore = relationshipTypeIndexState.lastCheckedEntityId + 1;
                            relationshipIdMissingFromStore < relationshipId
                                    && relationshipTypeIndexState.currentRange.covers(relationshipIdMissingFromStore);
                            relationshipIdMissingFromStore++) {
                        if (relationshipTypeIndexState.currentRange.tokens(relationshipIdMissingFromStore).length > 0) {
                            reporter.forRelationshipTypeScan(
                                            new TokenScanDocument(relationshipTypeIndexState.currentRange))
                                    .relationshipNotInUse(
                                            recordLoader.relationship(relationshipIdMissingFromStore, storeCursors));
                        }
                    }
                }
                relationshipTypeIndexState.currentRange = relationshipTypeRangeIterator.next();
                relationshipTypeIndexState.lastCheckedEntityId = Math.max(
                                fromRelationshipId,
                                relationshipTypeIndexState.currentRange.entities()[0])
                        - 1;
            } else {
                break;
            }
        }

        if (relationshipTypeIndexState.currentRange != null
                && relationshipTypeIndexState.currentRange.covers(relationshipId)) {
            for (long relationshipIdMissingFromStore = relationshipTypeIndexState.lastCheckedEntityId + 1;
                    relationshipIdMissingFromStore < relationshipId;
                    relationshipIdMissingFromStore++) {
                if (relationshipTypeIndexState.currentRange.tokens(relationshipIdMissingFromStore).length > 0) {
                    reporter.forRelationshipTypeScan(new TokenScanDocument(relationshipTypeIndexState.currentRange))
                            .relationshipNotInUse(
                                    recordLoader.relationship(relationshipIdMissingFromStore, storeCursors));
                }
            }
            int[] relationshipTypesInTypeIndex = relationshipTypeIndexState.currentRange.tokens(relationshipId);
            validateTypeIds(
                    relationshipRecord,
                    type,
                    relationshipTypesInTypeIndex,
                    relationshipTypeIndexState.currentRange,
                    storeCursors);
            relationshipTypeIndexState.lastCheckedEntityId = relationshipId;
        } else {
            TokenScanDocument document = new TokenScanDocument(null);
            reporter.forRelationshipTypeScan(document)
                    .relationshipTypeNotInIndex(recordLoader.relationship(relationshipId, storeCursors), type);
        }
    }

    private void reportRemainingRelationshipTypeIndexEntries(
            Iterator<EntityTokenRange> relationshipTypeRangeIterator,
            EntityTokenIndexCheckState relationshipTypeIndexState,
            long toRelationshipId,
            StoreCursors storeCursors) {
        if (relationshipTypeIndexState.currentRange == null && relationshipTypeRangeIterator.hasNext()) {
            // Seems that nobody touched this iterator before, i.e. no nodes in this whole range
            relationshipTypeIndexState.currentRange = relationshipTypeRangeIterator.next();
        }

        while (relationshipTypeIndexState.currentRange != null && !context.isCancelled()) {
            for (long relationshipIdMissingFromStore = relationshipTypeIndexState.lastCheckedEntityId + 1;
                    relationshipIdMissingFromStore < toRelationshipId
                            && !relationshipTypeIndexState.needToMoveRangeForwardToReachEntity(
                                    relationshipIdMissingFromStore);
                    relationshipIdMissingFromStore++) {
                if (relationshipTypeIndexState.currentRange.covers(relationshipIdMissingFromStore)
                        && relationshipTypeIndexState.currentRange.tokens(relationshipIdMissingFromStore).length > 0) {
                    reporter.forRelationshipTypeScan(new TokenScanDocument(relationshipTypeIndexState.currentRange))
                            .relationshipNotInUse(
                                    recordLoader.relationship(relationshipIdMissingFromStore, storeCursors));
                }
                relationshipTypeIndexState.lastCheckedEntityId = relationshipIdMissingFromStore;
            }
            relationshipTypeIndexState.currentRange =
                    relationshipTypeRangeIterator.hasNext() ? relationshipTypeRangeIterator.next() : null;
        }
    }

    private void validateTypeIds(
            RelationshipRecord relationshipRecord,
            int typeInStore,
            int[] relationshipTypesInTypeIndex,
            EntityTokenRange entityTokenRange,
            StoreCursors storeCursors) {
        compareTwoSortedIntArrays(
                SchemaPatternMatchingType.COMPLETE_ALL_TOKENS,
                new int[] {typeInStore},
                relationshipTypesInTypeIndex,
                indexType -> reporter.forRelationshipTypeScan(new TokenScanDocument(entityTokenRange))
                        .relationshipDoesNotHaveExpectedRelationshipType(
                                recordLoader.relationship(relationshipRecord.getId(), storeCursors), indexType),
                storeType -> reporter.forRelationshipTypeScan(new TokenScanDocument(entityTokenRange))
                        .relationshipTypeNotInIndex(
                                recordLoader.relationship(relationshipRecord.getId(), storeCursors), storeType));
    }

    private void checkRelationshipVsNode(
            CacheAccess.Client client,
            RelationshipRecord relationshipRecord,
            long node,
            boolean firstInChain,
            BiConsumer<RelationshipRecord, NodeRecord> reportNodeNotInUse,
            BiConsumer<RelationshipRecord, NodeRecord> reportNodeDoesNotReferenceBack,
            BiConsumer<RelationshipRecord, NodeRecord> reportNodeNotFirstInChain,
            BiConsumer<RelationshipRecord, NodeRecord> reportNodeHasNoChain,
            Consumer<RelationshipRecord> reportIllegalNode,
            StoreCursors storeCursors) {
        // Check validity of node reference
        if (node < 0) {
            reportIllegalNode.accept(recordLoader.relationship(relationshipRecord.getId(), storeCursors));
            return;
        }

        // Check if node is in use
        boolean nodeInUse = client.getBooleanFromCache(node, CacheSlots.NodeLink.SLOT_IN_USE);
        if (!nodeInUse) {
            reportNodeNotInUse.accept(
                    recordLoader.relationship(relationshipRecord.getId(), storeCursors),
                    recordLoader.node(node, storeCursors));
            return;
        }

        // Check if node has nextRel reference at all
        long nodeNextRel = client.getFromCache(node, CacheSlots.NodeLink.SLOT_RELATIONSHIP_ID);
        if (NULL_REFERENCE.is(nodeNextRel)) {
            reportNodeHasNoChain.accept(
                    recordLoader.relationship(relationshipRecord.getId(), storeCursors),
                    recordLoader.node(node, storeCursors));
            return;
        }

        // Check the node <--> relationship references
        boolean nodeIsDense = client.getBooleanFromCache(node, CacheSlots.NodeLink.SLOT_IS_DENSE);
        if (!nodeIsDense) {
            if (firstInChain) {
                if (nodeNextRel != relationshipRecord.getId()) {
                    // Report RELATIONSHIP -> NODE inconsistency
                    reportNodeDoesNotReferenceBack.accept(
                            recordLoader.relationship(relationshipRecord.getId(), storeCursors),
                            recordLoader.node(node, storeCursors));
                    // Before marking this node as fully checked we should also check and report any NODE ->
                    // RELATIONSHIP inconsistency
                    RelationshipRecord relationshipThatNodeActuallyReferences =
                            recordLoader.relationship(nodeNextRel, storeCursors);
                    if (!relationshipThatNodeActuallyReferences.inUse()) {
                        reporter.forNode(recordLoader.node(node, storeCursors))
                                .relationshipNotInUse(relationshipThatNodeActuallyReferences);
                    } else if (relationshipThatNodeActuallyReferences.getFirstNode() != node
                            && relationshipThatNodeActuallyReferences.getSecondNode() != node) {
                        reporter.forNode(recordLoader.node(node, storeCursors))
                                .relationshipForOtherNode(relationshipThatNodeActuallyReferences);
                    }
                }
                client.putToCacheSingle(node, CacheSlots.NodeLink.SLOT_CHECK_MARK, 0);
            }
            if (!firstInChain && nodeNextRel == relationshipRecord.getId()) {
                reportNodeNotFirstInChain.accept(
                        recordLoader.relationship(relationshipRecord.getId(), storeCursors),
                        recordLoader.node(node, storeCursors));
            }
        }
    }

    private void checkNodesReferencingUnusedRelationships(
            long fromNodeId, long toNodeId, CursorContextFactory contextFactory) {
        // Do this after we've done node.nextRel caching and checking of those. Checking also clears those values, so
        // simply
        // go through the cache and see if there are any relationship ids left and report them
        CacheAccess.Client client = cacheAccess.client();
        try (var cursorContext = contextFactory.create(UNUSED_RELATIONSHIP_CHECKER_TAG);
                var storeCursors = new CachedStoreCursors(this.context.neoStores, cursorContext)) {
            for (long id = fromNodeId; id < toNodeId && !context.isCancelled(); id++) {
                // Only check if we haven't come across this sparse node while checking relationships
                boolean nodeInUse = client.getBooleanFromCache(id, CacheSlots.NodeLink.SLOT_IN_USE);
                if (nodeInUse) {
                    boolean needsChecking = client.getBooleanFromCache(id, CacheSlots.NodeLink.SLOT_CHECK_MARK);
                    if (needsChecking) {
                        long nodeNextRel = client.getFromCache(id, CacheSlots.NodeLink.SLOT_RELATIONSHIP_ID);
                        boolean nodeIsDense = client.getBooleanFromCache(id, CacheSlots.NodeLink.SLOT_IS_DENSE);
                        if (!NULL_REFERENCE.is(nodeNextRel)) {
                            if (!nodeIsDense) {
                                RelationshipRecord relationship = recordLoader.relationship(nodeNextRel, storeCursors);
                                NodeRecord node = recordLoader.node(id, storeCursors);
                                if (!relationship.inUse()) {
                                    reporter.forNode(node).relationshipNotInUse(relationship);
                                } else {
                                    reporter.forNode(node).relationshipForOtherNode(relationship);
                                }
                            } else {
                                RelationshipGroupRecord group =
                                        recordLoader.relationshipGroup(nodeNextRel, storeCursors);
                                if (!group.inUse()) {
                                    reporter.forNode(recordLoader.node(id, storeCursors))
                                            .relationshipGroupNotInUse(group);
                                } else {
                                    reporter.forNode(recordLoader.node(id, storeCursors))
                                            .relationshipGroupHasOtherOwner(group);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    public String toString() {
        var relStore = neoStores.getRelationshipStore();
        return String.format(
                "%s[highId:%d,indexesToCheck:%d]",
                getClass().getSimpleName(), relStore.getIdGenerator().getHighId(), indexes.size());
    }
}
