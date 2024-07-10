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

import static java.util.Arrays.sort;
import static org.apache.commons.lang3.math.NumberUtils.max;
import static org.apache.commons.lang3.math.NumberUtils.min;
import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.consistency.checker.RecordLoading.checkValidToken;
import static org.neo4j.consistency.checker.RecordLoading.lightReplace;
import static org.neo4j.kernel.impl.store.record.Record.NO_LABELS_FIELD;
import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;
import static org.neo4j.token.api.TokenConstants.ANY_LABEL;

import java.util.Iterator;
import java.util.List;
import java.util.function.IntConsumer;
import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.neo4j.collection.PrimitiveArrays;
import org.neo4j.consistency.checking.ConsistencyFlags;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.cache.CacheSlots;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.consistency.store.synthetic.IndexEntry;
import org.neo4j.consistency.store.synthetic.TokenScanDocument;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.recordstorage.RelationshipCounter;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaPatternMatchingType;
import org.neo4j.internal.schema.constraints.PropertyTypeSet;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.impl.index.schema.EntityTokenRange;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NoStoreHeader;
import org.neo4j.kernel.impl.store.NodeLabelsField;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.storable.Value;

/**
 * Checks nodes and their properties, labels and schema and label indexes.
 */
class NodeChecker implements Checker {
    private static final String NODE_INDEXES_CHECKER_TAG = "nodeIndexesChecker";
    private static final String NODE_RANGE_CHECKER_TAG = "nodeRangeChecker";
    private final IntObjectMap<? extends IntSet> mandatoryProperties;
    private final IntObjectMap<? extends IntObjectMap<PropertyTypeSet>> allowedTypes;
    private final ProgressListener nodeProgress;
    private final CheckerContext context;
    private final ConsistencyReport.Reporter reporter;
    private final CountsState observedCounts;
    private final RecordLoading recordLoader;
    private final TokenHolders tokenHolders;
    private final NeoStores neoStores;
    private final List<IndexDescriptor> smallIndexes;

    NodeChecker(
            CheckerContext context,
            IntObjectMap<? extends IntSet> mandatoryProperties,
            IntObjectMap<? extends IntObjectMap<PropertyTypeSet>> allowedTypes) {
        this.context = context;
        this.reporter = context.reporter;
        this.observedCounts = context.observedCounts;
        this.recordLoader = context.recordLoader;
        this.tokenHolders = context.tokenHolders;
        this.neoStores = context.neoStores;
        this.mandatoryProperties = mandatoryProperties;
        this.allowedTypes = allowedTypes;
        // indices are checked in following method via shouldBeChecked and so can't be null
        this.smallIndexes = context.indexSizes.smallIndexes(NODE);
        this.nodeProgress = context.roundInsensitiveProgressReporter(
                this, "Nodes", neoStores.getNodeStore().getIdGenerator().getHighId());
    }

    @Override
    public void check(LongRange nodeIdRange, boolean firstRange, boolean lastRange) throws Exception {
        ParallelExecution execution = context.execution;
        execution.run(
                getClass().getSimpleName() + "-checkNodes",
                execution.partition(nodeIdRange, (from, to, last) -> () -> check(from, to, lastRange && last)));

        if (context.consistencyFlags.checkIndexes()) {
            execution.run(
                    getClass().getSimpleName() + "-checkIndexesVsNodes",
                    smallIndexes.stream()
                            .map(indexDescriptor -> (ParallelExecution.ThrowingRunnable)
                                    () -> checkIndexVsNodes(nodeIdRange, indexDescriptor, lastRange))
                            .toArray(ParallelExecution.ThrowingRunnable[]::new));
        }
    }

    @Override
    public boolean shouldBeChecked(ConsistencyFlags flags) {
        return flags.checkGraph() || flags.checkIndexes() && !smallIndexes.isEmpty();
    }

    private BoundedIterable<EntityTokenRange> getLabelIndexReader(
            long fromNodeId, long toNodeId, boolean last, CursorContext cursorContext) {
        if (context.nodeLabelIndex != null) {
            return context.nodeLabelIndex.newAllEntriesTokenReader(
                    fromNodeId, last ? Long.MAX_VALUE : toNodeId, cursorContext);
        }
        return BoundedIterable.empty();
    }

    private void check(long fromNodeId, long toNodeId, boolean last) throws Exception {
        long usedNodes = 0;
        try (var cursorContext = context.contextFactory.create(NODE_RANGE_CHECKER_TAG);
                var storeCursors = new CachedStoreCursors(context.neoStores, cursorContext);
                RecordReader<NodeRecord> nodeReader =
                        new RecordReader<>(context.neoStores.getNodeStore(), true, cursorContext);
                RecordReader<DynamicRecord> labelReader = new RecordReader<>(
                        context.neoStores.getNodeStore().getDynamicLabelStore(), false, cursorContext);
                BoundedIterable<EntityTokenRange> labelIndexReader =
                        getLabelIndexReader(fromNodeId, toNodeId, last, cursorContext);
                SafePropertyChainReader property = new SafePropertyChainReader(context, cursorContext);
                SchemaComplianceChecker schemaComplianceChecker = new SchemaComplianceChecker(
                        context, mandatoryProperties, allowedTypes, smallIndexes, cursorContext, storeCursors);
                var localProgress = nodeProgress.threadLocalReporter();
                var freeIdsIterator =
                        context.neoStores.getNodeStore().getIdGenerator().notUsedIdsIterator(fromNodeId, toNodeId)) {
            IntObjectHashMap<Value> propertyValues = new IntObjectHashMap<>();
            CacheAccess.Client client = context.cacheAccess.client();
            long[] nextRelCacheFields =
                    new long[] {-1, -1, 1 /*inUse*/, 0, 0, 1 /*note that this needs to be checked*/, 0, 0};
            Iterator<EntityTokenRange> nodeLabelRangeIterator = labelIndexReader.iterator();
            EntityTokenIndexCheckState labelIndexState = new EntityTokenIndexCheckState(null, fromNodeId - 1);
            long nextFreeId = NULL_REFERENCE.longValue();
            for (long nodeId = fromNodeId; nodeId < toNodeId && !context.isCancelled(); nodeId++) {
                localProgress.add(1);
                NodeRecord nodeRecord = nodeReader.read(nodeId);
                while (nextFreeId < nodeId && freeIdsIterator.hasNext()) {
                    nextFreeId = freeIdsIterator.next();
                }
                if (!nodeRecord.inUse()) {
                    if (nodeId != nextFreeId) {
                        reporter.forNode(nodeRecord).idIsNotFreed();
                    }
                    continue;
                }
                if (nodeId == nextFreeId) {
                    reporter.forNode(nodeRecord).idIsFreed();
                }

                // Cache nextRel
                long nextRel = nodeRecord.getNextRel();
                if (nextRel < NULL_REFERENCE.longValue()) {
                    reporter.forNode(nodeRecord).relationshipNotInUse(new RelationshipRecord(nextRel));
                    nextRel = NULL_REFERENCE.longValue();
                }

                nextRelCacheFields[CacheSlots.NodeLink.SLOT_RELATIONSHIP_ID] = nextRel;
                nextRelCacheFields[CacheSlots.NodeLink.SLOT_IS_DENSE] = CacheSlots.longOf(nodeRecord.isDense());
                usedNodes++;

                // Labels
                int[] unverifiedLabels = RecordLoading.safeGetNodeLabels(
                        context, storeCursors, nodeRecord.getId(), nodeRecord.getLabelField(), labelReader);
                int[] labels = checkNodeLabels(nodeRecord, unverifiedLabels, storeCursors);
                // Cache the label field, so that if it contains inlined labels then it's free.
                // Otherwise cache the dynamic labels in another data structure and point into it.
                long labelField = nodeRecord.getLabelField();
                boolean hasInlinedLabels =
                        !NodeLabelsField.fieldPointsToDynamicRecordOfLabels(nodeRecord.getLabelField());
                if (labels == null) {
                    // There was some inconsistency in the label field or dynamic label chain. Let's continue but w/o
                    // labels for this node
                    hasInlinedLabels = true;
                    labelField = NO_LABELS_FIELD.longValue();
                }
                boolean hasSingleLabel = labels != null && labels.length == 1;
                nextRelCacheFields[CacheSlots.NodeLink.SLOT_HAS_INLINED_LABELS] = CacheSlots.longOf(hasInlinedLabels);
                nextRelCacheFields[CacheSlots.NodeLink.SLOT_LABELS] = hasSingleLabel
                        // If this node has only a single label then put it straight in there w/o encoding, along w/
                        // SLOT_HAS_SINGLE_LABEL=1
                        // this makes RelationshipChecker "parse" the cached node labels more efficiently for
                        // single-label nodes
                        ? labels[0]
                        // Otherwise put the encoded label field if inlined, otherwise a ref to the cached dynamic
                        // labels
                        : hasInlinedLabels ? labelField : observedCounts.cacheDynamicNodeLabels(labels);
                nextRelCacheFields[CacheSlots.NodeLink.SLOT_HAS_SINGLE_LABEL] = CacheSlots.longOf(hasSingleLabel);

                // Properties
                propertyValues = lightReplace(propertyValues);
                boolean propertyChainIsOk = property.read(propertyValues, nodeRecord, reporter::forNode, storeCursors);

                // Label index
                if (labelIndexReader.maxCount() != 0) {
                    checkNodeVsLabelIndex(
                            nodeRecord,
                            nodeLabelRangeIterator,
                            labelIndexState,
                            nodeId,
                            labels,
                            fromNodeId,
                            storeCursors);
                }
                client.putToCache(nodeId, nextRelCacheFields);

                // Mandatory properties, type constraints and (some) indexing
                if (labels != null && propertyChainIsOk) {
                    schemaComplianceChecker.checkExistenceAndTypeConstraints(
                            nodeRecord, labels, propertyValues, reporter::forNode);
                    // Here only the very small indexes (or indexes that we can't read the values from, like fulltext
                    // indexes)
                    // gets checked this way, larger indexes will be checked in IndexChecker
                    if (context.consistencyFlags.checkIndexes()) {
                        schemaComplianceChecker.checkCorrectlyIndexed(
                                nodeRecord, labels, propertyValues, reporter::forNode);
                    }
                }
                // Large indexes are checked elsewhere, more efficiently than per-entity
            }
            if (!context.isCancelled() && labelIndexReader.maxCount() != 0) {
                reportRemainingLabelIndexEntries(
                        nodeLabelRangeIterator, labelIndexState, last ? Long.MAX_VALUE : toNodeId, storeCursors);
            }
        }
        observedCounts.incrementNodeLabel(ANY_LABEL, usedNodes);
    }

    private int[] checkNodeLabels(NodeRecord nodeRecord, int[] labels, StoreCursors storeCursors) {
        if (labels == null) {
            // Because there was something wrong with loading them
            return null;
        }

        boolean allGood = true;
        boolean valid = true;
        int prevLabel = -1;
        for (int i = 0; i < labels.length; i++) {
            int label = labels[i];
            if (!checkValidToken(
                    nodeRecord,
                    label,
                    tokenHolders.labelTokens(),
                    neoStores.getLabelTokenStore(),
                    (node, token) -> reporter.forNode(recordLoader.node(node.getId(), storeCursors))
                            .illegalLabel(),
                    (node, token) -> reporter.forNode(recordLoader.node(node.getId(), storeCursors))
                            .labelNotInUse(token),
                    storeCursors)) {
                valid = false;
            }
            if (prevLabel != label) {
                observedCounts.incrementNodeLabel(label, 1);
                prevLabel = label;
            }

            if (i > 0) {
                if (labels[i] == labels[i - 1]) {
                    reporter.forNode(nodeRecord).labelDuplicate(labels[i]);
                    allGood = false;
                    break;
                } else if (labels[i] < labels[i - 1]) {
                    reporter.forNode(nodeRecord).labelsOutOfOrder(max(labels), min(labels));
                    allGood = false;
                    break;
                }
            }
        }

        if (!valid) {
            return null;
        }
        return allGood ? labels : sortAndDeduplicate(labels);
    }

    private void checkNodeVsLabelIndex(
            NodeRecord nodeRecord,
            Iterator<EntityTokenRange> nodeLabelRangeIterator,
            EntityTokenIndexCheckState labelIndexState,
            long nodeId,
            int[] labels,
            long fromNodeId,
            StoreCursors storeCursors) {
        // Detect node-label combinations that exist in the label index, but not in the store
        while (labelIndexState.needToMoveRangeForwardToReachEntity(nodeId) && !context.isCancelled()) {
            if (nodeLabelRangeIterator.hasNext()) {
                if (labelIndexState.currentRange != null) {
                    for (long nodeIdMissingFromStore = labelIndexState.lastCheckedEntityId + 1;
                            nodeIdMissingFromStore < nodeId
                                    && labelIndexState.currentRange.covers(nodeIdMissingFromStore);
                            nodeIdMissingFromStore++) {
                        if (labelIndexState.currentRange.tokens(nodeIdMissingFromStore).length > 0) {
                            reporter.forNodeLabelScan(new TokenScanDocument(labelIndexState.currentRange))
                                    .nodeNotInUse(recordLoader.node(nodeIdMissingFromStore, storeCursors));
                        }
                    }
                }
                labelIndexState.currentRange = nodeLabelRangeIterator.next();
                labelIndexState.lastCheckedEntityId =
                        max(fromNodeId, labelIndexState.currentRange.entities()[0]) - 1;
            } else {
                break;
            }
        }

        if (labelIndexState.currentRange != null && labelIndexState.currentRange.covers(nodeId)) {
            for (long nodeIdMissingFromStore = labelIndexState.lastCheckedEntityId + 1;
                    nodeIdMissingFromStore < nodeId;
                    nodeIdMissingFromStore++) {
                if (labelIndexState.currentRange.tokens(nodeIdMissingFromStore).length > 0) {
                    reporter.forNodeLabelScan(new TokenScanDocument(labelIndexState.currentRange))
                            .nodeNotInUse(recordLoader.node(nodeIdMissingFromStore, storeCursors));
                }
            }
            int[] labelsInLabelIndex = labelIndexState.currentRange.tokens(nodeId);
            if (labels != null) {
                validateLabelIds(
                        nodeRecord,
                        labels,
                        sortAndDeduplicate(labelsInLabelIndex) /* TODO remove when fixed */,
                        labelIndexState.currentRange,
                        storeCursors);
            }
            labelIndexState.lastCheckedEntityId = nodeId;
        } else if (labels != null) {
            for (int label : labels) {
                reporter.forNodeLabelScan(new TokenScanDocument(null))
                        .nodeLabelNotInIndex(recordLoader.node(nodeId, storeCursors), label);
            }
        }
    }

    private void reportRemainingLabelIndexEntries(
            Iterator<EntityTokenRange> nodeLabelRangeIterator,
            EntityTokenIndexCheckState labelIndexState,
            long toNodeId,
            StoreCursors storeCursors) {
        if (labelIndexState.currentRange == null && nodeLabelRangeIterator.hasNext()) {
            // Seems that nobody touched this iterator before, i.e. no nodes in this whole range
            labelIndexState.currentRange = nodeLabelRangeIterator.next();
        }

        while (labelIndexState.currentRange != null && !context.isCancelled()) {
            for (long nodeIdMissingFromStore = labelIndexState.lastCheckedEntityId + 1;
                    nodeIdMissingFromStore < toNodeId
                            && !labelIndexState.needToMoveRangeForwardToReachEntity(nodeIdMissingFromStore);
                    nodeIdMissingFromStore++) {
                if (labelIndexState.currentRange.covers(nodeIdMissingFromStore)
                        && labelIndexState.currentRange.tokens(nodeIdMissingFromStore).length > 0) {
                    reporter.forNodeLabelScan(new TokenScanDocument(labelIndexState.currentRange))
                            .nodeNotInUse(recordLoader.node(nodeIdMissingFromStore, storeCursors));
                }
                labelIndexState.lastCheckedEntityId = nodeIdMissingFromStore;
            }
            labelIndexState.currentRange = nodeLabelRangeIterator.hasNext() ? nodeLabelRangeIterator.next() : null;
        }
    }

    private void validateLabelIds(
            NodeRecord node,
            int[] labelsInStore,
            int[] labelsInIndex,
            EntityTokenRange entityTokenRange,
            StoreCursors storeCursors) {
        compareTwoSortedIntArrays(
                SchemaPatternMatchingType.COMPLETE_ALL_TOKENS,
                labelsInStore,
                labelsInIndex,
                indexLabel -> reporter.forNodeLabelScan(new TokenScanDocument(entityTokenRange))
                        .nodeDoesNotHaveExpectedLabel(recordLoader.node(node.getId(), storeCursors), indexLabel),
                storeLabel -> reporter.forNodeLabelScan(new TokenScanDocument(entityTokenRange))
                        .nodeLabelNotInIndex(recordLoader.node(node.getId(), storeCursors), storeLabel));
    }

    static void compareTwoSortedIntArrays(
            SchemaPatternMatchingType schemaPatternMatchingType,
            int[] a,
            int[] b,
            IntConsumer bHasSomethingThatAIsMissingReport,
            IntConsumer aHasSomethingThatBIsMissingReport) {
        // The node must have all of the labels specified by the index.
        int bCursor = 0;
        int aCursor = 0;
        boolean anyFound = false;
        while (aCursor < a.length && bCursor < b.length && a[aCursor] != -1 && b[bCursor] != -1) {
            int bValue = b[bCursor];
            int aValue = a[aCursor];

            if (bValue < aValue) { // node store has a label which isn't in label scan store
                if (schemaPatternMatchingType == SchemaPatternMatchingType.COMPLETE_ALL_TOKENS) {
                    bHasSomethingThatAIsMissingReport.accept(bValue);
                }
                bCursor++;
            } else if (bValue > aValue) { // label scan store has a label which isn't in node store
                if (schemaPatternMatchingType == SchemaPatternMatchingType.COMPLETE_ALL_TOKENS) {
                    aHasSomethingThatBIsMissingReport.accept(aValue);
                }
                aCursor++;
            } else { // both match
                bCursor++;
                aCursor++;
                anyFound = true;
            }
        }

        if (schemaPatternMatchingType == SchemaPatternMatchingType.COMPLETE_ALL_TOKENS) {
            while (bCursor < b.length && b[bCursor] != -1) {
                bHasSomethingThatAIsMissingReport.accept(b[bCursor++]);
            }
            while (aCursor < a.length && a[aCursor] != -1) {
                aHasSomethingThatBIsMissingReport.accept(a[aCursor++]);
            }
        } else if (schemaPatternMatchingType == SchemaPatternMatchingType.PARTIAL_ANY_TOKEN) {
            if (!anyFound) {
                while (bCursor < b.length) {
                    bHasSomethingThatAIsMissingReport.accept(b[bCursor++]);
                }
            }
        }
    }

    private void checkIndexVsNodes(LongRange range, IndexDescriptor descriptor, boolean lastRange) throws Exception {
        CacheAccess.Client client = context.cacheAccess.client();
        IndexAccessor accessor = context.indexAccessors.accessorFor(descriptor);
        RelationshipCounter.NodeLabelsLookup nodeLabelsLookup = observedCounts.nodeLabelsLookup();
        SchemaDescriptor schema = descriptor.schema();
        SchemaPatternMatchingType schemaPatternMatchingType = schema.schemaPatternMatchingType();
        int[] indexEntityTokenIds = schema.getEntityTokenIds();
        indexEntityTokenIds = sortAndDeduplicate(indexEntityTokenIds);
        try (var cursorContext = context.contextFactory.create(NODE_INDEXES_CHECKER_TAG);
                var storeCursors = new CachedStoreCursors(context.neoStores, cursorContext);
                var allEntriesReader = accessor.newAllEntriesValueReader(
                        range.from(), lastRange ? Long.MAX_VALUE : range.to(), cursorContext)) {
            for (long entityId : allEntriesReader) {
                try {
                    boolean entityExists = client.getBooleanFromCache(entityId, CacheSlots.NodeLink.SLOT_IN_USE);
                    if (!entityExists) {
                        reporter.forIndexEntry(new IndexEntry(descriptor, context.tokenNameLookup, entityId))
                                .nodeNotInUse(recordLoader.node(entityId, storeCursors));
                    } else {
                        int[] entityTokenIds = nodeLabelsLookup.nodeLabels(entityId);
                        compareTwoSortedIntArrays(
                                schemaPatternMatchingType,
                                entityTokenIds,
                                indexEntityTokenIds,
                                indexLabel -> reporter.forIndexEntry(
                                                new IndexEntry(descriptor, context.tokenNameLookup, entityId))
                                        .nodeDoesNotHaveExpectedLabel(
                                                recordLoader.node(entityId, storeCursors), indexLabel),
                                storeLabel -> {
                                    /*here we're only interested in what the index has that the store doesn't have*/
                                });
                    }
                } catch (ArrayIndexOutOfBoundsException e) {
                    // OK so apparently the index has a node way outside node highId
                    reporter.forIndexEntry(new IndexEntry(descriptor, context.tokenNameLookup, entityId))
                            .nodeNotInUse(recordLoader.node(entityId, storeCursors));
                }
            }
        }
    }

    @Override
    public String toString() {
        CommonAbstractStore<NodeRecord, NoStoreHeader> nodeRecordNoStoreHeaderCommonAbstractStore =
                neoStores.getNodeStore();
        return String.format(
                "%s[highId:%d,indexesToCheck:%d]",
                getClass().getSimpleName(),
                nodeRecordNoStoreHeaderCommonAbstractStore.getIdGenerator().getHighId(),
                smallIndexes.size());
    }

    public static int[] sortAndDeduplicate(int[] labels) {
        if (ArrayUtils.isNotEmpty(labels)) {
            sort(labels);
            return PrimitiveArrays.deduplicate(labels);
        }
        return labels;
    }
}
