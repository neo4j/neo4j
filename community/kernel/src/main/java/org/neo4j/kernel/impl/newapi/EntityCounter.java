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
package org.neo4j.kernel.impl.newapi;

import static org.neo4j.function.Predicates.alwaysTrue;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.storageengine.api.txstate.TxStateVisitor.EMPTY;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import org.neo4j.common.EntityType;
import org.neo4j.counts.CountsVisitor;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipCursor;
import org.neo4j.internal.kernel.api.RelationshipIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.CountsDelta;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.api.txstate.TransactionCountingStateVisitor;
import org.neo4j.token.api.TokenConstants;

final class EntityCounter {

    // Specifies how often we should verify that the transaction is still open
    private static final int CHECK_TX_INTERVAL = 100_000;

    private final boolean multiVersioned;
    private final AssertOpen assertOpen;

    /**
     * Mulitversioned counter should return counts matching transaction visibility rules, which is impossible to get from
     * the current countstore. If {@param multiVersioned} is true all counting will by done via store or token index scan if possible.
     */
    public EntityCounter(boolean multiVersioned, AssertOpen assertOpen) {
        this.multiVersioned = multiVersioned;
        this.assertOpen = assertOpen;
    }

    long countsForNode(
            int labelId,
            AccessMode accessMode,
            StorageReader storageReader,
            CursorFactory cursors,
            CursorContext cursorContext,
            MemoryTracker memoryTracker,
            Read read,
            StoreCursors storageCursors,
            TxStateHolder txStateHolder) {
        if (!multiVersioned && accessMode.allowsTraverseAllNodesWithLabel(labelId)) {
            // All nodes with the specified label can be traversed, so the count store can be used.
            return storageReader.countsForNode(labelId, cursorContext)
                    + countsForNodeInTxState(
                            labelId, storageReader, cursorContext, storageCursors, txStateHolder, memoryTracker);
        } else if (accessMode.disallowsTraverseLabel(labelId)) {
            // No nodes with the specified label can't be traversed, so the count only ones in transaction state
            return countsForNodeInTxState(
                    labelId, storageReader, cursorContext, storageCursors, txStateHolder, memoryTracker);
        } else if (accessMode.hasNoTraverseNodePrivilege()) {
            // Only able to see nodes in transaction state
            return countsForNodeInTxState(
                    labelId, storageReader, cursorContext, storageCursors, txStateHolder, memoryTracker);
        }
        // Scanning adheres security and isolation requirements and includes transaction state too
        return countNodesByScan(labelId, cursors, cursorContext, memoryTracker, read);
    }

    private static long countsForNodeInTxState(
            int labelId,
            StorageReader storageReader,
            CursorContext cursorContext,
            StoreCursors storageCursors,
            TxStateHolder txStateHolder,
            MemoryTracker memoryTracker) {
        long count = 0;
        if (txStateHolder.hasTxStateWithChanges()) {
            CountsDelta counts = new CountsDelta();
            try {
                TransactionState txState = txStateHolder.txState();
                try (var countingVisitor = new TransactionCountingStateVisitor(
                        EMPTY, storageReader, txState, counts, cursorContext, storageCursors, memoryTracker)) {
                    txState.accept(countingVisitor);
                }
                if (counts.hasChanges()) {
                    count += counts.nodeCount(labelId);
                }
            } catch (KernelException e) {
                throw new IllegalArgumentException("Unexpected error: " + e.getMessage());
            }
        }
        return count;
    }

    private long countNodesByScan(
            int labelId, CursorFactory cursors, CursorContext cursorContext, MemoryTracker memoryTracker, Read read) {
        // We have a restriction on what part of the graph can be traversed, that can affect nodes with the
        // specified label.
        // This disables the count store entirely.
        // We need to calculate the counts through expensive operations.
        // We cannot use a NodeLabelScan without an expensive post-filtering, since it is not guaranteed that all
        // nodes with the label can be traversed.
        long count = 0;
        long scanned = 0;
        // DefaultNodeCursor already contains traversal checks within next()
        try (NodeCursor nodes = cursors.allocateNodeCursor(cursorContext, memoryTracker)) {
            read.allNodesScan(nodes);
            while (nodes.next()) {
                if ((++scanned % CHECK_TX_INTERVAL) == 0) {
                    assertOpen.assertOpen();
                }
                if (labelId == TokenConstants.ANY_LABEL || nodes.hasLabel(labelId)) {
                    count++;
                }
            }
        }
        return count;
    }

    public long countsForRelationship(
            int startLabelId,
            int typeId,
            int endLabelId,
            AccessMode accessMode,
            StorageReader storageReader,
            CursorFactory cursors,
            Read read,
            CursorContext cursorContext,
            MemoryTracker memoryTracker,
            StoreCursors storageCursors,
            SchemaRead schemaRead,
            TxStateHolder txStateHolder) {
        if (!multiVersioned
                && allowsTraverseAllRelsWithType(accessMode, typeId)
                && allowsTraverseAllNodesWithLabel(accessMode, startLabelId)
                && allowsTraverseAllNodesWithLabel(accessMode, endLabelId)) {
            // All relationships with the specified type, and all nodes with the specified start and end labels, can be
            // traversed, so the count store can be used. Note that allowsTraverseRelType/allowsTraverseNode are not
            // sufficient here: they only say that *some* entities with the token may be traversable, while the count
            // store also counts the ones that are not.
            return storageReader.countsForRelationship(startLabelId, typeId, endLabelId, cursorContext)
                    + countsForRelationshipInTxState(
                            startLabelId,
                            typeId,
                            endLabelId,
                            txStateHolder,
                            storageReader,
                            storageCursors,
                            cursorContext,
                            memoryTracker);
        }
        if (accessMode.disallowsTraverseRelType(typeId)
                || accessMode.disallowsTraverseLabel(startLabelId)
                || accessMode.disallowsTraverseLabel(endLabelId)) {
            // Not allowed to traverse any relationship with the specified relationship type, start node label and end
            // node label, so count only ones in transaction state.
            return countsForRelationshipInTxState(
                    startLabelId,
                    typeId,
                    endLabelId,
                    txStateHolder,
                    storageReader,
                    storageCursors,
                    cursorContext,
                    memoryTracker);
        }

        return countRelationshipByScan(
                startLabelId, typeId, endLabelId, cursors, read, schemaRead, cursorContext, memoryTracker);
    }

    private static boolean allowsTraverseAllRelsWithType(AccessMode accessMode, int typeId) {
        if (typeId == TokenConstants.ANY_RELATIONSHIP_TYPE) {
            return accessMode.allowsTraverseAllRelTypes() && accessMode.allowsTraverseAllLabels();
        }
        return accessMode.allowsTraverseAllRelsWithType(typeId);
    }

    private static boolean allowsTraverseAllNodesWithLabel(AccessMode accessMode, int labelId) {
        if (labelId == TokenConstants.ANY_LABEL) {
            return accessMode.allowsTraverseAllLabels();
        }
        return accessMode.allowsTraverseAllNodesWithLabel(labelId);
    }

    private long countRelationshipByScan(
            int startLabelId,
            int typeId,
            int endLabelId,
            CursorFactory cursors,
            Read read,
            SchemaRead schemaRead,
            CursorContext cursorContext,
            MemoryTracker memoryTracker) {
        // token index scan can only scan for single relationship type
        if (typeId != TokenConstants.ANY_RELATIONSHIP_TYPE) {
            try {
                var index = findUsableRelationshipTypeTokenIndex(schemaRead);
                if (index != IndexDescriptor.NO_INDEX) {
                    long count = 0;
                    try (var relationshipsWithType =
                                    cursors.allocateRelationshipTypeIndexCursor(cursorContext, memoryTracker);
                            NodeCursor sourceNode = cursors.allocateNodeCursor(cursorContext, memoryTracker);
                            NodeCursor targetNode = cursors.allocateNodeCursor(cursorContext, memoryTracker)) {
                        var session = read.tokenReadSession(index);
                        read.relationshipTypeScan(
                                session,
                                relationshipsWithType,
                                unconstrained(),
                                new TokenPredicate(typeId),
                                cursorContext);
                        count += countRelationshipsWithEndLabels(
                                relationshipsWithType, sourceNode, targetNode, startLabelId, endLabelId);
                    }
                    return count;
                }
            } catch (KernelException ignored) {
                // ignore, fallback to allRelationshipsScan
            }
        }

        long count;
        try (var rels = cursors.allocateRelationshipScanCursor(cursorContext, memoryTracker);
                NodeCursor sourceNode = cursors.allocateFullAccessNodeCursor(cursorContext, memoryTracker);
                NodeCursor targetNode = cursors.allocateFullAccessNodeCursor(cursorContext, memoryTracker)) {
            read.allRelationshipsScan(rels);
            Predicate<RelationshipScanCursor> predicate =
                    typeId == TokenConstants.ANY_RELATIONSHIP_TYPE ? alwaysTrue() : CursorPredicates.hasType(typeId);
            var filteredCursor = new FilteringRelationshipScanCursorWrapper(rels, predicate);
            count = countRelationshipsWithEndLabels(filteredCursor, sourceNode, targetNode, startLabelId, endLabelId);
        }
        return count;
    }

    private IndexDescriptor findUsableRelationshipTypeTokenIndex(SchemaRead schemaRead)
            throws IndexNotFoundKernelException {
        var descriptor = SchemaDescriptors.forAnyEntityTokens(EntityType.RELATIONSHIP);
        var index = schemaRead.index(descriptor, IndexType.LOOKUP);
        if (index != IndexDescriptor.NO_INDEX && schemaRead.indexGetState(index) == InternalIndexState.ONLINE) {
            return index;
        }
        return IndexDescriptor.NO_INDEX;
    }

    private long countRelationshipsWithEndLabels(
            RelationshipIndexCursor relationship,
            NodeCursor sourceNode,
            NodeCursor targetNode,
            int startLabelId,
            int endLabelId) {
        long internalCount = 0;
        long scanned = 0;
        while (relationship.next()) {
            if ((++scanned % CHECK_TX_INTERVAL) == 0) {
                assertOpen.assertOpen();
            }
            if (relationship.readFromStore()
                    && matchesLabels(relationship, sourceNode, targetNode, startLabelId, endLabelId)) {
                internalCount++;
            }
        }
        return internalCount;
    }

    private long countRelationshipsWithEndLabels(
            RelationshipScanCursor relationship,
            NodeCursor sourceNode,
            NodeCursor targetNode,
            int startLabelId,
            int endLabelId) {
        long internalCount = 0;
        long scanned = 0;
        while (relationship.next()) {
            if ((++scanned % CHECK_TX_INTERVAL) == 0) {
                assertOpen.assertOpen();
            }
            if (matchesLabels(relationship, sourceNode, targetNode, startLabelId, endLabelId)) {
                internalCount++;
            }
        }
        return internalCount;
    }

    private static boolean matchesLabels(
            RelationshipCursor relationship,
            NodeCursor sourceNode,
            NodeCursor targetNode,
            int startLabelId,
            int endLabelId) {
        relationship.source(sourceNode);
        relationship.target(targetNode);
        return sourceNode.next()
                && (startLabelId == TokenConstants.ANY_LABEL || sourceNode.hasLabel(startLabelId))
                && targetNode.next()
                && (endLabelId == TokenConstants.ANY_LABEL || targetNode.hasLabel(endLabelId));
    }

    private long countsForRelationshipInTxState(
            int startLabelId,
            int typeId,
            int endLabelId,
            TxStateHolder txStateHolder,
            StorageReader storageReader,
            StoreCursors storageCursors,
            CursorContext cursorContext,
            MemoryTracker memoryTracker) {
        long count = 0;
        if (txStateHolder.hasTxStateWithChanges()) {
            CountsDelta counts = new CountsDelta();
            try {
                TransactionState txState = txStateHolder.txState();
                try (var countingVisitor = new TransactionCountingStateVisitor(
                        EMPTY, storageReader, txState, counts, cursorContext, storageCursors, memoryTracker)) {
                    txState.accept(countingVisitor);
                }
                if (counts.hasChanges()) {
                    count += counts.relationshipCount(startLabelId, typeId, endLabelId);
                }
            } catch (KernelException e) {
                throw new IllegalArgumentException("Unexpected error: " + e.getMessage());
            }
        }
        return count;
    }

    private static class MostCommonLabelGivenRelTypeVisitor implements CountsVisitor {
        private final int relationshipType;
        private long labelCount = -1;
        public ArrayList<Integer> highest = new ArrayList<>();

        public MostCommonLabelGivenRelTypeVisitor(int relationshipType) {
            this.relationshipType = relationshipType;
        }

        @Override
        public void visitNodeCount(int labelId, long count) {}

        @Override
        public void visitRelationshipCount(int startLabelId, int typeId, int endLabelId, long count) {
            if (typeId == relationshipType
                    && (startLabelId > TokenConstants.ANY_LABEL ^ endLabelId > TokenConstants.ANY_LABEL)) {
                int labelId = startLabelId > TokenConstants.ANY_LABEL ? startLabelId : endLabelId;

                if (count > labelCount) {
                    labelCount = count;
                    highest = new ArrayList<>(List.of(labelId));
                } else if (count == labelCount) {
                    highest.add(labelId);
                }
            }
        }
    }

    public List<Integer> mostCommonLabelGivenRelationshipType(
            int type, StorageReader storageReader, CursorContext cursorContext) {
        var visitor = new MostCommonLabelGivenRelTypeVisitor(type);
        storageReader.visitAllCounts(visitor, cursorContext);
        return visitor.highest;
    }
}
