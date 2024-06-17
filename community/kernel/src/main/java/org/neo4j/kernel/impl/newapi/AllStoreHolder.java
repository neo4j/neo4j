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

import java.util.List;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.Locks;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.TokenReadSession;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.index.TokenIndexReader;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.api.IndexReaderCache;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.OverridableSecurityContext;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.parallel.ExecutionContextProcedureKernelTransaction;
import org.neo4j.kernel.impl.api.parallel.ThreadExecutionContext;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.api.txstate.RelationshipState;
import org.neo4j.values.storable.Value;

/**
 * Implementation of read operation of the Kernel API.
 * <p>
 * The relation between a transaction and All Store Holder can be one to many
 * and the instances of All Store Holder related to the same transaction can be used concurrently by multiple threads.
 * The thread safety aspect of the operation performed by All Store Holder must reflect it!
 * This means that All Store Holder must not use objects shared by multiple All Store Holders that are
 * not safe for multithreaded use!!!
 * This class has two implementations.
 * {@link ForTransactionScope} has one-to-one relation with a transaction and therefore it can safely
 * use {@link KernelTransactionImplementation}, its resources and generally any transaction-scoped resource
 * {@link ForThreadExecutionContextScope} has one-to-many relation with a transaction and therefore it CANNOT safely
 * use {@link KernelTransactionImplementation}, its resources and generally any transaction-scoped
 * resource which is not designed for concurrent use.
 * It should use resources in {@link org.neo4j.kernel.impl.api.parallel.ThreadExecutionContext} scope instead.
 */
public abstract class AllStoreHolder extends Read {
    private final IndexingService indexingService;
    private final MemoryTracker memoryTracker;
    private final IndexReaderCache<ValueIndexReader> valueIndexReaderCache;
    private final IndexReaderCache<TokenIndexReader> tokenIndexReaderCache;

    private final EntityCounter entityCounter;
    private final boolean applyAccessModeToTxState;

    AllStoreHolder(
            StorageReader storageReader,
            TokenRead tokenRead,
            IndexingService indexingService,
            MemoryTracker memoryTracker,
            DefaultPooledCursors cursors,
            StoreCursors storageCursors,
            Locks entityLocks,
            boolean multiVersioned,
            QueryContext queryContext,
            TxStateHolder txStateHolder,
            SchemaRead schemaRead) {
        super(storageReader, tokenRead, cursors, storageCursors, entityLocks, queryContext, txStateHolder, schemaRead);
        this.valueIndexReaderCache = new IndexReaderCache<>(
                index -> indexingService.getIndexProxy(index).newValueReader());
        this.tokenIndexReaderCache = new IndexReaderCache<>(
                index -> indexingService.getIndexProxy(index).newTokenReader());
        this.indexingService = indexingService;
        this.memoryTracker = memoryTracker;
        this.entityCounter = new EntityCounter(multiVersioned);
        this.applyAccessModeToTxState = multiVersioned;
    }

    @Override
    public boolean nodeExists(long reference) {
        performCheckBeforeOperation();

        if (txStateHolder.hasTxStateWithChanges()) {
            var txState = txStateHolder.txState();
            if (txState.nodeIsDeletedInThisBatch(reference)) {
                return false;
            } else if (txState.nodeIsAddedInThisBatch(reference)) {
                return true;
            }
        }

        boolean existsInNodeStore = storageReader.nodeExists(reference, storageCursors);

        if (getAccessMode().allowsTraverseAllLabels()) {
            return existsInNodeStore;
        } else if (!existsInNodeStore) {
            return false;
        } else {
            try (DefaultNodeCursor node = cursors.allocateNodeCursor(queryContext.cursorContext(), memoryTracker)) {
                singleNode(reference, node);
                return node.next();
            }
        }
    }

    @Override
    public boolean nodeDeletedInTransaction(long node) {
        performCheckBeforeOperation();
        return txStateHolder.hasTxStateWithChanges() && txStateHolder.txState().nodeIsDeletedInThisBatch(node);
    }

    @Override
    public boolean relationshipDeletedInTransaction(long relationship) {
        performCheckBeforeOperation();
        return txStateHolder.hasTxStateWithChanges()
                && txStateHolder.txState().relationshipIsDeletedInThisBatch(relationship);
    }

    @Override
    public Value nodePropertyChangeInBatchOrNull(long node, int propertyKeyId) {
        performCheckBeforeOperation();
        if (txStateHolder.hasTxStateWithChanges()) {
            if (applyAccessModeToTxState) {
                try (DefaultNodeCursor nodeCursor =
                        cursors.allocateNodeCursor(queryContext.cursorContext(), memoryTracker)) {
                    singleNode(node, nodeCursor);
                    nodeCursor.next();
                    try (DefaultPropertyCursor propertyCursor =
                            cursors.allocatePropertyCursor(queryContext.cursorContext(), memoryTracker)) {
                        nodeCursor.properties(propertyCursor, PropertySelection.selection(propertyKeyId));
                        return propertyCursor.allowed(propertyKeyId)
                                ? txStateHolder.txState().getNodeState(node).propertyValue(propertyKeyId)
                                : null;
                    }
                }
            } else {
                return txStateHolder.txState().getNodeState(node).propertyValue(propertyKeyId);
            }
        }
        return null;
    }

    @Override
    public Value relationshipPropertyChangeInBatchOrNull(long relationship, int propertyKeyId) {
        performCheckBeforeOperation();
        if (txStateHolder.hasTxStateWithChanges()) {
            RelationshipState relationshipState = txStateHolder.txState().getRelationshipState(relationship);
            return !applyAccessModeToTxState
                            || (relationshipState.hasPropertyChanges()
                                    && getAccessMode()
                                            .allowsReadRelationshipProperty(relationshipState::getType, propertyKeyId))
                    ? relationshipState.propertyValue(propertyKeyId)
                    : null;
        }
        return null;
    }

    @Override
    public long countsForNode(int labelId) {
        return entityCounter.countsForNode(
                labelId,
                getAccessMode(),
                storageReader,
                cursors,
                queryContext.cursorContext(),
                memoryTracker,
                this,
                storageCursors);
    }

    @Override
    public List<Integer> mostCommonLabelGivenRelationshipType(int type) {
        return entityCounter.mostCommonLabelGivenRelationshipType(type, storageReader, queryContext.cursorContext());
    }

    @Override
    public long estimateCountsForNode(int labelId) {
        return storageReader.estimateCountsForNode(labelId, queryContext.cursorContext());
    }

    @Override
    public long countsForRelationship(int startLabelId, int typeId, int endLabelId) {
        return entityCounter.countsForRelationship(
                startLabelId,
                typeId,
                endLabelId,
                getAccessMode(),
                storageReader,
                cursors,
                this,
                queryContext.cursorContext(),
                memoryTracker,
                storageCursors,
                schemaRead);
    }

    @Override
    public long estimateCountsForRelationships(int startLabelId, int typeId, int endLabelId) {
        return storageReader.estimateCountsForRelationship(
                startLabelId, typeId, endLabelId, queryContext.cursorContext());
    }

    @Override
    public boolean relationshipExists(long reference) {
        performCheckBeforeOperation();

        if (txStateHolder.hasTxStateWithChanges()) {
            var txState = txStateHolder.txState();
            if (txState.relationshipIsDeletedInThisBatch(reference)) {
                return false;
            } else if (txState.relationshipIsAddedInThisBatch(reference)) {
                return true;
            }
        }
        boolean existsInRelStore = storageReader.relationshipExists(reference, storageCursors);
        if (getAccessMode().allowsTraverseAllRelTypes()) {
            return existsInRelStore;
        } else if (!existsInRelStore) {
            return false;
        } else {
            try (DefaultRelationshipScanCursor rels = (DefaultRelationshipScanCursor)
                    cursors.allocateRelationshipScanCursor(queryContext.cursorContext(), memoryTracker)) {
                singleRelationship(reference, rels);
                return rels.next();
            }
        }
    }

    @Override
    public ValueIndexReader newValueIndexReader(IndexDescriptor index) throws IndexNotFoundKernelException {
        assertValidIndex(index);
        return indexingService.getIndexProxy(index).newValueReader();
    }

    public TokenIndexReader newTokenIndexReader(IndexDescriptor index) throws IndexNotFoundKernelException {
        assertValidIndex(index);
        return indexingService.getIndexProxy(index).newTokenReader();
    }

    @Override
    public IndexReadSession indexReadSession(IndexDescriptor index) throws IndexNotFoundKernelException {
        assertValidIndex(index);
        return new DefaultIndexReadSession(valueIndexReaderCache.getOrCreate(index), index);
    }

    @Override
    public TokenReadSession tokenReadSession(IndexDescriptor index) throws IndexNotFoundKernelException {
        assertValidIndex(index);
        return new DefaultTokenReadSession(tokenIndexReaderCache.getOrCreate(index), index);
    }

    @Override
    public long nodesGetCount() {
        return countsForNode(TokenRead.ANY_LABEL);
    }

    @Override
    public long relationshipsGetCount() {
        return countsForRelationship(TokenRead.ANY_LABEL, TokenRead.ANY_RELATIONSHIP_TYPE, TokenRead.ANY_LABEL);
    }

    @Override
    public boolean transactionStateHasChanges() {
        return txStateHolder.hasTxStateWithChanges();
    }

    static void assertValidIndex(IndexDescriptor index) throws IndexNotFoundKernelException {
        if (index == IndexDescriptor.NO_INDEX) {
            throw new IndexNotFoundKernelException("No index was found");
        }
    }

    public void release() {
        // Note: This only clears the caches, and does in fact not close the objects
        valueIndexReaderCache.close();
        tokenIndexReaderCache.close();
    }

    public static class ForTransactionScope extends AllStoreHolder {

        private final KernelTransactionImplementation ktx;
        private final AssertOpen assertOpen;

        public ForTransactionScope(
                StorageReader storageReader,
                TokenRead tokenRead,
                KernelTransactionImplementation ktx,
                Locks entityLocks,
                DefaultPooledCursors cursors,
                IndexingService indexingService,
                MemoryTracker memoryTracker,
                boolean multiVersioned,
                QueryContext queryContext,
                AssertOpen assertOpen,
                SchemaRead schemaRead) {
            super(
                    storageReader,
                    tokenRead,
                    indexingService,
                    memoryTracker,
                    cursors,
                    ktx.storeCursors(),
                    entityLocks,
                    multiVersioned,
                    queryContext,
                    ktx,
                    schemaRead);

            this.ktx = ktx;
            this.assertOpen = assertOpen;
        }

        @Override
        void performCheckBeforeOperation() {
            assertOpen.assertOpen();
        }

        @Override
        AccessMode getAccessMode() {
            return ktx.securityContext().mode();
        }
    }

    public static class ForThreadExecutionContextScope extends AllStoreHolder {

        private final OverridableSecurityContext overridableSecurityContext;
        private final ExecutionContextProcedureKernelTransaction kernelTransaction;

        public ForThreadExecutionContextScope(
                ThreadExecutionContext executionContext,
                StorageReader storageReader,
                IndexingService indexingService,
                DefaultPooledCursors cursors,
                StoreCursors storageCursors,
                Locks entityLocks,
                OverridableSecurityContext overridableSecurityContext,
                ExecutionContextProcedureKernelTransaction kernelTransaction,
                boolean multiVersioned,
                QueryContext queryContext,
                TxStateHolder txStateHolder,
                SchemaRead schemaRead) {
            super(
                    storageReader,
                    executionContext.tokenRead(),
                    indexingService,
                    executionContext.memoryTracker(),
                    cursors,
                    storageCursors,
                    entityLocks,
                    multiVersioned,
                    queryContext,
                    txStateHolder,
                    schemaRead);
            this.overridableSecurityContext = overridableSecurityContext;
            this.kernelTransaction = kernelTransaction;
        }

        @Override
        public long lockingNodeUniqueIndexSeek(
                IndexDescriptor index, NodeValueIndexCursor cursor, PropertyIndexQuery.ExactPredicate... predicates) {
            // This is currently a problematic operation for parallel execution, because it takes exclusive locks.
            // In transactions deadlocks is a problem for another day :) .
            throw new UnsupportedOperationException("Locking unique index seek not allowed during parallel execution");
        }

        @Override
        public long lockingRelationshipUniqueIndexSeek(
                IndexDescriptor index,
                RelationshipValueIndexCursor cursor,
                PropertyIndexQuery.ExactPredicate... predicates)
                throws KernelException {
            // This is currently a problematic operation for parallel execution, because it takes exclusive locks.
            // In transactions deadlocks is a problem for another day :) .
            throw new UnsupportedOperationException("Locking unique index seek not allowed during parallel execution");
        }

        @Override
        void performCheckBeforeOperation() {
            kernelTransaction.assertOpen();
        }

        @Override
        AccessMode getAccessMode() {
            return overridableSecurityContext.currentSecurityContext().mode();
        }
    }
}
