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

import static java.lang.String.format;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.kernel.impl.locking.ResourceIds.indexEntryResourceId;
import static org.neo4j.util.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.List;
import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.Locks;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PartitionedScan;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.TokenReadSession;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.AccessModeProvider;
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.index.TokenIndexReader;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.api.IndexReaderCache;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.api.txstate.validation.TransactionConflictException;
import org.neo4j.token.api.TokenConstants;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.Value;

/**
 * Implementation of read operation of the Kernel API.
 * <p>
 * The relation between a transaction and KernelRead can be one to many
 * and the instances of KernelRead related to the same transaction can be used concurrently by multiple threads.
 * The thread safety aspect of the operation performed by KernelRead must reflect it!
 * This means that KernelRead must not use objects shared by multiple KernelReads that are
 * not safe for multithreaded use!!!
 * Recorces for KernelRead should be provided either by:
 * - {@link KernelTransactionImplementation} itself for one-to-one relation with transaction
 * - Or by {@link org.neo4j.kernel.impl.api.parallel.ThreadExecutionContext} for one-to-many relation with a
 * transaction.
 * Transaction scoped and thread context scoped resources CANNOT be mixed.
 */
public class KernelRead implements Read {
    private final StorageReader storageReader;
    protected final CursorFactory cursors;
    private final IndexingService indexingService;
    protected final MemoryTracker memoryTracker;
    private final IndexReaderCache<ValueIndexReader> valueIndexReaderCache;
    private final IndexReaderCache<TokenIndexReader> tokenIndexReaderCache;
    private final EntityCounter entityCounter;
    private final boolean applyAccessModeToTxState;
    private final boolean multiVersioned;
    protected final TokenRead tokenRead;
    private final StoreCursors storageCursors;
    protected final QueryContext queryContext;
    private final Locks entityLocks;
    private final TxStateHolder txStateHolder;
    private final SchemaRead schemaRead;
    private final AssertOpen assertOpen;
    protected final AccessModeProvider accessModeProvider;
    private final boolean parallel;
    protected final Log log;

    public KernelRead(
            StorageReader storageReader,
            TokenRead tokenRead,
            CursorFactory cursors,
            StoreCursors storageCursors,
            Locks entityLocks,
            QueryContext queryContext,
            TxStateHolder txStateHolder,
            SchemaRead schemaRead,
            IndexingService indexingService,
            MemoryTracker memoryTracker,
            boolean multiVersioned,
            AssertOpen assertOpen,
            AccessModeProvider accessModeProvider,
            boolean parallel,
            LogProvider logProvider) {
        this.storageReader = storageReader;
        this.tokenRead = tokenRead;
        this.cursors = cursors;
        this.storageCursors = storageCursors;
        this.entityLocks = entityLocks;
        this.queryContext = queryContext;
        this.txStateHolder = txStateHolder;
        this.schemaRead = schemaRead;
        this.indexingService = indexingService;
        this.memoryTracker = memoryTracker;
        this.valueIndexReaderCache = new IndexReaderCache<>(
                index -> indexingService.getIndexProxy(index).newValueReader());
        this.tokenIndexReaderCache = new IndexReaderCache<>(
                index -> indexingService.getIndexProxy(index).newTokenReader());
        this.entityCounter = new EntityCounter(multiVersioned);
        this.applyAccessModeToTxState = multiVersioned;
        this.multiVersioned = multiVersioned;
        this.assertOpen = assertOpen;
        this.accessModeProvider = accessModeProvider;
        this.parallel = parallel;
        this.log = logProvider.getLog(getClass());
    }

    @Override
    public void nodeIndexSeek(
            QueryContext queryContext,
            IndexReadSession index,
            NodeValueIndexCursor cursor,
            IndexQueryConstraints constraints,
            boolean includeChangesFromThisTransaction,
            PropertyIndexQuery... query)
            throws IndexNotApplicableKernelException {
        performCheckBeforeOperation();
        DefaultIndexReadSession indexSession = (DefaultIndexReadSession) index;
        validateConstraints(constraints, indexSession);

        if (indexSession.reference().schema().entityType() != EntityType.NODE) {
            throw IndexNotApplicableKernelException.indexNotApplicable(
                    log,
                    index.reference().getName(),
                    "Node index seek can not be performed on index: "
                            + index.reference().userDescription(tokenRead));
        }

        EntityIndexSeekClient client = (EntityIndexSeekClient) cursor;
        client.initState(this, txStateHolder, accessModeProvider, includeChangesFromThisTransaction);
        CursorContext cursorContext =
                pickCursorContext(queryContext.cursorContext(), includeChangesFromThisTransaction);
        indexSession.reader().query(client, queryContext, cursorContext, constraints, query);
    }

    @Override
    public PartitionedScan<NodeValueIndexCursor> nodeIndexSeek(
            IndexReadSession index,
            int desiredNumberOfPartitions,
            QueryContext queryContext,
            PropertyIndexQuery... query)
            throws IndexNotApplicableKernelException {
        performCheckBeforeOperation();
        final var descriptor = index.reference();
        if (descriptor.schema().entityType() != EntityType.NODE) {
            throw IndexNotApplicableKernelException.indexNotApplicable(
                    log,
                    index.reference().getName(),
                    "Node index seek can not be performed on index: " + descriptor.userDescription(tokenRead));
        }
        return propertyIndexSeek(index, desiredNumberOfPartitions, queryContext, query);
    }

    @Override
    public void relationshipIndexSeek(
            QueryContext queryContext,
            IndexReadSession index,
            RelationshipValueIndexCursor cursor,
            IndexQueryConstraints constraints,
            boolean includeChangesFromThisTransaction,
            PropertyIndexQuery... query)
            throws IndexNotApplicableKernelException {
        performCheckBeforeOperation();
        DefaultIndexReadSession indexSession = (DefaultIndexReadSession) index;
        validateConstraints(constraints, indexSession);
        if (indexSession.reference().schema().entityType() != EntityType.RELATIONSHIP) {
            throw IndexNotApplicableKernelException.indexNotApplicable(
                    log,
                    index.reference().getName(),
                    "Relationship index seek can not be performed on index: "
                            + index.reference().userDescription(tokenRead));
        }

        EntityIndexSeekClient client = (EntityIndexSeekClient) cursor;
        client.initState(this, txStateHolder, accessModeProvider, includeChangesFromThisTransaction);
        CursorContext cursorContext =
                pickCursorContext(queryContext.cursorContext(), includeChangesFromThisTransaction);
        indexSession.reader().query(client, queryContext, cursorContext, constraints, query);
    }

    @Override
    public PartitionedScan<RelationshipValueIndexCursor> relationshipIndexSeek(
            IndexReadSession index,
            int desiredNumberOfPartitions,
            QueryContext queryContext,
            PropertyIndexQuery... query)
            throws IndexNotApplicableKernelException {
        performCheckBeforeOperation();
        final var descriptor = index.reference();
        if (descriptor.schema().entityType() != EntityType.RELATIONSHIP) {
            throw IndexNotApplicableKernelException.indexNotApplicable(
                    log,
                    index.reference().getName(),
                    "Relationship index seek can not be performed on index: " + descriptor.userDescription(tokenRead));
        }
        return propertyIndexSeek(index, desiredNumberOfPartitions, queryContext, query);
    }

    private CursorContext pickCursorContext(CursorContext cursorContext, boolean includeChangesFromThisTransaction) {
        return includeChangesFromThisTransaction ? cursorContext : cursorContext.noCurrentTransactionContext();
    }

    private void verifyNotParallel() {
        if (parallel) {
            // This is currently a problematic operation for parallel execution, because it takes exclusive locks.
            // In transactions deadlocks is a problem for another day :) .
            throw new UnsupportedOperationException("Locking unique index seek not allowed during parallel execution");
        }
    }

    @Override
    public long lockingNodeUniqueIndexSeek(
            IndexReadSession index, NodeValueIndexCursor cursor, PropertyIndexQuery.ExactPredicate... predicates)
            throws IndexNotApplicableKernelException, IndexNotFoundKernelException, IndexBrokenKernelException {
        return lockingUniqueIndexSeek(index, (DefaultEntityValueIndexCursor<?>) cursor, predicates);
    }

    @Override
    public long lockingRelationshipUniqueIndexSeek(
            IndexReadSession index,
            RelationshipValueIndexCursor cursor,
            PropertyIndexQuery.ExactPredicate... predicates)
            throws KernelException {
        return lockingUniqueIndexSeek(index, (DefaultEntityValueIndexCursor<?>) cursor, predicates);
    }

    private long lockingUniqueIndexSeek(
            IndexReadSession index,
            DefaultEntityValueIndexCursor<?> cursor,
            PropertyIndexQuery.ExactPredicate... predicates)
            throws IndexNotApplicableKernelException, IndexNotFoundKernelException, IndexBrokenKernelException {
        verifyNotParallel();
        DefaultIndexReadSession indexSession = (DefaultIndexReadSession) index;
        IndexDescriptor indexDescriptor = indexSession.reference();
        ValueIndexReader reader = indexSession.reader();
        CursorContext cursorContext = queryContext.cursorContext();
        assertIndexOnline(indexDescriptor);
        assertPredicatesMatchSchema(indexDescriptor, predicates);

        int[] entityTokenIds = indexDescriptor.schema().getEntityTokenIds();
        if (entityTokenIds.length != 1) {
            throw IndexNotApplicableKernelException.indexNotApplicable(
                    log, indexDescriptor.getName(), "Multi-token index " + index + " does not support uniqueness.");
        }
        long indexEntryId = indexEntryResourceId(entityTokenIds[0], predicates);
        // First try to find entity under a shared lock (no actual lock under mvcc and it's not needed)
        // If not found, upgrade to exclusive lock and retry with unbounded visibility to see all committed data
        entityLocks.acquireSharedIndexEntryLock(indexEntryId);
        exactEntityIndexSeek(cursor, cursorContext, reader, predicates);
        if (cursor.next()) {
            return cursor.reference();
        }
        entityLocks.releaseSharedIndexEntryLock(indexEntryId);
        entityLocks.acquireExclusiveIndexEntryLock(indexEntryId);
        CursorContext unboundedContext = cursorContext.createUnboundedRelatedContext();
        exactEntityIndexSeek(cursor, unboundedContext, reader, predicates);
        if (cursor.next()) {
            if (multiVersioned) {
                // in mvcc case throw transient exception so query is retried with updated visibility
                throw TransactionConflictException.uniqueIndexEntryConflict(
                        indexSession.reference().getName(), cursorContext.getVersionContext());
            }
            entityLocks.acquireSharedIndexEntryLock(indexEntryId);
            entityLocks.releaseExclusiveIndexEntryLock(indexEntryId);
            return cursor.reference();
        }
        return StatementConstants.NO_SUCH_ENTITY;
    }

    void indexSeekForExactProperty(
            EntityIndexSeekClient entityIndexSeekClient,
            CursorContext cursorContext,
            IndexDescriptor index,
            PropertyIndexQuery.ExactPredicate... query)
            throws IndexNotFoundKernelException, IndexNotApplicableKernelException {
        checkArgument(index.isUnique(), "This method should only be used to verify uniqueness constraints");
        try (var reader = newValueIndexReader(index)) {
            exactEntityIndexSeek(entityIndexSeekClient, cursorContext, reader, query);
        }
    }

    private void exactEntityIndexSeek(
            EntityIndexSeekClient cursor,
            CursorContext cursorContext,
            ValueIndexReader indexReader,
            PropertyIndexQuery.ExactPredicate... query)
            throws IndexNotApplicableKernelException {
        cursor.initState(this, txStateHolder, accessModeProvider, true);
        indexReader.query(cursor, queryContext, cursorContext, unconstrained(), query);
    }

    @Override
    public void nodeIndexScan(
            IndexReadSession index,
            NodeValueIndexCursor cursor,
            IndexQueryConstraints constraints,
            boolean includeChangesFromThisTransaction)
            throws KernelException {
        performCheckBeforeOperation();
        DefaultIndexReadSession indexSession = (DefaultIndexReadSession) index;

        if (indexSession.reference().schema().entityType() != EntityType.NODE) {
            throw IndexNotApplicableKernelException.indexNotApplicable(
                    log,
                    index.reference().getName(),
                    "Node index scan can not be performed on index: "
                            + index.reference().userDescription(tokenRead));
        }

        scanIndex(indexSession, (EntityIndexSeekClient) cursor, constraints, includeChangesFromThisTransaction);
    }

    @Override
    public PartitionedScan<NodeValueIndexCursor> nodeIndexScan(
            IndexReadSession index, int desiredNumberOfPartitions, QueryContext queryContext)
            throws IndexNotApplicableKernelException {
        performCheckBeforeOperation();
        final var descriptor = index.reference();
        if (descriptor.schema().entityType() != EntityType.NODE) {
            throw IndexNotApplicableKernelException.indexNotApplicable(
                    log,
                    index.reference().getName(),
                    "Node index scan can not be performed on index: " + descriptor.userDescription(tokenRead));
        }

        return propertyIndexScan(index, desiredNumberOfPartitions, queryContext);
    }

    @Override
    public void relationshipIndexScan(
            IndexReadSession index,
            RelationshipValueIndexCursor cursor,
            IndexQueryConstraints constraints,
            boolean includeChangesFromThisTransaction)
            throws KernelException {
        performCheckBeforeOperation();
        DefaultIndexReadSession indexSession = (DefaultIndexReadSession) index;

        if (indexSession.reference().schema().entityType() != EntityType.RELATIONSHIP) {
            throw IndexNotApplicableKernelException.indexNotApplicable(
                    log,
                    index.reference().getName(),
                    "Relationship index scan can not be performed on index: "
                            + index.reference().userDescription(tokenRead));
        }

        scanIndex(indexSession, (EntityIndexSeekClient) cursor, constraints, includeChangesFromThisTransaction);
    }

    @Override
    public PartitionedScan<RelationshipValueIndexCursor> relationshipIndexScan(
            IndexReadSession index, int desiredNumberOfPartitions, QueryContext queryContext)
            throws IndexNotApplicableKernelException {
        performCheckBeforeOperation();
        final var descriptor = index.reference();
        if (descriptor.schema().entityType() != EntityType.RELATIONSHIP) {
            throw IndexNotApplicableKernelException.indexNotApplicable(
                    log,
                    index.reference().getName(),
                    "Relationship index scan can not be performed on index: " + descriptor.userDescription(tokenRead));
        }

        return propertyIndexScan(index, desiredNumberOfPartitions, queryContext);
    }

    private void scanIndex(
            DefaultIndexReadSession indexSession,
            EntityIndexSeekClient indexSeekClient,
            IndexQueryConstraints constraints,
            boolean includeChangesFromThisTransaction)
            throws KernelException {
        indexSeekClient.initState(this, txStateHolder, accessModeProvider, includeChangesFromThisTransaction);
        CursorContext cursorContext =
                pickCursorContext(queryContext.cursorContext(), includeChangesFromThisTransaction);
        indexSession
                .reader()
                .query(indexSeekClient, queryContext, cursorContext, constraints, PropertyIndexQuery.allEntries());
    }

    @Override
    public PartitionedScan<NodeLabelIndexCursor> nodeLabelScan(
            TokenReadSession session, int desiredNumberOfPartitions, CursorContext cursorContext, TokenPredicate query)
            throws IndexNotApplicableKernelException {
        performCheckBeforeOperation();
        if (session.reference().schema().entityType() != EntityType.NODE) {
            throw IndexNotApplicableKernelException.indexNotApplicable(
                    log,
                    session.reference().userDescription(tokenRead),
                    "Node label index scan can not be performed on index: "
                            + session.reference().userDescription(tokenRead));
        }
        return tokenIndexScan(session, desiredNumberOfPartitions, cursorContext, query);
    }

    @Override
    public PartitionedScan<NodeLabelIndexCursor> nodeLabelScan(
            TokenReadSession session, PartitionedScan<NodeLabelIndexCursor> leadingPartitionScan, TokenPredicate query)
            throws IndexNotApplicableKernelException {
        performCheckBeforeOperation();
        if (session.reference().schema().entityType() != EntityType.NODE) {
            throw IndexNotApplicableKernelException.indexNotApplicable(
                    log,
                    session.reference().userDescription(tokenRead),
                    "Node label index scan can not be performed on index: "
                            + session.reference().userDescription(tokenRead));
        }
        return tokenIndexScan(session, leadingPartitionScan, query);
    }

    @Override
    public List<PartitionedScan<NodeLabelIndexCursor>> nodeLabelScans(
            TokenReadSession session,
            int desiredNumberOfPartitions,
            CursorContext cursorContext,
            TokenPredicate... queries)
            throws IndexNotApplicableKernelException {
        performCheckBeforeOperation();
        if (session.reference().schema().entityType() != EntityType.NODE) {
            throw IndexNotApplicableKernelException.indexNotApplicable(
                    log,
                    session.reference().userDescription(tokenRead),
                    "Node label index scan can not be performed on index: "
                            + session.reference().userDescription(tokenRead));
        }
        return tokenIndexScan(session, desiredNumberOfPartitions, cursorContext, queries);
    }

    @Override
    public void nodeLabelIndexScan(
            TokenReadSession session,
            NodeLabelIndexCursor cursor,
            IndexQueryConstraints constraints,
            TokenPredicate query,
            CursorContext cursorContext,
            boolean includeChangesFromThisTransaction)
            throws KernelException {
        performCheckBeforeOperation();

        if (session.reference().schema().entityType() != EntityType.NODE) {
            throw IndexNotApplicableKernelException.indexNotApplicable(
                    log,
                    session.reference().userDescription(tokenRead),
                    "Node label index scan can not be performed on index: "
                            + session.reference().userDescription(tokenRead));
        }

        var tokenSession = (DefaultTokenReadSession) session;

        DefaultNodeLabelIndexCursor indexCursor = (DefaultNodeLabelIndexCursor) cursor;
        indexCursor.initState(this, txStateHolder, accessModeProvider, includeChangesFromThisTransaction);
        CursorContext cursorContextForScan = pickCursorContext(cursorContext, includeChangesFromThisTransaction);
        tokenSession.reader().query(indexCursor, constraints, query, cursorContextForScan);
    }

    @Override
    public void allNodesScan(NodeCursor cursor, boolean includeChangesFromThisTransaction) {
        performCheckBeforeOperation();
        ((DefaultNodeCursor) cursor).scan(this, txStateHolder, accessModeProvider, includeChangesFromThisTransaction);
    }

    @Override
    public void singleNode(long reference, NodeCursor cursor) {
        performCheckBeforeOperation();
        ((DefaultNodeCursor) cursor).single(reference, this, txStateHolder, accessModeProvider);
    }

    @Override
    public PartitionedScan<NodeCursor> allNodesScan(int desiredNumberOfPartitions, CursorContext cursorContext) {
        performCheckBeforeOperation();
        long totalCount = storageReader.nodesGetCount(cursorContext);
        return new PartitionedNodeCursorScan(storageReader.allNodeScan(), desiredNumberOfPartitions, totalCount);
    }

    @Override
    public PartitionedScan<RelationshipScanCursor> allRelationshipsScan(
            int desiredNumberOfPartitions, CursorContext cursorContext) {
        performCheckBeforeOperation();
        long totalCount = storageReader.relationshipsGetCount(cursorContext);
        return new PartitionedRelationshipCursorScan(
                storageReader.allRelationshipScan(), desiredNumberOfPartitions, totalCount);
    }

    @Override
    public void singleRelationship(long reference, RelationshipScanCursor cursor) {
        performCheckBeforeOperation();
        ((DefaultRelationshipScanCursor) cursor).single(reference, this, txStateHolder, accessModeProvider);
    }

    @Override
    public void singleRelationship(
            long reference,
            long sourceNodeReference,
            int type,
            long targetNodeReference,
            RelationshipScanCursor cursor) {
        performCheckBeforeOperation();
        ((DefaultRelationshipScanCursor) cursor)
                .single(
                        reference,
                        sourceNodeReference,
                        type,
                        targetNodeReference,
                        this,
                        txStateHolder,
                        accessModeProvider);
    }

    @Override
    public void allRelationshipsScan(RelationshipScanCursor cursor, boolean includeChangesFromThisTransaction) {
        performCheckBeforeOperation();
        ((DefaultRelationshipScanCursor) cursor)
                .scan(this, txStateHolder, accessModeProvider, includeChangesFromThisTransaction);
    }

    @Override
    public PartitionedScan<RelationshipTypeIndexCursor> relationshipTypeScan(
            TokenReadSession session, int desiredNumberOfPartitions, CursorContext cursorContext, TokenPredicate query)
            throws IndexNotApplicableKernelException {
        performCheckBeforeOperation();
        if (session.reference().schema().entityType() != EntityType.RELATIONSHIP) {
            throw IndexNotApplicableKernelException.indexNotApplicable(
                    log,
                    session.reference().userDescription(tokenRead),
                    "Relationship type index scan can not be performed on index: "
                            + session.reference().userDescription(tokenRead));
        }
        return tokenIndexScan(session, desiredNumberOfPartitions, cursorContext, query);
    }

    @Override
    public PartitionedScan<RelationshipTypeIndexCursor> relationshipTypeScan(
            TokenReadSession session,
            PartitionedScan<RelationshipTypeIndexCursor> leadingPartitionScan,
            TokenPredicate query)
            throws IndexNotApplicableKernelException {
        performCheckBeforeOperation();
        if (session.reference().schema().entityType() != EntityType.RELATIONSHIP) {
            throw IndexNotApplicableKernelException.indexNotApplicable(
                    log,
                    session.reference().userDescription(tokenRead),
                    "Relationship type index scan can not be performed on index: "
                            + session.reference().userDescription(tokenRead));
        }
        return tokenIndexScan(session, leadingPartitionScan, query);
    }

    @Override
    public List<PartitionedScan<RelationshipTypeIndexCursor>> relationshipTypeScans(
            TokenReadSession session,
            int desiredNumberOfPartitions,
            CursorContext cursorContext,
            TokenPredicate... queries)
            throws IndexNotApplicableKernelException {
        performCheckBeforeOperation();
        if (session.reference().schema().entityType() != EntityType.RELATIONSHIP) {
            throw IndexNotApplicableKernelException.indexNotApplicable(
                    log,
                    session.reference().userDescription(tokenRead),
                    "Relationship type index scan can not be performed on index: "
                            + session.reference().userDescription(tokenRead));
        }
        return tokenIndexScan(session, desiredNumberOfPartitions, cursorContext, queries);
    }

    @Override
    public void relationshipTypeIndexScan(
            TokenReadSession session,
            RelationshipTypeIndexCursor cursor,
            IndexQueryConstraints constraints,
            TokenPredicate query,
            CursorContext cursorContext,
            boolean includeChangesFromThisTransaction)
            throws KernelException {
        performCheckBeforeOperation();

        if (session.reference().schema().entityType() != EntityType.RELATIONSHIP) {
            throw IndexNotApplicableKernelException.indexNotApplicable(
                    log,
                    session.reference().userDescription(tokenRead),
                    "Relationship type index scan can not be performed on index: "
                            + session.reference().userDescription(tokenRead));
        }

        var tokenSession = (DefaultTokenReadSession) session;

        var indexCursor = (InternalTokenIndexCursor) cursor;
        indexCursor.initState(this, txStateHolder, accessModeProvider, includeChangesFromThisTransaction);
        CursorContext cursorContextForScan = pickCursorContext(cursorContext, includeChangesFromThisTransaction);
        tokenSession.reader().query(indexCursor, constraints, query, cursorContextForScan);
    }

    @Override
    public void relationships(
            long nodeReference, long reference, RelationshipSelection selection, RelationshipTraversalCursor cursor) {
        ((DefaultRelationshipTraversalCursor) cursor)
                .init(nodeReference, reference, selection, this, txStateHolder, accessModeProvider);
    }

    @Override
    public void nodeProperties(
            long nodeReference, Reference reference, PropertySelection selection, PropertyCursor cursor) {
        ((DefaultPropertyCursor) cursor)
                .initNode(nodeReference, reference, selection, this, txStateHolder, accessModeProvider);
    }

    @Override
    public void relationshipProperties(
            long relationshipReference,
            int type,
            Reference reference,
            PropertySelection selection,
            PropertyCursor cursor) {
        ((DefaultPropertyCursor) cursor)
                .initRelationship(
                        relationshipReference, type, reference, selection, this, txStateHolder, accessModeProvider);
    }

    private void validateConstraints(IndexQueryConstraints constraints, DefaultIndexReadSession indexSession) {
        if (constraints.needsValues()
                && !indexSession.reference().getCapability().supportsReturningValues()) {
            throw new UnsupportedOperationException(format(
                    "%s index has no value capability", indexSession.reference().getIndexType()));
        }
    }

    private <C extends Cursor> PartitionedScan<C> propertyIndexScan(
            IndexReadSession index, int desiredNumberOfPartitions, QueryContext queryContext)
            throws IndexNotApplicableKernelException {
        performCheckBeforeOperation();
        return propertyIndexSeek(index, desiredNumberOfPartitions, queryContext, PropertyIndexQuery.allEntries());
    }

    private <C extends Cursor> PartitionedScan<C> propertyIndexSeek(
            IndexReadSession index,
            int desiredNumberOfPartitions,
            QueryContext queryContext,
            PropertyIndexQuery... query)
            throws IndexNotApplicableKernelException {
        performCheckBeforeOperation();
        final var descriptor = index.reference();
        if (!descriptor.getCapability().supportPartitionedScan(query)) {
            throw IndexNotApplicableKernelException.indexNotApplicable(
                    log,
                    index.reference().getName(),
                    "This index does not support partitioned scan for this query: "
                            + descriptor.userDescription(tokenRead));
        }
        if (txStateHolder.hasTxStateWithChanges()) {
            throw new IllegalStateException(
                    "Transaction contains changes; PartitionScan is only valid in Read-Only transactions.");
        }

        final var session = (DefaultIndexReadSession) index;
        final var valueSeek = session.reader().valueSeek(desiredNumberOfPartitions, queryContext, query);
        return new PartitionedValueIndexCursorSeek<>(descriptor, valueSeek, query);
    }

    private <C extends Cursor> PartitionedScan<C> tokenIndexScan(
            TokenReadSession session, int desiredNumberOfPartitions, CursorContext cursorContext, TokenPredicate query)
            throws IndexNotApplicableKernelException {
        performCheckBeforeOperation();
        final var descriptor = session.reference();
        if (!descriptor.getCapability().supportPartitionedScan(query)) {
            throw IndexNotApplicableKernelException.indexNotApplicable(
                    log,
                    descriptor.userDescription(tokenRead),
                    "This index does not support partitioned scan for this query: "
                            + descriptor.userDescription(tokenRead));
        }
        if (txStateHolder.hasTxStateWithChanges()) {
            throw new IllegalStateException(
                    "Transaction contains changes; PartitionScan is only valid in Read-Only transactions.");
        }

        final var defaultSession = (DefaultTokenReadSession) session;
        final var tokenScan = defaultSession.reader().entityTokenScan(desiredNumberOfPartitions, cursorContext, query);
        return new PartitionedTokenIndexCursorScan<>(query, tokenScan);
    }

    private <C extends Cursor> PartitionedScan<C> tokenIndexScan(
            TokenReadSession session, PartitionedScan<C> leadingPartitionScan, TokenPredicate query)
            throws IndexNotApplicableKernelException {
        performCheckBeforeOperation();
        final var descriptor = session.reference();
        if (!descriptor.getCapability().supportPartitionedScan(query)) {
            throw IndexNotApplicableKernelException.indexNotApplicable(
                    log,
                    descriptor.userDescription(tokenRead),
                    "This index does not support partitioned scan for this query: "
                            + descriptor.userDescription(tokenRead));
        }
        if (txStateHolder.hasTxStateWithChanges()) {
            throw new IllegalStateException(
                    "Transaction contains changes; PartitionScan is only valid in Read-Only transactions.");
        }

        final var defaultSession = (DefaultTokenReadSession) session;
        final var leadingTokenIndexCursorScan = (PartitionedTokenCursorScan<C>) leadingPartitionScan;
        final var tokenScan =
                defaultSession.reader().entityTokenScan(leadingTokenIndexCursorScan.getTokenScan(), query);
        return new PartitionedTokenIndexCursorScan<>(query, tokenScan);
    }

    private <C extends Cursor> List<PartitionedScan<C>> tokenIndexScan(
            TokenReadSession session,
            int desiredNumberOfPartitions,
            CursorContext cursorContext,
            TokenPredicate... queries)
            throws IndexNotApplicableKernelException {
        performCheckBeforeOperation();
        Preconditions.requireNonEmpty(queries);
        final var scans = new ArrayList<PartitionedScan<C>>(queries.length);
        final var leadingPartitionScan =
                this.<C>tokenIndexScan(session, desiredNumberOfPartitions, cursorContext, queries[0]);
        scans.add(leadingPartitionScan);
        for (int i = 1; i < queries.length; i++) {
            scans.add(tokenIndexScan(session, leadingPartitionScan, queries[i]));
        }
        return scans;
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
            try (NodeCursor node = cursors.allocateNodeCursor(queryContext.cursorContext(), memoryTracker)) {
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
        if (txStateHolder.hasTxStateWithChanges() && nodePropertyAllowed(node, propertyKeyId)) {
            return txStateHolder.txState().getNodeState(node).propertyValue(propertyKeyId);
        }
        return null;
    }

    private boolean nodePropertyAllowed(long node, int propertyKeyId) {
        return !applyAccessModeToTxState || checkNodePropertyAllowedUsingCursor(node, propertyKeyId);
    }

    private boolean checkNodePropertyAllowedUsingCursor(long node, int propertyKeyId) {
        try (var nodeCursor = cursors.allocateNodeCursor(queryContext.cursorContext(), memoryTracker)) {
            singleNode(node, nodeCursor);
            nodeCursor.next();
            try (var propertyCursor = cursors.allocatePropertyCursor(queryContext.cursorContext(), memoryTracker)) {
                nodeCursor.properties(propertyCursor, PropertySelection.selection(propertyKeyId));
                return propertyCursor.next();
            }
        }
    }

    @Override
    public Value relationshipPropertyChangeInBatchOrNull(long relationship, int propertyKeyId) {
        performCheckBeforeOperation();
        if (txStateHolder.hasTxStateWithChanges() && relPropertyAllowed(relationship, propertyKeyId)) {
            return txStateHolder.txState().getRelationshipState(relationship).propertyValue(propertyKeyId);
        }
        return null;
    }

    private boolean relPropertyAllowed(long relationship, int propertyKeyId) {
        return !applyAccessModeToTxState || checkRelPropertyAllowedUsingCursor(relationship, propertyKeyId);
    }

    private boolean checkRelPropertyAllowedUsingCursor(long relationship, int propertyKeyId) {
        try (var cursor = cursors.allocateRelationshipScanCursor(queryContext.cursorContext(), memoryTracker)) {
            singleRelationship(relationship, cursor);
            cursor.next();
            try (var propertyCursor = cursors.allocatePropertyCursor(queryContext.cursorContext(), memoryTracker)) {
                cursor.properties(propertyCursor, PropertySelection.selection(propertyKeyId));
                return propertyCursor.next();
            }
        }
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
                storageCursors,
                txStateHolder);
    }

    @Override
    public List<Integer> mostCommonLabelGivenRelationshipType(int type) {
        return entityCounter.mostCommonLabelGivenRelationshipType(type, storageReader, queryContext.cursorContext());
    }

    @Override
    public long estimateCountsForNode(int labelId) {
        return storageReader.countsForNode(labelId, queryContext.cursorContext());
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
                schemaRead,
                txStateHolder);
    }

    @Override
    public long estimateCountsForRelationships(int startLabelId, int typeId, int endLabelId) {
        return storageReader.countsForRelationship(startLabelId, typeId, endLabelId, queryContext.cursorContext());
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
        boolean existsInRelStore =
                storageReader.relationshipExists(reference, storageCursors, queryContext.cursorContext());
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

    public ValueIndexReader newValueIndexReader(IndexDescriptor index) throws IndexNotFoundKernelException {
        KernelSchemaRead.assertValidIndex(index);
        return indexingService.getIndexProxy(index).newValueReader();
    }

    private void assertIndexOnline(IndexDescriptor index)
            throws IndexNotFoundKernelException, IndexBrokenKernelException {
        if (schemaRead.indexGetState(index) == InternalIndexState.ONLINE) {
            return;
        }
        throw IndexBrokenKernelException.indexBroken(index.getName(), schemaRead.indexGetFailure(index));
    }

    private static void assertPredicatesMatchSchema(
            IndexDescriptor index, PropertyIndexQuery.ExactPredicate[] predicates)
            throws IndexNotApplicableKernelException {
        int[] propertyIds = index.schema().getPropertyIds();
        if (propertyIds.length != predicates.length) {
            throw IndexNotApplicableKernelException.internalError(
                    KernelRead.class.getSimpleName(),
                    format(
                            "The index specifies %d properties, but only %d lookup predicates were given.",
                            propertyIds.length, predicates.length));
        }
        for (int i = 0; i < predicates.length; i++) {
            if (predicates[i].propertyKeyId() != propertyIds[i]) {
                throw IndexNotApplicableKernelException.internalError(
                        KernelRead.class.getSimpleName(),
                        format(
                                "The index has the property id %d in position %d, but the lookup property id was %d.",
                                propertyIds[i], i, predicates[i].propertyKeyId()));
            }
        }
    }

    private void performCheckBeforeOperation() {
        assertOpen.assertOpen();
    }

    private AccessMode getAccessMode() {
        return accessModeProvider.getAccessMode();
    }

    public TokenIndexReader newTokenIndexReader(IndexDescriptor index) throws IndexNotFoundKernelException {
        KernelSchemaRead.assertValidIndex(index);
        return indexingService.getIndexProxy(index).newTokenReader();
    }

    @Override
    public IndexReadSession indexReadSession(IndexDescriptor index) throws IndexNotFoundKernelException {
        KernelSchemaRead.assertValidIndex(index);
        return new DefaultIndexReadSession(valueIndexReaderCache.getOrCreate(index), index);
    }

    @Override
    public TokenReadSession tokenReadSession(IndexDescriptor index) throws IndexNotFoundKernelException {
        KernelSchemaRead.assertValidIndex(index);
        return new DefaultTokenReadSession(tokenIndexReaderCache.getOrCreate(index), index);
    }

    @Override
    public long nodesGetCount() {
        return countsForNode(TokenConstants.ANY_LABEL);
    }

    @Override
    public long relationshipsGetCount() {
        return countsForRelationship(
                TokenConstants.ANY_LABEL, TokenConstants.ANY_RELATIONSHIP_TYPE, TokenConstants.ANY_LABEL);
    }

    @Override
    public boolean transactionStateHasChanges() {
        return txStateHolder.hasTxStateWithChanges();
    }

    public void release() {
        // Note: This only clears the caches, and does in fact not close the objects
        try (valueIndexReaderCache;
                tokenIndexReaderCache) {}
    }
}
