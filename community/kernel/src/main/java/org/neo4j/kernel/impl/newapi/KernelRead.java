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

import java.util.ArrayList;
import java.util.List;
import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.Cursor;
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
import org.neo4j.kernel.api.AssertOpen;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.index.TokenIndexReader;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.api.txstate.TxStateHolder;
import org.neo4j.kernel.impl.api.IndexReaderCache;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.api.txstate.RelationshipState;
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
public final class KernelRead implements Read {
    private final StorageReader storageReader;
    private final DefaultPooledCursors cursors;
    private final IndexingService indexingService;
    private final MemoryTracker memoryTracker;
    private final IndexReaderCache<ValueIndexReader> valueIndexReaderCache;
    private final IndexReaderCache<TokenIndexReader> tokenIndexReaderCache;
    private final EntityCounter entityCounter;
    private final boolean applyAccessModeToTxState;
    private final TokenRead tokenRead;
    private final StoreCursors storageCursors;
    private final QueryContext queryContext;
    private final Locks entityLocks;
    final TxStateHolder txStateHolder;
    private final SchemaRead schemaRead;
    private final AssertOpen assertOpen;
    private final AccessModeProvider accessModeProvider;
    private final boolean parallel;

    public KernelRead(
            StorageReader storageReader,
            TokenRead tokenRead,
            DefaultPooledCursors cursors,
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
            boolean parallel) {
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
        this.assertOpen = assertOpen;
        this.accessModeProvider = accessModeProvider;
        this.parallel = parallel;
    }

    @Override
    public void nodeIndexSeek(
            QueryContext queryContext,
            IndexReadSession index,
            NodeValueIndexCursor cursor,
            IndexQueryConstraints constraints,
            PropertyIndexQuery... query)
            throws IndexNotApplicableKernelException {
        performCheckBeforeOperation();
        DefaultIndexReadSession indexSession = (DefaultIndexReadSession) index;
        validateConstraints(constraints, indexSession);

        if (indexSession.reference.schema().entityType() != EntityType.NODE) {
            throw new IndexNotApplicableKernelException("Node index seek can not be performed on index: "
                    + index.reference().userDescription(tokenRead));
        }

        EntityIndexSeekClient client = (EntityIndexSeekClient) cursor;
        client.setRead(this);
        indexSession.reader.query(client, queryContext, constraints, query);
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
            throw new IndexNotApplicableKernelException(
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
            PropertyIndexQuery... query)
            throws IndexNotApplicableKernelException {
        performCheckBeforeOperation();
        DefaultIndexReadSession indexSession = (DefaultIndexReadSession) index;
        validateConstraints(constraints, indexSession);
        if (indexSession.reference.schema().entityType() != EntityType.RELATIONSHIP) {
            throw new IndexNotApplicableKernelException("Relationship index seek can not be performed on index: "
                    + index.reference().userDescription(tokenRead));
        }

        EntityIndexSeekClient client = (EntityIndexSeekClient) cursor;
        client.setRead(this);
        indexSession.reader.query(client, queryContext, constraints, query);
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
            throw new IndexNotApplicableKernelException(
                    "Relationship index seek can not be performed on index: " + descriptor.userDescription(tokenRead));
        }
        return propertyIndexSeek(index, desiredNumberOfPartitions, queryContext, query);
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
            IndexDescriptor index, NodeValueIndexCursor cursor, PropertyIndexQuery.ExactPredicate... predicates)
            throws IndexNotApplicableKernelException, IndexNotFoundKernelException, IndexBrokenKernelException {
        verifyNotParallel();
        assertIndexOnline(index);
        assertPredicatesMatchSchema(index, predicates);

        int[] entityTokenIds = index.schema().getEntityTokenIds();
        if (entityTokenIds.length != 1) {
            throw new IndexNotApplicableKernelException("Multi-token index " + index + " does not support uniqueness.");
        }
        long indexEntryId = indexEntryResourceId(entityTokenIds[0], predicates);

        // First try to find node under a shared lock
        // if not found upgrade to exclusive and try again
        entityLocks.acquireSharedIndexEntryLock(indexEntryId);
        try (IndexReaders readers = new IndexReaders(index, this)) {
            nodeIndexSeekWithFreshIndexReader((DefaultNodeValueIndexCursor) cursor, readers.createReader(), predicates);
            if (!cursor.next()) {
                entityLocks.releaseSharedIndexEntryLock(indexEntryId);
                entityLocks.acquireExclusiveIndexEntryLock(indexEntryId);
                nodeIndexSeekWithFreshIndexReader(
                        (DefaultNodeValueIndexCursor) cursor, readers.createReader(), predicates);
                if (cursor.next()) {
                    // we found it under the exclusive lock
                    // downgrade to a shared lock
                    entityLocks.acquireSharedIndexEntryLock(indexEntryId);
                    entityLocks.releaseExclusiveIndexEntryLock(indexEntryId);
                    return cursor.nodeReference();
                } else {
                    return StatementConstants.NO_SUCH_NODE;
                }
            }

            return cursor.nodeReference();
        }
    }

    @Override
    public long lockingRelationshipUniqueIndexSeek(
            IndexDescriptor index, RelationshipValueIndexCursor cursor, PropertyIndexQuery.ExactPredicate... predicates)
            throws KernelException {
        verifyNotParallel();
        assertIndexOnline(index);
        assertPredicatesMatchSchema(index, predicates);

        int[] entityTokenIds = index.schema().getEntityTokenIds();
        if (entityTokenIds.length != 1) {
            throw new IndexNotApplicableKernelException("Multi-token index " + index + " does not support uniqueness.");
        }
        long indexEntryId = indexEntryResourceId(entityTokenIds[0], predicates);

        // First try to find relationship under a shared lock
        // if not found upgrade to exclusive and try again
        entityLocks.acquireSharedIndexEntryLock(indexEntryId);
        try (IndexReaders readers = new IndexReaders(index, this)) {
            DefaultRelationshipValueIndexCursor indexCursor = (DefaultRelationshipValueIndexCursor) cursor;
            relationshipIndexSeekWithFreshIndexReader(indexCursor, readers.createReader(), predicates);
            if (!cursor.next()) {
                entityLocks.releaseSharedIndexEntryLock(indexEntryId);
                entityLocks.acquireExclusiveIndexEntryLock(indexEntryId);
                relationshipIndexSeekWithFreshIndexReader(indexCursor, readers.createReader(), predicates);
                if (cursor.next()) {
                    // we found it under the exclusive lock
                    // downgrade to a shared lock
                    entityLocks.acquireSharedIndexEntryLock(indexEntryId);
                    entityLocks.releaseExclusiveIndexEntryLock(indexEntryId);
                    return cursor.relationshipReference();
                } else {
                    return StatementConstants.NO_SUCH_RELATIONSHIP;
                }
            }

            return cursor.relationshipReference();
        }
    }

    public void nodeIndexSeekWithFreshIndexReader(
            DefaultNodeValueIndexCursor cursor,
            ValueIndexReader indexReader,
            PropertyIndexQuery.ExactPredicate... query)
            throws IndexNotApplicableKernelException {
        cursor.setRead(this);
        indexReader.query(cursor, queryContext, unconstrained(), query);
    }

    public void relationshipIndexSeekWithFreshIndexReader(
            DefaultRelationshipValueIndexCursor cursor,
            ValueIndexReader indexReader,
            PropertyIndexQuery.ExactPredicate... query)
            throws IndexNotApplicableKernelException {
        cursor.setRead(this);
        indexReader.query(cursor, queryContext, unconstrained(), query);
    }

    @Override
    public void nodeIndexScan(IndexReadSession index, NodeValueIndexCursor cursor, IndexQueryConstraints constraints)
            throws KernelException {
        performCheckBeforeOperation();
        DefaultIndexReadSession indexSession = (DefaultIndexReadSession) index;

        if (indexSession.reference.schema().entityType() != EntityType.NODE) {
            throw new IndexNotApplicableKernelException("Node index scan can not be performed on index: "
                    + index.reference().userDescription(tokenRead));
        }

        scanIndex(indexSession, (EntityIndexSeekClient) cursor, constraints);
    }

    @Override
    public PartitionedScan<NodeValueIndexCursor> nodeIndexScan(
            IndexReadSession index, int desiredNumberOfPartitions, QueryContext queryContext)
            throws IndexNotApplicableKernelException {
        performCheckBeforeOperation();
        final var descriptor = index.reference();
        if (descriptor.schema().entityType() != EntityType.NODE) {
            throw new IndexNotApplicableKernelException(
                    "Node index scan can not be performed on index: " + descriptor.userDescription(tokenRead));
        }

        return propertyIndexScan(index, desiredNumberOfPartitions, queryContext);
    }

    @Override
    public void relationshipIndexScan(
            IndexReadSession index, RelationshipValueIndexCursor cursor, IndexQueryConstraints constraints)
            throws KernelException {
        performCheckBeforeOperation();
        DefaultIndexReadSession indexSession = (DefaultIndexReadSession) index;

        if (indexSession.reference.schema().entityType() != EntityType.RELATIONSHIP) {
            throw new IndexNotApplicableKernelException("Relationship index scan can not be performed on index: "
                    + index.reference().userDescription(tokenRead));
        }

        scanIndex(indexSession, (EntityIndexSeekClient) cursor, constraints);
    }

    @Override
    public PartitionedScan<RelationshipValueIndexCursor> relationshipIndexScan(
            IndexReadSession index, int desiredNumberOfPartitions, QueryContext queryContext)
            throws IndexNotApplicableKernelException {
        performCheckBeforeOperation();
        final var descriptor = index.reference();
        if (descriptor.schema().entityType() != EntityType.RELATIONSHIP) {
            throw new IndexNotApplicableKernelException(
                    "Relationship index scan can not be performed on index: " + descriptor.userDescription(tokenRead));
        }

        return propertyIndexScan(index, desiredNumberOfPartitions, queryContext);
    }

    private void scanIndex(
            DefaultIndexReadSession indexSession,
            EntityIndexSeekClient indexSeekClient,
            IndexQueryConstraints constraints)
            throws KernelException {
        indexSeekClient.setRead(this);
        indexSession.reader.query(indexSeekClient, queryContext, constraints, PropertyIndexQuery.allEntries());
    }

    @Override
    public PartitionedScan<NodeLabelIndexCursor> nodeLabelScan(
            TokenReadSession session, int desiredNumberOfPartitions, CursorContext cursorContext, TokenPredicate query)
            throws IndexNotApplicableKernelException {
        performCheckBeforeOperation();
        if (session.reference().schema().entityType() != EntityType.NODE) {
            throw new IndexNotApplicableKernelException("Node label index scan can not be performed on index: "
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
            throw new IndexNotApplicableKernelException("Node label index scan can not be performed on index: "
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
            throw new IndexNotApplicableKernelException("Node label index scan can not be performed on index: "
                    + session.reference().userDescription(tokenRead));
        }
        return tokenIndexScan(session, desiredNumberOfPartitions, cursorContext, queries);
    }

    @Override
    public void nodeLabelScan(
            TokenReadSession session,
            NodeLabelIndexCursor cursor,
            IndexQueryConstraints constraints,
            TokenPredicate query,
            CursorContext cursorContext)
            throws KernelException {
        performCheckBeforeOperation();

        if (session.reference().schema().entityType() != EntityType.NODE) {
            throw new IndexNotApplicableKernelException("Node label index scan can not be performed on index: "
                    + session.reference().userDescription(tokenRead));
        }

        var tokenSession = (DefaultTokenReadSession) session;

        DefaultNodeLabelIndexCursor indexCursor = (DefaultNodeLabelIndexCursor) cursor;
        indexCursor.setRead(this);
        tokenSession.reader.query(indexCursor, constraints, query, cursorContext);
    }

    @Override
    public void allNodesScan(NodeCursor cursor) {
        performCheckBeforeOperation();
        ((DefaultNodeCursor) cursor).scan(this);
    }

    @Override
    public void singleNode(long reference, NodeCursor cursor) {
        performCheckBeforeOperation();
        ((DefaultNodeCursor) cursor).single(reference, this);
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
        ((DefaultRelationshipScanCursor) cursor).single(reference, this);
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
                .single(reference, sourceNodeReference, type, targetNodeReference, this);
    }

    @Override
    public void allRelationshipsScan(RelationshipScanCursor cursor) {
        performCheckBeforeOperation();
        ((DefaultRelationshipScanCursor) cursor).scan(this);
    }

    @Override
    public PartitionedScan<RelationshipTypeIndexCursor> relationshipTypeScan(
            TokenReadSession session, int desiredNumberOfPartitions, CursorContext cursorContext, TokenPredicate query)
            throws IndexNotApplicableKernelException {
        performCheckBeforeOperation();
        if (session.reference().schema().entityType() != EntityType.RELATIONSHIP) {
            throw new IndexNotApplicableKernelException("Relationship type index scan can not be performed on index: "
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
            throw new IndexNotApplicableKernelException("Relationship type index scan can not be performed on index: "
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
            throw new IndexNotApplicableKernelException("Relationship type index scan can not be performed on index: "
                    + session.reference().userDescription(tokenRead));
        }
        return tokenIndexScan(session, desiredNumberOfPartitions, cursorContext, queries);
    }

    @Override
    public void relationshipTypeScan(
            TokenReadSession session,
            RelationshipTypeIndexCursor cursor,
            IndexQueryConstraints constraints,
            TokenPredicate query,
            CursorContext cursorContext)
            throws KernelException {
        performCheckBeforeOperation();

        if (session.reference().schema().entityType() != EntityType.RELATIONSHIP) {
            throw new IndexNotApplicableKernelException("Relationship type index scan can not be performed on index: "
                    + session.reference().userDescription(tokenRead));
        }

        var tokenSession = (DefaultTokenReadSession) session;

        var indexCursor = (InternalRelationshipTypeIndexCursor) cursor;
        indexCursor.setRead(this);
        tokenSession.reader.query(indexCursor, constraints, query, cursorContext);
    }

    @Override
    public void relationships(
            long nodeReference, long reference, RelationshipSelection selection, RelationshipTraversalCursor cursor) {
        ((DefaultRelationshipTraversalCursor) cursor).init(nodeReference, reference, selection, this);
    }

    @Override
    public void nodeProperties(
            long nodeReference, Reference reference, PropertySelection selection, PropertyCursor cursor) {
        ((DefaultPropertyCursor) cursor).initNode(nodeReference, reference, selection, this);
    }

    @Override
    public void relationshipProperties(
            long relationshipReference,
            long startNodeReference,
            Reference reference,
            PropertySelection selection,
            PropertyCursor cursor) {
        ((DefaultPropertyCursor) cursor).initRelationship(relationshipReference, reference, selection, this);
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
            throw new IndexNotApplicableKernelException("This index does not support partitioned scan for this query: "
                    + descriptor.userDescription(tokenRead));
        }
        if (txStateHolder.hasTxStateWithChanges()) {
            throw new IllegalStateException(
                    "Transaction contains changes; PartitionScan is only valid in Read-Only transactions.");
        }

        final var session = (DefaultIndexReadSession) index;
        final var valueSeek = session.reader.valueSeek(desiredNumberOfPartitions, queryContext, query);
        return new PartitionedValueIndexCursorSeek<>(descriptor, valueSeek, query);
    }

    private <C extends Cursor> PartitionedScan<C> tokenIndexScan(
            TokenReadSession session, int desiredNumberOfPartitions, CursorContext cursorContext, TokenPredicate query)
            throws IndexNotApplicableKernelException {
        performCheckBeforeOperation();
        final var descriptor = session.reference();
        if (!descriptor.getCapability().supportPartitionedScan(query)) {
            throw new IndexNotApplicableKernelException("This index does not support partitioned scan for this query: "
                    + descriptor.userDescription(tokenRead));
        }
        if (txStateHolder.hasTxStateWithChanges()) {
            throw new IllegalStateException(
                    "Transaction contains changes; PartitionScan is only valid in Read-Only transactions.");
        }

        final var defaultSession = (DefaultTokenReadSession) session;
        final var tokenScan = defaultSession.reader.entityTokenScan(desiredNumberOfPartitions, cursorContext, query);
        return new PartitionedTokenIndexCursorScan<>(query, tokenScan);
    }

    private <C extends Cursor> PartitionedScan<C> tokenIndexScan(
            TokenReadSession session, PartitionedScan<C> leadingPartitionScan, TokenPredicate query)
            throws IndexNotApplicableKernelException {
        performCheckBeforeOperation();
        final var descriptor = session.reference();
        if (!descriptor.getCapability().supportPartitionedScan(query)) {
            throw new IndexNotApplicableKernelException("This index does not support partitioned scan for this query: "
                    + descriptor.userDescription(tokenRead));
        }
        if (txStateHolder.hasTxStateWithChanges()) {
            throw new IllegalStateException(
                    "Transaction contains changes; PartitionScan is only valid in Read-Only transactions.");
        }

        final var defaultSession = (DefaultTokenReadSession) session;
        final var leadingTokenIndexCursorScan = (PartitionedTokenIndexCursorScan<C>) leadingPartitionScan;
        final var tokenScan = defaultSession.reader.entityTokenScan(leadingTokenIndexCursorScan.getTokenScan(), query);
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

    public ValueIndexReader newValueIndexReader(IndexDescriptor index) throws IndexNotFoundKernelException {
        KernelSchemaRead.assertValidIndex(index);
        return indexingService.getIndexProxy(index).newValueReader();
    }

    private void assertIndexOnline(IndexDescriptor index)
            throws IndexNotFoundKernelException, IndexBrokenKernelException {
        if (schemaRead.indexGetState(index) == InternalIndexState.ONLINE) {
            return;
        }
        throw new IndexBrokenKernelException(schemaRead.indexGetFailure(index));
    }

    private static void assertPredicatesMatchSchema(
            IndexDescriptor index, PropertyIndexQuery.ExactPredicate[] predicates)
            throws IndexNotApplicableKernelException {
        int[] propertyIds = index.schema().getPropertyIds();
        if (propertyIds.length != predicates.length) {
            throw new IndexNotApplicableKernelException(format(
                    "The index specifies %d properties, but only %d lookup predicates were given.",
                    propertyIds.length, predicates.length));
        }
        for (int i = 0; i < predicates.length; i++) {
            if (predicates[i].propertyKeyId() != propertyIds[i]) {
                throw new IndexNotApplicableKernelException(format(
                        "The index has the property id %d in position %d, but the lookup property id was %d.",
                        propertyIds[i], i, predicates[i].propertyKeyId()));
            }
        }
    }

    void performCheckBeforeOperation() {
        assertOpen.assertOpen();
    }

    AccessMode getAccessMode() {
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

    public void release() {
        // Note: This only clears the caches, and does in fact not close the objects
        valueIndexReaderCache.close();
        tokenIndexReaderCache.close();
    }
}
