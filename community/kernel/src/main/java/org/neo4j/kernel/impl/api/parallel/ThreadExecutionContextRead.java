/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api.parallel;

import java.io.Closeable;
import java.util.List;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.IndexReadSession;
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
import org.neo4j.internal.kernel.api.Scan;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.kernel.api.TokenReadSession;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.newapi.DefaultPooledCursors;
import org.neo4j.kernel.impl.newapi.ReadSupport;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageEngineIndexingBehaviour;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.values.storable.Value;

public class ThreadExecutionContextRead implements Read, Closeable, QueryContext {
    private final ThreadExecutionContext context;
    private final Read read;
    private final StorageReader reader;
    private final ReadSupport readSupport;
    private final DefaultPooledCursors pooledCursors;

    ThreadExecutionContextRead(
            ThreadExecutionContext context,
            Read read,
            StorageReader reader,
            StoreCursors storeCursors,
            Config config,
            StorageEngineIndexingBehaviour indexingBehaviour) {
        this.context = context;
        this.read = read;
        this.reader = reader;
        this.pooledCursors = new DefaultPooledCursors(reader, storeCursors, config, indexingBehaviour);
        this.readSupport = new ReadSupport(reader, pooledCursors, this);
    }

    @Override
    public IndexReadSession indexReadSession(IndexDescriptor index) throws IndexNotFoundKernelException {
        return read.indexReadSession(index);
    }

    @Override
    public TokenReadSession tokenReadSession(IndexDescriptor index) throws IndexNotFoundKernelException {
        return read.tokenReadSession(index);
    }

    @Override
    public void nodeIndexSeek(
            QueryContext queryContext,
            IndexReadSession index,
            NodeValueIndexCursor cursor,
            IndexQueryConstraints constraints,
            PropertyIndexQuery... query)
            throws KernelException {
        read.nodeIndexSeek(queryContext, index, cursor, constraints, query);
    }

    @Override
    public void relationshipIndexSeek(
            QueryContext queryContext,
            IndexReadSession index,
            RelationshipValueIndexCursor cursor,
            IndexQueryConstraints constraints,
            PropertyIndexQuery... query)
            throws KernelException {
        read.relationshipIndexSeek(queryContext, index, cursor, constraints, query);
    }

    @Override
    public void nodeIndexScan(IndexReadSession index, NodeValueIndexCursor cursor, IndexQueryConstraints constraints)
            throws KernelException {
        read.nodeIndexScan(index, cursor, constraints);
    }

    @Override
    public void relationshipIndexScan(
            IndexReadSession index, RelationshipValueIndexCursor cursor, IndexQueryConstraints constraints)
            throws KernelException {
        read.relationshipIndexScan(index, cursor, constraints);
    }

    @Override
    public Scan<NodeLabelIndexCursor> nodeLabelScan(int label) {
        return read.nodeLabelScan(label);
    }

    @Override
    public PartitionedScan<NodeLabelIndexCursor> nodeLabelScan(
            TokenReadSession session, int desiredNumberOfPartitions, CursorContext cursorContext, TokenPredicate query)
            throws KernelException {
        return read.nodeLabelScan(session, desiredNumberOfPartitions, cursorContext, query);
    }

    @Override
    public PartitionedScan<NodeLabelIndexCursor> nodeLabelScan(
            TokenReadSession session, PartitionedScan<NodeLabelIndexCursor> leadingPartitionScan, TokenPredicate query)
            throws KernelException {
        return read.nodeLabelScan(session, leadingPartitionScan, query);
    }

    @Override
    public List<PartitionedScan<NodeLabelIndexCursor>> nodeLabelScans(
            TokenReadSession session,
            int desiredNumberOfPartitions,
            CursorContext cursorContext,
            TokenPredicate... queries)
            throws KernelException {
        return read.nodeLabelScans(session, desiredNumberOfPartitions, cursorContext, queries);
    }

    @Override
    public void nodeLabelScan(
            TokenReadSession session,
            NodeLabelIndexCursor cursor,
            IndexQueryConstraints constraints,
            TokenPredicate query,
            CursorContext cursorContext)
            throws KernelException {
        read.nodeLabelScan(session, cursor, constraints, query, cursorContext);
    }

    @Override
    public void allNodesScan(NodeCursor cursor) {
        read.allNodesScan(cursor);
    }

    @Override
    public Scan<NodeCursor> allNodesScan() {
        return read.allNodesScan();
    }

    @Override
    public void singleNode(long reference, NodeCursor cursor) {
        read.singleNode(reference, cursor);
    }

    @Override
    public PartitionedScan<NodeCursor> allNodesScan(int desiredNumberOfPartitions, CursorContext cursorContext) {
        return read.allNodesScan(desiredNumberOfPartitions, cursorContext);
    }

    @Override
    public PartitionedScan<RelationshipScanCursor> allRelationshipsScan(
            int desiredNumberOfPartitions, CursorContext cursorContext) {
        return read.allRelationshipsScan(desiredNumberOfPartitions, cursorContext);
    }

    @Override
    public boolean nodeExists(long reference) {
        return readSupport.nodeExistsWithoutTxState(
                reference, context.accessMode(), context.storeCursors(), context.cursorContext());
    }

    @Override
    public long countsForNode(int labelId) {
        return read.countsForNodeWithoutTxState(labelId);
    }

    @Override
    public long countsForNodeWithoutTxState(int labelId) {
        return read.countsForNodeWithoutTxState(labelId);
    }

    @Override
    public long countsForRelationship(int startLabelId, int typeId, int endLabelId) {
        return read.countsForRelationshipWithoutTxState(startLabelId, typeId, endLabelId);
    }

    @Override
    public long countsForRelationshipWithoutTxState(int startLabelId, int typeId, int endLabelId) {
        return read.countsForRelationshipWithoutTxState(startLabelId, typeId, endLabelId);
    }

    @Override
    public long nodesGetCount() {
        return read.nodesGetCount();
    }

    @Override
    public long relationshipsGetCount() {
        return read.relationshipsGetCount();
    }

    @Override
    public void singleRelationship(long reference, RelationshipScanCursor cursor) {
        read.singleRelationship(reference, cursor);
    }

    @Override
    public void allRelationshipsScan(RelationshipScanCursor cursor) {
        read.allRelationshipsScan(cursor);
    }

    @Override
    public Scan<RelationshipScanCursor> allRelationshipsScan() {
        return read.allRelationshipsScan();
    }

    @Override
    public PartitionedScan<RelationshipTypeIndexCursor> relationshipTypeScan(
            TokenReadSession session, int desiredNumberOfPartitions, CursorContext cursorContext, TokenPredicate query)
            throws KernelException {
        return read.relationshipTypeScan(session, desiredNumberOfPartitions, cursorContext, query);
    }

    @Override
    public PartitionedScan<RelationshipTypeIndexCursor> relationshipTypeScan(
            TokenReadSession session,
            PartitionedScan<RelationshipTypeIndexCursor> leadingPartitionScan,
            TokenPredicate query)
            throws KernelException {
        return read.relationshipTypeScan(session, leadingPartitionScan, query);
    }

    @Override
    public List<PartitionedScan<RelationshipTypeIndexCursor>> relationshipTypeScans(
            TokenReadSession session,
            int desiredNumberOfPartitions,
            CursorContext cursorContext,
            TokenPredicate... queries)
            throws KernelException {
        return read.relationshipTypeScans(session, desiredNumberOfPartitions, cursorContext, queries);
    }

    @Override
    public void relationshipTypeScan(
            TokenReadSession session,
            RelationshipTypeIndexCursor cursor,
            IndexQueryConstraints constraints,
            TokenPredicate query,
            CursorContext cursorContext)
            throws KernelException {
        read.relationshipTypeScan(session, cursor, constraints, query, cursorContext);
    }

    @Override
    public PartitionedScan<NodeValueIndexCursor> nodeIndexSeek(
            IndexReadSession index,
            int desiredNumberOfPartitions,
            QueryContext queryContext,
            PropertyIndexQuery... query)
            throws KernelException {
        return read.nodeIndexSeek(index, desiredNumberOfPartitions, queryContext, query);
    }

    @Override
    public PartitionedScan<RelationshipValueIndexCursor> relationshipIndexSeek(
            IndexReadSession index,
            int desiredNumberOfPartitions,
            QueryContext queryContext,
            PropertyIndexQuery... query)
            throws KernelException {
        return read.relationshipIndexSeek(index, desiredNumberOfPartitions, queryContext, query);
    }

    @Override
    public long lockingNodeUniqueIndexSeek(
            IndexDescriptor index, NodeValueIndexCursor cursor, PropertyIndexQuery.ExactPredicate... predicates)
            throws KernelException {
        return read.lockingNodeUniqueIndexSeek(index, cursor, predicates);
    }

    @Override
    public PartitionedScan<NodeValueIndexCursor> nodeIndexScan(
            IndexReadSession index, int desiredNumberOfPartitions, QueryContext queryContext) throws KernelException {
        return read.nodeIndexScan(index, desiredNumberOfPartitions, queryContext);
    }

    @Override
    public PartitionedScan<RelationshipValueIndexCursor> relationshipIndexScan(
            IndexReadSession index, int desiredNumberOfPartitions, QueryContext queryContext) throws KernelException {
        return read.relationshipIndexScan(index, desiredNumberOfPartitions, queryContext);
    }

    @Override
    public void singleRelationship(
            long reference,
            long sourceNodeReference,
            int type,
            long targetNodeReference,
            RelationshipScanCursor cursor) {
        read.singleRelationship(reference, sourceNodeReference, type, targetNodeReference, cursor);
    }

    @Override
    public boolean relationshipExists(long reference) {
        return readSupport.relationshipExistsWithoutTx(
                reference, context.accessMode(), context.storeCursors(), context.cursorContext());
    }

    @Override
    public void relationships(
            long nodeReference, long reference, RelationshipSelection selection, RelationshipTraversalCursor cursor) {
        read.relationships(nodeReference, reference, selection, cursor);
    }

    @Override
    public void nodeProperties(
            long nodeReference, Reference reference, PropertySelection selection, PropertyCursor cursor) {
        read.nodeProperties(nodeReference, reference, selection, cursor);
    }

    @Override
    public void relationshipProperties(
            long relationshipReference, Reference reference, PropertySelection selection, PropertyCursor cursor) {
        read.relationshipProperties(relationshipReference, reference, selection, cursor);
    }

    @Override
    public boolean nodeDeletedInTransaction(long node) {
        return false;
    }

    @Override
    public boolean relationshipDeletedInTransaction(long relationship) {
        return false;
    }

    @Override
    public Value nodePropertyChangeInTransactionOrNull(long node, int propertyKeyId) {
        return null;
    }

    @Override
    public Value relationshipPropertyChangeInTransactionOrNull(long relationship, int propertyKeyId) {
        return null;
    }

    @Override
    public boolean transactionStateHasChanges() {
        return false;
    }

    @Override
    public void close() {
        pooledCursors.assertClosed();
        pooledCursors.release();
        reader.close();
    }

    // -------------------------------------------------------------------------
    // QueryContext
    @Override
    public Read getRead() {
        return read;
    }

    @Override
    public CursorFactory cursors() {
        return pooledCursors;
    }

    @Override
    public ReadableTransactionState getTransactionStateOrNull() {
        return null;
    }

    @Override
    public CursorContext cursorContext() {
        return context.cursorContext();
    }

    @Override
    public MemoryTracker memoryTracker() {
        return EmptyMemoryTracker.INSTANCE;
    }

    @Override
    public IndexMonitor monitor() {
        return context.monitor();
    }
}
