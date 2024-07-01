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
package org.neo4j.internal.kernel.api.helpers;

import java.util.List;
import org.neo4j.exceptions.KernelException;
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
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.kernel.api.TokenReadSession;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.values.storable.Value;

public class StubRead implements Read {
    @Override
    public IndexReadSession indexReadSession(IndexDescriptor index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public TokenReadSession tokenReadSession(IndexDescriptor index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void nodeIndexSeek(
            QueryContext queryContext,
            IndexReadSession index,
            NodeValueIndexCursor cursor,
            IndexQueryConstraints constraints,
            PropertyIndexQuery... query) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PartitionedScan<NodeValueIndexCursor> nodeIndexSeek(
            IndexReadSession index,
            int desiredNumberOfPartitions,
            QueryContext queryContext,
            PropertyIndexQuery... query) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void relationshipIndexSeek(
            QueryContext queryContext,
            IndexReadSession index,
            RelationshipValueIndexCursor cursor,
            IndexQueryConstraints constraints,
            PropertyIndexQuery... query) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PartitionedScan<RelationshipValueIndexCursor> relationshipIndexSeek(
            IndexReadSession index,
            int desiredNumberOfPartitions,
            QueryContext queryContext,
            PropertyIndexQuery... query) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long lockingNodeUniqueIndexSeek(
            IndexDescriptor index, NodeValueIndexCursor cursor, PropertyIndexQuery.ExactPredicate... predicates) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long lockingRelationshipUniqueIndexSeek(
            IndexDescriptor index, RelationshipValueIndexCursor cursor, PropertyIndexQuery.ExactPredicate... predicates)
            throws KernelException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void nodeIndexScan(IndexReadSession index, NodeValueIndexCursor cursor, IndexQueryConstraints constraints) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PartitionedScan<NodeValueIndexCursor> nodeIndexScan(
            IndexReadSession index, int desiredNumberOfPartitions, QueryContext queryContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void relationshipIndexScan(
            IndexReadSession index, RelationshipValueIndexCursor cursor, IndexQueryConstraints constraints) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PartitionedScan<RelationshipValueIndexCursor> relationshipIndexScan(
            IndexReadSession index, int desiredNumberOfPartitions, QueryContext queryContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PartitionedScan<NodeLabelIndexCursor> nodeLabelScan(
            TokenReadSession session,
            int desiredNumberOfPartitions,
            CursorContext cursorContext,
            TokenPredicate query) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PartitionedScan<NodeLabelIndexCursor> nodeLabelScan(
            TokenReadSession session, PartitionedScan<NodeLabelIndexCursor> leadingPartitionScan, TokenPredicate query)
            throws IndexNotApplicableKernelException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<PartitionedScan<NodeLabelIndexCursor>> nodeLabelScans(
            TokenReadSession session,
            int desiredNumberOfPartitions,
            CursorContext cursorContext,
            TokenPredicate... queries) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void nodeLabelScan(
            TokenReadSession session,
            NodeLabelIndexCursor cursor,
            IndexQueryConstraints constraints,
            TokenPredicate query,
            CursorContext cursorContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void allNodesScan(NodeCursor cursor) {
        ((StubNodeCursor) cursor).scan();
    }

    @Override
    public void singleNode(long reference, NodeCursor cursor) {
        ((StubNodeCursor) cursor).single(reference);
    }

    @Override
    public PartitionedScan<NodeCursor> allNodesScan(int desiredNumberOfPartitions, CursorContext cursorContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PartitionedScan<RelationshipScanCursor> allRelationshipsScan(
            int desiredNumberOfPartitions, CursorContext cursorContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean nodeExists(long id) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long countsForNode(int labelId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long estimateCountsForNode(int labelId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Integer> mostCommonLabelGivenRelationshipType(int type) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long countsForRelationship(int startLabelId, int typeId, int endLabelId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long estimateCountsForRelationships(int startLabelId, int typeId, int endLabelId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long nodesGetCount() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long relationshipsGetCount() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void singleRelationship(long reference, RelationshipScanCursor cursor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void singleRelationship(
            long reference,
            long sourceNodeReference,
            int type,
            long targetNodeReference,
            RelationshipScanCursor cursor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean relationshipExists(long reference) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void allRelationshipsScan(RelationshipScanCursor cursor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PartitionedScan<RelationshipTypeIndexCursor> relationshipTypeScan(
            TokenReadSession session,
            int desiredNumberOfPartitions,
            CursorContext cursorContext,
            TokenPredicate query) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PartitionedScan<RelationshipTypeIndexCursor> relationshipTypeScan(
            TokenReadSession session,
            PartitionedScan<RelationshipTypeIndexCursor> leadingPartitionScan,
            TokenPredicate query)
            throws IndexNotApplicableKernelException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<PartitionedScan<RelationshipTypeIndexCursor>> relationshipTypeScans(
            TokenReadSession session,
            int desiredNumberOfPartitions,
            CursorContext cursorContext,
            TokenPredicate... queries) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void relationshipTypeScan(
            TokenReadSession session,
            RelationshipTypeIndexCursor cursor,
            IndexQueryConstraints constraints,
            TokenPredicate query,
            CursorContext cursorContext) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void relationships(
            long nodeReference, long reference, RelationshipSelection selection, RelationshipTraversalCursor cursor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void nodeProperties(
            long nodeReference, Reference reference, PropertySelection selection, PropertyCursor cursor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void relationshipProperties(
            long nodeReference,
            long startNodeReference,
            Reference reference,
            PropertySelection selection,
            PropertyCursor cursor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean nodeDeletedInTransaction(long node) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean relationshipDeletedInTransaction(long relationship) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Value nodePropertyChangeInBatchOrNull(long node, int propertyKeyId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Value relationshipPropertyChangeInBatchOrNull(long relationship, int propertyKeyId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean transactionStateHasChanges() {
        throw new UnsupportedOperationException();
    }
}
