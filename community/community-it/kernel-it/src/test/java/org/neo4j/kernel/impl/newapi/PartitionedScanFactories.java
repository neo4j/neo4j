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

import static org.neo4j.test.Tokens.Suppliers.UUID.LABEL;
import static org.neo4j.test.Tokens.Suppliers.UUID.PROPERTY_KEY;
import static org.neo4j.test.Tokens.Suppliers.UUID.RELATIONSHIP_TYPE;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PartitionedScan;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.kernel.api.TokenReadSession;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.newapi.PartitionedScanTestSuite.Query;
import org.neo4j.kernel.impl.newapi.PropertyIndexScanPartitionedScanTestSuite.PropertyKeyScanQuery;
import org.neo4j.kernel.impl.newapi.PropertyIndexSeekPartitionedScanTestSuite.PropertyKeySeekQuery;
import org.neo4j.kernel.impl.newapi.TokenIndexScanPartitionedScanTestSuite.TokenScanQuery;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.Tokens;

class PartitionedScanFactories {
    abstract static class PartitionedScanFactory<QUERY extends Query<?>, SESSION, CURSOR extends Cursor> {
        abstract PartitionedScanFactory<QUERY, SESSION, ? extends Cursor> getEntityTypeComplimentFactory();

        abstract SESSION getSession(KernelTransaction tx, String indexName) throws KernelException;

        abstract CursorWithContext<CURSOR> getCursor(CursorFactory cursors);

        abstract long getEntityReference(CURSOR cursor);

        abstract PartitionedScan<CURSOR> partitionedScan(
                KernelTransaction tx, SESSION session, int desiredNumberOfPartitions, QUERY query)
                throws KernelException;

        final PartitionedScan<CURSOR> partitionedScan(KernelTransaction tx, int desiredNumberOfPartitions, QUERY query)
                throws KernelException {
            return partitionedScan(tx, getSession(tx, query.indexName()), desiredNumberOfPartitions, query);
        }

        final String name() {
            return getClass().getSimpleName();
        }
    }

    abstract static class TokenIndex<CURSOR extends Cursor>
            extends PartitionedScanFactory<TokenScanQuery, TokenReadSession, CURSOR> {
        abstract PartitionedScan<CURSOR> partitionedScan(
                KernelTransaction tx,
                TokenReadSession session,
                PartitionedScan<CURSOR> leadingPartitionScan,
                TokenScanQuery query)
                throws KernelException;

        abstract List<PartitionedScan<CURSOR>> partitionedScans(
                KernelTransaction tx,
                TokenReadSession session,
                int desiredNumberOfPartitions,
                List<TokenScanQuery> queries)
                throws KernelException;

        protected final PartitionedScan<CURSOR> partitionedScan(
                KernelTransaction tx, PartitionedScan<CURSOR> leadingPartitionScan, TokenScanQuery query)
                throws KernelException {
            return partitionedScan(tx, getSession(tx, query.indexName()), leadingPartitionScan, query);
        }

        protected final List<PartitionedScan<CURSOR>> partitionedScans(
                KernelTransaction tx, int desiredNumberOfPartitions, List<TokenScanQuery> queries)
                throws KernelException {
            assert queries.stream().map(TokenScanQuery::indexName).distinct().count()
                    == 1; // assert all are on same index
            return partitionedScans(tx, getSession(tx, queries.get(0).indexName()), desiredNumberOfPartitions, queries);
        }

        @Override
        protected final TokenReadSession getSession(KernelTransaction tx, String indexName) throws KernelException {
            final var index = tx.schemaRead().indexGetForName(indexName);
            return tx.dataRead().tokenReadSession(index);
        }
    }

    static final class NodeLabelIndexScan extends TokenIndex<NodeLabelIndexCursor> {
        public static final NodeLabelIndexScan FACTORY = new NodeLabelIndexScan();

        private NodeLabelIndexScan() {}

        @Override
        RelationshipTypeIndexScan getEntityTypeComplimentFactory() {
            return RelationshipTypeIndexScan.FACTORY;
        }

        @Override
        PartitionedScan<NodeLabelIndexCursor> partitionedScan(
                KernelTransaction tx,
                TokenReadSession session,
                int desiredNumberOfPartitions,
                TokenScanQuery tokenScanQuery)
                throws KernelException {
            return tx.dataRead()
                    .nodeLabelScan(
                            session, desiredNumberOfPartitions, CursorContext.NULL_CONTEXT, tokenScanQuery.get());
        }

        @Override
        CursorWithContext<NodeLabelIndexCursor> getCursor(CursorFactory cursors) {
            return cursors::allocateNodeLabelIndexCursor;
        }

        @Override
        long getEntityReference(NodeLabelIndexCursor cursor) {
            return cursor.nodeReference();
        }

        @Override
        PartitionedScan<NodeLabelIndexCursor> partitionedScan(
                KernelTransaction tx,
                TokenReadSession session,
                PartitionedScan<NodeLabelIndexCursor> leadingPartitionScan,
                TokenScanQuery query)
                throws KernelException {
            return tx.dataRead().nodeLabelScan(session, leadingPartitionScan, query.get());
        }

        @Override
        List<PartitionedScan<NodeLabelIndexCursor>> partitionedScans(
                KernelTransaction tx,
                TokenReadSession session,
                int desiredNumberOfPartitions,
                List<TokenScanQuery> queries)
                throws KernelException {
            return tx.dataRead()
                    .nodeLabelScans(
                            session,
                            desiredNumberOfPartitions,
                            CursorContext.NULL_CONTEXT,
                            queries.stream().map(TokenScanQuery::get).toArray(TokenPredicate[]::new));
        }
    }

    static final class RelationshipTypeIndexScan extends TokenIndex<RelationshipTypeIndexCursor> {
        public static final RelationshipTypeIndexScan FACTORY = new RelationshipTypeIndexScan();

        private RelationshipTypeIndexScan() {}

        @Override
        NodeLabelIndexScan getEntityTypeComplimentFactory() {
            return NodeLabelIndexScan.FACTORY;
        }

        @Override
        PartitionedScan<RelationshipTypeIndexCursor> partitionedScan(
                KernelTransaction tx,
                TokenReadSession session,
                int desiredNumberOfPartitions,
                TokenScanQuery tokenScanQuery)
                throws KernelException {
            return tx.dataRead()
                    .relationshipTypeScan(
                            session, desiredNumberOfPartitions, CursorContext.NULL_CONTEXT, tokenScanQuery.get());
        }

        @Override
        CursorWithContext<RelationshipTypeIndexCursor> getCursor(CursorFactory cursors) {
            return cursors::allocateRelationshipTypeIndexCursor;
        }

        @Override
        long getEntityReference(RelationshipTypeIndexCursor cursor) {
            return cursor.relationshipReference();
        }

        @Override
        PartitionedScan<RelationshipTypeIndexCursor> partitionedScan(
                KernelTransaction tx,
                TokenReadSession session,
                PartitionedScan<RelationshipTypeIndexCursor> leadingPartitionScan,
                TokenScanQuery query)
                throws KernelException {
            return tx.dataRead().relationshipTypeScan(session, leadingPartitionScan, query.get());
        }

        @Override
        List<PartitionedScan<RelationshipTypeIndexCursor>> partitionedScans(
                KernelTransaction tx,
                TokenReadSession session,
                int desiredNumberOfPartitions,
                List<TokenScanQuery> queries)
                throws KernelException {
            return tx.dataRead()
                    .relationshipTypeScans(
                            session,
                            desiredNumberOfPartitions,
                            CursorContext.NULL_CONTEXT,
                            queries.stream().map(TokenScanQuery::get).toArray(TokenPredicate[]::new));
        }
    }

    abstract static class PropertyIndex<QUERY extends Query<?>, CURSOR extends Cursor>
            extends PartitionedScanFactory<QUERY, IndexReadSession, CURSOR> {
        abstract SchemaDescriptor getSchemaDescriptor(int tokenId, int... propKeyIds);

        abstract Tokens.Suppliers.Supplier<?> getTokenSupplier();

        final Tokens.Suppliers.PropertyKey getPropKeySupplier() {
            return PROPERTY_KEY;
        }

        final String getIndexName(int tokenId, int... propKeyIds) {
            return String.format(
                    "%s[%s[%d] {%s}]",
                    name(),
                    getTokenSupplier().name(),
                    tokenId,
                    Arrays.stream(propKeyIds).mapToObj(String::valueOf).collect(Collectors.joining(",")));
        }

        @Override
        protected final IndexReadSession getSession(KernelTransaction tx, String indexName) throws KernelException {
            final var index = tx.schemaRead().indexGetForName(indexName);
            return tx.dataRead().indexReadSession(index);
        }

        protected final IndexDescriptor getIndex(KernelTransaction tx, String name) {
            return tx.schemaRead().indexGetForName(name);
        }

        protected final IndexDescriptor getIndex(KernelTransaction tx, int tokenId, int... propKeyIds) {
            return getIndex(tx, getIndexName(tokenId, propKeyIds));
        }
    }

    static final class NodePropertyIndexSeek extends PropertyIndex<PropertyKeySeekQuery, NodeValueIndexCursor> {
        public static final NodePropertyIndexSeek FACTORY = new NodePropertyIndexSeek();

        private NodePropertyIndexSeek() {}

        @Override
        RelationshipPropertyIndexSeek getEntityTypeComplimentFactory() {
            return RelationshipPropertyIndexSeek.FACTORY;
        }

        @Override
        PartitionedScan<NodeValueIndexCursor> partitionedScan(
                KernelTransaction tx,
                IndexReadSession session,
                int desiredNumberOfPartitions,
                PropertyKeySeekQuery propertyKeySeekQuery)
                throws KernelException {
            return tx.dataRead()
                    .nodeIndexSeek(
                            session, desiredNumberOfPartitions, QueryContext.NULL_CONTEXT, propertyKeySeekQuery.get());
        }

        @Override
        CursorWithContext<NodeValueIndexCursor> getCursor(CursorFactory cursors) {
            return context -> cursors.allocateNodeValueIndexCursor(context, EmptyMemoryTracker.INSTANCE);
        }

        @Override
        long getEntityReference(NodeValueIndexCursor cursor) {
            return cursor.nodeReference();
        }

        @Override
        Tokens.Suppliers.Label getTokenSupplier() {
            return LABEL;
        }

        @Override
        SchemaDescriptor getSchemaDescriptor(int labelId, int... propKeyIds) {
            return SchemaDescriptors.forLabel(labelId, propKeyIds);
        }
    }

    static final class NodePropertyIndexScan extends PropertyIndex<PropertyKeyScanQuery, NodeValueIndexCursor> {
        public static final NodePropertyIndexScan FACTORY = new NodePropertyIndexScan();

        private NodePropertyIndexScan() {}

        @Override
        RelationshipPropertyIndexScan getEntityTypeComplimentFactory() {
            return RelationshipPropertyIndexScan.FACTORY;
        }

        @Override
        PartitionedScan<NodeValueIndexCursor> partitionedScan(
                KernelTransaction tx,
                IndexReadSession session,
                int desiredNumberOfPartitions,
                PropertyKeyScanQuery propertyKeyScanQuery)
                throws KernelException {
            return tx.dataRead().nodeIndexScan(session, desiredNumberOfPartitions, QueryContext.NULL_CONTEXT);
        }

        @Override
        CursorWithContext<NodeValueIndexCursor> getCursor(CursorFactory cursors) {
            return context -> cursors.allocateNodeValueIndexCursor(context, EmptyMemoryTracker.INSTANCE);
        }

        @Override
        long getEntityReference(NodeValueIndexCursor cursor) {
            return cursor.nodeReference();
        }

        @Override
        Tokens.Suppliers.Label getTokenSupplier() {
            return LABEL;
        }

        @Override
        SchemaDescriptor getSchemaDescriptor(int labelId, int... propKeyIds) {
            return SchemaDescriptors.forLabel(labelId, propKeyIds);
        }
    }

    static final class RelationshipPropertyIndexSeek
            extends PropertyIndex<PropertyKeySeekQuery, RelationshipValueIndexCursor> {
        public static final RelationshipPropertyIndexSeek FACTORY = new RelationshipPropertyIndexSeek();

        private RelationshipPropertyIndexSeek() {}

        @Override
        NodePropertyIndexSeek getEntityTypeComplimentFactory() {
            return NodePropertyIndexSeek.FACTORY;
        }

        @Override
        PartitionedScan<RelationshipValueIndexCursor> partitionedScan(
                KernelTransaction tx,
                IndexReadSession session,
                int desiredNumberOfPartitions,
                PropertyKeySeekQuery propertyKeySeekQuery)
                throws KernelException {
            return tx.dataRead()
                    .relationshipIndexSeek(
                            session, desiredNumberOfPartitions, QueryContext.NULL_CONTEXT, propertyKeySeekQuery.get());
        }

        @Override
        CursorWithContext<RelationshipValueIndexCursor> getCursor(CursorFactory cursors) {
            return context -> cursors.allocateRelationshipValueIndexCursor(context, EmptyMemoryTracker.INSTANCE);
        }

        @Override
        long getEntityReference(RelationshipValueIndexCursor cursor) {
            return cursor.relationshipReference();
        }

        @Override
        Tokens.Suppliers.RelationshipType getTokenSupplier() {
            return RELATIONSHIP_TYPE;
        }

        @Override
        SchemaDescriptor getSchemaDescriptor(int relTypeId, int... propKeyIds) {
            return SchemaDescriptors.forRelType(relTypeId, propKeyIds);
        }
    }

    static final class RelationshipPropertyIndexScan
            extends PropertyIndex<PropertyKeyScanQuery, RelationshipValueIndexCursor> {
        public static final RelationshipPropertyIndexScan FACTORY = new RelationshipPropertyIndexScan();

        private RelationshipPropertyIndexScan() {}

        @Override
        NodePropertyIndexScan getEntityTypeComplimentFactory() {
            return NodePropertyIndexScan.FACTORY;
        }

        @Override
        PartitionedScan<RelationshipValueIndexCursor> partitionedScan(
                KernelTransaction tx,
                IndexReadSession session,
                int desiredNumberOfPartitions,
                PropertyKeyScanQuery propertyKeyScanQuery)
                throws KernelException {
            return tx.dataRead().relationshipIndexScan(session, desiredNumberOfPartitions, QueryContext.NULL_CONTEXT);
        }

        @Override
        CursorWithContext<RelationshipValueIndexCursor> getCursor(CursorFactory cursors) {
            return context -> cursors.allocateRelationshipValueIndexCursor(context, EmptyMemoryTracker.INSTANCE);
        }

        @Override
        long getEntityReference(RelationshipValueIndexCursor cursor) {
            return cursor.relationshipReference();
        }

        @Override
        Tokens.Suppliers.RelationshipType getTokenSupplier() {
            return RELATIONSHIP_TYPE;
        }

        @Override
        SchemaDescriptor getSchemaDescriptor(int relTypeId, int... propKeyIds) {
            return SchemaDescriptors.forRelType(relTypeId, propKeyIds);
        }
    }

    @FunctionalInterface
    interface CursorWithContext<CURSOR extends Cursor> {
        CURSOR with(CursorContext cursorContext);
    }
}
