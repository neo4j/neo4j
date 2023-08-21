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
package org.neo4j.kernel.impl.newapi.index;

import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.values.storable.Values.stringValue;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.values.storable.Value;

public class RelationshipParams implements EntityParams<RelationshipValueIndexCursor> {
    @Override
    public long entityWithProp(Transaction tx, String token, String key, Object value) {
        Node sourceNode = tx.createNode();
        Node targetNode = tx.createNode();

        Relationship rel = sourceNode.createRelationshipTo(targetNode, RelationshipType.withName(token));

        rel.setProperty(key, value);
        return rel.getId();
    }

    @Override
    public long entityNoTokenWithProp(Transaction tx, String key, Object value) {
        throw new IllegalStateException("Relationship must have type");
    }

    @Override
    public long entityWithTwoProps(
            Transaction tx, String token, String key1, String value1, String key2, String value2) {
        Node sourceNode = tx.createNode();
        Node targetNode = tx.createNode();

        Relationship rel = sourceNode.createRelationshipTo(targetNode, RelationshipType.withName(token));

        rel.setProperty(key1, value1);
        rel.setProperty(key2, value2);
        return rel.getId();
    }

    @Override
    public boolean tokenlessEntitySupported() {
        return false;
    }

    @Override
    public RelationshipValueIndexCursor allocateEntityValueIndexCursor(
            KernelTransaction tx, CursorFactory cursorFactory) {
        return cursorFactory.allocateRelationshipValueIndexCursor(NULL_CONTEXT, tx.memoryTracker());
    }

    @Override
    public long entityReference(RelationshipValueIndexCursor cursor) {
        return cursor.relationshipReference();
    }

    @Override
    public void entityIndexSeek(
            KernelTransaction tx,
            IndexReadSession index,
            RelationshipValueIndexCursor cursor,
            IndexQueryConstraints constraints,
            PropertyIndexQuery... query)
            throws KernelException {
        tx.dataRead().relationshipIndexSeek(tx.queryContext(), index, cursor, constraints, query);
    }

    @Override
    public void entityIndexScan(
            KernelTransaction tx,
            IndexReadSession index,
            RelationshipValueIndexCursor cursor,
            IndexQueryConstraints constraints)
            throws KernelException {
        tx.dataRead().relationshipIndexScan(index, cursor, constraints);
    }

    @Override
    public void createEntityIndex(
            Transaction tx, String entityToken, String propertyKey, String indexName, IndexType indexType) {
        tx.schema()
                .indexFor(RelationshipType.withName(entityToken))
                .on(propertyKey)
                .withIndexType(indexType)
                .withName(indexName)
                .create();
    }

    @Override
    public void createCompositeEntityIndex(
            Transaction tx,
            String entityToken,
            String propertyKey1,
            String propertyKey2,
            String indexName,
            IndexType indexType) {
        tx.schema()
                .indexFor(RelationshipType.withName(entityToken))
                .on(propertyKey1)
                .on(propertyKey2)
                .withIndexType(indexType)
                .withName(indexName)
                .create();
    }

    @Override
    public void entitySetProperty(KernelTransaction tx, long entityId, int propId, String value)
            throws KernelException {
        tx.dataWrite().relationshipSetProperty(entityId, propId, stringValue(value));
    }

    @Override
    public void entitySetProperty(KernelTransaction tx, long entityId, int propId, Value value) throws KernelException {
        tx.dataWrite().relationshipSetProperty(entityId, propId, value);
    }

    @Override
    public int entityTokenId(KernelTransaction tx, String tokenName) {
        return tx.token().relationshipType(tokenName);
    }

    @Override
    public SchemaDescriptor schemaDescriptor(int tokenId, int propId) {
        return SchemaDescriptors.forRelType(tokenId, propId);
    }

    @Override
    public Value getPropertyValueFromStore(KernelTransaction tx, CursorFactory cursorFactory, long reference) {
        try (var storeCursor = cursorFactory.allocateRelationshipScanCursor(NULL_CONTEXT);
                var propertyCursor = cursorFactory.allocatePropertyCursor(NULL_CONTEXT, EmptyMemoryTracker.INSTANCE)) {
            tx.dataRead().singleRelationship(reference, storeCursor);
            storeCursor.next();
            storeCursor.properties(propertyCursor);
            propertyCursor.next();
            return propertyCursor.propertyValue();
        }
    }

    @Override
    public void entityDelete(KernelTransaction tx, long reference) throws InvalidTransactionTypeKernelException {
        tx.dataWrite().relationshipDelete(reference);
    }

    @Override
    public void entityRemoveToken(KernelTransaction tx, long entityId, int tokenId) {
        throw new IllegalStateException("Relationship must have type");
    }

    @Override
    public void entityAddToken(KernelTransaction tx, long entityId, int tokenId) {
        throw new IllegalStateException("Relationship must have type");
    }

    @Override
    public long entityCreateNew(KernelTransaction tx, int tokenId) throws KernelException {
        return tx.dataWrite()
                .relationshipCreate(
                        tx.dataWrite().nodeCreate(), tokenId, tx.dataWrite().nodeCreate());
    }
}
