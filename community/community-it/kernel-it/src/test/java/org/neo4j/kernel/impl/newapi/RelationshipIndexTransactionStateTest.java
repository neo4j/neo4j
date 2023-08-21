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

import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unordered;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class RelationshipIndexTransactionStateTest extends IndexTransactionStateTestBase {
    private static final String DEFAULT_REl_TYPE = "Rel";

    @Override
    Pair<Long, Value> entityWithProp(KernelTransaction tx, Object value) throws Exception {
        Write write = tx.dataWrite();
        long sourceNode = write.nodeCreate();
        long targetNode = write.nodeCreate();

        long rel = write.relationshipCreate(
                sourceNode, tx.tokenWrite().relationshipTypeGetOrCreateForName(DEFAULT_REl_TYPE), targetNode);

        Value val = Values.of(value);
        write.relationshipSetProperty(rel, tx.tokenWrite().propertyKeyGetOrCreateForName(DEFAULT_PROPERTY_NAME), val);
        return Pair.of(rel, val);
    }

    @Override
    void createIndex(IndexType indexType) {
        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            tx.schema()
                    .indexFor(RelationshipType.withName(DEFAULT_REl_TYPE))
                    .on(DEFAULT_PROPERTY_NAME)
                    .withIndexType(indexType)
                    .withName(INDEX_NAME)
                    .create();
            tx.commit();
        }

        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
        }
    }

    @Override
    void deleteEntity(KernelTransaction tx, long entity) throws Exception {
        tx.dataWrite().relationshipDelete(entity);
    }

    @Override
    boolean entityExists(KernelTransaction tx, long entity) {
        return tx.dataRead().relationshipExists(entity);
    }

    @Override
    void removeProperty(KernelTransaction tx, long entity) throws Exception {
        int propertyKey = tx.tokenRead().propertyKey(DEFAULT_PROPERTY_NAME);
        tx.dataWrite().relationshipRemoveProperty(entity, propertyKey);
    }

    @Override
    void setProperty(KernelTransaction tx, long entity, Value value) throws Exception {
        int propertyKey = tx.tokenRead().propertyKey(DEFAULT_PROPERTY_NAME);
        tx.dataWrite().relationshipSetProperty(entity, propertyKey, value);
    }

    @Override
    void assertEntityAndValueForSeek(
            Set<Pair<Long, Value>> expected,
            KernelTransaction tx,
            IndexDescriptor index,
            boolean needsValues,
            Object anotherValueFoundByQuery,
            PropertyIndexQuery... queries)
            throws Exception {
        try (RelationshipValueIndexCursor relationships =
                tx.cursors().allocateRelationshipValueIndexCursor(tx.cursorContext(), tx.memoryTracker())) {
            IndexReadSession indexSession = tx.dataRead().indexReadSession(index);
            tx.dataRead()
                    .relationshipIndexSeek(
                            tx.queryContext(), indexSession, relationships, unordered(needsValues), queries);
            assertEntityAndValue(
                    expected, tx, needsValues, anotherValueFoundByQuery, new RelationshipCursorAdapter(relationships));
        }
    }

    @Override
    void assertEntityAndValueForScan(
            Set<Pair<Long, Value>> expected,
            KernelTransaction tx,
            IndexDescriptor index,
            boolean needsValues,
            Object anotherValueFoundByQuery)
            throws Exception {
        IndexReadSession indexSession = tx.dataRead().indexReadSession(index);
        try (RelationshipValueIndexCursor relationships =
                tx.cursors().allocateRelationshipValueIndexCursor(tx.cursorContext(), tx.memoryTracker())) {
            tx.dataRead().relationshipIndexScan(indexSession, relationships, unordered(needsValues));
            assertEntityAndValue(
                    expected, tx, needsValues, anotherValueFoundByQuery, new RelationshipCursorAdapter(relationships));
        }
    }

    private static class RelationshipCursorAdapter implements EntityValueIndexCursor {

        private final RelationshipValueIndexCursor relationships;

        private RelationshipCursorAdapter(RelationshipValueIndexCursor relationships) {
            this.relationships = relationships;
        }

        @Override
        public boolean next() {
            return relationships.next();
        }

        @Override
        public Value propertyValue(int offset) {
            return relationships.propertyValue(offset);
        }

        @Override
        public long entityReference() {
            return relationships.relationshipReference();
        }
    }
}
