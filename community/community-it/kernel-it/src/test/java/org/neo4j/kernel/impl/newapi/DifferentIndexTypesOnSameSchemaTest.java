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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.exact;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class DifferentIndexTypesOnSameSchemaTest extends KernelAPIWriteTestBase<KernelAPIWriteTestSupport> {

    private static final String TOKEN = "TestToken";
    private static final String PROPERTY = "prop";

    @Override
    public KernelAPIWriteTestSupport newTestSupport() {
        return new WriteTestSupport();
    }

    @EnumSource(EntityType.class)
    @ParameterizedTest
    void testTextValueFromIndex(EntityType entityType) throws KernelException {
        entityType.createIndexes(graphDb);

        Value value = Values.stringValue("Some string");
        long entityId;
        try (KernelTransaction tx = beginTransaction()) {
            entityId = entityType.createEntity(tx, value);
            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            for (IndexType indexType : List.of(IndexType.RANGE, IndexType.TEXT)) {
                assertThat(entityType.getEntitiesByPropertyValue(tx, indexType, value))
                        .containsExactly(entityId);
            }

            tx.commit();
        }
    }

    @EnumSource(EntityType.class)
    @ParameterizedTest
    void testTextValueFromTxState(EntityType entityType) throws KernelException {
        entityType.createIndexes(graphDb);

        Value value = Values.stringValue("Some string");
        try (KernelTransaction tx = beginTransaction()) {
            long entityId = entityType.createEntity(tx, value);
            for (IndexType indexType : List.of(IndexType.RANGE, IndexType.TEXT)) {
                assertThat(entityType.getEntitiesByPropertyValue(tx, indexType, value))
                        .containsExactly(entityId);
            }

            tx.commit();
        }
    }

    @EnumSource(EntityType.class)
    @ParameterizedTest
    void testPointValueFromIndex(EntityType entityType) throws KernelException {
        entityType.createIndexes(graphDb);

        Value value = Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 1, 2);
        long entityId;
        try (KernelTransaction tx = beginTransaction()) {
            entityId = entityType.createEntity(tx, value);
            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            for (IndexType indexType : List.of(IndexType.RANGE, IndexType.POINT)) {
                assertThat(entityType.getEntitiesByPropertyValue(tx, indexType, value))
                        .containsExactly(entityId);
            }

            tx.commit();
        }
    }

    @EnumSource(EntityType.class)
    @ParameterizedTest
    void testPointValueFromTxState(EntityType entityType) throws KernelException {
        entityType.createIndexes(graphDb);

        Value value = Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 1, 2);
        try (KernelTransaction tx = beginTransaction()) {
            long entityId = entityType.createEntity(tx, value);
            for (IndexType indexType : List.of(IndexType.RANGE, IndexType.POINT)) {
                assertThat(entityType.getEntitiesByPropertyValue(tx, indexType, value))
                        .containsExactly(entityId);
            }

            tx.commit();
        }
    }

    private static String nameForType(IndexType indexType) {
        return indexType + "-IDX";
    }

    private enum EntityType {
        NODE {
            @Override
            void createIndexes(GraphDatabaseService db) {
                try (Transaction tx = db.beginTx()) {
                    List.of(IndexType.RANGE, IndexType.TEXT, IndexType.POINT).forEach(indexType -> tx.schema()
                            .indexFor(Label.label(TOKEN))
                            .on(PROPERTY)
                            .withName(nameForType(indexType))
                            .withIndexType(indexType)
                            .create());
                    tx.commit();
                }
                try (Transaction tx = db.beginTx()) {
                    tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
                    tx.commit();
                }
            }

            @Override
            List<Long> getEntitiesByPropertyValue(KernelTransaction tx, IndexType indexType, Value value)
                    throws KernelException {
                IndexReadSession indexSession =
                        tx.dataRead().indexReadSession(tx.schemaRead().indexGetForName(nameForType(indexType)));
                // let's check that the obtained session is actually from the intended index
                assertEquals(nameForType(indexType), indexSession.reference().getName());

                int property = tx.token().propertyKey(PROPERTY);
                try (NodeValueIndexCursor cursor =
                        tx.cursors().allocateNodeValueIndexCursor(NULL_CONTEXT, tx.memoryTracker())) {
                    tx.dataRead()
                            .nodeIndexSeek(
                                    tx.queryContext(), indexSession, cursor, unconstrained(), exact(property, value));

                    List<Long> hits = new ArrayList<>();
                    while (cursor.next()) {
                        hits.add(cursor.nodeReference());
                    }

                    return hits;
                }
            }

            @Override
            long createEntity(KernelTransaction tx, Value value) throws KernelException {
                long nodeId = tx.dataWrite().nodeCreate();
                tx.dataWrite().nodeAddLabel(nodeId, tx.token().nodeLabel(TOKEN));
                tx.dataWrite().nodeSetProperty(nodeId, tx.token().propertyKey(PROPERTY), value);
                return nodeId;
            }
        },

        RELATIONSHIP {
            @Override
            void createIndexes(GraphDatabaseService db) {
                try (Transaction tx = db.beginTx()) {
                    List.of(IndexType.RANGE, IndexType.TEXT, IndexType.POINT).forEach(indexType -> tx.schema()
                            .indexFor(RelationshipType.withName(TOKEN))
                            .on(PROPERTY)
                            .withName(nameForType(indexType))
                            .withIndexType(indexType)
                            .create());
                    tx.commit();
                }
                try (Transaction tx = db.beginTx()) {
                    tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
                    tx.commit();
                }
            }

            @Override
            List<Long> getEntitiesByPropertyValue(KernelTransaction tx, IndexType indexType, Value value)
                    throws KernelException {
                IndexReadSession indexSession =
                        tx.dataRead().indexReadSession(tx.schemaRead().indexGetForName(nameForType(indexType)));
                // let's check that the obtained session is actually from the intended index
                assertEquals(nameForType(indexType), indexSession.reference().getName());

                int property = tx.token().propertyKey(PROPERTY);
                try (RelationshipValueIndexCursor cursor =
                        tx.cursors().allocateRelationshipValueIndexCursor(NULL_CONTEXT, tx.memoryTracker())) {
                    tx.dataRead()
                            .relationshipIndexSeek(
                                    tx.queryContext(), indexSession, cursor, unconstrained(), exact(property, value));

                    List<Long> hits = new ArrayList<>();
                    while (cursor.next()) {
                        hits.add(cursor.relationshipReference());
                    }

                    return hits;
                }
            }

            @Override
            long createEntity(KernelTransaction tx, Value value) throws KernelException {
                long node1 = tx.dataWrite().nodeCreate();
                long node2 = tx.dataWrite().nodeCreate();
                long relationshipId =
                        tx.dataWrite().relationshipCreate(node1, tx.token().relationshipType(TOKEN), node2);
                tx.dataWrite()
                        .relationshipSetProperty(relationshipId, tx.token().propertyKey(PROPERTY), value);
                return relationshipId;
            }
        };

        abstract void createIndexes(GraphDatabaseService db);

        abstract List<Long> getEntitiesByPropertyValue(KernelTransaction tx, IndexType indexType, Value value)
                throws KernelException;

        abstract long createEntity(KernelTransaction tx, Value value) throws KernelException;
    }
}
