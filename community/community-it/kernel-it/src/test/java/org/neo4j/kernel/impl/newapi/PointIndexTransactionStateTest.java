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
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unordered;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class PointIndexTransactionStateTest extends KernelAPIWriteTestBase<WriteTestSupport> {
    private static final String INDEX_NAME = "myIndex";
    private static final String DEFAULT_PROPERTY_NAME = "prop";

    @ParameterizedTest
    @EnumSource(EntityOperations.class)
    void shouldPerformScan(EntityOperations ops) throws Exception {
        // given
        Set<Pair<Long, Value>> expected = new HashSet<>();
        long entityToDelete;
        long entityToChange;
        long entityToChange2;
        try (KernelTransaction tx = beginTransaction()) {
            expected.add(ops.entityWithProp(tx, point(-1, 1)));
            expected.add(ops.entityWithProp(tx, point(-2, 2)));
            entityToDelete = entityWithPropId(ops, tx, point(-3, 3));
            entityToChange = entityWithPropId(ops, tx, point(-4, 4));
            entityToChange2 = entityWithPropId(ops, tx, point(-5, 5));
            tx.commit();
        }

        ops.createIndex(graphDb);

        // when
        try (KernelTransaction tx = beginTransaction()) {
            expected.add(ops.entityWithProp(tx, point(-6, 6)));
            ops.entityWithProp(tx, "some string");
            ops.deleteEntity(tx, entityToDelete);
            ops.removeProperty(tx, entityToChange);
            ops.setProperty(tx, entityToChange2, "some string");

            IndexDescriptor index = tx.schemaRead().indexGetForName(INDEX_NAME);

            ops.assertEntityAndValueForScan(expected, tx, index, point(-7, 7));
        }
    }

    @ParameterizedTest
    @EnumSource(EntityOperations.class)
    void shouldPerformEqualitySeek(EntityOperations ops) throws Exception {
        // given
        Set<Pair<Long, Value>> expected = new HashSet<>();
        long entityToDelete;
        long entityToChange;
        long entityToChange2;
        long entityToChange3;
        try (KernelTransaction tx = beginTransaction()) {
            expected.add(ops.entityWithProp(tx, point(-1, 1)));
            expected.add(ops.entityWithProp(tx, point(-1, 1)));
            entityToDelete = entityWithPropId(ops, tx, point(-1, 1));
            entityToChange = entityWithPropId(ops, tx, point(-1, 1));
            entityToChange2 = entityWithPropId(ops, tx, point(-1, 1));
            entityToChange3 = entityWithPropId(ops, tx, point(-1, 1));
            tx.commit();
        }

        ops.createIndex(graphDb);

        // when
        try (KernelTransaction tx = beginTransaction()) {
            expected.add(ops.entityWithProp(tx, point(-1, 1)));
            ops.entityWithProp(tx, point(-2, 2));
            ops.entityWithProp(tx, "some string");
            ops.deleteEntity(tx, entityToDelete);
            ops.removeProperty(tx, entityToChange);
            ops.setProperty(tx, entityToChange2, "some string");
            ops.setProperty(tx, entityToChange3, point(-2, 2));

            IndexDescriptor index = tx.schemaRead().indexGetForName(INDEX_NAME);

            int prop = tx.tokenRead().propertyKey(DEFAULT_PROPERTY_NAME);
            ops.assertEntityAndValueForSeek(
                    expected, tx, index, point(-1, 1), PropertyIndexQuery.exact(prop, point(-1, 1)));
        }
    }

    @ParameterizedTest
    @EnumSource(EntityOperations.class)
    void shouldPerformBoundingBoxSeek(EntityOperations ops) throws Exception {
        // given
        Set<Pair<Long, Value>> expected = new HashSet<>();
        long entityToDelete;
        long entityToChange;
        long entityToChange2;
        long entityToChange3;
        try (KernelTransaction tx = beginTransaction()) {
            expected.add(ops.entityWithProp(tx, point(1, 1)));
            expected.add(ops.entityWithProp(tx, point(2, 2)));
            entityToDelete = entityWithPropId(ops, tx, point(1, 1));
            entityToChange = entityWithPropId(ops, tx, point(1, 1));
            entityToChange2 = entityWithPropId(ops, tx, point(1, 1));
            entityToChange3 = entityWithPropId(ops, tx, point(1, 1));
            tx.commit();
        }

        ops.createIndex(graphDb);

        // when
        try (KernelTransaction tx = beginTransaction()) {
            expected.add(ops.entityWithProp(tx, point(3, 3)));
            ops.entityWithProp(tx, point(-1, 1));
            ops.entityWithProp(tx, "some string");
            ops.deleteEntity(tx, entityToDelete);
            ops.removeProperty(tx, entityToChange);
            ops.setProperty(tx, entityToChange2, "some string");
            ops.setProperty(tx, entityToChange3, point(-1, 1));

            IndexDescriptor index = tx.schemaRead().indexGetForName(INDEX_NAME);

            int prop = tx.tokenRead().propertyKey(DEFAULT_PROPERTY_NAME);
            ops.assertEntityAndValueForSeek(
                    expected, tx, index, point(1, 1), PropertyIndexQuery.boundingBox(prop, point(0, 0), point(3, 3)));
        }
    }

    private PointValue point(int x, int y) {
        return Values.pointValue(CoordinateReferenceSystem.CARTESIAN, x, y);
    }

    @Override
    public WriteTestSupport newTestSupport() {
        return new WriteTestSupport();
    }

    static long entityWithPropId(EntityOperations ops, KernelTransaction tx, Object value) throws Exception {
        return ops.entityWithProp(tx, value).first();
    }

    private static void assertEntityAndValue(
            EntityOperations ops,
            Set<Pair<Long, Value>> expected,
            KernelTransaction tx,
            Object anotherValueFoundByQuery,
            EntityValueIndexCursor entities)
            throws Exception {
        // Modify tx state with changes that should not be reflected in the cursor,
        // since the cursor was already initialized in the code calling this method
        entityWithPropId(ops, tx, anotherValueFoundByQuery);

        Set<Pair<Long, Value>> found = new HashSet<>();
        while (entities.next()) {
            found.add(Pair.of(entities.entityReference(), entities.propertyValue(0)));
        }

        assertThat(found).isEqualTo(expected);
    }

    private enum EntityOperations {
        NODE {
            private static final String DEFAULT_LABEL = "Node";

            @Override
            Pair<Long, Value> entityWithProp(KernelTransaction tx, Object value) throws Exception {
                Write write = tx.dataWrite();
                long node = write.nodeCreate();
                write.nodeAddLabel(node, tx.tokenWrite().labelGetOrCreateForName(DEFAULT_LABEL));
                Value val = Values.of(value);
                write.nodeSetProperty(node, tx.tokenWrite().propertyKeyGetOrCreateForName(DEFAULT_PROPERTY_NAME), val);
                return Pair.of(node, val);
            }

            @Override
            void createIndex(GraphDatabaseService graphDb) {
                try (Transaction tx = graphDb.beginTx()) {
                    tx.schema()
                            .indexFor(Label.label(DEFAULT_LABEL))
                            .on(DEFAULT_PROPERTY_NAME)
                            .withIndexType(IndexType.POINT)
                            .withName(INDEX_NAME)
                            .create();
                    tx.commit();
                }

                try (Transaction tx = graphDb.beginTx()) {
                    tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
                }
            }

            @Override
            void deleteEntity(KernelTransaction tx, long entity) throws Exception {
                tx.dataWrite().nodeDelete(entity);
            }

            @Override
            void removeProperty(KernelTransaction tx, long entity) throws Exception {
                int propertyKey = tx.tokenRead().propertyKey(DEFAULT_PROPERTY_NAME);
                tx.dataWrite().nodeRemoveProperty(entity, propertyKey);
            }

            @Override
            void setProperty(KernelTransaction tx, long entity, Object value) throws Exception {
                int propertyKey = tx.tokenRead().propertyKey(DEFAULT_PROPERTY_NAME);
                tx.dataWrite().nodeSetProperty(entity, propertyKey, Values.of(value));
            }

            @Override
            void assertEntityAndValueForSeek(
                    Set<Pair<Long, Value>> expected,
                    KernelTransaction tx,
                    IndexDescriptor index,
                    Object anotherValueFoundByQuery,
                    PropertyIndexQuery query)
                    throws Exception {
                try (NodeValueIndexCursor nodes =
                        tx.cursors().allocateNodeValueIndexCursor(tx.cursorContext(), tx.memoryTracker())) {
                    IndexReadSession indexSession = tx.dataRead().indexReadSession(index);
                    tx.dataRead().nodeIndexSeek(tx.queryContext(), indexSession, nodes, unordered(true), query);
                    assertEntityAndValue(this, expected, tx, anotherValueFoundByQuery, new NodeCursorAdapter(nodes));
                }
            }

            @Override
            void assertEntityAndValueForScan(
                    Set<Pair<Long, Value>> expected,
                    KernelTransaction tx,
                    IndexDescriptor index,
                    Object anotherValueFoundByQuery)
                    throws Exception {
                IndexReadSession indexSession = tx.dataRead().indexReadSession(index);
                try (NodeValueIndexCursor nodes =
                        tx.cursors().allocateNodeValueIndexCursor(tx.cursorContext(), tx.memoryTracker())) {
                    tx.dataRead().nodeIndexScan(indexSession, nodes, unordered(true));
                    assertEntityAndValue(this, expected, tx, anotherValueFoundByQuery, new NodeCursorAdapter(nodes));
                }
            }
        },

        RELATIONSHIP {
            private static final String DEFAULT_REl_TYPE = "Rel";

            @Override
            Pair<Long, Value> entityWithProp(KernelTransaction tx, Object value) throws Exception {
                Write write = tx.dataWrite();
                long sourceNode = write.nodeCreate();
                long targetNode = write.nodeCreate();

                long rel = write.relationshipCreate(
                        sourceNode, tx.tokenWrite().relationshipTypeGetOrCreateForName(DEFAULT_REl_TYPE), targetNode);

                Value val = Values.of(value);
                write.relationshipSetProperty(
                        rel, tx.tokenWrite().propertyKeyGetOrCreateForName(DEFAULT_PROPERTY_NAME), val);
                return Pair.of(rel, val);
            }

            @Override
            void createIndex(GraphDatabaseService graphDb) {
                try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
                    tx.schema()
                            .indexFor(RelationshipType.withName(DEFAULT_REl_TYPE))
                            .on(DEFAULT_PROPERTY_NAME)
                            .withIndexType(IndexType.POINT)
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
            void removeProperty(KernelTransaction tx, long entity) throws Exception {
                int propertyKey = tx.tokenRead().propertyKey(DEFAULT_PROPERTY_NAME);
                tx.dataWrite().relationshipRemoveProperty(entity, propertyKey);
            }

            @Override
            void setProperty(KernelTransaction tx, long entity, Object value) throws Exception {
                int propertyKey = tx.tokenRead().propertyKey(DEFAULT_PROPERTY_NAME);
                tx.dataWrite().relationshipSetProperty(entity, propertyKey, Values.of(value));
            }

            @Override
            void assertEntityAndValueForSeek(
                    Set<Pair<Long, Value>> expected,
                    KernelTransaction tx,
                    IndexDescriptor index,
                    Object anotherValueFoundByQuery,
                    PropertyIndexQuery query)
                    throws Exception {
                try (RelationshipValueIndexCursor relationships =
                        tx.cursors().allocateRelationshipValueIndexCursor(tx.cursorContext(), tx.memoryTracker())) {
                    IndexReadSession indexSession = tx.dataRead().indexReadSession(index);
                    tx.dataRead()
                            .relationshipIndexSeek(
                                    tx.queryContext(), indexSession, relationships, unordered(true), query);
                    assertEntityAndValue(
                            this, expected, tx, anotherValueFoundByQuery, new RelationshipCursorAdapter(relationships));
                }
            }

            @Override
            void assertEntityAndValueForScan(
                    Set<Pair<Long, Value>> expected,
                    KernelTransaction tx,
                    IndexDescriptor index,
                    Object anotherValueFoundByQuery)
                    throws Exception {
                IndexReadSession indexSession = tx.dataRead().indexReadSession(index);
                try (RelationshipValueIndexCursor relationships =
                        tx.cursors().allocateRelationshipValueIndexCursor(tx.cursorContext(), tx.memoryTracker())) {
                    tx.dataRead().relationshipIndexScan(indexSession, relationships, unordered(true));
                    assertEntityAndValue(
                            this, expected, tx, anotherValueFoundByQuery, new RelationshipCursorAdapter(relationships));
                }
            }
        };

        abstract Pair<Long, Value> entityWithProp(KernelTransaction tx, Object value) throws Exception;

        abstract void createIndex(GraphDatabaseService graphDb);

        abstract void deleteEntity(KernelTransaction tx, long entity) throws Exception;

        abstract void removeProperty(KernelTransaction tx, long entity) throws Exception;

        abstract void setProperty(KernelTransaction tx, long entity, Object value) throws Exception;

        /**
         * Perform an index seek and assert that the correct entities and values were found.
         * <p>
         * Since this method modifies TX state for the test it is not safe to call this method more than once in the same transaction.
         *
         * @param expected the expected entities and values
         * @param tx the transaction
         * @param index the index
         * @param anotherValueFoundByQuery a value that would be found by the index query, if an entity with that value existed. This method
         * will create an entity with that value after initializing the cursor and assert that the new entity is not found.
         * @param query the index query
         */
        abstract void assertEntityAndValueForSeek(
                Set<org.neo4j.internal.helpers.collection.Pair<Long, Value>> expected,
                KernelTransaction tx,
                IndexDescriptor index,
                Object anotherValueFoundByQuery,
                PropertyIndexQuery query)
                throws Exception;

        /**
         * Perform an index scan and assert that the correct entities and values were found.
         * <p>
         * Since this method modifies TX state for the test it is not safe to call this method more than once in the same transaction.
         *
         * @param expected the expected entities and values
         * @param tx the transaction
         * @param index the index
         * @param anotherValueFoundByQuery a value that would be found, if an entity with that value existed. This method
         * will create an entity with that value after initializing the cursor and assert that the new entity is not found.
         */
        abstract void assertEntityAndValueForScan(
                Set<Pair<Long, Value>> expected,
                KernelTransaction tx,
                IndexDescriptor index,
                Object anotherValueFoundByQuery)
                throws Exception;
    }

    interface EntityValueIndexCursor {
        boolean next();

        Value propertyValue(int offset);

        long entityReference();
    }

    private static class NodeCursorAdapter implements EntityValueIndexCursor {
        private final NodeValueIndexCursor nodes;

        NodeCursorAdapter(NodeValueIndexCursor nodes) {
            this.nodes = nodes;
        }

        @Override
        public boolean next() {
            return nodes.next();
        }

        @Override
        public Value propertyValue(int offset) {
            return nodes.propertyValue(offset);
        }

        @Override
        public long entityReference() {
            return nodes.nodeReference();
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
