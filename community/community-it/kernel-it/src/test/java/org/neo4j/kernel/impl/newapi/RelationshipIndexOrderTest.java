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

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Nested;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class RelationshipIndexOrderTest {
    abstract static class RelationshipIndexOrderTestBase extends IndexOrderTestBase<RelationshipValueIndexCursor> {
        private static final String DEFAULT_REl_TYPE = "Rel";

        @Override
        protected Pair<Long, Value> entityWithProp(KernelTransaction tx, Object value) throws Exception {
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
        protected void createIndex() {
            try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
                tx.schema()
                        .indexFor(RelationshipType.withName(DEFAULT_REl_TYPE))
                        .on(DEFAULT_PROPERTY_NAME)
                        .withName(INDEX_NAME)
                        .withIndexType(indexType())
                        .create();
                tx.commit();
            }

            try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
                tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            }
        }

        @Override
        protected long entityReference(RelationshipValueIndexCursor cursor) {
            return cursor.relationshipReference();
        }

        @Override
        protected RelationshipValueIndexCursor getEntityValueIndexCursor(KernelTransaction tx) {
            return tx.cursors().allocateRelationshipValueIndexCursor(tx.cursorContext(), tx.memoryTracker());
        }

        @Override
        protected void entityIndexSeek(
                KernelTransaction tx,
                IndexReadSession index,
                RelationshipValueIndexCursor cursor,
                IndexQueryConstraints constraints,
                PropertyIndexQuery query)
                throws KernelException {
            tx.dataRead().relationshipIndexSeek(tx.queryContext(), index, cursor, constraints, query);
        }

        @Override
        protected void entityIndexScan(
                KernelTransaction tx,
                IndexReadSession index,
                RelationshipValueIndexCursor cursor,
                IndexQueryConstraints constraints)
                throws KernelException {
            tx.dataRead().relationshipIndexScan(index, cursor, constraints);
        }

        @Override
        protected void createCompositeIndex() {
            try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
                tx.schema()
                        .indexFor(RelationshipType.withName(DEFAULT_REl_TYPE))
                        .on(COMPOSITE_PROPERTY_1)
                        .on(COMPOSITE_PROPERTY_2)
                        .withName(INDEX_NAME)
                        .withIndexType(indexType())
                        .create();
                tx.commit();
            }

            try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
                tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            }
        }

        @Override
        protected Pair<Long, Value[]> entityWithTwoProps(KernelTransaction tx, Object value1, Object value2)
                throws Exception {
            Write write = tx.dataWrite();
            long sourceNode = write.nodeCreate();
            long targetNode = write.nodeCreate();

            long rel = write.relationshipCreate(
                    sourceNode, tx.tokenWrite().relationshipTypeGetOrCreateForName(DEFAULT_REl_TYPE), targetNode);

            Value val1 = Values.of(value1);
            Value val2 = Values.of(value2);
            write.relationshipSetProperty(
                    rel, tx.tokenWrite().propertyKeyGetOrCreateForName(COMPOSITE_PROPERTY_1), val1);
            write.relationshipSetProperty(
                    rel, tx.tokenWrite().propertyKeyGetOrCreateForName(COMPOSITE_PROPERTY_2), val2);
            return Pair.of(rel, new Value[] {val1, val2});
        }

        abstract IndexType indexType();
    }

    @Nested
    class RangeIndexTest extends RelationshipIndexOrderTestBase {

        @Override
        IndexType indexType() {
            return IndexType.RANGE;
        }
    }
}
