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
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class NodeIndexOrderTest {
    abstract static class NodeIndexOrderTestBase extends IndexOrderTestBase<NodeValueIndexCursor> {
        public static final String DEFAULT_LABEL = "Node";

        @Override
        protected Pair<Long, Value> entityWithProp(KernelTransaction tx, Object value) throws Exception {
            Write write = tx.dataWrite();
            long node = write.nodeCreate();
            write.nodeAddLabel(node, tx.tokenWrite().labelGetOrCreateForName(DEFAULT_LABEL));
            Value val = Values.of(value);
            write.nodeSetProperty(node, tx.tokenWrite().propertyKeyGetOrCreateForName(DEFAULT_PROPERTY_NAME), val);
            return Pair.of(node, val);
        }

        @Override
        protected void createIndex() {
            try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
                tx.schema()
                        .indexFor(Label.label(DEFAULT_LABEL))
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
        protected long entityReference(NodeValueIndexCursor cursor) {
            return cursor.nodeReference();
        }

        @Override
        protected NodeValueIndexCursor getEntityValueIndexCursor(KernelTransaction tx) {
            return tx.cursors().allocateNodeValueIndexCursor(tx.cursorContext(), tx.memoryTracker());
        }

        @Override
        protected void entityIndexSeek(
                KernelTransaction tx,
                IndexReadSession index,
                NodeValueIndexCursor cursor,
                IndexQueryConstraints constraints,
                PropertyIndexQuery query)
                throws KernelException {
            tx.dataRead().nodeIndexSeek(tx.queryContext(), index, cursor, constraints, query);
        }

        @Override
        protected void entityIndexScan(
                KernelTransaction tx,
                IndexReadSession index,
                NodeValueIndexCursor cursor,
                IndexQueryConstraints constraints)
                throws KernelException {
            tx.dataRead().nodeIndexScan(index, cursor, constraints);
        }

        @Override
        protected void createCompositeIndex() {
            try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
                tx.schema()
                        .indexFor(Label.label(DEFAULT_LABEL))
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
            long node = write.nodeCreate();
            TokenWrite tokenWrite = tx.tokenWrite();
            write.nodeAddLabel(node, tokenWrite.labelGetOrCreateForName(DEFAULT_LABEL));
            Value val1 = Values.of(value1);
            Value val2 = Values.of(value2);
            write.nodeSetProperty(node, tokenWrite.propertyKeyGetOrCreateForName(COMPOSITE_PROPERTY_1), val1);
            write.nodeSetProperty(node, tokenWrite.propertyKeyGetOrCreateForName(COMPOSITE_PROPERTY_2), val2);
            return Pair.of(node, new Value[] {val1, val2});
        }

        abstract IndexType indexType();
    }

    @Nested
    class RangeIndexTest extends NodeIndexOrderTestBase {

        @Override
        IndexType indexType() {
            return IndexType.RANGE;
        }
    }
}
