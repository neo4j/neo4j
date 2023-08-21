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
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class NodeIndexTransactionStateTest extends IndexTransactionStateTestBase {
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
    void createIndex(IndexType indexType) {
        try (Transaction tx = graphDb.beginTx()) {
            tx.schema()
                    .indexFor(Label.label(DEFAULT_LABEL))
                    .on(DEFAULT_PROPERTY_NAME)
                    .withIndexType(indexType)
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
    boolean entityExists(KernelTransaction tx, long entity) {
        return tx.dataRead().nodeExists(entity);
    }

    @Override
    void removeProperty(KernelTransaction tx, long entity) throws Exception {
        int propertyKey = tx.tokenRead().propertyKey(DEFAULT_PROPERTY_NAME);
        tx.dataWrite().nodeRemoveProperty(entity, propertyKey);
    }

    @Override
    void setProperty(KernelTransaction tx, long entity, Value value) throws Exception {
        int propertyKey = tx.tokenRead().propertyKey(DEFAULT_PROPERTY_NAME);
        tx.dataWrite().nodeSetProperty(entity, propertyKey, value);
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
        try (NodeValueIndexCursor nodes =
                tx.cursors().allocateNodeValueIndexCursor(tx.cursorContext(), tx.memoryTracker())) {
            IndexReadSession indexSession = tx.dataRead().indexReadSession(index);
            tx.dataRead().nodeIndexSeek(tx.queryContext(), indexSession, nodes, unordered(needsValues), queries);
            assertEntityAndValue(expected, tx, needsValues, anotherValueFoundByQuery, new NodeCursorAdapter(nodes));
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
        try (NodeValueIndexCursor nodes =
                tx.cursors().allocateNodeValueIndexCursor(tx.cursorContext(), tx.memoryTracker())) {
            tx.dataRead().nodeIndexScan(indexSession, nodes, unordered(needsValues));
            assertEntityAndValue(expected, tx, needsValues, anotherValueFoundByQuery, new NodeCursorAdapter(nodes));
        }
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
}
