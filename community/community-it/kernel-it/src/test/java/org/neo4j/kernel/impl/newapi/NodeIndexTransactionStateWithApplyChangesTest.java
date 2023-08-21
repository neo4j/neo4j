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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class NodeIndexTransactionStateWithApplyChangesTest extends IndexTransactionStateWithApplyChangesTestBase {
    private static final String DEFAULT_LABEL = "label";

    @ParameterizedTest
    @MethodSource("parameters")
    void applyChangesShouldAddLabelsInIndexTxState(IndexType indexType, boolean needsValues) throws Exception {
        createIndex(indexType);

        Set<EntityWithProps> expected = new HashSet<>();
        EntityWithProps entityWithProps;
        long node;
        try (var tx = beginTransaction()) {
            Write write = tx.dataWrite();
            node = write.nodeCreate();
            entityWithProps = setPropsOnEntity(tx, node, "books", "looks");
            tx.commit();
        }
        expected.add(entityWithProps);

        try (var tx = beginTransaction()) {
            tx.dataWrite()
                    .nodeAddLabel(entityWithProps.entityId(), tx.tokenWrite().labelGetOrCreateForName(DEFAULT_LABEL));

            IndexDescriptor index = tx.schemaRead().indexGetForName(INDEX_NAME);
            assertEntityAndValueForScan(expected, tx, index, needsValues, "something", "else");
        }
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void applyChangesShouldRemoveLabelsInIndexTxState(IndexType indexType, boolean needsValues) throws Exception {
        createIndex(indexType);

        EntityWithProps entityWithProps;
        try (var tx = beginTransaction()) {
            entityWithProps = entityWithProps(tx, "books", "looks");
            tx.commit();
        }

        Set<EntityWithProps> expected = new HashSet<>();
        try (var tx = beginTransaction()) {
            tx.dataWrite()
                    .nodeRemoveLabel(
                            entityWithProps.entityId(), tx.tokenWrite().labelGetOrCreateForName(DEFAULT_LABEL));

            IndexDescriptor index = tx.schemaRead().indexGetForName(INDEX_NAME);
            assertEntityAndValueForScan(expected, tx, index, needsValues, "something", "else");
        }
    }

    @Override
    EntityWithProps entityWithProps(KernelTransaction tx, Object val, Object val2) throws Exception {
        Write write = tx.dataWrite();
        long node = write.nodeCreate();
        int nodeLabel = tx.tokenWrite().labelGetOrCreateForName(DEFAULT_LABEL);
        write.nodeAddLabel(node, nodeLabel);
        return setPropsOnEntity(tx, node, val, val2);
    }

    @Override
    void createIndex(IndexType indexType) {
        try (Transaction tx = graphDb.beginTx()) {
            tx.schema()
                    .indexFor(Label.label(DEFAULT_LABEL))
                    .on(PROP1)
                    .on(PROP2)
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
    void removeProperties(KernelTransaction tx, long entity) throws Exception {
        int prop1Key = tx.tokenWrite().propertyKeyGetOrCreateForName(PROP1);
        int prop2Key = tx.tokenWrite().propertyKeyGetOrCreateForName(PROP2);
        MutableIntObjectMap<Value> propertiesToSet = IntObjectMaps.mutable.empty();
        propertiesToSet.put(prop1Key, Values.NO_VALUE);
        propertiesToSet.put(prop2Key, Values.NO_VALUE);
        tx.dataWrite().nodeApplyChanges(entity, IntSets.immutable.empty(), IntSets.immutable.empty(), propertiesToSet);
    }

    @Override
    EntityWithProps setProperties(KernelTransaction tx, long entity, Object val, Object val2) throws Exception {
        return setPropsOnEntity(tx, entity, val, val2);
    }

    private EntityWithProps setPropsOnEntity(KernelTransaction tx, long entity, Object val, Object val2)
            throws KernelException {
        int prop1Key = tx.tokenWrite().propertyKeyGetOrCreateForName(PROP1);
        int prop2Key = tx.tokenWrite().propertyKeyGetOrCreateForName(PROP2);
        MutableIntObjectMap<Value> propertiesToSet = IntObjectMaps.mutable.empty();
        Value value1 = Values.of(val);
        propertiesToSet.put(prop1Key, value1);
        Value value2 = Values.of(val2);
        propertiesToSet.put(prop2Key, value2);
        tx.dataWrite().nodeApplyChanges(entity, IntSets.immutable.empty(), IntSets.immutable.empty(), propertiesToSet);
        return new EntityWithProps(entity, value1, value2);
    }

    @Override
    void assertEntityAndValueForScan(
            Set<EntityWithProps> expected,
            KernelTransaction tx,
            IndexDescriptor index,
            boolean needsValues,
            Object anotherValueFoundByQuery,
            Object anotherValueFoundByQuery2)
            throws Exception {
        IndexReadSession indexSession = tx.dataRead().indexReadSession(index);
        try (NodeValueIndexCursor nodes =
                tx.cursors().allocateNodeValueIndexCursor(tx.cursorContext(), tx.memoryTracker())) {
            tx.dataRead().nodeIndexScan(indexSession, nodes, unordered(needsValues));
            assertEntityAndValue(
                    expected,
                    tx,
                    needsValues,
                    anotherValueFoundByQuery,
                    anotherValueFoundByQuery2,
                    new NodeCursorAdapter(nodes));
        }
    }

    static class NodeCursorAdapter implements EntityValueIndexCursor {
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
