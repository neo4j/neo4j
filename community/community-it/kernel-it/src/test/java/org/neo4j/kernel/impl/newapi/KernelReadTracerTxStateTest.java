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

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.constrained;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.kernel.api.InternalIndexState.ONLINE;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.newapi.TestKernelReadTracer.ON_ALL_NODES_SCAN;
import static org.neo4j.kernel.impl.newapi.TestKernelReadTracer.indexSeekEvent;
import static org.neo4j.kernel.impl.newapi.TestKernelReadTracer.labelScanEvent;
import static org.neo4j.kernel.impl.newapi.TestKernelReadTracer.nodeEvent;
import static org.neo4j.kernel.impl.newapi.TestKernelReadTracer.propertyEvent;
import static org.neo4j.kernel.impl.newapi.TestKernelReadTracer.relationshipEvent;
import static org.neo4j.kernel.impl.newapi.TestKernelReadTracer.relationshipTypeScanEvent;
import static org.neo4j.storageengine.api.RelationshipSelection.ALL_RELATIONSHIPS;

import java.util.Iterator;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.Label;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.kernel.api.TokenReadSession;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.values.storable.Values;

class KernelReadTracerTxStateTest extends KernelAPIWriteTestBase<WriteTestSupport> {
    @Test
    void shouldTraceAllNodesScan() throws Exception {
        // given
        TestKernelReadTracer tracer = new TestKernelReadTracer();

        try (KernelTransaction tx = beginTransaction();
                NodeCursor cursor = tx.cursors().allocateNodeCursor(tx.cursorContext())) {
            tx.dataWrite().nodeCreate();
            tx.dataWrite().nodeCreate();

            // when
            cursor.setTracer(tracer);
            tx.dataRead().allNodesScan(cursor);
            tracer.assertEvents(ON_ALL_NODES_SCAN);

            assertTrue(cursor.next());
            tracer.assertEvents(nodeEvent(cursor.nodeReference()));

            assertTrue(cursor.next());
            tracer.assertEvents(nodeEvent(cursor.nodeReference()));

            assertFalse(cursor.next());
            tracer.assertEvents();
        }
    }

    @Test
    void shouldTraceLabelScan() throws KernelException {
        // given
        TestKernelReadTracer tracer = new TestKernelReadTracer();

        try (KernelTransaction tx = beginTransaction();
                NodeLabelIndexCursor cursor = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT)) {
            int barId = tx.tokenWrite().labelGetOrCreateForName("Bar");
            long n = tx.dataWrite().nodeCreate();
            tx.dataWrite().nodeAddLabel(n, barId);

            // when
            cursor.setTracer(tracer);
            tx.dataRead()
                    .nodeLabelScan(
                            getTokenReadSession(tx, EntityType.NODE),
                            cursor,
                            IndexQueryConstraints.unconstrained(),
                            new TokenPredicate(barId),
                            tx.cursorContext());
            tracer.assertEvents(labelScanEvent(barId));

            assertTrue(cursor.next());
            tracer.assertEvents(nodeEvent(cursor.nodeReference()));

            assertFalse(cursor.next());
            tracer.assertEvents();
        }
    }

    @Test
    void shouldTraceIndexSeek() throws KernelException {
        // given
        TestKernelReadTracer tracer = new TestKernelReadTracer();

        String indexName = createIndex("User", "name");

        try (KernelTransaction tx = beginTransaction();
                NodeValueIndexCursor cursor =
                        tx.cursors().allocateNodeValueIndexCursor(NULL_CONTEXT, tx.memoryTracker())) {
            int name = tx.token().propertyKey("name");
            int user = tx.token().nodeLabel("User");
            long n = tx.dataWrite().nodeCreate();
            tx.dataWrite().nodeAddLabel(n, user);
            tx.dataWrite().nodeSetProperty(n, name, Values.stringValue("Bosse"));
            IndexDescriptor index = tx.schemaRead().indexGetForName(indexName);
            IndexReadSession session = tx.dataRead().indexReadSession(index);

            // when
            assertIndexSeekTracing(tracer, tx, cursor, session, IndexOrder.NONE, false, user);
            assertIndexSeekTracing(tracer, tx, cursor, session, IndexOrder.NONE, true, user);
            assertIndexSeekTracing(tracer, tx, cursor, session, IndexOrder.ASCENDING, false, user);
            assertIndexSeekTracing(tracer, tx, cursor, session, IndexOrder.ASCENDING, true, user);
        }
    }

    private static void assertIndexSeekTracing(
            TestKernelReadTracer tracer,
            KernelTransaction tx,
            NodeValueIndexCursor cursor,
            IndexReadSession session,
            IndexOrder order,
            boolean needsValues,
            int user)
            throws KernelException {
        cursor.setTracer(tracer);

        tx.dataRead()
                .nodeIndexSeek(
                        tx.queryContext(),
                        session,
                        cursor,
                        constrained(order, needsValues),
                        PropertyIndexQuery.stringPrefix(user, Values.stringValue("B")));
        tracer.assertEvents(indexSeekEvent());

        assertTrue(cursor.next());
        tracer.assertEvents(nodeEvent(cursor.nodeReference()));

        assertFalse(cursor.next());
        tracer.assertEvents();
    }

    @Test
    void shouldTraceSingleRelationship() throws Exception {
        // given
        TestKernelReadTracer tracer = new TestKernelReadTracer();

        try (KernelTransaction tx = beginTransaction();
                RelationshipScanCursor cursor = tx.cursors().allocateRelationshipScanCursor(tx.cursorContext())) {
            long n1 = tx.dataWrite().nodeCreate();
            long n2 = tx.dataWrite().nodeCreate();
            long r = tx.dataWrite().relationshipCreate(n1, tx.token().relationshipTypeGetOrCreateForName("R"), n2);

            // when
            cursor.setTracer(tracer);
            tx.dataRead().singleRelationship(r, cursor);

            assertTrue(cursor.next());
            tracer.assertEvents(relationshipEvent(r));

            long deleted =
                    tx.dataWrite().relationshipCreate(n1, tx.token().relationshipTypeGetOrCreateForName("R"), n2);
            tx.dataWrite().relationshipDelete(deleted);

            tx.dataRead().singleRelationship(deleted, cursor);
            assertFalse(cursor.next());
            tracer.assertEvents();
        }
    }

    @Test
    void shouldTraceRelationshipTraversal() throws Exception {
        // given
        TestKernelReadTracer tracer = new TestKernelReadTracer();

        try (KernelTransaction tx = beginTransaction();
                NodeCursor nodeCursor = tx.cursors().allocateNodeCursor(tx.cursorContext());
                RelationshipTraversalCursor cursor =
                        tx.cursors().allocateRelationshipTraversalCursor(tx.cursorContext())) {
            long n1 = tx.dataWrite().nodeCreate();
            long n2 = tx.dataWrite().nodeCreate();
            long r = tx.dataWrite().relationshipCreate(n1, tx.token().relationshipTypeGetOrCreateForName("R"), n2);

            // when
            cursor.setTracer(tracer);
            tx.dataRead().singleNode(n1, nodeCursor);
            assertTrue(nodeCursor.next());
            nodeCursor.relationships(cursor, ALL_RELATIONSHIPS);

            assertTrue(cursor.next());
            tracer.assertEvents(relationshipEvent(r));

            assertFalse(cursor.next());
            tracer.assertEvents();
        }
    }

    @Test
    void shouldTracePropertyAccess() throws Exception {
        // given
        TestKernelReadTracer tracer = new TestKernelReadTracer();

        try (KernelTransaction tx = beginTransaction();
                NodeCursor nodeCursor = tx.cursors().allocateNodeCursor(tx.cursorContext());
                PropertyCursor propertyCursor =
                        tx.cursors().allocatePropertyCursor(tx.cursorContext(), tx.memoryTracker())) {
            long n = tx.dataWrite().nodeCreate();
            int name = tx.token().propertyKey("name");
            tx.dataWrite().nodeSetProperty(n, name, Values.stringValue("Bosse"));

            // when
            propertyCursor.setTracer(tracer);

            tx.dataRead().singleNode(n, nodeCursor);
            assertTrue(nodeCursor.next());
            nodeCursor.properties(propertyCursor);

            assertTrue(propertyCursor.next());
            tracer.assertEvents(propertyEvent(name));

            assertFalse(propertyCursor.next());
            tracer.assertEvents();
        }
    }

    @Test
    void shouldTraceRelationshipIndexCursor() throws KernelException, TimeoutException {
        // given
        int connection;
        int name;
        String indexName = "myIndex";
        IndexDescriptor index;

        try (KernelTransaction tx = beginTransaction()) {
            connection = tx.tokenWrite().relationshipTypeGetOrCreateForName("Connection");
            name = tx.tokenWrite().propertyKeyGetOrCreateForName("name");
            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            SchemaDescriptor schema =
                    SchemaDescriptors.fulltext(EntityType.RELATIONSHIP, array(connection), array(name));
            IndexPrototype prototype = IndexPrototype.forSchema(schema, AllIndexProviderDescriptors.FULLTEXT_DESCRIPTOR)
                    .withName(indexName)
                    .withIndexType(IndexType.FULLTEXT);
            index = tx.schemaWrite().indexCreate(prototype);
            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            Predicates.awaitEx(() -> tx.schemaRead().indexGetState(index) == ONLINE, 1, MINUTES);
            long n1 = tx.dataWrite().nodeCreate();
            long n2 = tx.dataWrite().nodeCreate();
            long r = tx.dataWrite().relationshipCreate(n1, connection, n2);
            tx.dataWrite().relationshipSetProperty(r, name, Values.stringValue("transformational"));
            tx.commit();
        }

        // when
        TestKernelReadTracer tracer = new TestKernelReadTracer();

        try (KernelTransaction tx = beginTransaction();
                RelationshipValueIndexCursor cursor =
                        tx.cursors().allocateRelationshipValueIndexCursor(NULL_CONTEXT, tx.memoryTracker())) {
            cursor.setTracer(tracer);
            IndexReadSession indexReadSession = tx.dataRead().indexReadSession(index);
            tx.dataRead()
                    .relationshipIndexSeek(
                            tx.queryContext(),
                            indexReadSession,
                            cursor,
                            unconstrained(),
                            PropertyIndexQuery.fulltextSearch("transformational"));

            assertTrue(cursor.next());
            tracer.assertEvents(indexSeekEvent(), relationshipEvent(cursor.relationshipReference()));

            assertFalse(cursor.next());
            tracer.assertEvents();
        }
    }

    @Test
    void shouldTraceRelationshipTypeScan() throws KernelException {
        // given
        TestKernelReadTracer tracer = new TestKernelReadTracer();

        try (KernelTransaction tx = beginTransaction();
                RelationshipTypeIndexCursor cursor = tx.cursors().allocateRelationshipTypeIndexCursor(NULL_CONTEXT)) {
            int rType = tx.token().relationshipTypeGetOrCreateForName("R");

            long n1 = tx.dataWrite().nodeCreate();
            long n2 = tx.dataWrite().nodeCreate();
            tx.dataWrite().relationshipCreate(n1, rType, n2);

            // when
            cursor.setTracer(tracer);
            tx.dataRead()
                    .relationshipTypeScan(
                            getTokenReadSession(tx, EntityType.RELATIONSHIP),
                            cursor,
                            IndexQueryConstraints.unconstrained(),
                            new TokenPredicate(rType),
                            tx.cursorContext());
            tracer.assertEvents(relationshipTypeScanEvent(rType));

            assertTrue(cursor.next());
            tracer.assertEvents(relationshipEvent(cursor.relationshipReference()));

            assertFalse(cursor.next());
            tracer.assertEvents();
        }
    }

    private static TokenReadSession getTokenReadSession(KernelTransaction tx, EntityType entityType)
            throws IndexNotFoundKernelException {
        Iterator<IndexDescriptor> indexes = tx.schemaRead().index(SchemaDescriptors.forAnyEntityTokens(entityType));
        IndexDescriptor index = indexes.next();
        assertFalse(indexes.hasNext());
        return tx.dataRead().tokenReadSession(index);
    }

    private static int[] array(int... elements) {
        return elements;
    }

    @SuppressWarnings("SameParameterValue")
    private String createIndex(String label, String propertyKey) {
        String indexName;
        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            indexName = tx.schema()
                    .indexFor(Label.label(label))
                    .on(propertyKey)
                    .create()
                    .getName();
            tx.commit();
        }

        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            tx.schema().awaitIndexesOnline(2, MINUTES);
        }

        return indexName;
    }

    @Override
    public WriteTestSupport newTestSupport() {
        return new WriteTestSupport();
    }
}
