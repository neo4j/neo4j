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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.constrained;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.newapi.TestKernelReadTracer.ON_ALL_NODES_SCAN;
import static org.neo4j.kernel.impl.newapi.TestKernelReadTracer.hasLabelEvent;
import static org.neo4j.kernel.impl.newapi.TestKernelReadTracer.indexSeekEvent;
import static org.neo4j.kernel.impl.newapi.TestKernelReadTracer.labelScanEvent;
import static org.neo4j.kernel.impl.newapi.TestKernelReadTracer.nodeEvent;
import static org.neo4j.kernel.impl.newapi.TestKernelReadTracer.propertyEvent;
import static org.neo4j.kernel.impl.newapi.TestKernelReadTracer.relationshipEvent;
import static org.neo4j.kernel.impl.newapi.TestKernelReadTracer.relationshipTypeScanEvent;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.RelationshipSelection.ALL_RELATIONSHIPS;
import static org.neo4j.storageengine.api.RelationshipSelection.selection;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.RelationshipTypeIndexCursor;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.kernel.api.TokenReadSession;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.schema.IndexDefinitionImpl;
import org.neo4j.kernel.impl.newapi.TestKernelReadTracer.TraceEvent;
import org.neo4j.memory.EmptyMemoryTracker;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class KernelReadTracerTest extends KernelAPIReadTestBase<ReadTestSupport> {
    private long foo;
    private long bar;
    private long bare;

    private long has;
    private long is;

    private IndexDescriptor index;
    private IndexDescriptor relIndex;

    @Override
    public void createTestGraph(GraphDatabaseService graphDb) {
        Node deleted;
        try (Transaction tx = graphDb.beginTx()) {
            Node foo = tx.createNode(label("Foo"));
            Node bar = tx.createNode(label("Bar"));
            tx.createNode(label("Baz"));
            tx.createNode(label("Bar"), label("Baz"));
            deleted = tx.createNode();
            Node bare = tx.createNode();

            Relationship has = foo.createRelationshipTo(bar, RelationshipType.withName("HAS"));
            foo.createRelationshipTo(bar, RelationshipType.withName("HAS"));
            foo.createRelationshipTo(bar, RelationshipType.withName("IS"));
            foo.createRelationshipTo(bar, RelationshipType.withName("HAS"));
            foo.createRelationshipTo(bar, RelationshipType.withName("HAS"));

            is = bar.createRelationshipTo(bare, RelationshipType.withName("IS")).getId();

            this.foo = foo.getId();
            this.has = has.getId();
            this.bar = bar.getId();
            this.bare = bare.getId();

            foo.setProperty("p1", 1);
            has.setProperty("p1", 1);
            foo.setProperty("p2", 2);
            foo.setProperty("p3", 3);
            foo.setProperty("p4", 4);

            tx.commit();
        }

        try (Transaction tx = graphDb.beginTx()) {
            index = ((IndexDefinitionImpl)
                            tx.schema().indexFor(label("Foo")).on("p1").create())
                    .getIndexReference();
            tx.commit();
        }

        try (Transaction tx = graphDb.beginTx()) {
            relIndex = ((IndexDefinitionImpl) tx.schema()
                            .indexFor(RelationshipType.withName("HAS"))
                            .on("p1")
                            .create())
                    .getIndexReference();
            tx.commit();
        }

        try (Transaction tx = graphDb.beginTx()) {
            tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES);
            tx.commit();
        }

        try (Transaction tx = graphDb.beginTx()) {
            tx.getNodeById(deleted.getId()).delete();
            tx.commit();
        }
    }

    @Test
    void shouldTraceAllNodesScan() {
        // given
        TestKernelReadTracer tracer = new TestKernelReadTracer();

        List<TraceEvent> expectedEvents = new ArrayList<>();
        expectedEvents.add(ON_ALL_NODES_SCAN);

        try (NodeCursor nodes = cursors.allocateNodeCursor(NULL_CONTEXT)) {
            // when
            nodes.setTracer(tracer);
            read.allNodesScan(nodes);
            while (nodes.next()) {
                expectedEvents.add(nodeEvent(nodes.nodeReference()));
            }
        }

        // then
        tracer.assertEvents(expectedEvents);
    }

    @Test
    void shouldTraceSingleNode() {
        // given
        TestKernelReadTracer tracer = new TestKernelReadTracer();

        try (NodeCursor cursor = cursors.allocateNodeCursor(NULL_CONTEXT)) {
            // when
            cursor.setTracer(tracer);
            read.singleNode(foo, cursor);
            assertTrue(cursor.next());
            tracer.assertEvents(nodeEvent(foo));

            read.singleNode(bar, cursor);
            assertTrue(cursor.next());
            tracer.assertEvents(nodeEvent(bar));

            read.singleNode(bare, cursor);
            assertTrue(cursor.next());
            tracer.assertEvents(nodeEvent(bare));
        }
    }

    @Test
    void shouldStopAndRestartTracing() {
        // given
        TestKernelReadTracer tracer = new TestKernelReadTracer();

        try (NodeCursor cursor = cursors.allocateNodeCursor(NULL_CONTEXT)) {
            // when
            cursor.setTracer(tracer);
            read.singleNode(foo, cursor);
            assertTrue(cursor.next());
            tracer.assertEvents(nodeEvent(foo));

            cursor.removeTracer();
            read.singleNode(bar, cursor);
            assertTrue(cursor.next());
            tracer.assertEvents();

            cursor.setTracer(tracer);
            read.singleNode(bare, cursor);
            assertTrue(cursor.next());
            tracer.assertEvents(nodeEvent(bare));
        }
    }

    @Test
    void shouldTraceLabelScan() throws KernelException {
        // given
        TestKernelReadTracer tracer = new TestKernelReadTracer();
        int barId = token.labelGetOrCreateForName("Bar");

        List<TraceEvent> expectedEvents = new ArrayList<>();
        expectedEvents.add(labelScanEvent(barId));

        try (NodeLabelIndexCursor cursor = cursors.allocateNodeLabelIndexCursor(NULL_CONTEXT)) {
            // when
            cursor.setTracer(tracer);
            read.nodeLabelScan(
                    getTokenReadSession(tx, EntityType.NODE),
                    cursor,
                    IndexQueryConstraints.unconstrained(),
                    new TokenPredicate(barId),
                    NULL_CONTEXT);
            while (cursor.next()) {
                expectedEvents.add(nodeEvent(cursor.nodeReference()));
            }
        }

        // then
        tracer.assertEvents(expectedEvents);
    }

    @Test
    void shouldTraceNodeIndexSeek() throws KernelException {
        // given
        TestKernelReadTracer tracer = new TestKernelReadTracer();

        try (NodeValueIndexCursor cursor =
                cursors.allocateNodeValueIndexCursor(NULL_CONTEXT, EmptyMemoryTracker.INSTANCE)) {
            int p1 = token.propertyKey("p1");
            IndexReadSession session = read.indexReadSession(index);

            assertIndexSeekTracing(tracer, cursor, session, IndexOrder.NONE, p1);
            assertIndexSeekTracing(tracer, cursor, session, IndexOrder.ASCENDING, p1);
        }
    }

    private void assertIndexSeekTracing(
            TestKernelReadTracer tracer,
            NodeValueIndexCursor cursor,
            IndexReadSession session,
            IndexOrder order,
            int prop)
            throws KernelException {
        // when
        cursor.setTracer(tracer);
        read.nodeIndexSeek(
                tx.queryContext(),
                session,
                cursor,
                constrained(order, false),
                PropertyIndexQuery.range(prop, 0, false, 10, false));

        tracer.assertEvents(indexSeekEvent());

        assertTrue(cursor.next());
        tracer.assertEvents(nodeEvent(cursor.nodeReference()));

        assertFalse(cursor.next());
        tracer.assertEvents();
    }

    @Test
    void shouldTraceSingleRelationship() {
        // given
        TestKernelReadTracer tracer = new TestKernelReadTracer();

        try (RelationshipScanCursor cursor = cursors.allocateRelationshipScanCursor(NULL_CONTEXT)) {
            // when
            cursor.setTracer(tracer);
            read.singleRelationship(has, cursor);
            assertTrue(cursor.next());
            tracer.assertEvents(relationshipEvent(has));

            cursor.removeTracer();
            read.singleRelationship(is, cursor);
            assertTrue(cursor.next());
            tracer.assertEvents();

            cursor.setTracer(tracer);
            read.singleRelationship(is, cursor);
            assertTrue(cursor.next());
            tracer.assertEvents(relationshipEvent(is));

            assertFalse(cursor.next());
            tracer.assertEvents();
        }
    }

    @Test
    void shouldTraceRelationshipTraversal() {
        // given
        TestKernelReadTracer tracer = new TestKernelReadTracer();

        try (NodeCursor nodeCursor = cursors.allocateNodeCursor(NULL_CONTEXT);
                RelationshipTraversalCursor cursor = cursors.allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            // when
            cursor.setTracer(tracer);

            read.singleNode(foo, nodeCursor);
            assertTrue(nodeCursor.next());
            nodeCursor.relationships(cursor, ALL_RELATIONSHIPS);

            assertTrue(cursor.next());
            tracer.assertEvents(relationshipEvent(cursor.relationshipReference()));

            cursor.removeTracer();
            assertTrue(cursor.next());
            tracer.assertEvents();

            cursor.setTracer(tracer);
            assertTrue(cursor.next());
            tracer.assertEvents(relationshipEvent(cursor.relationshipReference()));

            assertTrue(cursor.next()); // skip last two
            assertTrue(cursor.next());
            tracer.clear();

            assertFalse(cursor.next());
            tracer.assertEvents();
        }
    }

    @Test
    void shouldTraceLazySelectionRelationshipTraversal() {
        // given
        TestKernelReadTracer tracer = new TestKernelReadTracer();

        try (NodeCursor nodeCursor = cursors.allocateNodeCursor(NULL_CONTEXT);
                RelationshipTraversalCursor cursor = cursors.allocateRelationshipTraversalCursor(NULL_CONTEXT)) {
            // when
            cursor.setTracer(tracer);

            read.singleNode(foo, nodeCursor);
            assertTrue(nodeCursor.next());

            int type = token.relationshipType("HAS");
            nodeCursor.relationships(cursor, selection(type, Direction.OUTGOING));

            assertTrue(cursor.next());
            tracer.assertEvents(relationshipEvent(cursor.relationshipReference()));

            cursor.removeTracer();

            cursor.setTracer(tracer);
            assertTrue(cursor.next());
            tracer.assertEvents(relationshipEvent(cursor.relationshipReference()));

            assertTrue(cursor.next());
            assertTrue(cursor.next()); // skip last one
            tracer.clear();

            assertFalse(cursor.next());
            tracer.assertEvents();
        }
    }

    @Test
    void shouldTracePropertyAccess() {
        // given
        TestKernelReadTracer tracer = new TestKernelReadTracer();

        try (NodeCursor nodeCursor = cursors.allocateNodeCursor(NULL_CONTEXT);
                PropertyCursor propertyCursor = cursors.allocatePropertyCursor(NULL_CONTEXT, INSTANCE)) {
            // when
            propertyCursor.setTracer(tracer);

            read.singleNode(foo, nodeCursor);
            assertTrue(nodeCursor.next());
            nodeCursor.properties(propertyCursor);

            assertTrue(propertyCursor.next());
            tracer.assertEvents(propertyEvent(propertyCursor.propertyKey()));

            assertTrue(propertyCursor.next());
            tracer.assertEvents(propertyEvent(propertyCursor.propertyKey()));

            propertyCursor.removeTracer();
            assertTrue(propertyCursor.next());
            tracer.assertEvents();

            propertyCursor.setTracer(tracer);
            assertTrue(propertyCursor.next());
            tracer.assertEvents(propertyEvent(propertyCursor.propertyKey()));

            assertFalse(propertyCursor.next());
            tracer.assertEvents();
        }
    }

    @Test
    void shouldTraceLabelCheck() {
        // given
        int label = 42;
        TestKernelReadTracer tracer = new TestKernelReadTracer();

        try (NodeCursor nodeCursor = cursors.allocateNodeCursor(NULL_CONTEXT)) {
            // when
            nodeCursor.setTracer(tracer);

            read.singleNode(foo, nodeCursor);
            assertTrue(nodeCursor.next());
            nodeCursor.hasLabel(label);

            tracer.assertEvents(nodeEvent(foo), hasLabelEvent(42));

            nodeCursor.removeTracer();

            nodeCursor.hasLabel(label);
            tracer.assertEvents();

            nodeCursor.setTracer(tracer);
            nodeCursor.hasLabel(label);
            tracer.assertEvents(hasLabelEvent(42));
        }
    }

    @Test
    void shouldTraceHasAnyLabelCheck() {
        // given
        int label = 42;
        TestKernelReadTracer tracer = new TestKernelReadTracer();

        try (NodeCursor nodeCursor = cursors.allocateNodeCursor(NULL_CONTEXT)) {
            // when
            nodeCursor.setTracer(tracer);

            read.singleNode(foo, nodeCursor);
            assertTrue(nodeCursor.next());
            nodeCursor.hasLabel();

            tracer.assertEvents(nodeEvent(foo), hasLabelEvent());

            nodeCursor.removeTracer();

            nodeCursor.hasLabel();
            tracer.assertEvents();

            nodeCursor.setTracer(tracer);
            nodeCursor.hasLabel();
            tracer.assertEvents(hasLabelEvent());
        }
    }

    @Test
    void shouldTraceRelationshipTypeScan() throws KernelException {
        // given
        TestKernelReadTracer tracer = new TestKernelReadTracer();
        int hasId = token.relationshipTypeGetOrCreateForName("HAS");

        List<TraceEvent> expectedEvents = new ArrayList<>();
        expectedEvents.add(relationshipTypeScanEvent(hasId));

        try (RelationshipTypeIndexCursor cursor = cursors.allocateRelationshipTypeIndexCursor(NULL_CONTEXT)) {
            // when
            cursor.setTracer(tracer);
            read.relationshipTypeScan(
                    getTokenReadSession(tx, EntityType.RELATIONSHIP),
                    cursor,
                    IndexQueryConstraints.unconstrained(),
                    new TokenPredicate(hasId),
                    NULL_CONTEXT);
            while (cursor.next()) {
                expectedEvents.add(relationshipEvent(cursor.relationshipReference()));
            }
        }

        // then
        tracer.assertEvents(expectedEvents);
    }

    @Test
    void shouldTraceRelationshipIndexSeek() throws KernelException {
        // given
        TestKernelReadTracer tracer = new TestKernelReadTracer();

        try (RelationshipValueIndexCursor cursor =
                cursors.allocateRelationshipValueIndexCursor(NULL_CONTEXT, INSTANCE)) {
            int p1 = token.propertyKey("p1");
            IndexReadSession session = read.indexReadSession(relIndex);

            assertRelationshipIndexSeekTracing(tracer, cursor, session, tx.queryContext(), IndexOrder.NONE, p1);
            assertRelationshipIndexSeekTracing(tracer, cursor, session, tx.queryContext(), IndexOrder.ASCENDING, p1);
        }
    }

    private static TokenReadSession getTokenReadSession(KernelTransaction tx, EntityType entityType)
            throws IndexNotFoundKernelException {
        Iterator<IndexDescriptor> indexes = tx.schemaRead().index(SchemaDescriptors.forAnyEntityTokens(entityType));
        IndexDescriptor index = indexes.next();
        assertFalse(indexes.hasNext());
        return tx.dataRead().tokenReadSession(index);
    }

    private void assertRelationshipIndexSeekTracing(
            TestKernelReadTracer tracer,
            RelationshipValueIndexCursor cursor,
            IndexReadSession session,
            QueryContext queryContext,
            IndexOrder order,
            int prop)
            throws KernelException {
        // when
        cursor.setTracer(tracer);
        read.relationshipIndexSeek(
                queryContext,
                session,
                cursor,
                constrained(order, false),
                PropertyIndexQuery.range(prop, 0, false, 10, false));

        tracer.assertEvents(indexSeekEvent());

        assertTrue(cursor.next());
        tracer.assertEvents(relationshipEvent(cursor.relationshipReference()));

        assertFalse(cursor.next());
        tracer.assertEvents();
    }

    @Override
    public ReadTestSupport newTestSupport() {
        return new ReadTestSupport();
    }
}
