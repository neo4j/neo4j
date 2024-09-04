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
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.internal.kernel.api.InternalIndexState.ONLINE;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.RelationshipSelection.ALL_RELATIONSHIPS;

import java.util.ArrayList;
import java.util.Iterator;
import org.junit.jupiter.api.Test;
import org.neo4j.common.EntityType;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.kernel.api.TokenReadSession;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.AllIndexProviderDescriptors;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.memory.EmptyMemoryTracker;

public abstract class DefaultPooledCursorsTestBase<G extends KernelAPIReadTestSupport>
        extends KernelAPIReadTestBase<G> {
    private static long startNode, relationship, propNode;
    private static final String NODE_PROP_INDEX_NAME = "nodeProp";

    @Override
    public void createTestGraph(GraphDatabaseService graphDb) {
        try (Transaction tx = graphDb.beginTx()) {
            tx.schema()
                    .indexFor(label("Node"))
                    .on("prop")
                    .withName(NODE_PROP_INDEX_NAME)
                    .create();
            tx.commit();
        }

        try (Transaction tx = graphDb.beginTx()) {
            Node a = tx.createNode(label("Foo"));
            Node b = tx.createNode(label("Bar"));
            startNode = a.getId();
            relationship = a.createRelationshipTo(b, withName("REL")).getId();
            propNode = createNodeWithProperty(tx, "prop", true);
            tx.commit();
        }
    }

    @Test
    void shouldReuseNodeCursor() {
        NodeCursor c1 = cursors.allocateNodeCursor(NULL_CONTEXT);
        read.singleNode(startNode, c1);
        c1.close();

        NodeCursor c2 = cursors.allocateNodeCursor(NULL_CONTEXT);
        assertThat(c1).isSameAs(c2);
        c2.close();
    }

    @Test
    void shouldReuseFullAccessNodeCursor() {
        NodeCursor c1 = cursors.allocateFullAccessNodeCursor(NULL_CONTEXT, EmptyMemoryTracker.INSTANCE);
        read.singleNode(startNode, c1);
        c1.close();

        NodeCursor c2 = cursors.allocateFullAccessNodeCursor(NULL_CONTEXT, EmptyMemoryTracker.INSTANCE);
        assertThat(c1).isSameAs(c2);
        c2.close();
    }

    @Test
    void shouldReuseRelationshipScanCursor() {
        RelationshipScanCursor c1 = cursors.allocateRelationshipScanCursor(NULL_CONTEXT);
        read.singleRelationship(relationship, c1);
        c1.close();

        RelationshipScanCursor c2 = cursors.allocateRelationshipScanCursor(NULL_CONTEXT);
        assertThat(c1).isSameAs(c2);
        c2.close();
    }

    @Test
    void shouldReuseFullAccessRelationshipScanCursor() {
        RelationshipScanCursor c1 =
                cursors.allocateFullAccessRelationshipScanCursor(NULL_CONTEXT, EmptyMemoryTracker.INSTANCE);
        read.singleRelationship(relationship, c1);
        c1.close();

        RelationshipScanCursor c2 =
                cursors.allocateFullAccessRelationshipScanCursor(NULL_CONTEXT, EmptyMemoryTracker.INSTANCE);
        assertThat(c1).isSameAs(c2);
        c2.close();
    }

    @Test
    void shouldReuseRelationshipTraversalCursor() {
        NodeCursor node = cursors.allocateNodeCursor(NULL_CONTEXT);
        RelationshipTraversalCursor c1 = cursors.allocateRelationshipTraversalCursor(NULL_CONTEXT);

        read.singleNode(startNode, node);
        node.next();
        node.relationships(c1, ALL_RELATIONSHIPS);

        node.close();
        c1.close();

        RelationshipTraversalCursor c2 = cursors.allocateRelationshipTraversalCursor(NULL_CONTEXT);
        assertThat(c1).isSameAs(c2);
        c2.close();
    }

    @Test
    void shouldReuseFullAccessRelationshipTraversalCursor() {
        NodeCursor node = cursors.allocateNodeCursor(NULL_CONTEXT);
        RelationshipTraversalCursor c1 =
                cursors.allocateFullAccessRelationshipTraversalCursor(NULL_CONTEXT, EmptyMemoryTracker.INSTANCE);

        read.singleNode(startNode, node);
        node.next();
        node.relationships(c1, ALL_RELATIONSHIPS);

        node.close();
        c1.close();

        RelationshipTraversalCursor c2 =
                cursors.allocateFullAccessRelationshipTraversalCursor(NULL_CONTEXT, EmptyMemoryTracker.INSTANCE);
        assertThat(c1).isSameAs(c2);
        c2.close();
    }

    @Test
    void shouldReusePropertyCursor() {
        NodeCursor node = cursors.allocateNodeCursor(NULL_CONTEXT);
        PropertyCursor c1 = cursors.allocatePropertyCursor(NULL_CONTEXT, INSTANCE);

        read.singleNode(propNode, node);
        node.next();
        node.properties(c1);

        node.close();
        c1.close();

        PropertyCursor c2 = cursors.allocatePropertyCursor(NULL_CONTEXT, INSTANCE);
        assertThat(c1).isSameAs(c2);
        c2.close();
    }

    @Test
    void shouldReuseFullAccessPropertyCursor() {
        NodeCursor node = cursors.allocateNodeCursor(NULL_CONTEXT);
        PropertyCursor c1 = cursors.allocateFullAccessPropertyCursor(NULL_CONTEXT, INSTANCE);

        read.singleNode(propNode, node);
        node.next();
        node.properties(c1);

        node.close();
        c1.close();

        PropertyCursor c2 = cursors.allocateFullAccessPropertyCursor(NULL_CONTEXT, INSTANCE);
        assertThat(c1).isSameAs(c2);
        c2.close();
    }

    @Test
    void shouldReuseNodeValueIndexCursor() throws Exception {
        int prop = token.propertyKey("prop");
        IndexDescriptor indexDescriptor = tx.schemaRead().indexGetForName(NODE_PROP_INDEX_NAME);
        Predicates.awaitEx(() -> tx.schemaRead().indexGetState(indexDescriptor) == ONLINE, 1, MINUTES);
        IndexReadSession indexSession = tx.dataRead().indexReadSession(indexDescriptor);

        NodeValueIndexCursor c1 = cursors.allocateNodeValueIndexCursor(NULL_CONTEXT, EmptyMemoryTracker.INSTANCE);
        read.nodeIndexSeek(
                tx.queryContext(),
                indexSession,
                c1,
                IndexQueryConstraints.unconstrained(),
                PropertyIndexQuery.exact(prop, "zero"));
        c1.close();

        NodeValueIndexCursor c2 = cursors.allocateNodeValueIndexCursor(NULL_CONTEXT, EmptyMemoryTracker.INSTANCE);
        assertThat(c1).isSameAs(c2);
        c2.close();
    }

    @Test
    void shouldReuseFullAccessNodeValueIndexCursor() throws Exception {
        int prop = token.propertyKey("prop");
        IndexDescriptor indexDescriptor = tx.schemaRead().indexGetForName(NODE_PROP_INDEX_NAME);
        Predicates.awaitEx(() -> tx.schemaRead().indexGetState(indexDescriptor) == ONLINE, 1, MINUTES);
        IndexReadSession indexSession = tx.dataRead().indexReadSession(indexDescriptor);

        NodeValueIndexCursor c1 =
                cursors.allocateFullAccessNodeValueIndexCursor(NULL_CONTEXT, EmptyMemoryTracker.INSTANCE);
        read.nodeIndexSeek(
                tx.queryContext(),
                indexSession,
                c1,
                IndexQueryConstraints.unconstrained(),
                PropertyIndexQuery.exact(prop, "zero"));
        c1.close();

        NodeValueIndexCursor c2 =
                cursors.allocateFullAccessNodeValueIndexCursor(NULL_CONTEXT, EmptyMemoryTracker.INSTANCE);
        assertThat(c1).isSameAs(c2);
        c2.close();
    }

    @Test
    void shouldReuseNodeLabelIndexCursor() throws Exception {
        try (KernelTransaction tx = beginTransaction()) {
            NodeLabelIndexCursor c1 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
            tx.dataRead()
                    .nodeLabelScan(
                            getTokenReadSession(tx, EntityType.NODE),
                            c1,
                            IndexQueryConstraints.unconstrained(),
                            new TokenPredicate(1),
                            tx.cursorContext());
            c1.close();

            NodeLabelIndexCursor c2 = tx.cursors().allocateNodeLabelIndexCursor(NULL_CONTEXT);
            assertThat(c1).isSameAs(c2);
            c2.close();
        }
    }

    @Test
    void shouldReuseFullAccessNodeLabelIndexCursor() throws Exception {
        try (KernelTransaction tx = beginTransaction()) {
            NodeLabelIndexCursor c1 = tx.cursors().allocateFullAccessNodeLabelIndexCursor(NULL_CONTEXT);
            tx.dataRead()
                    .nodeLabelScan(
                            getTokenReadSession(tx, EntityType.NODE),
                            c1,
                            IndexQueryConstraints.unconstrained(),
                            new TokenPredicate(1),
                            tx.cursorContext());
            c1.close();

            NodeLabelIndexCursor c2 = tx.cursors().allocateFullAccessNodeLabelIndexCursor(NULL_CONTEXT);
            assertThat(c1).isSameAs(c2);
            c2.close();
        }
    }

    @Test
    void shouldReuseRelationshipIndexCursors() throws Exception {
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

        Predicates.awaitEx(() -> tx.schemaRead().indexGetState(index) == ONLINE, 1, MINUTES);

        RelationshipValueIndexCursor c1 =
                cursors.allocateRelationshipValueIndexCursor(NULL_CONTEXT, EmptyMemoryTracker.INSTANCE);
        IndexReadSession indexSession = tx.dataRead().indexReadSession(index);
        read.relationshipIndexSeek(
                tx.queryContext(),
                indexSession,
                c1,
                IndexQueryConstraints.unconstrained(),
                PropertyIndexQuery.fulltextSearch("hello"));
        c1.close();

        RelationshipValueIndexCursor c2 =
                cursors.allocateRelationshipValueIndexCursor(NULL_CONTEXT, EmptyMemoryTracker.INSTANCE);
        assertThat(c1).isSameAs(c2);
        c2.close();
    }

    @Test
    void shouldNotReuseReleasedRelationshipValueIndexCursor() throws Exception {
        RelationshipValueIndexCursor c1;
        KernelTransaction tx = beginTransaction();
        try (tx) {
            c1 = tx.cursors().allocateFullAccessRelationshipValueIndexCursor(NULL_CONTEXT, EmptyMemoryTracker.INSTANCE);
            c1.close();
            tx.commit();
        }

        // Find the same transaction again to test re-use
        ArrayList<KernelTransaction> txs = new ArrayList<>();
        try {
            for (int i = 0; i < 10; i++) {
                KernelTransaction tx1 = beginTransaction();
                txs.add(tx1);
                if (tx1 != tx) {
                    continue;
                }

                RelationshipValueIndexCursor c2 = tx1.cursors()
                        .allocateFullAccessRelationshipValueIndexCursor(NULL_CONTEXT, EmptyMemoryTracker.INSTANCE);
                // We should not have been able to get the same cursor again, it should have been released properly
                assertThat(c1).isNotSameAs(c2);
                c2.close();
                break;
            }
        } finally {
            IOUtils.closeAllUnchecked(txs);
        }
    }

    private static int[] array(int... elements) {
        return elements;
    }

    private static long createNodeWithProperty(Transaction tx, String propertyKey, Object value) {
        Node p = tx.createNode();
        p.setProperty(propertyKey, value);
        return p.getId();
    }

    private static TokenReadSession getTokenReadSession(KernelTransaction tx, EntityType entityType)
            throws IndexNotFoundKernelException {
        Iterator<IndexDescriptor> indexes = tx.schemaRead().index(SchemaDescriptors.forAnyEntityTokens(entityType));
        IndexDescriptor index = indexes.next();
        assertThat(indexes.hasNext()).isFalse();
        return tx.dataRead().tokenReadSession(index);
    }
}
