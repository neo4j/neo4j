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
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.IntStream;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.junit.jupiter.api.Test;
import org.neo4j.common.EntityType;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.kernel.api.TokenReadSession;
import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.kernel.api.security.TestAccessMode;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

@SuppressWarnings("Duplicates")
public abstract class NodeTransactionStateTestBase<G extends KernelAPIWriteTestSupport>
        extends KernelAPIWriteTestBase<G> {
    @Test
    void shouldSeeNodeInTransaction() throws Exception {
        long nodeId;
        try (KernelTransaction tx = beginTransaction()) {
            nodeId = tx.dataWrite().nodeCreate();
            try (NodeCursor node = tx.cursors().allocateNodeCursor(tx.cursorContext())) {
                tx.dataRead().singleNode(nodeId, node);
                assertTrue(node.next(), "should access node");
                assertEquals(nodeId, node.nodeReference());
                assertFalse(node.next(), "should only find one node");
            }
            tx.commit();
        }

        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            assertEquals(nodeId, tx.getNodeById(nodeId).getId());
        }
    }

    @Test
    void shouldSeeNewLabeledNodeInTransaction() throws Exception {
        long nodeId;
        int labelId;
        final String labelName = "Town";

        try (KernelTransaction tx = beginTransaction()) {
            nodeId = tx.dataWrite().nodeCreate();
            labelId = tx.token().labelGetOrCreateForName(labelName);
            tx.dataWrite().nodeAddLabel(nodeId, labelId);

            try (NodeCursor node = tx.cursors().allocateNodeCursor(tx.cursorContext())) {
                tx.dataRead().singleNode(nodeId, node);
                assertTrue(node.next(), "should access node");

                TokenSet labels = node.labels();
                assertEquals(1, labels.numberOfTokens());
                assertEquals(labelId, labels.token(0));
                assertTrue(node.hasLabel(labelId));
                assertFalse(node.hasLabel(labelId + 1));
                assertFalse(node.next(), "should only find one node");
            }
            tx.commit();
        }

        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            assertThat(tx.getNodeById(nodeId).getLabels()).isEqualTo(Iterables.iterable(label(labelName)));
        }
    }

    @Test
    void shouldSeeLabelChangesInTransaction() throws Exception {
        long nodeId;
        int toRetain, toDelete, toAdd, toRegret;
        final String toRetainName = "ToRetain";
        final String toDeleteName = "ToDelete";
        final String toAddName = "ToAdd";
        final String toRegretName = "ToRegret";

        try (KernelTransaction tx = beginTransaction()) {
            nodeId = tx.dataWrite().nodeCreate();
            toRetain = tx.token().labelGetOrCreateForName(toRetainName);
            toDelete = tx.token().labelGetOrCreateForName(toDeleteName);
            tx.dataWrite().nodeAddLabel(nodeId, toRetain);
            tx.dataWrite().nodeAddLabel(nodeId, toDelete);
            tx.commit();
        }

        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            assertThat(tx.getNodeById(nodeId).getLabels()).contains(label(toRetainName), label(toDeleteName));
        }

        try (KernelTransaction tx = beginTransaction()) {
            toAdd = tx.token().labelGetOrCreateForName(toAddName);
            tx.dataWrite().nodeAddLabel(nodeId, toAdd);
            tx.dataWrite().nodeRemoveLabel(nodeId, toDelete);

            toRegret = tx.token().labelGetOrCreateForName(toRegretName);
            tx.dataWrite().nodeAddLabel(nodeId, toRegret);
            tx.dataWrite().nodeRemoveLabel(nodeId, toRegret);

            try (NodeCursor node = tx.cursors().allocateNodeCursor(tx.cursorContext())) {
                tx.dataRead().singleNode(nodeId, node);
                assertTrue(node.next(), "should access node");

                assertLabels(node.labels(), toRetain, toAdd);
                assertTrue(node.hasLabel(toAdd));
                assertTrue(node.hasLabel(toRetain));
                assertFalse(node.hasLabel(toDelete));
                assertFalse(node.hasLabel(toRegret));
                assertFalse(node.next(), "should only find one node");
            }
            tx.commit();
        }

        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            assertThat(tx.getNodeById(nodeId).getLabels()).contains(label(toRetainName), label(toAddName));
        }
    }

    @Test
    void hasAnyLabelShouldSeeNewlyCreatedLabel() throws Exception {
        long nodeId;
        final String labelName = "Town";

        try (KernelTransaction tx = beginTransaction()) {
            nodeId = tx.dataWrite().nodeCreate();
            int labelId = tx.token().labelGetOrCreateForName(labelName);

            try (NodeCursor node = tx.cursors().allocateNodeCursor(tx.cursorContext())) {
                tx.dataRead().singleNode(nodeId, node);
                assertTrue(node.next(), "should access node");
                assertFalse(node.hasLabel());
            }

            // add a label
            tx.dataWrite().nodeAddLabel(nodeId, labelId);
            try (NodeCursor node = tx.cursors().allocateNodeCursor(tx.cursorContext())) {
                tx.dataRead().singleNode(nodeId, node);
                assertTrue(node.next(), "should access node");
                assertTrue(node.hasLabel());
            }

            tx.commit();
        }

        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            assertThat(tx.getNodeById(nodeId).getLabels()).isEqualTo(Iterables.iterable(label(labelName)));
        }
    }

    @Test
    void hasAnyLabelShouldNotSeeNewlyCreatedLabelAndThenDeleted() throws Exception {
        long nodeId;
        final String labelName = "Town";

        try (KernelTransaction tx = beginTransaction()) {
            nodeId = tx.dataWrite().nodeCreate();
            int labelId = tx.token().labelGetOrCreateForName(labelName);

            try (NodeCursor node = tx.cursors().allocateNodeCursor(tx.cursorContext())) {
                tx.dataRead().singleNode(nodeId, node);
                assertTrue(node.next(), "should access node");
                assertFalse(node.hasLabel());
            }

            // add a label
            tx.dataWrite().nodeAddLabel(nodeId, labelId);
            // remove label
            tx.dataWrite().nodeRemoveLabel(nodeId, labelId);
            try (NodeCursor node = tx.cursors().allocateNodeCursor(tx.cursorContext())) {
                tx.dataRead().singleNode(nodeId, node);
                assertTrue(node.next(), "should access node");
                assertFalse(node.hasLabel());
            }

            tx.commit();
        }
    }

    @Test
    void hasAnyLabelShouldHandleIfAllLabelsAreDeleted() throws Exception {
        long nodeId;
        int numberOfLabels = 100;
        String[] names = IntStream.range(0, numberOfLabels)
                .mapToObj(value -> "L" + value)
                .toArray(String[]::new);
        int[] labelIds = new int[numberOfLabels];

        try (KernelTransaction tx = beginTransaction()) {
            nodeId = tx.dataWrite().nodeCreate();
            tx.token().labelGetOrCreateForNames(names, labelIds);

            try (NodeCursor node = tx.cursors().allocateNodeCursor(tx.cursorContext())) {
                tx.dataRead().singleNode(nodeId, node);
                assertTrue(node.next(), "should access node");
                assertFalse(node.hasLabel());
            }

            // add labels
            for (int labelId : labelIds) {
                tx.dataWrite().nodeAddLabel(nodeId, labelId);
            }
            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            for (int i = 0; i < numberOfLabels; i++) {
                tx.dataWrite().nodeRemoveLabel(nodeId, labelIds[i]);
                try (NodeCursor node = tx.cursors().allocateNodeCursor(tx.cursorContext())) {
                    tx.dataRead().singleNode(nodeId, node);
                    assertTrue(node.next(), "should access node");
                    assertThat(node.hasLabel()).isEqualTo(i < numberOfLabels - 1);
                }
            }
            tx.commit();
        }
    }

    @Test
    void shouldDiscoverDeletedNodeInTransaction() throws Exception {
        long nodeId;
        try (KernelTransaction tx = beginTransaction()) {
            nodeId = tx.dataWrite().nodeCreate();
            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            assertTrue(tx.dataWrite().nodeDelete(nodeId));
            try (NodeCursor node = tx.cursors().allocateNodeCursor(tx.cursorContext())) {
                tx.dataRead().singleNode(nodeId, node);
                assertFalse(node.next());
            }
            tx.commit();
        }
    }

    @Test
    void shouldHandleMultipleNodeDeletions() throws Exception {
        long nodeId;
        try (KernelTransaction tx = beginTransaction()) {
            nodeId = tx.dataWrite().nodeCreate();
            tx.commit();
        }

        try (KernelTransaction tx = beginTransaction()) {
            assertTrue(tx.dataWrite().nodeDelete(nodeId));
            assertFalse(tx.dataWrite().nodeDelete(nodeId));
            tx.commit();
        }
    }

    @Test
    void shouldSeeNewNodePropertyInTransaction() throws Exception {
        long nodeId;
        String propKey1 = "prop1";
        String propKey2 = "prop2";

        try (KernelTransaction tx = beginTransaction()) {
            nodeId = tx.dataWrite().nodeCreate();
            int prop1 = tx.token().propertyKeyGetOrCreateForName(propKey1);
            int prop2 = tx.token().propertyKeyGetOrCreateForName(propKey2);
            assertEquals(NO_VALUE, tx.dataWrite().nodeSetProperty(nodeId, prop1, stringValue("hello")));
            assertEquals(NO_VALUE, tx.dataWrite().nodeSetProperty(nodeId, prop2, stringValue("world")));

            try (NodeCursor node = tx.cursors().allocateNodeCursor(tx.cursorContext());
                    PropertyCursor property =
                            tx.cursors().allocatePropertyCursor(tx.cursorContext(), tx.memoryTracker())) {
                tx.dataRead().singleNode(nodeId, node);
                assertTrue(node.next(), "should access node");

                node.properties(property);
                IntObjectHashMap<Value> foundProperties = IntObjectHashMap.newMap();
                while (property.next()) {
                    assertNull(
                            foundProperties.put(property.propertyKey(), property.propertyValue()),
                            "should only find each property once");
                }

                assertThat(foundProperties).hasSize(2);
                assertThat(foundProperties.get(prop1)).isEqualTo(stringValue("hello"));
                assertThat(foundProperties.get(prop2)).isEqualTo(stringValue("world"));

                assertFalse(node.next(), "should only find one node");
            }
            tx.commit();
        }
    }

    @Test
    void shouldSeeAddedPropertyFromExistingNodeWithoutPropertiesInTransaction() throws Exception {
        // Given
        long nodeId;
        String propKey = "prop1";
        try (KernelTransaction tx = beginTransaction()) {
            nodeId = tx.dataWrite().nodeCreate();
            tx.commit();
        }

        // When/Then
        try (KernelTransaction tx = beginTransaction()) {
            int propToken = tx.token().propertyKeyGetOrCreateForName(propKey);
            assertEquals(NO_VALUE, tx.dataWrite().nodeSetProperty(nodeId, propToken, stringValue("hello")));

            try (NodeCursor node = tx.cursors().allocateNodeCursor(tx.cursorContext());
                    PropertyCursor property =
                            tx.cursors().allocatePropertyCursor(tx.cursorContext(), tx.memoryTracker())) {
                tx.dataRead().singleNode(nodeId, node);
                assertTrue(node.next(), "should access node");

                node.properties(property);
                assertTrue(property.next());
                assertEquals(propToken, property.propertyKey());
                assertEquals(property.propertyValue(), stringValue("hello"));

                assertFalse(property.next(), "should only find one properties");
                assertFalse(node.next(), "should only find one node");
            }

            tx.commit();
        }

        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            assertThat(tx.getNodeById(nodeId).getProperty(propKey)).isEqualTo("hello");
        }
    }

    @Test
    void shouldSeeAddedPropertyFromExistingNodeWithPropertiesInTransaction() throws Exception {
        // Given
        long nodeId;
        String propKey1 = "prop1";
        String propKey2 = "prop2";
        int propToken1;
        int propToken2;
        try (KernelTransaction tx = beginTransaction()) {
            nodeId = tx.dataWrite().nodeCreate();
            propToken1 = tx.token().propertyKeyGetOrCreateForName(propKey1);
            assertEquals(NO_VALUE, tx.dataWrite().nodeSetProperty(nodeId, propToken1, stringValue("hello")));
            tx.commit();
        }

        // When/Then
        try (KernelTransaction tx = beginTransaction()) {
            propToken2 = tx.token().propertyKeyGetOrCreateForName(propKey2);
            assertEquals(NO_VALUE, tx.dataWrite().nodeSetProperty(nodeId, propToken2, stringValue("world")));

            try (NodeCursor node = tx.cursors().allocateNodeCursor(tx.cursorContext());
                    PropertyCursor property =
                            tx.cursors().allocatePropertyCursor(tx.cursorContext(), tx.memoryTracker())) {
                tx.dataRead().singleNode(nodeId, node);
                assertTrue(node.next(), "should access node");

                node.properties(property);

                // property 2, start with tx state
                assertTrue(property.next());
                assertEquals(propToken2, property.propertyKey());
                assertEquals(property.propertyValue(), stringValue("world"));

                // property 1, from disk
                assertTrue(property.next());
                assertEquals(propToken1, property.propertyKey());
                assertEquals(property.propertyValue(), stringValue("hello"));

                assertFalse(property.next(), "should only find two properties");
                assertFalse(node.next(), "should only find one node");
            }
            tx.commit();
        }

        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            assertThat(tx.getNodeById(nodeId).getProperty(propKey1)).isEqualTo("hello");
            assertThat(tx.getNodeById(nodeId).getProperty(propKey2)).isEqualTo("world");
        }
    }

    @Test
    void shouldSeeUpdatedPropertyFromExistingNodeWithPropertiesInTransaction() throws Exception {
        // Given
        long nodeId;
        String propKey = "prop1";
        int propToken;
        try (KernelTransaction tx = beginTransaction()) {
            nodeId = tx.dataWrite().nodeCreate();
            propToken = tx.token().propertyKeyGetOrCreateForName(propKey);
            assertEquals(NO_VALUE, tx.dataWrite().nodeSetProperty(nodeId, propToken, stringValue("hello")));
            tx.commit();
        }

        // When/Then
        try (KernelTransaction tx = beginTransaction()) {
            assertEquals(tx.dataWrite().nodeSetProperty(nodeId, propToken, stringValue("world")), stringValue("hello"));
            try (NodeCursor node = tx.cursors().allocateNodeCursor(tx.cursorContext());
                    PropertyCursor property =
                            tx.cursors().allocatePropertyCursor(tx.cursorContext(), tx.memoryTracker())) {
                tx.dataRead().singleNode(nodeId, node);
                assertTrue(node.next(), "should access node");

                node.properties(property);

                assertTrue(property.next());
                assertEquals(propToken, property.propertyKey());
                assertEquals(property.propertyValue(), stringValue("world"));

                assertFalse(property.next(), "should only find one property");
                assertFalse(node.next(), "should only find one node");
            }

            tx.commit();
        }

        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            assertThat(tx.getNodeById(nodeId).getProperty(propKey)).isEqualTo("world");
        }
    }

    @Test
    void shouldSeeRemovedPropertyInTransaction() throws Exception {
        // Given
        long nodeId;
        String propKey = "prop1";
        int propToken;
        try (KernelTransaction tx = beginTransaction()) {
            nodeId = tx.dataWrite().nodeCreate();
            propToken = tx.token().propertyKeyGetOrCreateForName(propKey);
            assertEquals(NO_VALUE, tx.dataWrite().nodeSetProperty(nodeId, propToken, stringValue("hello")));
            tx.commit();
        }

        // When/Then
        try (KernelTransaction tx = beginTransaction()) {
            assertEquals(tx.dataWrite().nodeRemoveProperty(nodeId, propToken), stringValue("hello"));
            try (NodeCursor node = tx.cursors().allocateNodeCursor(tx.cursorContext());
                    PropertyCursor property =
                            tx.cursors().allocatePropertyCursor(tx.cursorContext(), tx.memoryTracker())) {
                tx.dataRead().singleNode(nodeId, node);
                assertTrue(node.next(), "should access node");

                node.properties(property);
                assertFalse(property.next(), "should not find any properties");
                assertFalse(node.next(), "should only find one node");
            }

            tx.commit();
        }

        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            assertFalse(tx.getNodeById(nodeId).hasProperty(propKey));
        }
    }

    @Test
    void shouldSeeRemovedThenAddedPropertyInTransaction() throws Exception {
        // Given
        long nodeId;
        String propKey = "prop1";
        int propToken;
        try (KernelTransaction tx = beginTransaction()) {
            nodeId = tx.dataWrite().nodeCreate();
            propToken = tx.token().propertyKeyGetOrCreateForName(propKey);
            assertEquals(NO_VALUE, tx.dataWrite().nodeSetProperty(nodeId, propToken, stringValue("hello")));
            tx.commit();
        }

        // When/Then
        try (KernelTransaction tx = beginTransaction()) {
            assertEquals(tx.dataWrite().nodeRemoveProperty(nodeId, propToken), stringValue("hello"));
            assertEquals(NO_VALUE, tx.dataWrite().nodeSetProperty(nodeId, propToken, stringValue("world")));
            try (NodeCursor node = tx.cursors().allocateNodeCursor(tx.cursorContext());
                    PropertyCursor property =
                            tx.cursors().allocatePropertyCursor(tx.cursorContext(), tx.memoryTracker())) {
                tx.dataRead().singleNode(nodeId, node);
                assertTrue(node.next(), "should access node");

                node.properties(property);
                assertTrue(property.next());
                assertEquals(propToken, property.propertyKey());
                assertEquals(property.propertyValue(), stringValue("world"));

                assertFalse(property.next(), "should not find any properties");
                assertFalse(node.next(), "should only find one node");
            }

            tx.commit();
        }

        try (org.neo4j.graphdb.Transaction tx = graphDb.beginTx()) {
            assertThat(tx.getNodeById(nodeId).getProperty(propKey)).isEqualTo("world");
        }
    }

    @Test
    void shouldSeeExistingNode() throws Exception {
        // Given
        long node;
        try (KernelTransaction tx = beginTransaction()) {
            node = tx.dataWrite().nodeCreate();
            tx.commit();
        }

        // Then
        try (KernelTransaction tx = beginTransaction()) {
            assertTrue(tx.dataRead().nodeExists(node));
        }
    }

    @Test
    void shouldNotSeeNonExistingNode() throws Exception {
        // Given, empty db

        // Then
        try (KernelTransaction tx = beginTransaction()) {
            assertFalse(tx.dataRead().nodeExists(1337L));
        }
    }

    @Test
    void shouldSeeNodeExistingInTxOnly() throws Exception {
        try (KernelTransaction tx = beginTransaction()) {
            long node = tx.dataWrite().nodeCreate();
            assertTrue(tx.dataRead().nodeExists(node));
        }
    }

    @Test
    void shouldNotSeeDeletedNode() throws Exception {
        // Given
        long node;
        try (KernelTransaction tx = beginTransaction()) {
            node = tx.dataWrite().nodeCreate();
            tx.commit();
        }

        // Then
        try (KernelTransaction tx = beginTransaction()) {
            tx.dataWrite().nodeDelete(node);
            assertFalse(tx.dataRead().nodeExists(node));
        }
    }

    @Test
    void shouldSeeTxStateSkipUntil() throws Exception {
        // Setup
        clearIdGeneratorCaches();
        // Given
        Node node = createNode("label");
        createNode("label");
        createNode("label");

        try (KernelTransaction tx = beginTransaction();
                NodeLabelIndexCursor cursor = tx.cursors().allocateNodeLabelIndexCursor(tx.cursorContext())) {

            tx.dataWrite().nodeCreateWithLabels(node.labels);
            var id = tx.dataWrite().nodeCreateWithLabels(node.labels);
            tx.dataWrite().nodeCreateWithLabels(node.labels);

            tx.dataRead()
                    .nodeLabelScan(
                            getTokenReadSession(tx, EntityType.NODE),
                            cursor,
                            IndexQueryConstraints.ordered(IndexOrder.ASCENDING),
                            new TokenPredicate(node.labels[0]),
                            tx.cursorContext());
            // when
            cursor.skipUntil(id);
            // then
            assertThat(cursor.next()).isTrue();
            assertThat(cursor.next()).isTrue();
            assertThat(cursor.next()).isFalse();
        }
    }

    @Test
    void shouldSeeDeletesInTXSkipUntil() throws Exception {
        // Setup
        clearIdGeneratorCaches();
        // Given
        Node node = createNode("label");
        Node nodeToDelete = createNode("label");
        createNode("label");

        try (KernelTransaction tx = beginTransaction();
                NodeLabelIndexCursor cursor = tx.cursors().allocateNodeLabelIndexCursor(tx.cursorContext())) {

            tx.dataWrite().nodeDelete(nodeToDelete.node);

            tx.dataRead()
                    .nodeLabelScan(
                            getTokenReadSession(tx, EntityType.NODE),
                            cursor,
                            IndexQueryConstraints.ordered(IndexOrder.ASCENDING),
                            new TokenPredicate(node.labels[0]),
                            tx.cursorContext());
            // when
            cursor.skipUntil(nodeToDelete.node);
            // then
            assertThat(cursor.next()).isTrue();
            assertThat(cursor.next()).isFalse();
        }
    }

    @Test
    void shouldSkipUntilWithRemovedLabel() throws Exception {
        // Setup
        clearIdGeneratorCaches();
        // Given
        createNode("label");
        createNode("label");

        // We will remove the label of node and then skip to it,
        // resulting in seeing the nodes AFTER it but not it itself
        Node node = createNode("label");
        createNode("label");
        createNode("label");

        try (KernelTransaction tx = beginTransaction();
                NodeLabelIndexCursor cursor = tx.cursors().allocateNodeLabelIndexCursor(tx.cursorContext())) {

            tx.dataWrite().nodeRemoveLabel(node.node, node.labels[0]);

            tx.dataRead()
                    .nodeLabelScan(
                            getTokenReadSession(tx, EntityType.NODE),
                            cursor,
                            IndexQueryConstraints.ordered(IndexOrder.ASCENDING),
                            new TokenPredicate(node.labels[0]),
                            tx.cursorContext());
            // when
            cursor.skipUntil(node.node);
            // then
            assertThat(cursor.next()).isTrue();
            assertThat(cursor.next()).isTrue();
            assertThat(cursor.next()).isFalse();
        }
    }

    @Test
    void shouldSkipUntilLabelAddedInTx() throws Exception {
        // Setup
        clearIdGeneratorCaches();
        // Given
        Node nodeWithLabel = createNode("label");
        Node nodeWithoutLabel = createNode();
        createNode("label");

        try (KernelTransaction tx = beginTransaction();
                NodeLabelIndexCursor cursor = tx.cursors().allocateNodeLabelIndexCursor(tx.cursorContext())) {

            tx.dataWrite().nodeAddLabel(nodeWithoutLabel.node, nodeWithLabel.labels[0]);

            tx.dataRead()
                    .nodeLabelScan(
                            getTokenReadSession(tx, EntityType.NODE),
                            cursor,
                            IndexQueryConstraints.ordered(IndexOrder.ASCENDING),
                            new TokenPredicate(nodeWithLabel.labels[0]),
                            tx.cursorContext());
            // when
            cursor.skipUntil(nodeWithoutLabel.node);
            // then
            assertThat(cursor.next()).isTrue();
            assertThat(cursor.next()).isTrue();
            assertThat(cursor.next()).isFalse();
        }
    }

    @Test
    void shouldNotFindDeletedNodeInLabelScan() throws Exception {
        // Given
        Node node = createNode("label");

        try (KernelTransaction tx = beginTransaction();
                NodeLabelIndexCursor cursor = tx.cursors().allocateNodeLabelIndexCursor(tx.cursorContext())) {
            // when
            tx.dataWrite().nodeDelete(node.node);
            tx.dataRead()
                    .nodeLabelScan(
                            getTokenReadSession(tx, EntityType.NODE),
                            cursor,
                            IndexQueryConstraints.unconstrained(),
                            new TokenPredicate(node.labels[0]),
                            tx.cursorContext());

            // then
            assertFalse(cursor.next());
        }
    }

    @Test
    void shouldNotFindNodeWithRemovedLabelInLabelScan() throws Exception {
        // Given
        Node node = createNode("label");

        try (KernelTransaction tx = beginTransaction();
                NodeLabelIndexCursor cursor = tx.cursors().allocateNodeLabelIndexCursor(tx.cursorContext())) {
            // when
            tx.dataWrite().nodeRemoveLabel(node.node, node.labels[0]);
            tx.dataRead()
                    .nodeLabelScan(
                            getTokenReadSession(tx, EntityType.NODE),
                            cursor,
                            IndexQueryConstraints.unconstrained(),
                            new TokenPredicate(node.labels[0]),
                            tx.cursorContext());

            // then
            assertFalse(cursor.next());
        }
    }

    @Test
    void shouldFindUpdatedNodeInInLabelScan() throws Exception {
        // Given
        Node node = createNode();

        try (KernelTransaction tx = beginTransaction();
                NodeLabelIndexCursor cursor = tx.cursors().allocateNodeLabelIndexCursor(tx.cursorContext())) {
            // when
            int label = tx.tokenWrite().labelGetOrCreateForName("label");
            tx.dataWrite().nodeAddLabel(node.node, label);
            tx.dataRead()
                    .nodeLabelScan(
                            getTokenReadSession(tx, EntityType.NODE),
                            cursor,
                            IndexQueryConstraints.unconstrained(),
                            new TokenPredicate(label),
                            tx.cursorContext());

            // then
            assertTrue(cursor.next());
            assertEquals(node.node, cursor.nodeReference());
        }
    }

    @Test
    void shouldFindSwappedNodeInLabelScan() throws Exception {
        // Given
        Node node1 = createNode("label");
        Node node2 = createNode();

        try (KernelTransaction tx = beginTransaction();
                NodeLabelIndexCursor cursor = tx.cursors().allocateNodeLabelIndexCursor(tx.cursorContext())) {
            // when
            tx.dataWrite().nodeRemoveLabel(node1.node, node1.labels[0]);
            tx.dataWrite().nodeAddLabel(node2.node, node1.labels[0]);
            tx.dataRead()
                    .nodeLabelScan(
                            getTokenReadSession(tx, EntityType.NODE),
                            cursor,
                            IndexQueryConstraints.unconstrained(),
                            new TokenPredicate(node1.labels[0]),
                            tx.cursorContext());

            // then
            assertTrue(cursor.next());
            assertEquals(node2.node, cursor.nodeReference());
        }
    }

    @Test
    void shouldCountNewLabelsFromTxState() throws Exception {
        // Given
        Node node1 = createNode("label");
        Node node2 = createNode();

        try (KernelTransaction tx = beginTransaction()) {
            // when
            tx.dataWrite().nodeAddLabel(node2.node, node1.labels[0]);
            long countTxState = tx.dataRead().countsForNode(node1.labels[0]);

            // then
            assertEquals(2, countTxState);
        }
    }

    @Test
    void shouldCountNewNodesFromTxState() throws Exception {
        // Given
        createNode();
        createNode();

        try (KernelTransaction tx = beginTransaction()) {
            // when
            tx.dataWrite().nodeCreate();
            long countTxState = tx.dataRead().countsForNode(-1);

            // then
            assertEquals(3, countTxState);
        }
    }

    @Test
    void shouldNotCountRemovedLabelsFromTxState() throws Exception {
        // Given
        Node node1 = createNode("label");
        Node node2 = createNode("label");

        try (KernelTransaction tx = beginTransaction()) {
            // when
            tx.dataWrite().nodeRemoveLabel(node2.node, node2.labels[0]);
            long countTxState = tx.dataRead().countsForNode(node1.labels[0]);

            // then
            assertEquals(1, countTxState);
        }
    }

    @Test
    void shouldNotCountRemovedNodesFromTxState() throws Exception {
        // Given
        Node node1 = createNode("label");
        Node node2 = createNode("label");

        try (KernelTransaction tx = beginTransaction()) {
            // when
            tx.dataWrite().nodeDelete(node2.node);
            long countTxState = tx.dataRead().countsForNode(node1.labels[0]);

            // then
            assertEquals(1, countTxState);
        }
    }

    @Test
    void shouldCountNewLabelsFromTxStateRestrictedUser() throws Exception {
        // Given
        Node node1 = createNode("label");
        Node node2 = createNode();

        SecurityContext loginContext = new SecurityContext(
                AuthSubject.AUTH_DISABLED,
                new TestAccessMode(true, false, true, false, false),
                EMBEDDED_CONNECTION,
                null);
        try (KernelTransaction tx = beginTransaction(loginContext)) {
            // when
            tx.dataWrite().nodeAddLabel(node2.node, node1.labels[0]);
            long countTxState = tx.dataRead().countsForNode(node1.labels[0]);

            // then
            assertEquals(2, countTxState);
        }
    }

    @Test
    void shouldCountNewNodesFromTxStateRestrictedUser() throws Exception {
        // Given
        createNode();
        createNode();

        SecurityContext loginContext = new SecurityContext(
                AuthSubject.AUTH_DISABLED,
                new TestAccessMode(true, false, true, false, false),
                EMBEDDED_CONNECTION,
                null);
        try (KernelTransaction tx = beginTransaction(loginContext)) {
            // when
            tx.dataWrite().nodeCreate();
            long countTxState = tx.dataRead().countsForNode(-1);

            // then
            assertEquals(3, countTxState);
        }
    }

    @Test
    void shouldNotCountRemovedLabelsFromTxStateRestrictedUser() throws Exception {
        // Given
        Node node1 = createNode("label");
        Node node2 = createNode("label");

        SecurityContext loginContext = new SecurityContext(
                AuthSubject.AUTH_DISABLED,
                new TestAccessMode(true, false, true, false, false),
                EMBEDDED_CONNECTION,
                null);
        try (KernelTransaction tx = beginTransaction(loginContext)) {
            // when
            tx.dataWrite().nodeRemoveLabel(node2.node, node2.labels[0]);
            long countTxState = tx.dataRead().countsForNode(node1.labels[0]);

            // then
            assertEquals(1, countTxState);
        }
    }

    @Test
    void shouldNotCountRemovedNodesFromTxStateRestrictedUser() throws Exception {
        // Given
        Node node1 = createNode("label");
        Node node2 = createNode("label");

        SecurityContext loginContext = new SecurityContext(
                AuthSubject.AUTH_DISABLED,
                new TestAccessMode(true, false, true, false, false),
                EMBEDDED_CONNECTION,
                null);
        try (KernelTransaction tx = beginTransaction(loginContext)) {
            // when
            tx.dataWrite().nodeDelete(node2.node);
            long countTxState = tx.dataRead().countsForNode(node1.labels[0]);

            // then
            assertEquals(1, countTxState);
        }
    }

    @Test
    void hasPropertiesShouldSeeNewlyCreatedProperties() throws Exception {
        // Given
        long node;
        try (KernelTransaction tx = beginTransaction()) {
            node = tx.dataWrite().nodeCreate();
            tx.commit();
        }

        // Then
        try (KernelTransaction tx = beginTransaction()) {
            try (NodeCursor cursor = tx.cursors().allocateNodeCursor(tx.cursorContext());
                    PropertyCursor props =
                            tx.cursors().allocatePropertyCursor(tx.cursorContext(), tx.memoryTracker())) {
                tx.dataRead().singleNode(node, cursor);
                assertTrue(cursor.next());
                assertFalse(hasProperties(cursor, props));
                tx.dataWrite()
                        .nodeSetProperty(
                                node, tx.tokenWrite().propertyKeyGetOrCreateForName("prop"), stringValue("foo"));
                assertTrue(hasProperties(cursor, props));
            }
        }
    }

    private static boolean hasProperties(NodeCursor cursor, PropertyCursor props) {
        cursor.properties(props);
        return props.next();
    }

    @Test
    void hasPropertiesShouldSeeNewlyCreatedPropertiesOnNewlyCreatedNode() throws Exception {
        try (KernelTransaction tx = beginTransaction()) {
            long node = tx.dataWrite().nodeCreate();
            try (NodeCursor cursor = tx.cursors().allocateNodeCursor(tx.cursorContext());
                    PropertyCursor props =
                            tx.cursors().allocatePropertyCursor(tx.cursorContext(), tx.memoryTracker())) {
                tx.dataRead().singleNode(node, cursor);
                assertTrue(cursor.next());
                assertFalse(hasProperties(cursor, props));
                tx.dataWrite()
                        .nodeSetProperty(
                                node, tx.tokenWrite().propertyKeyGetOrCreateForName("prop"), stringValue("foo"));
                assertTrue(hasProperties(cursor, props));
            }
        }
    }

    @Test
    void hasPropertiesShouldSeeNewlyRemovedProperties() throws Exception {
        // Given
        long node;
        int prop1, prop2, prop3;
        try (KernelTransaction tx = beginTransaction()) {
            node = tx.dataWrite().nodeCreate();
            prop1 = tx.tokenWrite().propertyKeyGetOrCreateForName("prop1");
            prop2 = tx.tokenWrite().propertyKeyGetOrCreateForName("prop2");
            prop3 = tx.tokenWrite().propertyKeyGetOrCreateForName("prop3");
            tx.dataWrite().nodeSetProperty(node, prop1, longValue(1));
            tx.dataWrite().nodeSetProperty(node, prop2, longValue(2));
            tx.dataWrite().nodeSetProperty(node, prop3, longValue(3));
            tx.commit();
        }

        // Then
        try (KernelTransaction tx = beginTransaction()) {
            try (NodeCursor cursor = tx.cursors().allocateNodeCursor(tx.cursorContext());
                    PropertyCursor props =
                            tx.cursors().allocatePropertyCursor(tx.cursorContext(), tx.memoryTracker())) {
                tx.dataRead().singleNode(node, cursor);
                assertTrue(cursor.next());

                assertTrue(hasProperties(cursor, props));
                tx.dataWrite().nodeRemoveProperty(node, prop1);
                assertTrue(hasProperties(cursor, props));
                tx.dataWrite().nodeRemoveProperty(node, prop2);
                assertTrue(hasProperties(cursor, props));
                tx.dataWrite().nodeRemoveProperty(node, prop3);
                assertFalse(hasProperties(cursor, props));
            }
        }
    }

    @Test
    void propertyTypeShouldBeTxStateAware() throws Exception {
        // Given
        long node;
        try (KernelTransaction tx = beginTransaction()) {
            node = tx.dataWrite().nodeCreate();
            tx.commit();
        }

        // Then
        try (KernelTransaction tx = beginTransaction()) {
            try (NodeCursor nodes = tx.cursors().allocateNodeCursor(tx.cursorContext());
                    PropertyCursor properties =
                            tx.cursors().allocatePropertyCursor(tx.cursorContext(), tx.memoryTracker())) {
                tx.dataRead().singleNode(node, nodes);
                assertTrue(nodes.next());
                assertFalse(hasProperties(nodes, properties));
                int prop = tx.tokenWrite().propertyKeyGetOrCreateForName("prop");
                tx.dataWrite().nodeSetProperty(node, prop, stringValue("foo"));
                nodes.properties(properties);

                assertTrue(properties.next());
                assertThat(properties.propertyType()).isEqualTo(ValueGroup.TEXT);
            }
        }
    }

    private static void assertLabels(TokenSet labels, int... expected) {
        assertEquals(expected.length, labels.numberOfTokens());
        Arrays.sort(expected);
        int[] labelArray = new int[labels.numberOfTokens()];
        for (int i = 0; i < labels.numberOfTokens(); i++) {
            labelArray[i] = labels.token(i);
        }
        Arrays.sort(labelArray);
        assertArrayEquals(expected, labelArray, "labels match expected");
    }

    public Node createNode(String... labels) throws Exception {
        long node;
        int[] labelIds = new int[labels.length];
        try (KernelTransaction tx = beginTransaction()) {
            Write write = tx.dataWrite();
            node = write.nodeCreate();

            for (int i = 0; i < labels.length; i++) {
                labelIds[i] = tx.tokenWrite().labelGetOrCreateForName(labels[i]);
                write.nodeAddLabel(node, labelIds[i]);
            }
            tx.commit();
        }
        return new Node(node, labelIds);
    }

    private static class Node {
        private final long node;
        private final int[] labels;

        private Node(long node, int[] labels) {
            this.node = node;
            this.labels = labels;
        }

        public long node() {
            return node;
        }

        public int[] labels() {
            return labels;
        }
    }

    private static TokenReadSession getTokenReadSession(KernelTransaction tx, EntityType entityType)
            throws IndexNotFoundKernelException {
        Iterator<IndexDescriptor> indexes = tx.schemaRead().index(SchemaDescriptors.forAnyEntityTokens(entityType));
        IndexDescriptor index = indexes.next();
        assertThat(indexes.hasNext()).isFalse();
        return tx.dataRead().tokenReadSession(index);
    }

    private void clearIdGeneratorCaches() {
        // Some tests assume node ID order to be according to creation order, clearing the Id generator cache ensures
        // this
        ((GraphDatabaseAPI) graphDb)
                .getDependencyResolver()
                .resolveDependency(IdGeneratorFactory.class)
                .clearCache(true, CursorContext.NULL_CONTEXT);
    }
}
