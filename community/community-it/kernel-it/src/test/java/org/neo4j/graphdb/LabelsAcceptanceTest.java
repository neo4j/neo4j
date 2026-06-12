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
package org.neo4j.graphdb;

import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.db_format;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.helpers.collection.Iterables.asList;
import static org.neo4j.internal.helpers.collection.Iterables.count;
import static org.neo4j.internal.helpers.collection.Iterables.map;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.store.format.FormatFamily;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.util.concurrent.BinaryLatch;

@ImpermanentDbmsExtension
class LabelsAcceptanceTest {
    @Inject
    protected GraphDatabaseAPI db;

    private enum Labels implements Label {
        MY_LABEL,
        MY_OTHER_LABEL
    }

    /** https://github.com/neo4j/neo4j/issues/1279 */
    @Test
    void shouldInsertLabelsWithoutDuplicatingThem() {
        // Given
        String nodeId;

        // When
        try (Transaction tx = db.beginTx()) {
            var node = tx.createNode();
            node.addLabel(Labels.MY_LABEL);
            nodeId = node.getElementId();

            tx.commit();
        }

        // POST "FOOBAR"
        try (Transaction tx = db.beginTx()) {
            tx.getNodeByElementId(nodeId).addLabel(Labels.MY_LABEL);
            tx.commit();
        }

        // POST ["BAZQUX"]
        try (Transaction tx = db.beginTx()) {
            tx.getNodeByElementId(nodeId).addLabel(label("BAZQUX"));
            tx.commit();
        }
        // PUT ["BAZQUX"]
        try (Transaction tx = db.beginTx()) {
            var labeledNode = tx.getNodeByElementId(nodeId);
            for (Label label : labeledNode.getLabels()) {
                labeledNode.removeLabel(label);
            }
            labeledNode.addLabel(label("BAZQUX"));
            tx.commit();
        }

        // GET
        List<Label> labels = new ArrayList<>();
        try (Transaction tx = db.beginTx()) {
            var labeledNode = tx.getNodeByElementId(nodeId);
            for (Label label : labeledNode.getLabels()) {
                labels.add(label);
            }
            tx.commit();
        }
        assertEquals(1, labels.size(), labels.toString());
        assertEquals("BAZQUX", labels.get(0).name());
    }

    @Test
    void addingALabelUsingAValidIdentifierShouldSucceed() {
        // Given
        String myNodeId;

        // When
        try (Transaction tx = db.beginTx()) {
            var myNode = tx.createNode();
            myNode.addLabel(Labels.MY_LABEL);
            myNodeId = myNode.getElementId();

            tx.commit();
        }

        // Then
        try (Transaction transaction = db.beginTx()) {
            var node = transaction.getNodeByElementId(myNodeId);
            assertTrue(node.hasLabel(Labels.MY_LABEL));
        }
    }

    @Test
    void addingALabelUsingAnInvalidIdentifierShouldFail() {
        // When I set an empty label
        try (Transaction tx = db.beginTx()) {
            assertThrows(IllegalArgumentException.class, () -> tx.createNode().addLabel(label("")));
        }

        // And When I set a null label
        try (Transaction tx = db.beginTx()) {
            assertThrows(IllegalArgumentException.class, () -> tx.createNode().addLabel(() -> null));
        }
    }

    @Test
    void addingALabelThatAlreadyExistsBehavesAsNoOp() {
        // Given
        String myNodeId;

        // When
        try (Transaction tx = db.beginTx()) {
            var myNode = tx.createNode();
            myNode.addLabel(Labels.MY_LABEL);
            myNode.addLabel(Labels.MY_LABEL);
            myNodeId = myNode.getElementId();

            tx.commit();
        }

        // Then
        try (Transaction transaction = db.beginTx()) {
            var node = transaction.getNodeByElementId(myNodeId);
            assertTrue(node.hasLabel(Labels.MY_LABEL));
        }
    }

    @Test
    void oversteppingMaxNumberOfLabelsShouldFailGracefully() throws IOException {
        try (EphemeralFileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction()) {

            DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder()
                    .setFileSystem(fileSystem)
                    .noOpSystemGraphInitializer()
                    .setConfig(db_format, FormatFamily.ALIGNED.name())
                    .build();

            GraphDatabaseService graphDatabase = managementService.database(DEFAULT_DATABASE_NAME);
            IdGenerator idGenerator = ((GraphDatabaseAPI) graphDatabase)
                    .getDependencyResolver()
                    .resolveDependency(RecordStorageEngine.class)
                    .testAccessNeoStores()
                    .getLabelTokenStore()
                    .getIdGenerator();
            idGenerator.setHighId(Integer.MAX_VALUE + 1L);
            idGenerator.markHighestWrittenAtHighId();

            // When
            try (Transaction tx = graphDatabase.beginTx()) {
                assertThrows(
                        ConstraintViolationException.class,
                        () -> tx.createNode().addLabel(Labels.MY_LABEL));
            }

            managementService.shutdown();
        }
    }

    @Test
    void removingCommittedLabel() {
        // Given
        Label label = Labels.MY_LABEL;
        String myNode = createNode(db, label);

        // When
        try (Transaction tx = db.beginTx()) {
            tx.getNodeByElementId(myNode).removeLabel(label);
            tx.commit();
        }

        // Then
        try (Transaction transaction = db.beginTx()) {
            var node = transaction.getNodeByElementId(myNode);
            assertFalse(node.hasLabel(label));
        }
    }

    @Test
    void createNodeWithLabels() {
        // WHEN
        String node;
        try (Transaction tx = db.beginTx()) {
            node = tx.createNode(Labels.values()).getElementId();
            tx.commit();
        }

        // THEN

        Set<String> names = Stream.of(Labels.values()).map(Labels::name).collect(toSet());
        try (Transaction transaction = db.beginTx()) {
            var n = transaction.getNodeByElementId(node);
            for (String labelName : names) {
                assertTrue(n.hasLabel(label(labelName)));
            }
        }
    }

    @Test
    void removingNonExistentLabel() {
        // Given
        Label label = Labels.MY_LABEL;

        // When
        String myNodeId;
        try (Transaction tx = db.beginTx()) {
            var myNode = tx.createNode();
            myNodeId = myNode.getElementId();
            myNode.removeLabel(label);
            tx.commit();
        }

        // THEN
        try (Transaction transaction = db.beginTx()) {
            var node = transaction.getNodeByElementId(myNodeId);
            assertFalse(node.hasLabel(label));
        }
    }

    @Test
    void removingExistingLabelFromUnlabeledNode() {
        // Given
        Label label = Labels.MY_LABEL;
        createNode(db, label);
        String myNode = createNode(db);

        // When
        try (Transaction tx = db.beginTx()) {
            tx.getNodeByElementId(myNode).removeLabel(label);
            tx.commit();
        }

        // THEN
        try (Transaction transaction = db.beginTx()) {
            var node = transaction.getNodeByElementId(myNode);
            assertFalse(node.hasLabel(label));
        }
    }

    @Test
    void removingUncommittedLabel() {
        // Given
        Label label = Labels.MY_LABEL;

        // When
        Node myNode;
        try (Transaction tx = db.beginTx()) {
            myNode = tx.createNode();
            myNode.addLabel(label);
            myNode.removeLabel(label);

            // THEN
            assertFalse(myNode.hasLabel(label));

            tx.commit();
        }
    }

    @Test
    void shouldBeAbleToListLabelsForANode() {
        // GIVEN
        String nodeId;
        Set<String> expected = asSet(Labels.MY_LABEL.name(), Labels.MY_OTHER_LABEL.name());
        try (Transaction tx = db.beginTx()) {
            var node = tx.createNode();
            nodeId = node.getElementId();
            for (String label : expected) {
                node.addLabel(label(label));
            }
            tx.commit();
        }

        try (Transaction transaction = db.beginTx()) {
            var n = transaction.getNodeByElementId(nodeId);
            for (String label : expected) {
                assertTrue(n.hasLabel(label(label)));
            }
        }
    }

    @Test
    void shouldReturnEmptyListIfNoLabels() {
        // GIVEN
        String node = createNode(db);

        // WHEN THEN
        try (Transaction transaction = db.beginTx()) {
            var n = transaction.getNodeByElementId(node);
            assertEquals(0, count(n.getLabels()));
        }
    }

    @Test
    void getNodesWithLabelCommitted() {
        // When
        Node node;
        try (Transaction tx = db.beginTx()) {
            node = tx.createNode();
            node.addLabel(Labels.MY_LABEL);
            tx.commit();
        }

        // THEN
        try (Transaction transaction = db.beginTx()) {
            assertTrue(Iterators.count(transaction.findNodes(Labels.MY_LABEL)) > 0);
            assertEquals(0, Iterators.count(transaction.findNodes(Labels.MY_OTHER_LABEL)));
        }
    }

    @Test
    void getNodesWithLabelsWithTxAddsAndRemoves() {
        // GIVEN
        String node1Id = createNode(db, Labels.MY_LABEL, Labels.MY_OTHER_LABEL);
        String node2Id = createNode(db, Labels.MY_LABEL, Labels.MY_OTHER_LABEL);

        // WHEN
        try (Transaction tx = db.beginTx()) {
            Node node1 = tx.getNodeByElementId(node1Id);
            Node node2 = tx.getNodeByElementId(node2Id);
            Node node3 = tx.createNode(Labels.MY_LABEL);
            node2.removeLabel(Labels.MY_LABEL);
            // THEN
            assertEquals(asSet(node1, node3), asSet(tx.findNodes(Labels.MY_LABEL)));
            assertEquals(asSet(node1, node2), asSet(tx.findNodes(Labels.MY_OTHER_LABEL)));
            tx.commit();
        }
    }

    @Test
    void shouldListAllExistingLabels() {
        // Given
        createNode(db, Labels.MY_LABEL, Labels.MY_OTHER_LABEL);
        List<Label> labels;

        // When
        try (Transaction transaction = db.beginTx()) {
            labels = asList(transaction.getAllLabels());
        }

        // Then
        assertEquals(2, labels.size());
        assertThat(map(labels, Label::name)).contains(Labels.MY_LABEL.name(), Labels.MY_OTHER_LABEL.name());
    }

    @Test
    void shouldListAllLabelsInUse() {
        // Given
        createNode(db, Labels.MY_LABEL);
        String node = createNode(db, Labels.MY_OTHER_LABEL);
        try (Transaction tx = db.beginTx()) {
            tx.getNodeByElementId(node).delete();
            tx.commit();
        }

        // When
        List<Label> labels;
        try (Transaction tx = db.beginTx()) {
            labels = asList(tx.getAllLabelsInUse());
        }

        // Then
        assertEquals(1, labels.size());
        assertThat(map(labels, Label::name)).contains(Labels.MY_LABEL.name());
    }

    @Test
    void shouldListAllLabelsInUseEvenWhenExclusiveLabelLocksAreTaken() {
        assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
            // Given
            createNode(db, Labels.MY_LABEL);
            String node = createNode(db, Labels.MY_OTHER_LABEL);
            try (Transaction tx = db.beginTx()) {
                tx.getNodeByElementId(node).delete();
                tx.commit();
            }

            BinaryLatch indexCreateStarted = new BinaryLatch();
            BinaryLatch indexCreateAllowToFinish = new BinaryLatch();
            Thread indexCreator = new Thread(() -> {
                try (Transaction tx = db.beginTx()) {
                    tx.schema().indexFor(Labels.MY_LABEL).on("prop").create();
                    indexCreateStarted.release();
                    indexCreateAllowToFinish.await();
                    tx.commit();
                }
            });
            indexCreator.start();

            // When
            indexCreateStarted.await();
            List<Label> labels;
            try (Transaction tx = db.beginTx()) {
                labels = asList(tx.getAllLabelsInUse());
            }
            indexCreateAllowToFinish.release();
            indexCreator.join();

            // Then
            assertEquals(1, labels.size());
            assertThat(map(labels, Label::name)).contains(Labels.MY_LABEL.name());
        });
    }

    @Test
    void shouldListAllRelationshipTypesInUseEvenWhenExclusiveRelationshipTypeLocksAreTaken() {
        assertTimeoutPreemptively(Duration.ofSeconds(30), () -> {
            // Given
            RelationshipType relType = RelationshipType.withName("REL");
            String nodeId = createNode(db, Labels.MY_LABEL);
            try (Transaction tx = db.beginTx()) {
                var node = tx.getNodeByElementId(nodeId);
                node.createRelationshipTo(node, relType).setProperty("prop", "val");
                tx.commit();
            }

            BinaryLatch indexCreateStarted = new BinaryLatch();
            BinaryLatch indexCreateAllowToFinish = new BinaryLatch();
            Thread indexCreator = new Thread(() -> {
                try (Transaction tx = db.beginTx()) {
                    tx.execute("CREATE FULLTEXT INDEX myIndex FOR ()-[r:REL]-() ON EACH [r.prop]")
                            .close();
                    indexCreateStarted.release();
                    indexCreateAllowToFinish.await();
                    tx.commit();
                }
            });
            indexCreator.start();

            // When
            indexCreateStarted.await();
            List<RelationshipType> relTypes;
            try (Transaction transaction = db.beginTx()) {
                relTypes = asList(transaction.getAllRelationshipTypesInUse());
            }
            indexCreateAllowToFinish.release();
            indexCreator.join();

            // Then
            assertEquals(1, relTypes.size());
            assertThat(map(relTypes, RelationshipType::name)).contains(relType.name());
        });
    }

    @Test
    void deleteAllNodesAndTheirLabels() {
        // GIVEN
        final Label label = label("A");
        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode();
            node.addLabel(label);
            node.setProperty("name", "bla");
            tx.commit();
        }

        // WHEN
        try (Transaction tx = db.beginTx();
                ResourceIterable<Node> allNodes = tx.getAllNodes()) {
            for (final Node node : allNodes) {
                node.removeLabel(label); // remove Label ...
                node.delete(); // ... and afterwards the node
            }
            tx.commit();
        } // tx.close(); - here comes the exception

        // THEN
        try (Transaction tx = db.beginTx()) {
            assertEquals(0, count(tx.getAllNodes()));
        }
    }

    @Test
    void removingLabelDoesNotBreakPreviouslyCreatedLabelsIterator() {
        // GIVEN
        Label label1 = label("A");
        Label label2 = label("B");

        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode(label1, label2);

            for (Label next : node.getLabels()) {
                node.removeLabel(next);
            }
            tx.commit();
        }
    }

    @Test
    void removingPropertyDoesNotBreakPreviouslyCreatedNodePropertyKeysIterator() {
        // GIVEN
        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode();
            node.setProperty("name", "Horst");
            node.setProperty("age", "72");

            for (String key : node.getPropertyKeys()) {
                node.removeProperty(key);
            }
            tx.commit();
        }
    }

    @Test
    void shouldCreateNodeWithLotsOfLabelsAndThenRemoveMostOfThem() {
        // given
        final int TOTAL_NUMBER_OF_LABELS = 200;
        final int NUMBER_OF_PRESERVED_LABELS = 20;
        String nodeId;
        try (Transaction tx = db.beginTx()) {
            var node = tx.createNode();
            nodeId = node.getElementId();
            for (int i = 0; i < TOTAL_NUMBER_OF_LABELS; i++) {
                node.addLabel(label("label:" + i));
            }

            tx.commit();
        }

        // when
        try (Transaction tx = db.beginTx()) {
            var labeledNode = tx.getNodeByElementId(nodeId);
            for (int i = NUMBER_OF_PRESERVED_LABELS; i < TOTAL_NUMBER_OF_LABELS; i++) {
                labeledNode.removeLabel(label("label:" + i));
            }

            tx.commit();
        }

        // then
        try (Transaction tx = db.beginTx()) {
            List<String> labels = new ArrayList<>();
            var labeledNode = tx.getNodeByElementId(nodeId);
            for (Label label : labeledNode.getLabels()) {
                labels.add(label.name());
            }
            assertEquals(NUMBER_OF_PRESERVED_LABELS, labels.size(), "labels on node: " + labels);
        }
    }

    @Test
    void shouldAllowManyLabelsAndPropertyCursor() {
        int propertyCount = 10;
        int labelCount = 15;

        String nodeId;
        try (Transaction tx = db.beginTx()) {
            var node = tx.createNode();
            nodeId = node.getElementId();
            for (int i = 0; i < propertyCount; i++) {
                node.setProperty("foo" + i, "bar");
            }
            for (int i = 0; i < labelCount; i++) {
                node.addLabel(label("label" + i));
            }
            tx.commit();
        }

        Set<Integer> seenProperties = new HashSet<>();
        Set<Integer> seenLabels = new HashSet<>();
        try (InternalTransaction tx = (InternalTransaction) db.beginTx()) {
            KernelTransaction ktx = tx.kernelTransaction();
            try (NodeCursor nodes = ktx.cursors().allocateNodeCursor(CursorContext.NULL_CONTEXT);
                    PropertyCursor propertyCursor =
                            ktx.cursors().allocatePropertyCursor(CursorContext.NULL_CONTEXT, INSTANCE)) {
                ktx.dataRead().singleNode(tx.elementIdMapper().nodeId(nodeId), nodes);
                while (nodes.next()) {
                    nodes.properties(propertyCursor);
                    while (propertyCursor.next()) {
                        seenProperties.add(propertyCursor.propertyKey());
                    }

                    TokenSet labels = nodes.labels();
                    for (int i = 0; i < labels.numberOfTokens(); i++) {
                        seenLabels.add(labels.token(i));
                    }
                }
            }
            tx.commit();
        }

        assertEquals(propertyCount, seenProperties.size());
        assertEquals(labelCount, seenLabels.size());
    }

    @Test
    void nodeWithManyLabels() {
        int labels = 500;
        int halveLabels = labels / 2;
        String nodeId = createNode(db);

        addLabels(nodeId, 0, halveLabels);
        addLabels(nodeId, halveLabels, halveLabels);

        verifyLabels(nodeId, 0, labels);

        removeLabels(nodeId, halveLabels, halveLabels);
        verifyLabels(nodeId, 0, halveLabels);

        removeLabels(nodeId, 0, halveLabels - 2);
        verifyLabels(nodeId, halveLabels - 2, 2);
    }

    private void addLabels(String nodeId, int startLabelIndex, int count) {
        try (Transaction tx = db.beginTx()) {
            Node node = tx.getNodeByElementId(nodeId);
            int endLabelIndex = startLabelIndex + count;
            for (int i = startLabelIndex; i < endLabelIndex; i++) {
                node.addLabel(labelWithIndex(i));
            }
            tx.commit();
        }
    }

    private void verifyLabels(String nodeId, int startLabelIndex, int count) {
        try (Transaction tx = db.beginTx()) {
            Node node = tx.getNodeByElementId(nodeId);
            Set<String> labelNames =
                    Iterables.asList(node.getLabels()).stream().map(Label::name).collect(toSet());

            assertEquals(count, labelNames.size());
            int endLabelIndex = startLabelIndex + count;
            for (int i = startLabelIndex; i < endLabelIndex; i++) {
                assertTrue(labelNames.contains(labelName(i)));
            }
            tx.commit();
        }
    }

    private void removeLabels(String nodeId, int startLabelIndex, int count) {
        try (Transaction tx = db.beginTx()) {
            Node node = tx.getNodeByElementId(nodeId);
            int endLabelIndex = startLabelIndex + count;
            for (int i = startLabelIndex; i < endLabelIndex; i++) {
                node.removeLabel(labelWithIndex(i));
            }
            tx.commit();
        }
    }

    private static Label labelWithIndex(int index) {
        return label(labelName(index));
    }

    private static String labelName(int index) {
        return "Label-" + index;
    }

    private static String createNode(GraphDatabaseService db, Label... labels) {
        try (Transaction tx = db.beginTx()) {
            String node = tx.createNode(labels).getElementId();
            tx.commit();
            return node;
        }
    }
}
