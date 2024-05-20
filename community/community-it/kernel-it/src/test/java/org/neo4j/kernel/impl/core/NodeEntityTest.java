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
package org.neo4j.kernel.impl.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.internal.helpers.NamedThreadFactory.named;
import static org.neo4j.test.DoubleLatch.awaitLatch;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
public class NodeEntityTest extends EntityTest {

    @Inject
    RandomSupport random;

    @Override
    protected long createEntity(Transaction tx) {
        return tx.createNode().getId();
    }

    @Override
    protected Entity lookupEntity(Transaction transaction, long id) {
        return transaction.getNodeById(id);
    }

    @Test
    void createDropNodeLongStringProperty(TestInfo testInfo) {
        Label markerLabel = Label.label("marker_" + testInfo.getTestMethod());
        String testPropertyKey = "testProperty";
        String propertyValue = RandomStringUtils.randomAscii(255);

        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode(markerLabel);
            node.setProperty(testPropertyKey, propertyValue);
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            Node node = Iterators.single(tx.findNodes(markerLabel));
            assertEquals(propertyValue, node.getProperty(testPropertyKey));
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            Node node = Iterators.single(tx.findNodes(markerLabel));
            node.removeProperty(testPropertyKey);
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            Node node = Iterators.single(tx.findNodes(markerLabel));
            assertFalse(node.hasProperty(testPropertyKey));
            tx.commit();
        }
    }

    @Test
    void createDropNodeLongArrayProperty(TestInfo testInfo) {
        Label markerLabel = Label.label("marker_" + testInfo.getTestMethod());
        String testPropertyKey = "testProperty";
        byte[] propertyValue = random.nextBytes(1024);

        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode(markerLabel);
            node.setProperty(testPropertyKey, propertyValue);
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            Node node = Iterators.single(tx.findNodes(markerLabel));
            assertArrayEquals(propertyValue, (byte[]) node.getProperty(testPropertyKey));
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            Node node = Iterators.single(tx.findNodes(markerLabel));
            node.removeProperty(testPropertyKey);
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            Node node = Iterators.single(tx.findNodes(markerLabel));
            assertFalse(node.hasProperty(testPropertyKey));
            tx.commit();
        }
    }

    @Test
    void deletionOfSameNodeTwiceInOneTransactionShouldNotRollbackIt() {
        // Given
        Node node;
        try (Transaction tx = db.beginTx()) {
            node = tx.createNode();
            tx.commit();
        }

        // When
        Exception exceptionThrownBySecondDelete = null;

        try (Transaction tx = db.beginTx()) {
            tx.getNodeById(node.getId()).delete();
            try {
                tx.getNodeById(node.getId()).delete();
            } catch (Exception e) {
                exceptionThrownBySecondDelete = e;
            }
            tx.commit();
        }

        // Then
        assertThat(exceptionThrownBySecondDelete).isInstanceOf(NotFoundException.class);

        assertThrows(NotFoundException.class, () -> {
            try (Transaction tx = db.beginTx()) {
                tx.getNodeById(node.getId()); // should throw NotFoundException
                tx.commit();
            }
        });
    }

    @Test
    void deletionOfAlreadyDeletedNodeShouldThrow() {
        // Given
        Node node;
        try (Transaction tx = db.beginTx()) {
            node = tx.createNode();
            tx.commit();
        }
        try (Transaction tx = db.beginTx()) {
            tx.getNodeById(node.getId()).delete();
            tx.commit();
        }

        // When
        assertThrows(NotFoundException.class, () -> {
            try (Transaction tx = db.beginTx()) {
                tx.getNodeById(node.getId()).delete(); // should throw NotFoundException as this node is already deleted
                tx.commit();
            }
        });
    }

    @Test
    void getAllPropertiesShouldWorkFineWithConcurrentPropertyModifications() throws Exception {
        // Given
        ExecutorService executor = Executors.newFixedThreadPool(2, named("Test-executor-thread"));
        try {
            final int propertiesCount = 100;

            final long nodeId;
            try (Transaction tx = db.beginTx()) {
                Node node = tx.createNode();
                nodeId = node.getId();
                for (int i = 0; i < propertiesCount; i++) {
                    node.setProperty("property-" + i, i);
                }
                tx.commit();
            }

            final CountDownLatch start = new CountDownLatch(1);
            final AtomicBoolean writerDone = new AtomicBoolean();

            Runnable writer = () -> {
                try {
                    awaitLatch(start);
                    int propertyKey = 0;
                    while (propertyKey < propertiesCount) {
                        try (Transaction tx = db.beginTx()) {
                            Node node = tx.getNodeById(nodeId);
                            for (int i = 0; i < 10 && propertyKey < propertiesCount; i++, propertyKey++) {
                                node.setProperty(
                                        "property-" + propertyKey,
                                        UUID.randomUUID().toString());
                            }
                            tx.commit();
                        }
                    }
                } finally {
                    writerDone.set(true);
                }
            };
            Runnable reader = () -> {
                try (Transaction tx = db.beginTx()) {
                    Node node = tx.getNodeById(nodeId);
                    awaitLatch(start);
                    while (!writerDone.get()) {
                        int size = node.getAllProperties().size();
                        assertThat(size).isGreaterThan(0);
                    }
                    tx.commit();
                }
            };

            Future<?> readerFuture = executor.submit(reader);
            Future<?> writerFuture = executor.submit(writer);

            start.countDown();

            // When
            writerFuture.get();
            readerFuture.get();

            // Then
            try (Transaction tx = db.beginTx()) {
                assertEquals(
                        propertiesCount,
                        tx.getNodeById(nodeId).getAllProperties().size());
                tx.commit();
            }
        } finally {
            executor.shutdown();
        }
    }

    @Test
    void shouldBeAbleToForceTypeChangeOfProperty() {
        // Given
        Node node;
        try (Transaction tx = db.beginTx()) {
            node = tx.createNode();
            node.setProperty("prop", 1337);
            tx.commit();
        }

        // When
        try (Transaction tx = db.beginTx()) {
            tx.getNodeById(node.getId()).setProperty("prop", 1337.0);
            tx.commit();
        }

        // Then
        try (Transaction tx = db.beginTx()) {
            assertThat(tx.getNodeById(node.getId()).getProperty("prop")).isInstanceOf(Double.class);
        }
    }

    @Test
    void shouldThrowCorrectExceptionOnLabelTokensExceeded() throws KernelException {
        // given
        var transaction = mockedTransactionWithDepletedTokens();
        NodeEntity nodeEntity = new NodeEntity(transaction, 5);

        // when
        assertThrows(ConstraintViolationException.class, () -> nodeEntity.addLabel(Label.label("Label")));
    }

    @Test
    void shouldThrowCorrectExceptionOnPropertyKeyTokensExceeded() throws KernelException {
        // given
        NodeEntity nodeEntity = new NodeEntity(mockedTransactionWithDepletedTokens(), 5);

        // when
        assertThrows(ConstraintViolationException.class, () -> nodeEntity.setProperty("key", "value"));
    }

    @Test
    void shouldThrowCorrectExceptionOnRelationshipTypeTokensExceeded() throws KernelException {
        // given
        InternalTransaction transaction = mockedTransactionWithDepletedTokens();
        NodeEntity nodeEntity = new NodeEntity(transaction, 5);

        // when
        assertThrows(
                ConstraintViolationException.class,
                () -> nodeEntity.createRelationshipTo(
                        new NodeEntity(transaction, 6), RelationshipType.withName("type")));
    }

    @Test
    void shouldWorkWithNodeElementIds() {
        // given
        String nodeId1;
        String nodeId2;
        try (Transaction tx = db.beginTx()) {
            var node1 = tx.createNode();
            node1.setProperty("name", "Node 1");
            nodeId1 = node1.getElementId();
            var node2 = tx.createNode();
            node2.setProperty("name", "Node 2");
            nodeId2 = node2.getElementId();
            tx.commit();
        }

        // when/then
        try (Transaction tx = db.beginTx()) {
            var node1 = tx.getNodeByElementId(nodeId1);
            var node2 = tx.getNodeByElementId(nodeId2);
            assertThat(node1.getProperty("name")).isEqualTo("Node 1");
            assertThat(node2.getProperty("name")).isEqualTo("Node 2");
            tx.commit();
        }
    }

    @Test
    void nodeNotFoundByElementId() {
        String elementId;
        try (Transaction transaction = db.beginTx()) {
            Node node = transaction.createNode();
            elementId = node.getElementId();
            transaction.rollback();
        }
        assertThatThrownBy(() -> {
                    try (Transaction tx = db.beginTx()) {
                        tx.getNodeByElementId(elementId);
                    }
                })
                .isInstanceOf(NotFoundException.class)
                .hasMessageContaining(elementId + " not found")
                .hasRootCauseInstanceOf(EntityNotFoundException.class)
                .rootCause()
                .hasMessageContaining("Unable to load NODE " + elementId);
    }

    @Test
    void shouldWorkWithRelationshipElementIds() {
        // given
        String relationshipId1;
        String relationshipId2;
        try (Transaction tx = db.beginTx()) {
            var relationship1 =
                    tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("KNOWS"));
            relationship1.setProperty("name", "Relationship 1");
            relationshipId1 = relationship1.getElementId();
            var relationship2 =
                    tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("KNOWS"));
            relationship2.setProperty("name", "Relationship 2");
            relationshipId2 = relationship2.getElementId();
            tx.commit();
        }

        // when/then
        try (Transaction tx = db.beginTx()) {
            var relationship1 = tx.getRelationshipByElementId(relationshipId1);
            var relationship2 = tx.getRelationshipByElementId(relationshipId2);
            assertThat(relationship1.getProperty("name")).isEqualTo("Relationship 1");
            assertThat(relationship2.getProperty("name")).isEqualTo("Relationship 2");
            tx.commit();
        }
    }

    @Test
    void shouldThrowCorrectExceptionOnTokensTransientFailureCreateNode() throws KernelException {
        var transaction = transactionWithTransientlyFailingTokenWrite();
        assertThrows(TransientTransactionFailureException.class, () -> transaction.createNode(Label.label("label")));
    }

    @Test
    void shouldThrowCorrectExceptionOnTokensTransientFailureAddLabel() throws KernelException {
        var transaction = transactionWithTransientlyFailingTokenWrite();
        NodeEntity nodeEntity = new NodeEntity(transaction, 5);
        assertThrows(TransientTransactionFailureException.class, () -> nodeEntity.addLabel(Label.label("Label")));
    }

    @Test
    void shouldThrowCorrectExceptionOnTokensTransientFailureSetProperty() throws KernelException {
        NodeEntity nodeEntity = new NodeEntity(transactionWithTransientlyFailingTokenWrite(), 5);
        assertThrows(TransientTransactionFailureException.class, () -> nodeEntity.setProperty("key", "value"));
    }

    @Test
    void shouldThrowCorrectExceptionOnTokensTransientFailureCreateRelationship() throws KernelException {
        InternalTransaction transaction = transactionWithTransientlyFailingTokenWrite();
        NodeEntity nodeEntity = new NodeEntity(transaction, 5);
        assertThrows(
                TransientTransactionFailureException.class,
                () -> nodeEntity.createRelationshipTo(
                        new NodeEntity(transaction, 6), RelationshipType.withName("type")));
    }
}
