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
package org.neo4j.kernel.impl.event;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.internal.helpers.collection.Iterables.count;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.graphdb.event.TransactionEventListenerAdapter;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.recordstorage.TestRelType;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.event.GlobalTransactionEventListeners;
import org.neo4j.kernel.internal.event.InternalTransactionEventListener;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.TestLabels;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
class TestTransactionEvents {
    @Inject
    private DatabaseManagementService dbms;

    @Inject
    private GraphDatabaseAPI db;

    private static final TimeUnit AWAIT_INDEX_UNIT = TimeUnit.SECONDS;
    private static final int AWAIT_INDEX_DURATION = 60;

    @Test
    void forbidToRegisterTransactionEventListenerOnSystemDatabase() {
        assertThrows(
                IllegalArgumentException.class,
                () -> dbms.registerTransactionEventListener(
                        SYSTEM_DATABASE_NAME, new DummyTransactionEventListener<>(0)));
    }

    @Test
    void forbidToRegisterNullTransactionEventListener() {
        assertThrows(
                NullPointerException.class, () -> dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, null));
    }

    @Test
    void forbidToRegisterTransactionEventListenerForDatabaseNull() {
        assertThrows(
                NullPointerException.class,
                () -> dbms.registerTransactionEventListener(null, new DummyTransactionEventListener<>(0)));
    }

    @Test
    void testRegisterUnregisterListeners() {
        Object value1 = 10;
        Object value2 = 3.5D;
        DummyTransactionEventListener<Integer> listener1 = new DummyTransactionEventListener<>((Integer) value1);
        DummyTransactionEventListener<Double> listener2 = new DummyTransactionEventListener<>((Double) value2);

        assertThrows(
                IllegalStateException.class,
                () -> dbms.unregisterTransactionEventListener(DEFAULT_DATABASE_NAME, listener1));

        dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener1);
        dbms.unregisterTransactionEventListener(DEFAULT_DATABASE_NAME, listener1);

        assertThrows(
                IllegalStateException.class,
                () -> dbms.unregisterTransactionEventListener(DEFAULT_DATABASE_NAME, listener1));

        dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener1);
        dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener2);
        dbms.unregisterTransactionEventListener(DEFAULT_DATABASE_NAME, listener1);
        dbms.unregisterTransactionEventListener(DEFAULT_DATABASE_NAME, listener2);

        dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener1);
        try (Transaction tx = db.beginTx()) {
            tx.createNode().delete();
            tx.commit();
        }

        assertNotNull(listener1.beforeCommit);
        assertNotNull(listener1.afterCommit);
        assertNull(listener1.afterRollback);
        assertEquals(value1, listener1.receivedState);
        assertNotNull(listener1.receivedTransactionData);
        dbms.unregisterTransactionEventListener(DEFAULT_DATABASE_NAME, listener1);
    }

    @Test
    void makeSureListenersCantBeRegisteredTwice() {
        DummyTransactionEventListener<Object> listener = new DummyTransactionEventListener<>(null);
        dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        try (Transaction tx = db.beginTx()) {
            tx.createNode().delete();
            tx.commit();
        }
        assertEquals(Integer.valueOf(0), listener.beforeCommit);
        assertEquals(Integer.valueOf(1), listener.afterCommit);
        assertNull(listener.afterRollback);

        dbms.unregisterTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
    }

    @Test
    void onlyTransientErrorsShouldBeRethrown() {
        // Given
        DeadlockDetectedException transientException = new DeadlockDetectedException("transient error");
        ExceptionThrowingEventListener transientThrowingListener =
                new ExceptionThrowingEventListener(transientException, null, null);
        Exception otherException = new Exception("other error");
        ExceptionThrowingEventListener exceptionThrowingListener =
                new ExceptionThrowingEventListener(otherException, null, null);

        // When
        dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, transientThrowingListener);
        try (Transaction tx = db.beginTx()) {
            tx.createNode().delete();
            // Then
            assertThatThrownBy(tx::commit).isEqualTo(transientException);
        } finally {
            dbms.unregisterTransactionEventListener(DEFAULT_DATABASE_NAME, transientThrowingListener);
        }

        // When
        dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, exceptionThrowingListener);
        try (Transaction tx = db.beginTx()) {
            tx.createNode().delete();
            // Then
            assertThatThrownBy(tx::commit)
                    .isNotEqualTo(otherException)
                    .isInstanceOf(TransactionFailureException.class)
                    .hasRootCause(otherException);
        } finally {
            dbms.unregisterTransactionEventListener(DEFAULT_DATABASE_NAME, exceptionThrowingListener);
        }
    }

    @Test
    void shouldGetCorrectTransactionDataUponCommit() {
        // Create new data, nothing modified, just added/created
        ExpectedTransactionData expectedData = new ExpectedTransactionData();
        VerifyingTransactionEventListener listener = new VerifyingTransactionEventListener(expectedData);
        dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        Node node1;
        Node node2;
        Node node3;
        Relationship rel1;
        Relationship rel2;
        try {
            try (Transaction tx = db.beginTx()) {
                node1 = tx.createNode();
                expectedData.expectedCreatedNodes.add(node1);

                node2 = tx.createNode();
                expectedData.expectedCreatedNodes.add(node2);

                rel1 = node1.createRelationshipTo(node2, RelTypes.TXEVENT);
                expectedData.expectedCreatedRelationships.add(rel1);

                node1.setProperty("name", "Mattias");
                expectedData.assignedProperty(node1, "name", "Mattias", null);

                node1.setProperty("last name", "Persson");
                expectedData.assignedProperty(node1, "last name", "Persson", null);

                node1.setProperty("counter", 10);
                expectedData.assignedProperty(node1, "counter", 10, null);

                rel1.setProperty("description", "A description");
                expectedData.assignedProperty(rel1, "description", "A description", null);

                rel1.setProperty("number", 4.5D);
                expectedData.assignedProperty(rel1, "number", 4.5D, null);

                node3 = tx.createNode();
                expectedData.expectedCreatedNodes.add(node3);
                rel2 = node3.createRelationshipTo(node2, RelTypes.TXEVENT);
                expectedData.expectedCreatedRelationships.add(rel2);

                node3.setProperty("name", "Node 3");
                expectedData.assignedProperty(node3, "name", "Node 3", null);
                tx.commit();
            }

            assertTrue(listener.hasBeenCalled(), "Should have been invoked");
            Throwable failure = listener.failure();
            if (failure != null) {
                throw new RuntimeException(failure);
            }
        } finally {
            dbms.unregisterTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }

        // Use the above data and modify it, change properties, delete stuff
        expectedData = new ExpectedTransactionData();
        listener = new VerifyingTransactionEventListener(expectedData);
        dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        try {
            try (Transaction tx = db.beginTx()) {
                Node newNode = tx.createNode();
                expectedData.expectedCreatedNodes.add(newNode);

                Node tempNode = tx.createNode();
                Relationship tempRel = tempNode.createRelationshipTo(node1, RelTypes.TXEVENT);
                tempNode.setProperty("something", "Some value");
                tempRel.setProperty("someproperty", 101010);
                tempNode.removeProperty("nothing");

                node3 = tx.getNodeById(node3.getId());
                node3.setProperty("test", "hello");
                node3.setProperty("name", "No name");
                node3.delete();
                expectedData.expectedDeletedNodes.add(node3);
                expectedData.removedProperty(node3, "name", "Node 3");

                node1 = tx.getNodeById(node1.getId());
                node1.setProperty("new name", "A name");
                node1.setProperty("new name", "A better name");
                expectedData.assignedProperty(node1, "new name", "A better name", null);
                node1.setProperty("name", "Nothing");
                node1.setProperty("name", "Mattias Persson");
                expectedData.assignedProperty(node1, "name", "Mattias Persson", "Mattias");
                node1.removeProperty("counter");
                expectedData.removedProperty(node1, "counter", 10);
                node1.removeProperty("last name");
                node1.setProperty("last name", "Hi");
                expectedData.assignedProperty(node1, "last name", "Hi", "Persson");

                rel2 = tx.getRelationshipById(rel2.getId());
                rel2.delete();
                expectedData.expectedDeletedRelationships.add(rel2);

                rel1 = tx.getRelationshipById(rel1.getId());
                rel1.removeProperty("number");
                expectedData.removedProperty(rel1, "number", 4.5D);
                rel1.setProperty("description", "Ignored");
                rel1.setProperty("description", "New");
                expectedData.assignedProperty(rel1, "description", "New", "A description");

                tempRel.delete();
                tempNode.delete();
                tx.commit();
            }

            assertTrue(listener.hasBeenCalled(), "Should have been invoked");
            Throwable failure = listener.failure();
            if (failure != null) {
                throw new RuntimeException(failure);
            }
        } finally {
            dbms.unregisterTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }
    }

    @Test
    void makeSureBeforeAfterAreCalledCorrectly() {
        List<TransactionEventListener<Object>> listeners = new ArrayList<>();
        listeners.add(new FailingEventListener<>(new DummyTransactionEventListener<>(null), false));
        listeners.add(new FailingEventListener<>(new DummyTransactionEventListener<>(null), false));
        listeners.add(new FailingEventListener<>(new DummyTransactionEventListener<>(null), true));
        listeners.add(new FailingEventListener<>(new DummyTransactionEventListener<>(null), false));
        for (TransactionEventListener<Object> listener : listeners) {
            dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }

        try {
            assertThatThrownBy(() -> {
                        try (Transaction tx = db.beginTx()) {
                            tx.createNode().delete();
                            tx.commit();
                        }
                    })
                    .isInstanceOf(TransactionFailureException.class);
            verifyListenerCalls(listeners, false);

            dbms.unregisterTransactionEventListener(DEFAULT_DATABASE_NAME, listeners.remove(2));
            for (TransactionEventListener<Object> listener : listeners) {
                ((DummyTransactionEventListener<Object>) ((FailingEventListener<Object>) listener).source).reset();
            }
            try (Transaction transaction = db.beginTx()) {
                transaction.createNode().delete();
                transaction.commit();
            }
            verifyListenerCalls(listeners, true);
        } finally {
            for (TransactionEventListener<Object> listener : listeners) {
                dbms.unregisterTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
            }
        }
    }

    @Test
    void shouldBeAbleToAccessExceptionThrownInEventHook() {
        class MyFancyException extends Exception {}

        ExceptionThrowingEventListener listener =
                new ExceptionThrowingEventListener(new MyFancyException(), null, null);
        dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);

        try {
            Transaction tx = db.beginTx();
            try {
                tx.createNode().delete();
                tx.commit();
                fail("Should fail commit");
            } catch (TransactionFailureException e) {
                Throwable currentEx = e;
                do {
                    currentEx = currentEx.getCause();
                    if (currentEx instanceof MyFancyException) {
                        return;
                    }
                } while (currentEx.getCause() != null);
                fail("Expected to find the exception thrown in the event hook as the cause of transaction failure.");
            }
        } finally {
            dbms.unregisterTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }
    }

    @Test
    void deleteNodeRelTriggerPropertyRemoveEvents() {
        Node node1;
        Node node2;
        Relationship rel;
        try (Transaction tx = db.beginTx()) {
            node1 = tx.createNode();
            node2 = tx.createNode();
            rel = node1.createRelationshipTo(node2, RelTypes.TXEVENT);
            node1.setProperty("test1", "stringvalue");
            node1.setProperty("test2", 1L);
            rel.setProperty("test1", "stringvalue");
            rel.setProperty("test2", 1L);
            rel.setProperty("test3", new int[] {1, 2, 3});
            tx.commit();
        }
        MyTxEventListener listener = new MyTxEventListener();
        dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        try (Transaction tx = db.beginTx()) {
            tx.getRelationshipById(rel.getId()).delete();
            tx.getNodeById(node1.getId()).delete();
            tx.getNodeById(node2.getId()).delete();
            tx.commit();
        }
        assertEquals("stringvalue", listener.nodeProps.get("test1"));
        assertEquals("stringvalue", listener.relProps.get("test1"));
        assertEquals(1L, listener.nodeProps.get("test2"));
        assertEquals(1L, listener.relProps.get("test2"));
        int[] intArray = (int[]) listener.relProps.get("test3");
        assertEquals(3, intArray.length);
        assertEquals(1, intArray[0]);
        assertEquals(2, intArray[1]);
        assertEquals(3, intArray[2]);
    }

    @Test
    void makeSureListenerIsntCalledWhenTxRolledBack() {
        DummyTransactionEventListener<Integer> listener = new DummyTransactionEventListener<>(10);
        dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        try {
            try (Transaction tx = db.beginTx()) {
                tx.createNode().delete();
            }
            assertNull(listener.beforeCommit);
            assertNull(listener.afterCommit);
            assertNull(listener.afterRollback);
        } finally {
            dbms.unregisterTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }
    }

    @Test
    void modifiedPropertyCanByFurtherModifiedInBeforeCommit() {
        // Given
        // -- create node and set property on it in one transaction
        final String key = "key";
        final Object value1 = "the old value";
        final Object value2 = "the new value";
        final Node node;
        try (Transaction tx = db.beginTx()) {
            node = tx.createNode();
            node.setProperty(key, "initial value");
            tx.commit();
        }
        // -- register a tx listener which will override a property
        TransactionEventListener<Void> listener = new TransactionEventListenerAdapter<>() {
            @Override
            public Void beforeCommit(
                    TransactionData data, Transaction transaction, GraphDatabaseService databaseService) {
                Node modifiedNode =
                        data.assignedNodeProperties().iterator().next().entity();
                assertEquals(node, modifiedNode);
                modifiedNode.setProperty(key, value2);
                return null;
            }
        };
        dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);

        try (Transaction tx = db.beginTx()) {
            // When
            tx.getNodeById(node.getId()).setProperty(key, value1);
            tx.commit();
        }
        // Then
        try (Transaction transaction = db.beginTx()) {
            var n = transaction.getNodeById(node.getId());
            assertEquals(value2, n.getProperty(key));
        }
        dbms.unregisterTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
    }

    @Test
    void nodeCanBecomeSchemaIndexableInBeforeCommitByAddingProperty() {
        // Given we have a schema index...
        Label label = label("Label");
        try (Transaction tx = db.beginTx()) {
            tx.schema().indexFor(label).on("indexed").create();
            tx.commit();
        }

        // ... and a transaction event listener that likes to add the indexed property on nodes
        dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, new TransactionEventListenerAdapter<>() {
            @Override
            public Object beforeCommit(
                    TransactionData data, Transaction transaction, GraphDatabaseService databaseService) {
                Iterator<Node> nodes = data.createdNodes().iterator();
                if (nodes.hasNext()) {
                    Node node = nodes.next();
                    node.setProperty("indexed", "value");
                }
                return null;
            }
        });

        // When we create a node with the right label, but not the right property...
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(AWAIT_INDEX_DURATION, AWAIT_INDEX_UNIT);
            Node node = tx.createNode(label);
            node.setProperty("random", 42);
            tx.commit();
        }

        // Then we should be able to look it up through the index.
        try (Transaction tx = db.beginTx()) {
            Node node = tx.findNode(label, "indexed", "value");
            assertThat(node.getProperty("random")).isEqualTo(42);
        }
    }

    @Test
    void nodeCanBecomeSchemaIndexableInBeforeCommitByAddingLabel() {
        // Given we have a schema index...
        final Label label = label("Label");
        try (Transaction tx = db.beginTx()) {
            tx.schema().indexFor(label).on("indexed").create();
            tx.commit();
        }

        // ... and a transaction event listener that likes to add the indexed property on nodes
        dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, new TransactionEventListenerAdapter<>() {
            @Override
            public Object beforeCommit(
                    TransactionData data, Transaction transaction, GraphDatabaseService databaseService) {
                Iterator<Node> nodes = data.createdNodes().iterator();
                if (nodes.hasNext()) {
                    Node node = nodes.next();
                    node.addLabel(label);
                }
                return null;
            }
        });

        // When we create a node with the right property, but not the right label...
        try (Transaction tx = db.beginTx()) {
            tx.schema().awaitIndexesOnline(AWAIT_INDEX_DURATION, AWAIT_INDEX_UNIT);
            Node node = tx.createNode();
            node.setProperty("indexed", "value");
            node.setProperty("random", 42);
            tx.commit();
        }

        // Then we should be able to look it up through the index.
        try (Transaction tx = db.beginTx()) {
            Node node = tx.findNode(label, "indexed", "value");
            assertThat(node.getProperty("random")).isEqualTo(42);
        }
    }

    @Test
    void shouldAccessAssignedLabels() {
        // given
        ChangedLabels labels = new ChangedLabels();
        dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, labels);
        try {
            // when
            try (Transaction tx = db.beginTx()) {
                Node node1 = tx.createNode();
                Node node2 = tx.createNode();
                Node node3 = tx.createNode();

                labels.add(node1, "Foo");
                labels.add(node2, "Bar");
                labels.add(node3, "Baz");
                labels.add(node3, "Bar");

                labels.activate();
                tx.commit();
            }
            // then
            assertTrue(labels.isEmpty());
        } finally {
            dbms.unregisterTransactionEventListener(DEFAULT_DATABASE_NAME, labels);
        }
    }

    @Test
    void shouldAccessRemovedLabels() {
        // given

        ChangedLabels labels = new ChangedLabels();
        dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, labels);
        try {
            Node node1;
            Node node2;
            Node node3;
            try (Transaction tx = db.beginTx()) {
                node1 = tx.createNode();
                node2 = tx.createNode();
                node3 = tx.createNode();

                labels.add(node1, "Foo");
                labels.add(node2, "Bar");
                labels.add(node3, "Baz");
                labels.add(node3, "Bar");

                tx.commit();
            }
            labels.clear();

            // when
            try (Transaction tx = db.beginTx()) {
                labels.remove(tx.getNodeById(node1.getId()), "Foo");
                labels.remove(tx.getNodeById(node2.getId()), "Bar");
                labels.remove(tx.getNodeById(node3.getId()), "Baz");
                labels.remove(tx.getNodeById(node3.getId()), "Bar");

                labels.activate();
                tx.commit();
            }
            // then
            assertTrue(labels.isEmpty());
        } finally {
            dbms.unregisterTransactionEventListener(DEFAULT_DATABASE_NAME, labels);
        }
    }

    @Test
    void shouldAccessRelationshipDataInAfterCommit() {
        // GIVEN
        final AtomicInteger accessCount = new AtomicInteger();
        final Map<Long, RelationshipData> expectedRelationshipData = new HashMap<>();
        TransactionEventListener<Void> listener = new TransactionEventListenerAdapter<>() {
            @Override
            public void afterCommit(TransactionData data, Void state, GraphDatabaseService databaseService) {
                accessCount.set(0);
                try (Transaction tx = db.beginTx()) {
                    for (Relationship relationship : data.createdRelationships()) {
                        accessData(tx.getRelationshipById(relationship.getId()));
                    }
                    for (PropertyEntry<Relationship> change : data.assignedRelationshipProperties()) {
                        accessData(tx.getRelationshipById(change.entity().getId()));
                    }
                    for (PropertyEntry<Relationship> change : data.removedRelationshipProperties()) {
                        accessData(change.entity());
                    }
                    tx.commit();
                }
            }

            private void accessData(Relationship relationship) {
                accessCount.incrementAndGet();
                RelationshipData expectancy = expectedRelationshipData.get(relationship.getId());
                assertNotNull(expectancy);
                assertEquals(expectancy.startNode, relationship.getStartNode());
                assertEquals(expectancy.type, relationship.getType().name());
                assertEquals(expectancy.endNode, relationship.getEndNode());
            }
        };
        dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, listener);

        // WHEN
        try {
            Relationship relationship;
            try (Transaction tx = db.beginTx()) {
                relationship = tx.createNode().createRelationshipTo(tx.createNode(), MyRelTypes.TEST);
                expectedRelationshipData.put(relationship.getId(), new RelationshipData(relationship));
                tx.commit();
            }
            // THEN
            assertEquals(1, accessCount.get());

            // and WHEN
            try (Transaction tx = db.beginTx()) {
                relationship = tx.getRelationshipById(relationship.getId());
                relationship.setProperty("name", "Smith");
                Relationship otherRelationship =
                        tx.createNode().createRelationshipTo(tx.createNode(), MyRelTypes.TEST2);
                expectedRelationshipData.put(otherRelationship.getId(), new RelationshipData(otherRelationship));
                tx.commit();
            }
            // THEN
            assertEquals(2, accessCount.get());

            // and WHEN
            try (Transaction tx = db.beginTx()) {
                tx.getRelationshipById(relationship.getId()).delete();
                tx.commit();
            }
            // THEN
            assertEquals(1, accessCount.get());
        } finally {
            dbms.unregisterTransactionEventListener(DEFAULT_DATABASE_NAME, listener);
        }
    }

    @Test
    void shouldProvideTheCorrectRelationshipData() {
        // create a rel type so the next type id is non zero
        try (Transaction tx = db.beginTx()) {
            tx.createNode().createRelationshipTo(tx.createNode(), withName("TYPE"));
        }

        RelationshipType livesIn = withName("LIVES_IN");
        long relId;

        try (Transaction tx = db.beginTx()) {
            Node person = tx.createNode(label("Person"));

            Node city = tx.createNode(label("City"));

            Relationship rel = person.createRelationshipTo(city, livesIn);
            rel.setProperty("since", 2009);
            relId = rel.getId();
            tx.commit();
        }

        final Set<String> changedRelationships = new HashSet<>();

        dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, new TransactionEventListenerAdapter<Void>() {
            @Override
            public Void beforeCommit(
                    TransactionData data, Transaction transaction, GraphDatabaseService databaseService) {
                for (PropertyEntry<Relationship> entry : data.assignedRelationshipProperties()) {
                    changedRelationships.add(entry.entity().getType().name());
                }

                return null;
            }
        });

        try (Transaction tx = db.beginTx()) {
            Relationship rel = tx.getRelationshipById(relId);
            rel.setProperty("since", 2010);
            tx.commit();
        }

        assertEquals(1, changedRelationships.size());
        assertTrue(changedRelationships.contains(livesIn.name()), livesIn + " not in " + changedRelationships);
    }

    @Test
    void shouldNotFireEventForReadOnlyTransaction() {
        // GIVEN
        Node root = createTree(3, 3);
        dbms.registerTransactionEventListener(
                DEFAULT_DATABASE_NAME, new ExceptionThrowingEventListener(new RuntimeException("Just failing")));

        // WHEN
        try (Transaction tx = db.beginTx()) {
            count(tx.traversalDescription().traverse(tx.getNodeById(root.getId())));
            tx.commit();
        }
    }

    @Test
    void shouldNotFireEventForNonDataTransactions() {
        // GIVEN
        final AtomicInteger counter = new AtomicInteger();
        dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, new TransactionEventListenerAdapter<Void>() {
            @Override
            public Void beforeCommit(
                    TransactionData data, Transaction transaction, GraphDatabaseService databaseService) {
                assertTrue(
                        data.createdNodes().iterator().hasNext()
                                || data.createdRelationships().iterator().hasNext(),
                        "Expected only transactions that had nodes or relationships created");
                counter.incrementAndGet();
                return null;
            }
        });
        Label label = label("Label");
        String key = "key";
        assertEquals(0, counter.get());

        // WHEN creating a label token
        try (Transaction tx = db.beginTx()) {
            tx.createNode(label);
            tx.commit();
        }
        assertEquals(1, counter.get());
        // ... a property key token
        try (Transaction tx = db.beginTx()) {
            tx.createNode().setProperty(key, "value");
            tx.commit();
        }
        assertEquals(2, counter.get());
        // ... and a relationship type
        try (Transaction tx = db.beginTx()) {
            tx.createNode().createRelationshipTo(tx.createNode(), withName("A_TYPE"));
            tx.commit();
        }
        assertEquals(3, counter.get());
        // ... also when creating an index
        try (Transaction tx = db.beginTx()) {
            tx.schema().indexFor(label).on(key).create();
            tx.commit();
        }
        // ... or a constraint
        try (Transaction tx = db.beginTx()) {
            tx.schema().constraintFor(label).assertPropertyIsUnique("otherkey").create();
            tx.commit();
        }

        // THEN only three transaction events (all including graph data) should've been fired
        assertEquals(3, counter.get());
    }

    @Test
    void shouldBeAbleToTouchDataOutsideTxDataInAfterCommit() {
        // GIVEN
        final Node node = createNode("one", "Two", "three", "Four");
        dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, new TransactionEventListenerAdapter<>() {
            @Override
            public void afterCommit(TransactionData data, Object nothing, GraphDatabaseService databaseService) {
                try (Transaction tx = db.beginTx()) {
                    var listenerNode = tx.getNodeById(node.getId());
                    for (String key : listenerNode.getPropertyKeys()) { // Just to see if one can reach them
                        listenerNode.getProperty(key);
                    }
                    tx.commit();
                }
            }
        });

        try (Transaction tx = db.beginTx()) {
            // WHEN/THEN
            tx.createNode();
            tx.getNodeById(node.getId()).setProperty("five", "Six");
            tx.commit();
        }
    }

    @Test
    void shouldAllowToStringOnCreatedRelationshipInAfterCommit() {
        // GIVEN
        Relationship relationship;
        Node startNode;
        Node endNode;
        RelationshipType type = MyRelTypes.TEST;
        try (Transaction tx = db.beginTx()) {
            startNode = tx.createNode();
            endNode = tx.createNode();
            relationship = startNode.createRelationshipTo(endNode, type);
            tx.commit();
        }

        // WHEN
        AtomicReference<String> deletedToString = new AtomicReference<>();
        dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, new TransactionEventListenerAdapter<>() {
            @Override
            public void afterCommit(TransactionData data, Object state, GraphDatabaseService databaseService) {
                for (Relationship relationship : data.deletedRelationships()) {
                    deletedToString.set(relationship.toString());
                }
            }
        });
        try (Transaction tx = db.beginTx()) {
            tx.getRelationshipById(relationship.getId()).delete();
            tx.commit();
        }

        // THEN
        assertNotNull(deletedToString.get());
        assertThat(deletedToString.get()).contains(type.name());
        assertThat(deletedToString.get()).contains(format("(%d)", startNode.getId()));
        assertThat(deletedToString.get()).contains(format("(%d)", endNode.getId()));
    }

    @Test
    void shouldHaveTransactionIdInAfterCommit() {
        AtomicBoolean called = new AtomicBoolean();
        dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, new TransactionEventListenerAdapter<>() {
            @Override
            public void afterCommit(TransactionData data, Object state, GraphDatabaseService databaseService) {
                data.getTransactionId();
                called.set(true);
            }
        });
        long nodeId = 0;

        // Must not throw on plain write transactions.
        try (Transaction tx = db.beginTx()) {
            nodeId = tx.createNode().getId();
            tx.commit();
        }
        assertTrue(called.getAndSet(false));

        // Must not throw on plain read transactions.
        try (Transaction tx = db.beginTx()) {
            tx.getNodeById(nodeId);
            tx.commit();
        }
        assertFalse(called.getAndSet(false)); // No afterCommit on pure read transactions.

        // Must not throw on schema transactions.
        try (Transaction tx = db.beginTx()) {
            tx.schema().indexFor(label("Label")).on("prop").create();
            tx.commit();
        }
        assertFalse(called.getAndSet(false)); // No afterCommit when there's been no data changes.

        // Must not throw on zero-change write transactions.
        try (Transaction tx = db.beginTx()) {
            tx.createNode().delete();
            tx.commit();
        }
        assertTrue(called.getAndSet(false));
    }

    @Test
    void shouldGetCallToAfterRollbackEvenIfBeforeCommitFailed() {
        // given
        CapturingEventListener<Integer> firstWorkingListener = new CapturingEventListener<>(() -> 5);
        String failureMessage = "Massive fail";
        CapturingEventListener<Integer> faultyListener = new CapturingEventListener<>(() -> {
            throw new RuntimeException(failureMessage);
        });
        CapturingEventListener<Integer> otherWorkingListener = new CapturingEventListener<>(() -> 10);
        dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, firstWorkingListener);
        dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, faultyListener);
        dbms.registerTransactionEventListener(DEFAULT_DATABASE_NAME, otherWorkingListener);

        boolean failed = false;
        try (Transaction tx = db.beginTx()) {
            // when
            tx.createNode();
            tx.commit();
        } catch (Exception e) {
            assertTrue(Exceptions.contains(e, failureMessage, RuntimeException.class));
            failed = true;
        }
        assertTrue(failed);

        // then
        assertTrue(firstWorkingListener.beforeCommitCalled);
        assertTrue(firstWorkingListener.afterRollbackCalled);
        assertEquals(5, firstWorkingListener.afterRollbackState.intValue());
        assertTrue(faultyListener.beforeCommitCalled);
        assertTrue(faultyListener.afterRollbackCalled);
        assertNull(faultyListener.afterRollbackState);
        assertTrue(otherWorkingListener.beforeCommitCalled);
        assertTrue(otherWorkingListener.afterRollbackCalled);
        assertEquals(10, otherWorkingListener.afterRollbackState.intValue());
    }

    @Test
    void shouldNotInvokeListenersForInternalTokenTransactions() {
        var databaseName = DEFAULT_DATABASE_NAME;
        var listener = new CommitCountingEventListener();
        dbms.registerTransactionEventListener(databaseName, listener);

        var lastClosedTxIdBefore = lastClosedTxId(databaseName, dbms);

        // commit a transaction that introduces multiple tokens
        commitTxWithMultipleNewTokens(databaseName, dbms);

        var lastClosedTxIdAfter = lastClosedTxId(databaseName, dbms);

        // more than one transaction should be committed
        assertThat(lastClosedTxIdAfter).isGreaterThan(lastClosedTxIdBefore + 1);

        // but listener should be only invoked once
        assertEquals(1, listener.beforeCommitInvocations.get());
        assertEquals(1, listener.afterCommitInvocations.get());
        assertEquals(0, listener.afterRollbackInvocations.get());
    }

    @Test
    void shouldInvokeInternalListenersForInternalTokenTransactions() {
        var databaseName = DEFAULT_DATABASE_NAME;
        var listener = new CommitCountingEventInternalListener(); // note internal listener
        dbms.registerTransactionEventListener(databaseName, listener);

        var lastClosedTxIdBefore = lastClosedTxId(databaseName, dbms);

        // commit a transaction that introduces multiple tokens
        commitTxWithMultipleNewTokens(databaseName, dbms);

        var lastClosedTxIdAfter = lastClosedTxId(databaseName, dbms);

        // more than one transaction should be committed
        assertThat(lastClosedTxIdAfter).isGreaterThan(lastClosedTxIdBefore + 1);

        // and the listener should be invoked for all transactions since it is internal
        var committedTransactions = lastClosedTxIdAfter - lastClosedTxIdBefore;
        assertEquals(committedTransactions, listener.beforeCommitInvocations.get());
        assertEquals(committedTransactions, listener.afterCommitInvocations.get());
        assertEquals(0, listener.afterRollbackInvocations.get());
    }

    @Test
    void shouldInvokeListenersForAllTransactionsOnSystemDatabase() {
        var databaseName = SYSTEM_DATABASE_NAME;
        var listener = new CommitCountingEventListener();
        registerTransactionEventListenerForSystemDb(dbms, listener);

        var lastClosedTxIdBefore = lastClosedTxId(databaseName, dbms);

        // commit a transaction that introduces multiple tokens
        commitTxWithMultipleNewTokens(databaseName, dbms);

        var lastClosedTxIdAfter = lastClosedTxId(databaseName, dbms);
        var committedTransactions = lastClosedTxIdAfter - lastClosedTxIdBefore;

        // more than one transaction should be committed
        assertThat(committedTransactions).isGreaterThan(1L);

        // listener should be invoked the same number of times as the number of committed transactions
        assertEquals(committedTransactions, listener.beforeCommitInvocations.get());
        assertEquals(committedTransactions, listener.afterCommitInvocations.get());
        assertEquals(0, listener.afterRollbackInvocations.get());
    }

    @Test
    void shouldNotInvokeListenerForReadOnlyTransaction() {
        var databaseName = DEFAULT_DATABASE_NAME;
        var listener = new CommitCountingEventListener();
        dbms.registerTransactionEventListener(databaseName, listener);

        commitReadOnlyTransaction(databaseName, dbms);

        // listener should never be invoked
        assertEquals(0, listener.beforeCommitInvocations.get());
        assertEquals(0, listener.afterCommitInvocations.get());
        assertEquals(0, listener.afterRollbackInvocations.get());
    }

    @Test
    void shouldNotInvokeListenerForReadOnlySystemDatabaseTransaction() {
        var listener = new CommitCountingEventListener();
        registerTransactionEventListenerForSystemDb(dbms, listener);

        commitReadOnlyTransaction(SYSTEM_DATABASE_NAME, dbms);

        // listener should never be invoked
        assertEquals(0, listener.beforeCommitInvocations.get());
        assertEquals(0, listener.afterCommitInvocations.get());
        assertEquals(0, listener.afterRollbackInvocations.get());
    }

    private static void commitTxWithMultipleNewTokens(
            String databaseName, DatabaseManagementService managementService) {
        var db = managementService.database(databaseName);
        try (var tx = db.beginTx()) {
            var node = tx.createNode(TestLabels.LABEL_ONE, TestLabels.LABEL_TWO, TestLabels.LABEL_THREE);
            node.createRelationshipTo(node, TestRelType.LOOP);
            tx.commit();
        }
    }

    private static void commitReadOnlyTransaction(String databaseName, DatabaseManagementService managementService) {
        var db = managementService.database(databaseName);
        try (var tx = db.beginTx();
                ResourceIterable<Node> allNodes = tx.getAllNodes();
                var nodesIterator = allNodes.iterator()) {
            // perform some read-only activity
            while (nodesIterator.hasNext()) {
                assertNotNull(nodesIterator.next());
            }
            tx.commit();
        }
    }

    private static long lastClosedTxId(String databaseName, DatabaseManagementService managementService) {
        var db = (GraphDatabaseAPI) managementService.database(databaseName);
        var txIdStore = db.getDependencyResolver().resolveDependency(TransactionIdStore.class);
        return txIdStore.getLastClosedTransactionId();
    }

    private static void registerTransactionEventListenerForSystemDb(
            DatabaseManagementService managementService, TransactionEventListener<Object> listener) {
        var db = (GraphDatabaseAPI) managementService.database(SYSTEM_DATABASE_NAME);
        var globalListeners = db.getDependencyResolver().resolveDependency(GlobalTransactionEventListeners.class);
        globalListeners.registerTransactionEventListener(SYSTEM_DATABASE_NAME, listener);
    }

    private static class MyTxEventListener implements TransactionEventListener<Object> {
        Map<String, Object> nodeProps = new HashMap<>();
        Map<String, Object> relProps = new HashMap<>();

        @Override
        public void afterCommit(TransactionData data, Object state, GraphDatabaseService databaseService) {
            for (PropertyEntry<Node> entry : data.removedNodeProperties()) {
                String key = entry.key();
                Object value = entry.previouslyCommittedValue();
                nodeProps.put(key, value);
            }
            for (PropertyEntry<Relationship> entry : data.removedRelationshipProperties()) {
                relProps.put(entry.key(), entry.previouslyCommittedValue());
            }
        }

        @Override
        public void afterRollback(TransactionData data, Object state, GraphDatabaseService databaseService) {}

        @Override
        public Object beforeCommit(
                TransactionData data, Transaction transaction, GraphDatabaseService databaseService) {
            return null;
        }
    }

    private static void verifyListenerCalls(List<TransactionEventListener<Object>> listeners, boolean txSuccess) {
        for (TransactionEventListener<Object> listener : listeners) {
            DummyTransactionEventListener<Object> realListener =
                    (DummyTransactionEventListener<Object>) ((FailingEventListener<Object>) listener).source;
            if (txSuccess) {
                assertEquals(Integer.valueOf(0), realListener.beforeCommit);
                assertEquals(Integer.valueOf(1), realListener.afterCommit);
            } else {
                if (realListener.counter > 0) {
                    assertEquals(Integer.valueOf(0), realListener.beforeCommit);
                    assertEquals(Integer.valueOf(1), realListener.afterRollback);
                }
            }
        }
    }

    private enum RelTypes implements RelationshipType {
        TXEVENT
    }

    private static class FailingEventListener<T> implements TransactionEventListener<T> {
        private final TransactionEventListener<T> source;
        private final boolean willFail;

        FailingEventListener(TransactionEventListener<T> source, boolean willFail) {
            this.source = source;
            this.willFail = willFail;
        }

        @Override
        public void afterCommit(TransactionData data, T state, GraphDatabaseService databaseService) {
            source.afterCommit(data, state, databaseService);
        }

        @Override
        public void afterRollback(TransactionData data, T state, GraphDatabaseService databaseService) {
            source.afterRollback(data, state, databaseService);
        }

        @Override
        public T beforeCommit(TransactionData data, Transaction transaction, GraphDatabaseService databaseService)
                throws Exception {
            try {
                return source.beforeCommit(data, transaction, databaseService);
            } finally {
                if (willFail) {
                    throw new Exception("Just failing commit, that's all");
                }
            }
        }
    }

    private static class ExceptionThrowingEventListener implements TransactionEventListener<Object> {
        private final Exception beforeCommitException;
        private final Exception afterCommitException;
        private final Exception afterRollbackException;

        ExceptionThrowingEventListener(Exception exceptionForAll) {
            this(exceptionForAll, exceptionForAll, exceptionForAll);
        }

        ExceptionThrowingEventListener(
                Exception beforeCommitException, Exception afterCommitException, Exception afterRollbackException) {
            this.beforeCommitException = beforeCommitException;
            this.afterCommitException = afterCommitException;
            this.afterRollbackException = afterRollbackException;
        }

        @Override
        public Object beforeCommit(TransactionData data, Transaction transaction, GraphDatabaseService databaseService)
                throws Exception {
            if (beforeCommitException != null) {
                throw beforeCommitException;
            }
            return null;
        }

        @Override
        public void afterCommit(TransactionData data, Object state, GraphDatabaseService databaseService) {
            if (afterCommitException != null) {
                throw new RuntimeException(afterCommitException);
            }
        }

        @Override
        public void afterRollback(TransactionData data, Object state, GraphDatabaseService databaseService) {
            if (afterRollbackException != null) {
                throw new RuntimeException(afterRollbackException);
            }
        }
    }

    private static class DummyTransactionEventListener<T> implements TransactionEventListener<T> {
        private final T object;
        private TransactionData receivedTransactionData;
        private T receivedState;
        private int counter;
        private Integer beforeCommit;
        private Integer afterCommit;
        private Integer afterRollback;

        DummyTransactionEventListener(T object) {
            this.object = object;
        }

        @Override
        public void afterCommit(TransactionData data, T state, GraphDatabaseService databaseService) {
            assertNotNull(data);
            this.receivedState = state;
            this.afterCommit = counter++;
        }

        @Override
        public void afterRollback(TransactionData data, T state, GraphDatabaseService databaseService) {
            assertNotNull(data);
            this.receivedState = state;
            this.afterRollback = counter++;
        }

        @Override
        public T beforeCommit(TransactionData data, Transaction transaction, GraphDatabaseService databaseService) {
            assertNotNull(data);
            this.receivedTransactionData = data;
            this.beforeCommit = counter++;
            if (this.beforeCommit == 2) {
                new Exception("blabla").printStackTrace();
            }
            return object;
        }

        void reset() {
            receivedTransactionData = null;
            receivedState = null;
            counter = 0;
            beforeCommit = null;
            afterCommit = null;
            afterRollback = null;
        }
    }

    private Node createNode(String... properties) {
        try (Transaction tx = db.beginTx()) {
            Node node = tx.createNode();
            for (int i = 0; i < properties.length; i++) {
                node.setProperty(properties[i++], properties[i]);
            }
            tx.commit();
            return node;
        }
    }

    private Node createTree(int depth, int width) {
        try (Transaction tx = db.beginTx()) {
            Node root = tx.createNode(TestLabels.LABEL_ONE);
            createTree(tx, root, depth, width, 0);
            tx.commit();
            return root;
        }
    }

    private static void createTree(Transaction tx, Node parent, int maxDepth, int width, int currentDepth) {
        if (currentDepth > maxDepth) {
            return;
        }
        for (int i = 0; i < width; i++) {
            Node child = tx.createNode(TestLabels.LABEL_TWO);
            parent.createRelationshipTo(child, MyRelTypes.TEST);
            createTree(tx, child, maxDepth, width, currentDepth + 1);
        }
    }

    private static final class ChangedLabels extends TransactionEventListenerAdapter<Void> {
        private final Map<Node, Set<String>> added = new HashMap<>();
        private final Map<Node, Set<String>> removed = new HashMap<>();
        private boolean active;

        @Override
        public Void beforeCommit(TransactionData data, Transaction transaction, GraphDatabaseService databaseService) {
            if (active) {
                check(added, "added to", data.assignedLabels());
                check(removed, "removed from", data.removedLabels());
            }
            active = false;
            return null;
        }

        private static void check(Map<Node, Set<String>> expected, String change, Iterable<LabelEntry> changes) {
            for (LabelEntry entry : changes) {
                Set<String> labels = expected.get(entry.node());
                String message =
                        String.format("':%s' should not be %s %s", entry.label().name(), change, entry.node());
                assertNotNull(labels, message);
                assertTrue(labels.remove(entry.label().name()), message);
                if (labels.isEmpty()) {
                    expected.remove(entry.node());
                }
            }
            assertTrue(expected.isEmpty(), String.format("Expected more labels %s nodes: %s", change, expected));
        }

        public boolean isEmpty() {
            return added.isEmpty() && removed.isEmpty();
        }

        public void add(Node node, String label) {
            node.addLabel(label(label));
            put(added, node, label);
        }

        public void remove(Node node, String label) {
            node.removeLabel(label(label));
            put(removed, node, label);
        }

        private static void put(Map<Node, Set<String>> changes, Node node, String label) {
            Set<String> labels = changes.computeIfAbsent(node, k -> new HashSet<>());
            labels.add(label);
        }

        void activate() {
            assertFalse(isEmpty());
            active = true;
        }

        public void clear() {
            added.clear();
            removed.clear();
            active = false;
        }
    }

    private static class RelationshipData {
        final Node startNode;
        final String type;
        final Node endNode;

        RelationshipData(Relationship relationship) {
            this.startNode = relationship.getStartNode();
            this.type = relationship.getType().name();
            this.endNode = relationship.getEndNode();
        }
    }

    private static class CapturingEventListener<T> implements TransactionEventListener<T> {
        private final Supplier<T> stateSource;
        boolean beforeCommitCalled;
        boolean afterCommitCalled;
        T afterCommitState;
        boolean afterRollbackCalled;
        T afterRollbackState;

        CapturingEventListener(Supplier<T> stateSource) {
            this.stateSource = stateSource;
        }

        @Override
        public T beforeCommit(TransactionData data, Transaction transaction, GraphDatabaseService databaseService) {
            beforeCommitCalled = true;
            return stateSource.get();
        }

        @Override
        public void afterCommit(TransactionData data, T state, GraphDatabaseService databaseService) {
            afterCommitCalled = true;
            afterCommitState = state;
        }

        @Override
        public void afterRollback(TransactionData data, T state, GraphDatabaseService databaseService) {
            afterRollbackCalled = true;
            afterRollbackState = state;
        }
    }

    private static class CommitCountingEventBase {
        final AtomicInteger beforeCommitInvocations = new AtomicInteger();
        final AtomicInteger afterCommitInvocations = new AtomicInteger();
        final AtomicInteger afterRollbackInvocations = new AtomicInteger();

        public Object beforeCommit(
                TransactionData data, Transaction transaction, GraphDatabaseService databaseService) {
            beforeCommitInvocations.incrementAndGet();
            return null;
        }

        public void afterCommit(TransactionData data, Object state, GraphDatabaseService databaseService) {
            afterCommitInvocations.incrementAndGet();
        }

        public void afterRollback(TransactionData data, Object state, GraphDatabaseService databaseService) {
            afterRollbackInvocations.incrementAndGet();
        }
    }

    private static class CommitCountingEventListener extends CommitCountingEventBase
            implements TransactionEventListener<Object> {}

    private static class CommitCountingEventInternalListener extends CommitCountingEventBase
            implements InternalTransactionEventListener<Object> {}
}
