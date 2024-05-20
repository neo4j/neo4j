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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.internal.helpers.collection.Iterables.addAll;
import static org.neo4j.kernel.impl.MyRelTypes.TEST;
import static org.neo4j.kernel.impl.MyRelTypes.TEST2;
import static org.neo4j.kernel.impl.MyRelTypes.TEST_TRAVERSAL;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;

class TestRelationship extends AbstractNeo4jTestCase {
    private static final String key1 = "key1";
    private static final String key2 = "key2";
    private static final String key3 = "key3";

    @Test
    void testSimple1() {
        Node node1 = createNode();
        Node node2 = createNode();
        try (Transaction transaction = getGraphDb().beginTx()) {
            node1 = transaction.getNodeById(node1.getId());
            node2 = transaction.getNodeById(node2.getId());

            for (int i = 0; i < 3; i++) {
                node1.createRelationshipTo(node2, TEST);
                node1.createRelationshipTo(node2, TEST_TRAVERSAL);
                node1.createRelationshipTo(node2, TEST2);
            }
            allGetRelationshipMethods(node1, Direction.OUTGOING);
            allGetRelationshipMethods(node2, Direction.INCOMING);
            transaction.commit();
        }
        try (Transaction transaction = getGraphDb().beginTx()) {
            node1 = transaction.getNodeById(node1.getId());
            node2 = transaction.getNodeById(node2.getId());

            allGetRelationshipMethods(node1, Direction.OUTGOING);
            allGetRelationshipMethods(node2, Direction.INCOMING);
            deleteFirst(node1.getRelationships(Direction.OUTGOING, TEST));
            deleteFirst(node1.getRelationships(Direction.OUTGOING, TEST_TRAVERSAL));
            deleteFirst(node1.getRelationships(Direction.OUTGOING, TEST2));
            node1.createRelationshipTo(node2, TEST);
            node1.createRelationshipTo(node2, TEST_TRAVERSAL);
            node1.createRelationshipTo(node2, TEST2);
            allGetRelationshipMethods(node1, Direction.OUTGOING);
            allGetRelationshipMethods(node2, Direction.INCOMING);
            transaction.commit();
        }
        try (Transaction transaction = getGraphDb().beginTx()) {
            node1 = transaction.getNodeById(node1.getId());
            node2 = transaction.getNodeById(node2.getId());

            allGetRelationshipMethods(node1, Direction.OUTGOING);
            allGetRelationshipMethods(node2, Direction.INCOMING);
            Iterables.forEach(node1.getRelationships(), Relationship::delete);
            node1.delete();
            node2.delete();
            transaction.commit();
        }
    }

    @Test
    void testSimple2() {
        Node node1 = createNode();
        Node node2 = createNode();
        try (Transaction transaction = getGraphDb().beginTx()) {
            node1 = transaction.getNodeById(node1.getId());
            node2 = transaction.getNodeById(node2.getId());

            for (int i = 0; i < 1; i++) {
                node1.createRelationshipTo(node2, TEST);
                node1.createRelationshipTo(node2, TEST_TRAVERSAL);
                node1.createRelationshipTo(node2, TEST2);
            }
            allGetRelationshipMethods2(node1, Direction.OUTGOING);
            allGetRelationshipMethods2(node2, Direction.INCOMING);
            transaction.commit();
        }
        try (Transaction transaction = getGraphDb().beginTx()) {
            node1 = transaction.getNodeById(node1.getId());
            node2 = transaction.getNodeById(node2.getId());

            allGetRelationshipMethods2(node1, Direction.OUTGOING);
            allGetRelationshipMethods2(node2, Direction.INCOMING);
            deleteFirst(node1.getRelationships(Direction.OUTGOING, TEST));
            deleteFirst(node1.getRelationships(Direction.OUTGOING, TEST_TRAVERSAL));
            deleteFirst(node1.getRelationships(Direction.OUTGOING, TEST2));
            node1.createRelationshipTo(node2, TEST);
            node1.createRelationshipTo(node2, TEST_TRAVERSAL);
            node1.createRelationshipTo(node2, TEST2);
            allGetRelationshipMethods2(node1, Direction.OUTGOING);
            allGetRelationshipMethods2(node2, Direction.INCOMING);
            transaction.commit();
        }
        try (Transaction transaction = getGraphDb().beginTx()) {
            node1 = transaction.getNodeById(node1.getId());
            node2 = transaction.getNodeById(node2.getId());

            allGetRelationshipMethods2(node1, Direction.OUTGOING);
            allGetRelationshipMethods2(node2, Direction.INCOMING);
            Iterables.forEach(node1.getRelationships(), Relationship::delete);
            node1.delete();
            node2.delete();
            transaction.commit();
        }
    }

    @Test
    void testSimple3() {
        Node node1 = createNode();
        Node node2 = createNode();
        try (Transaction transaction = getGraphDb().beginTx()) {
            node1 = transaction.getNodeById(node1.getId());
            node2 = transaction.getNodeById(node2.getId());

            for (int i = 0; i < 2; i++) {
                node1.createRelationshipTo(node2, TEST);
                node1.createRelationshipTo(node2, TEST_TRAVERSAL);
                node1.createRelationshipTo(node2, TEST2);
            }
            allGetRelationshipMethods3(node1, Direction.OUTGOING);
            allGetRelationshipMethods3(node2, Direction.INCOMING);
            transaction.commit();
        }
        try (Transaction transaction = getGraphDb().beginTx()) {
            node1 = transaction.getNodeById(node1.getId());
            node2 = transaction.getNodeById(node2.getId());

            allGetRelationshipMethods3(node1, Direction.OUTGOING);
            allGetRelationshipMethods3(node2, Direction.INCOMING);
            deleteFirst(node1.getRelationships(Direction.OUTGOING, TEST));
            deleteAtIndex(node1.getRelationships(Direction.OUTGOING, TEST_TRAVERSAL), 1);
            deleteFirst(node1.getRelationships(Direction.OUTGOING, TEST2));
            node1.createRelationshipTo(node2, TEST);
            node1.createRelationshipTo(node2, TEST_TRAVERSAL);
            node1.createRelationshipTo(node2, TEST2);
            allGetRelationshipMethods3(node1, Direction.OUTGOING);
            allGetRelationshipMethods3(node2, Direction.INCOMING);
            transaction.commit();
        }
        try (Transaction transaction = getGraphDb().beginTx()) {
            node1 = transaction.getNodeById(node1.getId());
            node2 = transaction.getNodeById(node2.getId());

            allGetRelationshipMethods3(node1, Direction.OUTGOING);
            allGetRelationshipMethods3(node2, Direction.INCOMING);
            Iterables.forEach(node1.getRelationships(), Relationship::delete);
            node1.delete();
            node2.delete();
            transaction.commit();
        }
    }

    private static void allGetRelationshipMethods(Node node, Direction dir) {
        countRelationships(9, node.getRelationships());
        countRelationships(9, node.getRelationships(dir));
        countRelationships(9, node.getRelationships(TEST, TEST2, TEST_TRAVERSAL));
        countRelationships(6, node.getRelationships(TEST, TEST2));
        countRelationships(6, node.getRelationships(TEST, TEST_TRAVERSAL));
        countRelationships(6, node.getRelationships(TEST2, TEST_TRAVERSAL));
        countRelationships(3, node.getRelationships(TEST));
        countRelationships(3, node.getRelationships(TEST2));
        countRelationships(3, node.getRelationships(TEST_TRAVERSAL));
        countRelationships(3, node.getRelationships(dir, TEST));
        countRelationships(3, node.getRelationships(dir, TEST2));
        countRelationships(3, node.getRelationships(dir, TEST_TRAVERSAL));
    }

    private static void allGetRelationshipMethods2(Node node, Direction dir) {
        countRelationships(3, node.getRelationships());
        countRelationships(3, node.getRelationships(dir));
        countRelationships(3, node.getRelationships(TEST, TEST2, TEST_TRAVERSAL));
        countRelationships(2, node.getRelationships(TEST, TEST2));
        countRelationships(2, node.getRelationships(TEST, TEST_TRAVERSAL));
        countRelationships(2, node.getRelationships(TEST2, TEST_TRAVERSAL));
        countRelationships(1, node.getRelationships(TEST));
        countRelationships(1, node.getRelationships(TEST2));
        countRelationships(1, node.getRelationships(TEST_TRAVERSAL));
        countRelationships(1, node.getRelationships(dir, TEST));
        countRelationships(1, node.getRelationships(dir, TEST2));
        countRelationships(1, node.getRelationships(dir, TEST_TRAVERSAL));
    }

    private static void allGetRelationshipMethods3(Node node, Direction dir) {
        countRelationships(6, node.getRelationships());
        countRelationships(6, node.getRelationships(dir));
        countRelationships(6, node.getRelationships(TEST, TEST2, TEST_TRAVERSAL));
        countRelationships(4, node.getRelationships(TEST, TEST2));
        countRelationships(4, node.getRelationships(TEST, TEST_TRAVERSAL));
        countRelationships(4, node.getRelationships(TEST2, TEST_TRAVERSAL));
        countRelationships(2, node.getRelationships(TEST));
        countRelationships(2, node.getRelationships(TEST2));
        countRelationships(2, node.getRelationships(TEST_TRAVERSAL));
        countRelationships(2, node.getRelationships(dir, TEST));
        countRelationships(2, node.getRelationships(dir, TEST2));
        countRelationships(2, node.getRelationships(dir, TEST_TRAVERSAL));
    }

    private static void countRelationships(int expectedCount, Iterable<Relationship> rels) {
        assertEquals(expectedCount, (int) Iterables.count(rels));
    }

    private static void deleteFirst(ResourceIterable<Relationship> iterable) {
        deleteAtIndex(iterable, 0);
    }

    private static void deleteAtIndex(ResourceIterable<Relationship> relationships, int index) {
        int pos = 0;
        try (relationships) {
            for (final var relationship : relationships) {
                if (pos == index) {
                    relationship.delete();
                    break;
                }
                pos++;
            }
        }
    }

    @Test
    void testRelationshipCreateAndDelete() {
        Node node1 = createNode();
        Node node2 = createNode();
        try (Transaction transaction = getGraphDb().beginTx()) {
            node1 = transaction.getNodeById(node1.getId());
            node2 = transaction.getNodeById(node2.getId());

            Relationship relationship = node1.createRelationshipTo(node2, TEST);
            Relationship[] relArray1 = getRelationshipArray(node1.getRelationships());
            Relationship[] relArray2 = getRelationshipArray(node2.getRelationships());
            assertEquals(1, relArray1.length);
            assertEquals(relationship, relArray1[0]);
            assertEquals(1, relArray2.length);
            assertEquals(relationship, relArray2[0]);
            relArray1 = getRelationshipArray(node1.getRelationships(TEST));
            assertEquals(1, relArray1.length);
            assertEquals(relationship, relArray1[0]);
            relArray2 = getRelationshipArray(node2.getRelationships(TEST));
            assertEquals(1, relArray2.length);
            assertEquals(relationship, relArray2[0]);
            relArray1 = getRelationshipArray(node1.getRelationships(Direction.OUTGOING, TEST));
            assertEquals(1, relArray1.length);
            relArray2 = getRelationshipArray(node2.getRelationships(Direction.INCOMING, TEST));
            assertEquals(1, relArray2.length);
            relArray1 = getRelationshipArray(node1.getRelationships(Direction.INCOMING, TEST));
            assertEquals(0, relArray1.length);
            relArray2 = getRelationshipArray(node2.getRelationships(Direction.OUTGOING, TEST));
            assertEquals(0, relArray2.length);
            relationship.delete();
            node2.delete();
            node1.delete();
            transaction.commit();
        }
    }

    private static Relationship[] getRelationshipArray(Iterable<Relationship> relsIterable) {
        return Iterables.asArray(Relationship.class, relsIterable);
    }

    @Test
    void testDeleteWithRelationship() {
        // do some evil stuff
        Node node1 = createNode();
        Node node2 = createNode();
        try (Transaction transaction = getGraphDb().beginTx()) {
            node1 = transaction.getNodeById(node1.getId());
            node2 = transaction.getNodeById(node2.getId());

            node1.createRelationshipTo(node2, TEST);
            node1.delete();
            node2.delete();
            assertThrows(Exception.class, transaction::commit);
        }
    }

    @Test
    void testDeletedRelationship() {
        Node node1 = createNode();
        Node node2 = createNode();
        try (Transaction transaction = getGraphDb().beginTx()) {
            node1 = transaction.getNodeById(node1.getId());
            node2 = transaction.getNodeById(node2.getId());
            Relationship relationship = node1.createRelationshipTo(node2, TEST);
            relationship.delete();
            assertThrows(Exception.class, () -> relationship.setProperty("key1", 1));
            node1.delete();
            node2.delete();
            transaction.commit();
        }
    }

    @Test
    void testRelationshipAddPropertyWithNullKey() {
        Node node1 = createNode();
        Node node2 = createNode();
        try (Transaction transaction = getGraphDb().beginTx()) {
            node1 = transaction.getNodeById(node1.getId());
            Relationship rel1 = node1.createRelationshipTo(node2, TEST);
            assertThrows(Exception.class, () -> rel1.setProperty(null, "bar"));
            transaction.commit();
        }
    }

    @Test
    void testRelationshipAddPropertyWithNullValue() {
        Node node1 = createNode();
        Node node2 = createNode();
        try (Transaction transaction = getGraphDb().beginTx()) {
            node1 = transaction.getNodeById(node1.getId());

            Relationship rel1 = node1.createRelationshipTo(node2, TEST);
            assertThrows(Exception.class, () -> rel1.setProperty("foo", null));
        }
    }

    @Test
    void testRelationshipAddProperty() {
        Node node1 = createNode();
        Node node2 = createNode();
        try (Transaction transaction = getGraphDb().beginTx()) {
            node1 = transaction.getNodeById(node1.getId());
            node2 = transaction.getNodeById(node2.getId());

            Relationship rel1 = node1.createRelationshipTo(node2, TEST);
            Relationship rel2 = node2.createRelationshipTo(node1, TEST);

            Integer int1 = 1;
            Integer int2 = 2;
            String string1 = "1";
            String string2 = "2";

            // add property
            rel1.setProperty(key1, int1);
            rel2.setProperty(key1, string1);
            rel1.setProperty(key2, string2);
            rel2.setProperty(key2, int2);
            assertTrue(rel1.hasProperty(key1));
            assertTrue(rel2.hasProperty(key1));
            assertTrue(rel1.hasProperty(key2));
            assertTrue(rel2.hasProperty(key2));
            assertFalse(rel1.hasProperty(key3));
            assertFalse(rel2.hasProperty(key3));
            assertEquals(int1, rel1.getProperty(key1));
            assertEquals(string1, rel2.getProperty(key1));
            assertEquals(string2, rel1.getProperty(key2));
            assertEquals(int2, rel2.getProperty(key2));
        }
    }

    @Test
    void testRelationshipRemoveProperty() {
        Integer int1 = 1;
        Integer int2 = 2;
        String string1 = "1";
        String string2 = "2";

        Node node1 = createNode();
        Node node2 = createNode();
        try (Transaction transaction = getGraphDb().beginTx()) {
            node1 = transaction.getNodeById(node1.getId());
            node2 = transaction.getNodeById(node2.getId());

            Relationship rel1 = node1.createRelationshipTo(node2, TEST);
            Relationship rel2 = node2.createRelationshipTo(node1, TEST);
            // verify that we can rely on PL to remove non existing properties
            if (rel1.removeProperty(key1) != null) {
                fail("Remove of non existing property should return null");
            }

            rel1.setProperty(key1, int1);
            rel2.setProperty(key1, string1);
            rel1.setProperty(key2, string2);
            rel2.setProperty(key2, int2);

            // test remove property
            assertEquals(int1, rel1.removeProperty(key1));
            assertEquals(string1, rel2.removeProperty(key1));
            // test remove of non existing property
            if (rel2.removeProperty(key1) != null) {
                fail("Remove of non existing property should return null");
            }

            rel1.delete();
            rel2.delete();
            node1.delete();
            node2.delete();
            transaction.commit();
        }
    }

    @Test
    void testRelationshipChangeProperty() {
        Integer int1 = 1;
        Integer int2 = 2;
        String string1 = "1";
        String string2 = "2";

        Node node1 = createNode();
        Node node2 = createNode();
        try (Transaction transaction = getGraphDb().beginTx()) {
            node1 = transaction.getNodeById(node1.getId());
            node2 = transaction.getNodeById(node2.getId());

            Relationship rel1 = node1.createRelationshipTo(node2, TEST);
            Relationship rel2 = node2.createRelationshipTo(node1, TEST);
            rel1.setProperty(key1, int1);
            rel2.setProperty(key1, string1);
            rel1.setProperty(key2, string2);
            rel2.setProperty(key2, int2);

            assertThrows(IllegalArgumentException.class, () -> rel1.setProperty(null, null));

            // test type change of existing property
            // cannot test this for now because of exceptions in PL
            rel2.setProperty(key1, int1);

            rel1.delete();
            rel2.delete();
            node2.delete();
            node1.delete();
            transaction.commit();
        }
    }

    @Test
    void testRelationshipChangeProperty2() {
        Integer int1 = 1;
        Integer int2 = 2;
        String string1 = "1";
        String string2 = "2";

        Node node1 = createNode();
        Node node2 = createNode();
        try (Transaction transaction = getGraphDb().beginTx()) {
            node1 = transaction.getNodeById(node1.getId());
            node2 = transaction.getNodeById(node2.getId());
            Relationship rel1 = node1.createRelationshipTo(node2, TEST);
            rel1.setProperty(key1, int1);
            rel1.setProperty(key1, int2);
            assertEquals(int2, rel1.getProperty(key1));
            rel1.removeProperty(key1);
            rel1.setProperty(key1, string1);
            rel1.setProperty(key1, string2);
            assertEquals(string2, rel1.getProperty(key1));
            rel1.removeProperty(key1);
            rel1.setProperty(key1, true);
            rel1.setProperty(key1, false);
            assertEquals(false, rel1.getProperty(key1));
            rel1.removeProperty(key1);

            rel1.delete();
            node2.delete();
            node1.delete();
            transaction.commit();
        }
    }

    @Test
    void testRelGetProperties() {
        Integer int1 = 1;
        Integer int2 = 2;
        String string = "3";

        Node node = createNode();
        Node node2 = createNode();
        try (Transaction transaction = getGraphDb().beginTx()) {
            var node1 = transaction.getNodeById(node.getId());
            node2 = transaction.getNodeById(node2.getId());

            Relationship rel1 = node1.createRelationshipTo(node2, TEST);
            assertThrows(NotFoundException.class, () -> rel1.getProperty(key1));
            assertThrows(IllegalArgumentException.class, () -> rel1.getProperty(null));

            assertFalse(rel1.hasProperty(key1));
            assertFalse(rel1.hasProperty(null));
            rel1.setProperty(key1, int1);
            rel1.setProperty(key2, int2);
            rel1.setProperty(key3, string);
            assertTrue(rel1.hasProperty(key1));
            assertTrue(rel1.hasProperty(key2));
            assertTrue(rel1.hasProperty(key3));

            Map<String, Object> properties = rel1.getAllProperties();
            assertEquals(properties.get(key1), int1);
            assertEquals(properties.get(key2), int2);
            assertEquals(properties.get(key3), string);
            properties = rel1.getProperties(key1, key2);
            assertEquals(properties.get(key1), int1);
            assertEquals(properties.get(key2), int2);
            assertFalse(properties.containsKey(key3));

            properties = node1.getProperties();
            assertTrue(properties.isEmpty());

            assertThrows(NullPointerException.class, () -> node1.getProperties((String[]) null));

            assertThrows(NullPointerException.class, () -> {
                String[] names = new String[] {null};
                node1.getProperties(names);
                fail();
            });

            assertDoesNotThrow(() -> rel1.removeProperty(key3), "Remove of property failed.");

            assertFalse(rel1.hasProperty(key3));
            assertFalse(rel1.hasProperty(null));
            rel1.delete();
            node2.delete();
            node1.delete();
            transaction.commit();
        }
    }

    @Test
    void testDirectedRelationship() {
        Node node1 = createNode();
        Node node2 = createNode();
        try (Transaction transaction = getGraphDb().beginTx()) {
            node1 = transaction.getNodeById(node1.getId());
            node2 = transaction.getNodeById(node2.getId());

            Relationship rel2 = node1.createRelationshipTo(node2, TEST);
            Relationship rel3 = node2.createRelationshipTo(node1, TEST);
            Node[] nodes = rel2.getNodes();
            assertEquals(2, nodes.length);
            assertTrue(nodes[0].equals(node1) && nodes[1].equals(node2));
            nodes = rel3.getNodes();
            assertEquals(2, nodes.length);
            assertTrue(nodes[0].equals(node2) && nodes[1].equals(node1));
            assertEquals(node1, rel2.getStartNode());
            assertEquals(node2, rel2.getEndNode());
            assertEquals(node2, rel3.getStartNode());
            assertEquals(node1, rel3.getEndNode());

            Relationship[] relArray = getRelationshipArray(node1.getRelationships(Direction.OUTGOING, TEST));
            assertEquals(1, relArray.length);
            assertEquals(rel2, relArray[0]);
            relArray = getRelationshipArray(node1.getRelationships(Direction.INCOMING, TEST));
            assertEquals(1, relArray.length);
            assertEquals(rel3, relArray[0]);

            relArray = getRelationshipArray(node2.getRelationships(Direction.OUTGOING, TEST));
            assertEquals(1, relArray.length);
            assertEquals(rel3, relArray[0]);
            relArray = getRelationshipArray(node2.getRelationships(Direction.INCOMING, TEST));
            assertEquals(1, relArray.length);
            assertEquals(rel2, relArray[0]);

            rel2.delete();
            rel3.delete();
            node1.delete();
            node2.delete();
            transaction.commit();
        }
    }

    @Test
    void testRollbackDeleteRelationship() {
        Node node1 = createNode();
        Node node2 = createNode();
        Relationship rel1;
        try (Transaction transaction = getGraphDb().beginTx()) {
            node1 = transaction.getNodeById(node1.getId());

            rel1 = node1.createRelationshipTo(node2, TEST);
            transaction.commit();
        }
        try (Transaction transaction = getGraphDb().beginTx()) {
            transaction.getNodeById(node1.getId()).delete();
            transaction.getRelationshipById(rel1.getId()).delete();
        }
        try (Transaction transaction = getGraphDb().beginTx()) {
            transaction.getNodeById(node1.getId()).delete();
            transaction.getNodeById(node2.getId()).delete();
            transaction.getRelationshipById(rel1.getId()).delete();
            transaction.commit();
        }
    }

    @Test
    void testCreateRelationshipWithCommits() {
        Node n1 = createNode();
        Node n2 = createNode();
        try (Transaction transaction = getGraphDb().beginTx()) {
            n1 = transaction.getNodeById(n1.getId());
            n2 = transaction.getNodeById(n2.getId());

            n1.createRelationshipTo(n2, TEST);
            transaction.commit();
        }
        try (Transaction transaction = getGraphDb().beginTx()) {
            n1 = transaction.getNodeById(n1.getId());
            n2 = transaction.getNodeById(n2.getId());

            Relationship[] relArray = getRelationshipArray(n1.getRelationships());
            assertEquals(1, relArray.length);
            relArray = getRelationshipArray(n1.getRelationships());
            relArray[0].delete();
            n1.delete();
            n2.delete();
            transaction.commit();
        }
    }

    @Test
    void testAddPropertyThenDelete() {
        Node node1 = createNode();
        Node node2 = createNode();
        Relationship rel;
        try (Transaction transaction = getGraphDb().beginTx()) {
            node1 = transaction.getNodeById(node1.getId());
            rel = node1.createRelationshipTo(node2, TEST);
            rel.setProperty("test", "test");
            transaction.commit();
        }
        try (Transaction transaction = getGraphDb().beginTx()) {
            rel = transaction.getRelationshipById(rel.getId());
            rel.setProperty("test2", "test2");
            rel.delete();
            transaction.getNodeById(node1.getId()).delete();
            transaction.getNodeById(node2.getId()).delete();
            transaction.commit();
        }
    }

    @Test
    void testRelationshipIsType() {
        Node node1 = createNode();
        Node node2 = createNode();
        try (Transaction transaction = getGraphDb().beginTx()) {
            node1 = transaction.getNodeById(node1.getId());
            node2 = transaction.getNodeById(node2.getId());
            Relationship rel = node1.createRelationshipTo(node2, TEST);
            assertTrue(rel.isType(TEST));
            assertTrue(rel.isType(TEST::name));
            assertFalse(rel.isType(TEST_TRAVERSAL));
            rel.delete();
            node1.delete();
            node2.delete();
            transaction.commit();
        }
    }

    @Test
    void testChangeProperty() {
        Node node1 = createNode();
        Node node2 = createNode();
        Relationship rel;
        try (Transaction transaction = getGraphDb().beginTx()) {
            node1 = transaction.getNodeById(node1.getId());
            node2 = transaction.getNodeById(node2.getId());

            rel = node1.createRelationshipTo(node2, TEST);
            rel.setProperty("test", "test1");
            transaction.commit();
        }
        try (Transaction transaction = getGraphDb().beginTx()) {
            rel = transaction.getRelationshipById(rel.getId());
            rel.setProperty("test", "test2");
            rel.removeProperty("test");
            rel.setProperty("test", "test3");
            assertEquals("test3", rel.getProperty("test"));
            rel.removeProperty("test");
            rel.setProperty("test", "test4");
            transaction.commit();
        }
        try (Transaction transaction = getGraphDb().beginTx()) {
            rel = transaction.getRelationshipById(rel.getId());
            assertEquals("test4", rel.getProperty("test"));
            transaction.commit();
        }
    }

    @Test
    void testChangeProperty2() {
        // Create relationship with "test"="test1"
        Node node1 = createNode();
        Node node2 = createNode();
        Relationship rel;
        try (Transaction transaction = getGraphDb().beginTx()) {
            node1 = transaction.getNodeById(node1.getId());
            node2 = transaction.getNodeById(node2.getId());
            rel = node1.createRelationshipTo(node2, TEST);
            rel.setProperty("test", "test1");
            transaction.commit();
        }

        try (Transaction transaction = getGraphDb().beginTx()) {
            rel = transaction.getRelationshipById(rel.getId());
            // Remove "test" and set "test"="test3" instead
            rel.removeProperty("test");
            rel.setProperty("test", "test3");
            assertEquals("test3", rel.getProperty("test"));
            transaction.commit();
        }

        try (Transaction transaction = getGraphDb().beginTx()) {
            rel = transaction.getRelationshipById(rel.getId());
            // Remove "test" and set "test"="test4" instead
            assertEquals("test3", rel.getProperty("test"));
            rel.removeProperty("test");
            rel.setProperty("test", "test4");
            transaction.commit();
        }

        try (Transaction transaction = getGraphDb().beginTx()) {
            rel = transaction.getRelationshipById(rel.getId());
            // Should still be "test4"
            assertEquals("test4", rel.getProperty("test"));
            transaction.commit();
        }
    }

    @Test
    void makeSureLazyLoadingRelationshipsWorksEvenIfOtherIteratorAlsoLoadsInTheSameIteration() {
        int numEdges = 100;

        /* create 256 nodes */
        Node[] nodes = new Node[256];
        try (Transaction transaction = getGraphDb().beginTx()) {
            for (int numNodes = 0; numNodes < nodes.length; numNodes += 1) {
                nodes[numNodes] = transaction.createNode();
            }
            transaction.commit();
        }

        int nextID = 7;
        Node hub = nodes[4];
        RelationshipType outtie;
        RelationshipType innie;
        try (Transaction transaction = getGraphDb().beginTx()) {
            hub = transaction.getNodeById(hub.getId());
            /* create random outgoing relationships from node 5 */
            outtie = withName("outtie");
            innie = withName("innie");
            for (int k = 0; k < numEdges; k++) {
                Node neighbor = transaction.getNodeById(nodes[nextID].getId());
                nextID += 7;
                nextID &= 255;
                if (nextID == 0) {
                    nextID = 1;
                }
                hub.createRelationshipTo(neighbor, outtie);
            }
            transaction.commit();
        }

        try (Transaction transaction = getGraphDb().beginTx()) {
            hub = transaction.getNodeById(hub.getId());

            /* create random incoming relationships to node 5 */
            for (int k = 0; k < numEdges; k += 1) {
                Node neighbor = transaction.getNodeById(nodes[nextID].getId());
                nextID += 7;
                nextID &= 255;
                if (nextID == 0) {
                    nextID = 1;
                }
                neighbor.createRelationshipTo(hub, innie);
            }
            transaction.commit();
        }

        try (Transaction transaction = getGraphDb().beginTx()) {
            hub = transaction.getNodeById(hub.getId());

            var theHub = hub;
            try (Stream<Relationship> stream = hub.getRelationships().stream()) {
                int count = stream.map(r -> Iterables.count(theHub.getRelationships()))
                        .mapToInt(Long::intValue)
                        .sum();
                assertEquals(40000, count);
            }

            try (Stream<Relationship> stream = hub.getRelationships().stream()) {
                int count = stream.map(r -> Iterables.count(theHub.getRelationships()))
                        .mapToInt(Long::intValue)
                        .sum();
                assertEquals(40000, count);
            }
            transaction.commit();
        }
    }

    @Test
    void createRelationshipAfterClearedCache() {
        // Assumes relationship grab size 100
        Node node1 = createNode();
        Node node2 = createNode();
        int expectedCount = 0;
        try (Transaction transaction = getGraphDb().beginTx()) {
            node1 = transaction.getNodeById(node1.getId());

            for (int i = 0; i < 150; i++) {
                node1.createRelationshipTo(node2, TEST);
                expectedCount++;
            }
            transaction.commit();
        }
        try (Transaction transaction = getGraphDb().beginTx()) {
            node1 = transaction.getNodeById(node1.getId());

            for (int i = 0; i < 50; i++) {
                node1.createRelationshipTo(node2, TEST);
                expectedCount++;
            }
            assertEquals(expectedCount, Iterables.count(node1.getRelationships()));
            transaction.commit();
        }
        try (Transaction transaction = getGraphDb().beginTx()) {
            node1 = transaction.getNodeById(node1.getId());

            assertEquals(expectedCount, Iterables.count(node1.getRelationships()));
            transaction.commit();
        }
    }

    @Test
    void getAllRelationships() {
        Set<Relationship> existingRelationships = new HashSet<>();
        try (Transaction transaction = getGraphDb().beginTx()) {
            addAll(existingRelationships, transaction.getAllRelationships());
            transaction.commit();
        }

        Set<Relationship> createdRelationships = new HashSet<>();
        Node node = createNode();
        try (Transaction transaction = getGraphDb().beginTx()) {
            node = transaction.getNodeById(node.getId());
            for (int i = 0; i < 100; i++) {
                createdRelationships.add(node.createRelationshipTo(transaction.createNode(), TEST));
            }
            transaction.commit();
        }

        Set<Relationship> allRelationships = new HashSet<>();
        allRelationships.addAll(existingRelationships);
        allRelationships.addAll(createdRelationships);
        try (Transaction transaction = getGraphDb().beginTx();
                ResourceIterable<Relationship> allRelationships1 = transaction.getAllRelationships()) {
            int count = 0;
            for (Relationship rel : allRelationships1) {
                assertTrue(
                        allRelationships.contains(rel),
                        "Unexpected rel " + rel + ", expected one of " + allRelationships);
                count++;
            }
            assertEquals(allRelationships.size(), count);
            transaction.commit();
        }
    }

    @Test
    void createAndClearCacheBeforeCommit() {
        Node node = createNode();
        try (Transaction transaction = getGraphDb().beginTx()) {
            node = transaction.getNodeById(node.getId());
            node.createRelationshipTo(transaction.createNode(), TEST);
            assertEquals(1, Iterables.count(node.getRelationships()));
            transaction.commit();
        }
    }

    @Test
    void setPropertyAndClearCacheBeforeCommit() {
        Node node = createNode();
        try (Transaction transaction = getGraphDb().beginTx()) {
            node = transaction.getNodeById(node.getId());
            node.setProperty("name", "Test");
            assertEquals("Test", node.getProperty("name"));
            transaction.commit();
        }
    }

    @Test
    void shouldNotGetTheSameRelationshipMoreThanOnceWhenAskingForTheSameTypeMultipleTimes() {
        // given
        Node node = createNode();
        try (Transaction transaction = getGraphDb().beginTx()) {
            node = transaction.getNodeById(node.getId());
            node.createRelationshipTo(transaction.createNode(), withName("FOO"));

            // when
            long relationships = Iterables.count(node.getRelationships(withName("FOO"), withName("FOO")));

            // then
            assertEquals(1, relationships);
            transaction.commit();
        }
    }

    @Test
    void shouldLoadAllRelationships() {
        // GIVEN
        GraphDatabaseService db = getGraphDbAPI();
        Node node;
        try (Transaction tx = db.beginTx()) {
            node = tx.createNode();
            for (int i = 0; i < 112; i++) {
                node.createRelationshipTo(tx.createNode(), TEST);
                tx.createNode().createRelationshipTo(node, TEST);
            }
            tx.commit();
        }
        // WHEN
        long one;
        long two;
        try (Transaction tx = db.beginTx()) {
            node = tx.getNodeById(node.getId());
            one = Iterables.count(node.getRelationships(Direction.OUTGOING, TEST));
            two = Iterables.count(node.getRelationships(Direction.OUTGOING, TEST));
            tx.commit();
        }

        // THEN
        assertEquals(two, one);
    }

    @Test
    void deletionOfSameRelationshipTwiceInOneTransactionShouldNotRollbackIt() {
        // Given
        GraphDatabaseService db = getGraphDb();

        // transaction is opened by test
        Node node1 = createNode();
        Node node2 = createNode();
        Relationship relationship;
        try (Transaction transaction = getGraphDb().beginTx()) {
            node1 = transaction.getNodeById(node1.getId());
            node2 = transaction.getNodeById(node2.getId());
            relationship = node1.createRelationshipTo(node2, TEST);
            transaction.commit();
        }

        try (Transaction tx = db.beginTx()) {
            relationship = tx.getRelationshipById(relationship.getId());
            relationship.delete();
            assertThrows(NotFoundException.class, relationship::delete);
            tx.commit();
        }

        try (Transaction tx = db.beginTx()) {
            var relId = relationship.getId();
            assertThrows(NotFoundException.class, () -> tx.getRelationshipById(relId));
            tx.commit();
        }
    }

    @Test
    void deletionOfAlreadyDeletedRelationshipShouldThrow() {
        // Given
        GraphDatabaseService db = getGraphDb();

        // transaction is opened by test
        Node node1 = createNode();
        Node node2 = createNode();
        Relationship relationship;
        try (Transaction transaction = getGraphDb().beginTx()) {
            node1 = transaction.getNodeById(node1.getId());
            relationship = node1.createRelationshipTo(node2, TEST);
            transaction.commit();
        }

        try (Transaction tx = db.beginTx()) {
            tx.getRelationshipById(relationship.getId()).delete();
            tx.commit();
        }

        // When
        try (Transaction tx = db.beginTx()) {
            assertThrows(NotFoundException.class, () -> tx.getRelationshipById(relationship.getId())
                    .delete());
        }
    }
}
