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
package org.neo4j.metatest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphDescription.Graph;
import org.neo4j.test.GraphDescription.NODE;
import org.neo4j.test.GraphDescription.PROP;
import org.neo4j.test.GraphHolder;
import org.neo4j.test.TestData;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

public class TestGraphDescription implements GraphHolder {
    private static GraphDatabaseService graphdb;

    @RegisterExtension
    public TestData<Map<String, Node>> data = TestData.producedThrough(GraphDescription.createGraphFor(this));

    private static DatabaseManagementService managementService;

    @Test
    public void havingNoGraphAnnotationCreatesAnEmptyDataCollection() {
        assertTrue(data.get().isEmpty(), "collection was not empty");
    }

    @Test
    @Graph("I know you")
    public void canCreateGraphFromSingleString() {
        verifyIKnowYou("know", "I");
    }

    @Test
    @Graph({"a TO b", "b TO c", "c TO a"})
    public void canCreateGraphFromMultipleStrings() {
        Map<String, Node> graph = data.get();
        Set<Node> unique = new HashSet<>();
        Node n = graph.get("a");
        while (unique.add(n)) {
            try (Transaction tx = graphdb.beginTx()) {
                n = tx.getNodeById(n.getId())
                        .getSingleRelationship(RelationshipType.withName("TO"), Direction.OUTGOING)
                        .getEndNode();
            }
        }
        assertEquals(graph.size(), unique.size());
    }

    @Test
    @Graph({"a:Person EATS b:Banana"})
    public void ensurePeopleCanEatBananas() {
        Map<String, Node> graph = data.get();
        Node a = graph.get("a");
        Node b = graph.get("b");

        try (Transaction tx = graphdb.beginTx()) {
            assertTrue(tx.getNodeById(a.getId()).hasLabel(label("Person")));
            assertTrue(tx.getNodeById(b.getId()).hasLabel(label("Banana")));
        }
    }

    @Test
    @Graph({"a:Person EATS b:Banana", "a EATS b:Apple"})
    public void ensurePeopleCanEatBananasAndApples() {
        Map<String, Node> graph = data.get();
        Node a = graph.get("a");
        Node b = graph.get("b");

        try (Transaction tx = graphdb.beginTx()) {
            assertTrue(tx.getNodeById(a.getId()).hasLabel(label("Person")), "Person label missing");
            assertTrue(tx.getNodeById(b.getId()).hasLabel(label("Banana")), "Banana label missing");
            assertTrue(tx.getNodeById(b.getId()).hasLabel(label("Apple")), "Apple label missing");
        }
    }

    @Graph(
            value = {"I know you"},
            nodes = {
                @NODE(
                        name = "I",
                        properties = {@PROP(key = "name", value = "me")})
            })
    private void verifyIKnowYou(String type, String myName) {
        Map<String, Node> graph = data.get();
        try (Transaction tx = graphdb.beginTx()) {
            assertEquals(2, graph.size(), "Wrong graph size.");
            Node iNode = tx.getNodeById(graph.get("I").getId());
            assertNotNull(iNode, "The node 'I' was not defined");
            Node you = tx.getNodeById(graph.get("you").getId());
            assertNotNull(you, "The node 'you' was not defined");
            assertEquals(myName, iNode.getProperty("name"), "'I' has wrong 'name'.");
            assertEquals("you", you.getProperty("name"), "'you' has wrong 'name'.");

            try (ResourceIterable<Relationship> relationships = iNode.getRelationships();
                    ResourceIterator<Relationship> rels = relationships.iterator()) {
                assertTrue(rels.hasNext(), "'I' has too few relationships");
                Relationship rel = rels.next();
                assertEquals(you, rel.getOtherNode(iNode), "'I' is not related to 'you'");
                assertEquals(type, rel.getType().name(), "Wrong relationship type.");
                assertFalse(rels.hasNext(), "'I' has too many relationships");
            }

            try (ResourceIterable<Relationship> relationships = you.getRelationships();
                    ResourceIterator<Relationship> rels = relationships.iterator()) {
                assertTrue(rels.hasNext(), "'you' has too few relationships");
                Relationship rel = rels.next();
                assertEquals(iNode, rel.getOtherNode(you), "'you' is not related to 'i'");
                assertEquals(type, rel.getType().name(), "Wrong relationship type.");
                assertFalse(rels.hasNext(), "'you' has too many relationships");

                assertEquals(iNode, rel.getStartNode(), "wrong direction");
            }
        }
    }

    @BeforeAll
    public static void startDatabase() {
        managementService =
                new TestDatabaseManagementServiceBuilder().impermanent().build();
        graphdb = managementService.database(DEFAULT_DATABASE_NAME);
    }

    @AfterAll
    public static void stopDatabase() {
        if (graphdb != null) {
            managementService.shutdown();
        }
        graphdb = null;
    }

    @Override
    public GraphDatabaseService graphdb() {
        return graphdb;
    }
}
