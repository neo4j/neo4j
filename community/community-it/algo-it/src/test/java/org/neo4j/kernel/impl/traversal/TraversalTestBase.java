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
package org.neo4j.kernel.impl.traversal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.GraphDefinition;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

@DbmsExtension(configurationCallback = "configure")
abstract class TraversalTestBase {
    @Inject
    private GraphDatabaseAPI graphDb;

    public GraphDatabaseService getGraphDb() {
        return graphDb;
    }

    private Map<String, Node> nodes;

    protected Node node(String name) {
        return nodes.get(name);
    }

    protected Transaction beginTx() {
        return getGraphDb().beginTx();
    }

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(GraphDatabaseInternalSettings.track_cursor_close, false);
    }

    protected void createGraph(String... description) {
        resetDatabase();
        nodes = createGraph(GraphDescription.create(description));
    }

    private void resetDatabase() {
        try (Transaction transaction = getGraphDb().beginTx();
                ResourceIterable<Node> allNodes = transaction.getAllNodes()) {
            for (Node node : allNodes) {
                Iterables.forEach(node.getRelationships(), Relationship::delete);
                node.delete();
            }
            transaction.commit();
        }
    }

    private Map<String, Node> createGraph(GraphDefinition graph) {
        resetDatabase();
        return graph.create(getGraphDb());
    }

    protected static Node getNodeWithName(Transaction transaction, String name) {
        try (ResourceIterable<Node> allNodes = transaction.getAllNodes()) {
            for (final var node : allNodes) {
                {
                    String nodeName = (String) node.getProperty("name", null);
                    if (nodeName != null && nodeName.equals(name)) {
                        return node;
                    }
                }
            }
        }

        return null;
    }

    static void assertLevels(Traverser traverser, Stack<Set<String>> levels) {
        Set<String> current = levels.pop();

        for (Path position : traverser) {
            String nodeName = (String) position.endNode().getProperty("name");
            if (current.isEmpty()) {
                current = levels.pop();
            }
            assertTrue(
                    current.remove(nodeName),
                    "Should not contain node (" + nodeName + ") at level " + (3 - levels.size()));
        }

        assertTrue(levels.isEmpty(), "Should have no more levels");
        assertTrue(current.isEmpty(), "Should be empty");
    }

    static final Representation<Entity> NAME_PROPERTY_REPRESENTATION = new PropertyRepresentation("name");

    private static final Representation<Relationship> RELATIONSHIP_TYPE_REPRESENTATION =
            item -> item.getType().name();

    protected interface Representation<T> {
        String represent(T item);
    }

    protected static final class PropertyRepresentation implements Representation<Entity> {
        PropertyRepresentation(String key) {
            this.key = key;
        }

        private final String key;

        @Override
        public String represent(Entity item) {
            return (String) item.getProperty(key);
        }
    }

    protected static final class RelationshipRepresentation implements Representation<Relationship> {
        private final Representation<? super Node> nodes;
        private final Representation<? super Relationship> rel;

        RelationshipRepresentation(Representation<? super Node> nodes) {
            this(nodes, RELATIONSHIP_TYPE_REPRESENTATION);
        }

        RelationshipRepresentation(Representation<? super Node> nodes, Representation<? super Relationship> rel) {
            this.nodes = nodes;
            this.rel = rel;
        }

        @Override
        public String represent(Relationship item) {
            return nodes.represent(item.getStartNode()) + " "
                    + rel.represent(item) + " "
                    + nodes.represent(item.getEndNode());
        }
    }

    protected static final class NodePathRepresentation implements Representation<Path> {
        private final Representation<? super Node> nodes;

        NodePathRepresentation(Representation<? super Node> nodes) {
            this.nodes = nodes;
        }

        @Override
        public String represent(Path item) {
            StringBuilder builder = new StringBuilder();
            for (Node node : item.nodes()) {
                builder.append(builder.length() > 0 ? "," : "");
                builder.append(nodes.represent(node));
            }
            return builder.toString();
        }
    }

    private static <T> void expect(Iterable<? extends T> items, Representation<T> representation, String... expected) {
        expect(items, representation, new HashSet<>(Arrays.asList(expected)));
    }

    private static <T> void expect(
            Iterable<? extends T> items, Representation<T> representation, Set<String> expected) {
        Collection<String> encounteredItems = new ArrayList<>();
        for (T item : items) {
            String repr = representation.represent(item);
            assertTrue(expected.remove(repr), repr + " not expected ");
            encounteredItems.add(repr);
        }

        if (!expected.isEmpty()) {
            fail("The expected elements " + expected + " were not returned. Returned were: " + encounteredItems);
        }
    }

    static void expectNodes(Traverser traverser, String... nodes) {
        expect(traverser.nodes(), NAME_PROPERTY_REPRESENTATION, nodes);
    }

    static void expectRelationships(Traverser traverser, String... relationships) {
        expect(traverser.relationships(), new RelationshipRepresentation(NAME_PROPERTY_REPRESENTATION), relationships);
    }

    static void expectPaths(Traverser traverser, String... paths) {
        expectPaths(traverser, new HashSet<>(Arrays.asList(paths)));
    }

    static void expectPaths(Traverser traverser, Set<String> expected) {
        expect(traverser, new NodePathRepresentation(NAME_PROPERTY_REPRESENTATION), expected);
    }

    static <E> void assertContains(Iterable<E> actual, E... expected) {
        Set<E> expectation = new HashSet<>(Arrays.asList(expected));
        for (E element : actual) {
            if (!expectation.remove(element)) {
                fail("unexpected element <" + element + ">");
            }
        }
        if (!expectation.isEmpty()) {
            fail("the expected elements <" + expectation + "> were not contained");
        }
    }

    private static <T> void assertContainsInOrder(Collection<T> collection, T... expectedItems) {
        String collectionString = join(", ", collection.toArray());
        assertEquals(expectedItems.length, collection.size(), collectionString);
        Iterator<T> itr = collection.iterator();
        for (int i = 0; itr.hasNext(); i++) {
            assertEquals(expectedItems[i], itr.next());
        }
    }

    static <T> void assertContainsInOrder(Iterable<T> collection, T... expectedItems) {
        assertContainsInOrder(Iterables.asCollection(collection), expectedItems);
    }

    private static <T> String join(String delimiter, T... items) {
        StringBuilder buffer = new StringBuilder();
        for (T item : items) {
            if (buffer.length() > 0) {
                buffer.append(delimiter);
            }
            buffer.append(item);
        }
        return buffer.toString();
    }
}
