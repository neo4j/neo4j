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
package org.neo4j.graphalgo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.track_cursor_close;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

/**
 * Base class for test cases working on a NeoService. It sets up a NeoService
 * and a transaction.
 */
@ImpermanentDbmsExtension(configurationCallback = "configure")
public abstract class Neo4jAlgoTestCase {
    protected SimpleGraphBuilder graph;

    @Inject
    protected GraphDatabaseAPI graphDb;

    public enum MyRelTypes implements RelationshipType {
        R1,
        R2,
        R3
    }

    @ExtensionCallback
    void configure(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(track_cursor_close, false);
    }

    @BeforeEach
    public void setUpGraphDb() {
        graph = new SimpleGraphBuilder(graphDb, MyRelTypes.R1);
    }

    protected static void assertPathDef(Path path, String... names) {
        int i = 0;
        for (Node node : path.nodes()) {
            assertEquals(
                    names[i++],
                    node.getProperty(SimpleGraphBuilder.KEY_ID),
                    "Wrong node " + i + " in " + getPathDef(path));
        }
        assertEquals(names.length, i);
    }

    protected static void assertPath(
            SimpleGraphBuilder graph, Transaction tx, Path path, String commaSeparatedNodePath) {
        String[] nodeIds = commaSeparatedNodePath.split(",");
        Node[] nodes = new Node[nodeIds.length];
        int i = 0;
        for (String id : nodeIds) {
            nodes[i] = tx.getNodeById(graph.getNode(tx, id).getId());
            i++;
        }
        assertPath(path, nodes);
    }

    protected static void assertPath(Path path, Node... nodes) {
        int i = 0;
        for (Node node : path.nodes()) {
            assertEquals(
                    nodes[i++].getProperty(SimpleGraphBuilder.KEY_ID),
                    node.getProperty(SimpleGraphBuilder.KEY_ID),
                    "Wrong node " + i + " in " + getPathDef(path));
        }
        assertEquals(nodes.length, i);
    }

    protected static <E> void assertContains(Iterable<E> actual, E... expected) {
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

    protected static String getPathDef(Path path) {
        StringBuilder builder = new StringBuilder();
        for (Node node : path.nodes()) {
            if (builder.length() > 0) {
                builder.append(',');
            }
            builder.append(node.getProperty(SimpleGraphBuilder.KEY_ID));
        }
        return builder.toString();
    }

    private static void assertPaths(Iterable<? extends Path> paths, List<String> pathDefs) {
        List<String> unexpectedDefs = new ArrayList<>();
        try (ResourceIterator<? extends Path> iterator = Iterators.asResourceIterator(paths.iterator())) {
            while (iterator.hasNext()) {
                Path path = iterator.next();

                String pathDef = getPathDef(path);
                int index = pathDefs.indexOf(pathDef);
                if (index != -1) {
                    pathDefs.remove(index);
                } else {
                    unexpectedDefs.add(getPathDef(path));
                }
            }
        }
        assertTrue(
                unexpectedDefs.isEmpty(),
                "These unexpected paths were found: " + unexpectedDefs
                        + ". In addition these expected paths weren't found:" + pathDefs);
        assertTrue(pathDefs.isEmpty(), "These were expected, but not found: " + pathDefs);
    }

    protected static void assertPaths(Iterable<? extends Path> paths, String... pathDefinitions) {
        assertPaths(paths, new ArrayList<>(Arrays.asList(pathDefinitions)));
    }

    protected static void assertPathsWithPaths(Iterable<? extends Path> actualPaths, Path... expectedPaths) {
        List<String> pathDefs = new ArrayList<>();
        for (Path path : expectedPaths) {
            pathDefs.add(getPathDef(path));
        }
        assertPaths(actualPaths, pathDefs);
    }

    protected static void assertPathDef(Path expected, Path actual) {
        int expectedLength = expected.length();
        int actualLength = actual.length();
        assertEquals(
                expectedLength,
                actualLength,
                "Actual path length " + actualLength + " differ from expected path length " + expectedLength);
        Iterator<Node> expectedNodes = expected.nodes().iterator();
        Iterator<Node> actualNodes = actual.nodes().iterator();
        int position = 0;
        while (expectedNodes.hasNext() && actualNodes.hasNext()) {
            assertEquals(
                    expectedNodes.next().getProperty(SimpleGraphBuilder.KEY_ID),
                    actualNodes.next().getProperty(SimpleGraphBuilder.KEY_ID),
                    "Path differ on position " + position + ". Expected "
                            + getPathDef(expected) + ", actual "
                            + getPathDef(actual));
            position++;
        }
    }
}
