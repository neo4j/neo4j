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
package org.neo4j.graphalgo.shortestpath;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.CommonEvaluators;
import org.neo4j.graphalgo.Neo4jAlgoTestCase;
import org.neo4j.graphalgo.SimpleGraphBuilder;
import org.neo4j.graphalgo.impl.shortestpath.Dijkstra;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Transaction;

class DijkstraTest extends Neo4jAlgoTestCase {
    private static Dijkstra<Double> getDijkstra(
            Transaction transaction, SimpleGraphBuilder graph, Double startCost, String startNode, String endNode) {
        return new Dijkstra<>(
                startCost,
                graph.getNode(transaction, startNode),
                graph.getNode(transaction, endNode),
                CommonEvaluators.doubleCostEvaluator("cost"),
                new org.neo4j.graphalgo.impl.util.DoubleAdder(),
                Double::compareTo,
                Direction.BOTH,
                MyRelTypes.R1);
    }

    /**
     * Test case for just a single node (path length zero)
     */
    @Test
    void testDijkstraMinimal() {
        try (Transaction transaction = graphDb.beginTx()) {
            graph.makeNode(transaction, "lonely");
            Dijkstra<Double> dijkstra = getDijkstra(transaction, graph, 0.0, "lonely", "lonely");
            assertEquals(0.0, dijkstra.getCost(), 0.0);
            assertEquals(1, dijkstra.getPathAsNodes().size());
            dijkstra = getDijkstra(transaction, graph, 3.0, "lonely", "lonely");
            assertEquals(6.0, dijkstra.getCost(), 0.0);
            assertEquals(1, dijkstra.getPathAsNodes().size());
            assertEquals(1, dijkstra.getPathsAsNodes().size());
            transaction.commit();
        }
    }

    /**
     * Test case for a path of length zero, with some surrounding nodes
     */
    @Test
    void testDijkstraMinimal2() {
        try (Transaction transaction = graphDb.beginTx()) {
            graph.makeEdge(transaction, "a", "b", "cost", (double) 1);
            graph.makeEdge(transaction, "a", "c", "cost", (float) 1);
            graph.makeEdge(transaction, "a", "d", "cost", (long) 1);
            graph.makeEdge(transaction, "a", "e", "cost", 1);
            graph.makeEdge(transaction, "b", "c", "cost", (byte) 1);
            graph.makeEdge(transaction, "c", "d", "cost", (short) 1);
            graph.makeEdge(transaction, "d", "e", "cost", (double) 1);
            graph.makeEdge(transaction, "e", "f", "cost", (double) 1);
            Dijkstra<Double> dijkstra = getDijkstra(transaction, graph, 0.0, "a", "a");
            assertEquals(0.0, dijkstra.getCost(), 0.0);
            assertEquals(1, dijkstra.getPathAsNodes().size());
            dijkstra = getDijkstra(transaction, graph, 3.0, "a", "a");
            assertEquals(6.0, dijkstra.getCost(), 0.0);
            assertEquals(1, dijkstra.getPathAsNodes().size());
            assertEquals(0, dijkstra.getPathAsRelationships().size());
            assertEquals(1, dijkstra.getPath().size());
            assertEquals(1, dijkstra.getPathsAsNodes().size());
            transaction.commit();
        }
    }

    @Test
    void testDijkstraChain() {
        try (Transaction transaction = graphDb.beginTx()) {
            graph.makeEdge(transaction, "a", "b", "cost", (double) 1);
            graph.makeEdge(transaction, "b", "c", "cost", (float) 2);
            graph.makeEdge(transaction, "c", "d", "cost", (byte) 3);
            Dijkstra<Double> dijkstra = getDijkstra(transaction, graph, 0.0, "a", "d");
            assertEquals(6.0, dijkstra.getCost(), 0.0);
            assertNotNull(dijkstra.getPathAsNodes());
            assertEquals(4, dijkstra.getPathAsNodes().size());
            assertEquals(1, dijkstra.getPathsAsNodes().size());
            dijkstra = getDijkstra(transaction, graph, 0.0, "d", "a");
            assertEquals(6.0, dijkstra.getCost(), 0.0);
            assertEquals(4, dijkstra.getPathAsNodes().size());
            dijkstra = getDijkstra(transaction, graph, 0.0, "d", "b");
            assertEquals(5.0, dijkstra.getCost(), 0.0);
            assertEquals(3, dijkstra.getPathAsNodes().size());
            assertEquals(2, dijkstra.getPathAsRelationships().size());
            assertEquals(5, dijkstra.getPath().size());
            transaction.commit();
        }
    }

    /**
     * /--2--A--7--B--2--\ S E \----7---C---7----/
     */
    @Test
    void testDijkstraTraverserMeeting() {
        try (Transaction transaction = graphDb.beginTx()) {
            graph.makeEdge(transaction, "s", "c", "cost", (double) 7);
            graph.makeEdge(transaction, "c", "e", "cost", (float) 7);
            graph.makeEdge(transaction, "s", "a", "cost", (long) 2);
            graph.makeEdge(transaction, "a", "b", "cost", 7);
            graph.makeEdge(transaction, "b", "e", "cost", (byte) 2);
            Dijkstra<Double> dijkstra = getDijkstra(transaction, graph, 0.0, "s", "e");
            assertEquals(11.0, dijkstra.getCost(), 0.0);
            assertNotNull(dijkstra.getPathAsNodes());
            assertEquals(4, dijkstra.getPathAsNodes().size());
            assertEquals(1, dijkstra.getPathsAsNodes().size());
            transaction.commit();
        }
    }
}
