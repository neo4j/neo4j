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
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.CommonEvaluators;
import org.neo4j.graphalgo.Neo4jAlgoTestCase;
import org.neo4j.graphalgo.SimpleGraphBuilder;
import org.neo4j.graphalgo.impl.shortestpath.Dijkstra;
import org.neo4j.graphalgo.impl.util.DoubleAdder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

class DijkstraIteratorTest extends Neo4jAlgoTestCase {
    @Test
    void testRun() {
        try (Transaction transaction = graphDb.beginTx()) {
            new TestDijkstra().runTest(graph, transaction);
            transaction.commit();
        }
    }

    static class TestDijkstra extends Dijkstra<Double> {
        TestDijkstra() {
            super(
                    0.0,
                    null,
                    null,
                    CommonEvaluators.doubleCostEvaluator("cost"),
                    new DoubleAdder(),
                    Double::compareTo,
                    Direction.BOTH,
                    MyRelTypes.R1);
        }

        class TestIterator extends Dijkstra<Double>.DijkstraIterator {
            TestIterator(
                    Node startNode,
                    Map<Node, List<Relationship>> predecessors,
                    Map<Node, Double> mySeen,
                    Map<Node, Double> otherSeen,
                    Map<Node, Double> myDistances,
                    Map<Node, Double> otherDistances,
                    boolean backwards) {
                super(startNode, predecessors, mySeen, otherSeen, myDistances, otherDistances, backwards);
            }
        }

        void runTest(SimpleGraphBuilder graph, Transaction transaction) {
            graph.makeEdge(transaction, "start", "a", "cost", (double) 1);
            graph.makeEdge(transaction, "a", "x", "cost", (double) 9);
            graph.makeEdge(transaction, "a", "b", "cost", (float) 1);
            graph.makeEdge(transaction, "b", "x", "cost", (double) 7);
            graph.makeEdge(transaction, "b", "c", "cost", (long) 1);
            graph.makeEdge(transaction, "c", "x", "cost", 5);
            graph.makeEdge(transaction, "c", "d", "cost", (byte) 1);
            graph.makeEdge(transaction, "d", "x", "cost", (short) 3);
            graph.makeEdge(transaction, "d", "e", "cost", (double) 1);
            graph.makeEdge(transaction, "e", "x", "cost", (double) 1);
            Map<Node, Double> seen1 = new HashMap<>();
            Map<Node, Double> seen2 = new HashMap<>();
            Map<Node, Double> dists1 = new HashMap<>();
            Map<Node, Double> dists2 = new HashMap<>();
            DijkstraIterator iter1 = new TestIterator(
                    graph.getNode(transaction, "start"), predecessors1, seen1, seen2, dists1, dists2, false);
            // while ( iter1.hasNext() && !limitReached() && !iter1.isDone() )
            assertEquals(iter1.next(), graph.getNode(transaction, "start"));
            assertEquals(iter1.next(), graph.getNode(transaction, "a"));
            assertEquals(10.0, seen1.get(graph.getNode(transaction, "x")), 0.0);
            assertEquals(iter1.next(), graph.getNode(transaction, "b"));
            assertEquals(9.0, seen1.get(graph.getNode(transaction, "x")), 0.0);
            assertEquals(iter1.next(), graph.getNode(transaction, "c"));
            assertEquals(8.0, seen1.get(graph.getNode(transaction, "x")), 0.0);
            assertEquals(iter1.next(), graph.getNode(transaction, "d"));
            assertEquals(7.0, seen1.get(graph.getNode(transaction, "x")), 0.0);
            assertEquals(iter1.next(), graph.getNode(transaction, "e"));
            assertEquals(6.0, seen1.get(graph.getNode(transaction, "x")), 0.0);
            assertEquals(iter1.next(), graph.getNode(transaction, "x"));
            assertEquals(6.0, seen1.get(graph.getNode(transaction, "x")), 0.0);
            assertFalse(iter1.hasNext());
            seen1 = new HashMap<>();
            seen2 = new HashMap<>();
            dists1 = new HashMap<>();
            dists2 = new HashMap<>();
            iter1 = new TestIterator(
                    graph.getNode(transaction, "start"), predecessors1, seen1, seen2, dists1, dists2, false);
            this.numberOfNodesTraversed = 0;
            this.limitMaxNodesToTraverse(3);
            int count = 0;
            while (iter1.hasNext()) {
                iter1.next();
                ++count;
            }
            assertEquals(3, count);
        }
    }
}
