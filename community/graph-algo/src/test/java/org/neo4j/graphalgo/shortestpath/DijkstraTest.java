/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.shortestpath;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.neo4j.graphalgo.CommonEvaluators;
import org.neo4j.graphalgo.impl.shortestpath.Dijkstra;
import org.neo4j.graphdb.Direction;

import common.Neo4jAlgoTestCase;
import common.SimpleGraphBuilder;

public class DijkstraTest extends Neo4jAlgoTestCase
{
    protected Dijkstra<Double> getDijkstra( SimpleGraphBuilder graph,
        Double startCost, String startNode, String endNode )
    {
        return new Dijkstra<Double>( startCost, graph.getNode( startNode ),
            graph.getNode( endNode ),
            CommonEvaluators.doubleCostEvaluator( "cost" ),
            new org.neo4j.graphalgo.impl.util.DoubleAdder(),
            new org.neo4j.graphalgo.impl.util.DoubleComparator(),
            Direction.BOTH, MyRelTypes.R1 );
    }

    /**
     * Test case for just a single node (path length zero)
     */
    @Test
    public void testDijkstraMinimal()
    {
        graph.makeNode( "lonely" );
        Dijkstra<Double> dijkstra = getDijkstra( graph, 0.0, "lonely", "lonely" );
        assertTrue( dijkstra.getCost() == 0.0 );
        assertTrue( dijkstra.getPathAsNodes().size() == 1 );
        dijkstra = getDijkstra( graph, 3.0, "lonely", "lonely" );
        assertTrue( dijkstra.getCost() == 6.0 );
        assertTrue( dijkstra.getPathAsNodes().size() == 1 );
        assertTrue( dijkstra.getPathsAsNodes().size() == 1 );
    }

    /**
     * Test case for a path of length zero, with some surrounding nodes
     */
    @Test
    public void testDijkstraMinimal2()
    {
        graph.makeEdge( "a", "b", "cost", (double) 1 );
        graph.makeEdge( "a", "c", "cost", (float) 1 );
        graph.makeEdge( "a", "d", "cost", (long) 1 );
        graph.makeEdge( "a", "e", "cost", (int) 1 );
        graph.makeEdge( "b", "c", "cost", (byte) 1 );
        graph.makeEdge( "c", "d", "cost", (short) 1 );
        graph.makeEdge( "d", "e", "cost", (double) 1 );
        graph.makeEdge( "e", "f", "cost", (double) 1 );
        Dijkstra<Double> dijkstra = getDijkstra( graph, 0.0, "a", "a" );
        assertTrue( dijkstra.getCost() == 0.0 );
        assertTrue( dijkstra.getPathAsNodes().size() == 1 );
        dijkstra = getDijkstra( graph, 3.0, "a", "a" );
        assertTrue( dijkstra.getCost() == 6.0 );
        assertTrue( dijkstra.getPathAsNodes().size() == 1 );
        assertTrue( dijkstra.getPathAsRelationships().size() == 0 );
        assertTrue( dijkstra.getPath().size() == 1 );
        assertTrue( dijkstra.getPathsAsNodes().size() == 1 );
    }

    @Test
    public void testDijkstraChain()
    {
        graph.makeEdge( "a", "b", "cost", (double) 1 );
        graph.makeEdge( "b", "c", "cost", (float) 2 );
        graph.makeEdge( "c", "d", "cost", (byte) 3 );
        Dijkstra<Double> dijkstra = getDijkstra( graph, 0.0, "a", "d" );
        assertTrue( dijkstra.getCost() == 6.0 );
        assertTrue( dijkstra.getPathAsNodes() != null );
        assertTrue( dijkstra.getPathAsNodes().size() == 4 );
        assertTrue( dijkstra.getPathsAsNodes().size() == 1 );
        dijkstra = getDijkstra( graph, 0.0, "d", "a" );
        assertTrue( dijkstra.getCost() == 6.0 );
        assertTrue( dijkstra.getPathAsNodes().size() == 4 );
        dijkstra = getDijkstra( graph, 0.0, "d", "b" );
        assertTrue( dijkstra.getCost() == 5.0 );
        assertTrue( dijkstra.getPathAsNodes().size() == 3 );
        assertTrue( dijkstra.getPathAsRelationships().size() == 2 );
        assertTrue( dijkstra.getPath().size() == 5 );
    }

    /**
     * /--2--A--7--B--2--\ S E \----7---C---7----/
     */
    @Test
    public void testDijstraTraverserMeeting()
    {
        graph.makeEdge( "s", "c", "cost", (double) 7 );
        graph.makeEdge( "c", "e", "cost", (float) 7 );
        graph.makeEdge( "s", "a", "cost", (long) 2 );
        graph.makeEdge( "a", "b", "cost", (int) 7 );
        graph.makeEdge( "b", "e", "cost", (byte) 2 );
        Dijkstra<Double> dijkstra = getDijkstra( graph, 0.0, "s", "e" );
        assertTrue( dijkstra.getCost() == 11.0 );
        assertTrue( dijkstra.getPathAsNodes() != null );
        assertTrue( dijkstra.getPathAsNodes().size() == 4 );
        assertTrue( dijkstra.getPathsAsNodes().size() == 1 );
    }
}
