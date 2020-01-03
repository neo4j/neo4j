/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import common.Neo4jAlgoTestCase;
import common.SimpleGraphBuilder;
import org.junit.Test;

import org.neo4j.graphalgo.CommonEvaluators;
import org.neo4j.graphalgo.impl.shortestpath.Dijkstra;
import org.neo4j.graphdb.Direction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DijkstraTest extends Neo4jAlgoTestCase
{
    protected Dijkstra<Double> getDijkstra( SimpleGraphBuilder graph,
        Double startCost, String startNode, String endNode )
    {
        return new Dijkstra<>( startCost, graph.getNode( startNode ), graph.getNode( endNode ), CommonEvaluators.doubleCostEvaluator( "cost" ),
                new org.neo4j.graphalgo.impl.util.DoubleAdder(), Double::compareTo,
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
        assertEquals( 0.0, dijkstra.getCost(), 0.0 );
        assertEquals( 1, dijkstra.getPathAsNodes().size() );
        dijkstra = getDijkstra( graph, 3.0, "lonely", "lonely" );
        assertEquals( 6.0, dijkstra.getCost(), 0.0 );
        assertEquals( 1, dijkstra.getPathAsNodes().size() );
        assertEquals( 1, dijkstra.getPathsAsNodes().size() );
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
        graph.makeEdge( "a", "e", "cost", 1 );
        graph.makeEdge( "b", "c", "cost", (byte) 1 );
        graph.makeEdge( "c", "d", "cost", (short) 1 );
        graph.makeEdge( "d", "e", "cost", (double) 1 );
        graph.makeEdge( "e", "f", "cost", (double) 1 );
        Dijkstra<Double> dijkstra = getDijkstra( graph, 0.0, "a", "a" );
        assertEquals( 0.0, dijkstra.getCost(), 0.0 );
        assertEquals( 1, dijkstra.getPathAsNodes().size() );
        dijkstra = getDijkstra( graph, 3.0, "a", "a" );
        assertEquals( 6.0, dijkstra.getCost(), 0.0 );
        assertEquals( 1, dijkstra.getPathAsNodes().size() );
        assertEquals( 0, dijkstra.getPathAsRelationships().size() );
        assertEquals( 1, dijkstra.getPath().size() );
        assertEquals( 1, dijkstra.getPathsAsNodes().size() );
    }

    @Test
    public void testDijkstraChain()
    {
        graph.makeEdge( "a", "b", "cost", (double) 1 );
        graph.makeEdge( "b", "c", "cost", (float) 2 );
        graph.makeEdge( "c", "d", "cost", (byte) 3 );
        Dijkstra<Double> dijkstra = getDijkstra( graph, 0.0, "a", "d" );
        assertEquals( 6.0, dijkstra.getCost(), 0.0 );
        assertNotNull( dijkstra.getPathAsNodes() );
        assertEquals( 4, dijkstra.getPathAsNodes().size() );
        assertEquals( 1, dijkstra.getPathsAsNodes().size() );
        dijkstra = getDijkstra( graph, 0.0, "d", "a" );
        assertEquals( 6.0, dijkstra.getCost(), 0.0 );
        assertEquals( 4, dijkstra.getPathAsNodes().size() );
        dijkstra = getDijkstra( graph, 0.0, "d", "b" );
        assertEquals( 5.0, dijkstra.getCost(), 0.0 );
        assertEquals( 3, dijkstra.getPathAsNodes().size() );
        assertEquals( 2, dijkstra.getPathAsRelationships().size() );
        assertEquals( 5, dijkstra.getPath().size() );
    }

    /**
     * /--2--A--7--B--2--\ S E \----7---C---7----/
     */
    @Test
    public void testDijkstraTraverserMeeting()
    {
        graph.makeEdge( "s", "c", "cost", (double) 7 );
        graph.makeEdge( "c", "e", "cost", (float) 7 );
        graph.makeEdge( "s", "a", "cost", (long) 2 );
        graph.makeEdge( "a", "b", "cost", 7 );
        graph.makeEdge( "b", "e", "cost", (byte) 2 );
        Dijkstra<Double> dijkstra = getDijkstra( graph, 0.0, "s", "e" );
        assertEquals( 11.0, dijkstra.getCost(), 0.0 );
        assertNotNull( dijkstra.getPathAsNodes() );
        assertEquals( 4, dijkstra.getPathAsNodes().size() );
        assertEquals( 1, dijkstra.getPathsAsNodes().size() );
    }
}
