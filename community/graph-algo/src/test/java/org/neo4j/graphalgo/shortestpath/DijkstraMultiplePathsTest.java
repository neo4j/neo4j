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

import java.util.List;

import org.junit.Test;
import org.neo4j.graphalgo.CommonEvaluators;
import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.impl.shortestpath.Dijkstra;
import org.neo4j.graphalgo.impl.util.DoubleAdder;
import org.neo4j.graphalgo.impl.util.DoubleComparator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

import common.Neo4jAlgoTestCase;
import common.SimpleGraphBuilder;

public class DijkstraMultiplePathsTest extends Neo4jAlgoTestCase
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
     * A triangle with 0 cost should generate two paths between every pair of
     * nodes.
     */
    @Test
    public void testTriangle()
    {
        graph.makeEdge( "a", "b", "cost", (double) 0 );
        graph.makeEdge( "b", "c", "cost", (double) 0 );
        graph.makeEdge( "c", "a", "cost", (double) 0 );
        Dijkstra<Double> dijkstra;
        String[] nodes = { "a", "b", "c" };
        for ( String node1 : nodes )
        {
            for ( String node2 : nodes )
            {
                dijkstra = getDijkstra( graph, 0.0, node1, node2 );
                int nrPaths = dijkstra.getPathsAsNodes().size();
                if ( !node1.equals( node2 ) )
                {
                    assertTrue( "Number of paths (" + node1 + "->" + node2
                        + "): " + nrPaths, nrPaths == 2 );
                }
                assertTrue( dijkstra.getCost() == 0.0 );
            }
        }
    }

    /**
     * From each direction 2 ways are possible so 4 ways should be the total.
     */
    @Test
    public void test1()
    {
        graph.makeEdge( "a", "b", "cost", (double) 1 );
        graph.makeEdge( "b", "d", "cost", (float) 1 );
        graph.makeEdge( "a", "c", "cost", (int) 1 );
        graph.makeEdge( "c", "d", "cost", (long) 1 );
        graph.makeEdge( "d", "e", "cost", (short) 1 );
        graph.makeEdge( "e", "f", "cost", (byte) 1 );
        graph.makeEdge( "f", "h", "cost", (float) 1 );
        graph.makeEdge( "e", "g", "cost", (double) 1 );
        graph.makeEdge( "g", "h", "cost", (double) 1 );
        Dijkstra<Double> dijkstra = getDijkstra( graph, 0.0, "a", "h" );
        assertTrue( dijkstra.getPaths().size() == 4 );
        assertTrue( dijkstra.getPathsAsNodes().size() == 4 );
        assertTrue( dijkstra.getPathsAsRelationships().size() == 4 );
        assertTrue( dijkstra.getCost() == 5.0 );
    }

    /**
     * Two different ways. This is supposed to test when the traversers meet in
     * several places.
     */
    @Test
    public void test2()
    {
        graph.makeEdge( "a", "b", "cost", (double) 1 );
        graph.makeEdge( "a", "f", "cost", (float) 1 );
        graph.makeEdge( "b", "c", "cost", (long) 1 );
        graph.makeEdge( "f", "g", "cost", (int) 1 );
        graph.makeEdge( "c", "d", "cost", (short) 1 );
        graph.makeEdge( "g", "h", "cost", (byte) 1 );
        graph.makeEdge( "d", "e", "cost", (float) 1 );
        graph.makeEdge( "h", "e", "cost", (double) 1 );
        Dijkstra<Double> dijkstra = getDijkstra( graph, 0.0, "a", "e" );
        assertTrue( dijkstra.getPaths().size() == 2 );
        assertTrue( dijkstra.getPathsAsNodes().size() == 2 );
        assertTrue( dijkstra.getPathsAsRelationships().size() == 2 );
        assertTrue( dijkstra.getCost() == 4.0 );
    }

    /**
     * One side finding several paths to one node previously visited by the
     * other side. The other side is kept busy with a chain of cost zero.
     */
    @Test
    public void test3()
    {
        // "zero" side
        graph.makeEdge( "a", "b", "cost", (double) 0 );
        graph.makeEdge( "b", "c", "cost", (float) 0 );
        graph.makeEdge( "c", "d", "cost", (long) 0 );
        graph.makeEdge( "d", "e", "cost", (int) 0 );
        graph.makeEdge( "e", "f", "cost", (byte) 0 );
        graph.makeEdge( "f", "g", "cost", (float) 0 );
        graph.makeEdge( "g", "h", "cost", (short) 0 );
        graph.makeEdge( "h", "i", "cost", (double) 0 );
        graph.makeEdge( "i", "j", "cost", (double) 0 );
        graph.makeEdge( "j", "k", "cost", (double) 0 );
        // "discovering" side
        graph.makeEdge( "z", "y", "cost", (double) 0 );
        graph.makeEdge( "y", "x", "cost", (double) 0 );
        graph.makeEdge( "x", "w", "cost", (double) 0 );
        graph.makeEdge( "w", "b", "cost", (double) 1 );
        graph.makeEdge( "x", "b", "cost", (float) 2 );
        graph.makeEdge( "y", "b", "cost", (long) 1 );
        graph.makeEdge( "z", "b", "cost", (int) 1 );
        graph.makeEdge( "zz", "z", "cost", (double) 0 );
        Dijkstra<Double> dijkstra = getDijkstra( graph, 0.0, "a", "zz" );
        assertTrue( dijkstra.getPathsAsNodes().size() == 3 );
        assertTrue( dijkstra.getCost() == 1.0 );
    }

    /**
     * another variant of the test above, but the discovering is a bit mixed.
     */
    @Test
    public void test4()
    {
        // "zero" side
        graph.makeEdge( "a", "b", "cost", (double) 0 );
        graph.makeEdge( "b", "c", "cost", (double) 0 );
        graph.makeEdge( "c", "d", "cost", (double) 0 );
        graph.makeEdge( "d", "e", "cost", (double) 0 );
        graph.makeEdge( "e", "f", "cost", (double) 0 );
        graph.makeEdge( "f", "g", "cost", (double) 0 );
        graph.makeEdge( "g", "h", "cost", (double) 0 );
        graph.makeEdge( "h", "i", "cost", (double) 0 );
        graph.makeEdge( "i", "j", "cost", (double) 0 );
        graph.makeEdge( "j", "k", "cost", (double) 0 );
        // "discovering" side
        graph.makeEdge( "z", "y", "cost", (double) 0 );
        graph.makeEdge( "y", "x", "cost", (double) 0 );
        graph.makeEdge( "x", "w", "cost", (double) 0 );
        graph.makeEdge( "w", "b", "cost", (double) 1 );
        graph.makeEdge( "x", "b", "cost", (float) 2 );
        graph.makeEdge( "y", "b", "cost", (long) 1 );
        graph.makeEdge( "z", "b", "cost", (int) 1 );
        Dijkstra<Double> dijkstra = getDijkstra( graph, 0.0, "a", "z" );
        assertTrue( dijkstra.getPathsAsNodes().size() == 3 );
        assertTrue( dijkstra.getCost() == 1.0 );
    }

    /**
     * "Diamond" shape, with some weights to resemble the test case above.
     */
    @Test
    public void test5()
    {
        graph.makeEdge( "a", "b", "cost", (double) 0 );
        graph.makeEdge( "z", "y", "cost", (float) 0 );
        graph.makeEdge( "y", "b", "cost", (long) 1 );
        graph.makeEdge( "z", "b", "cost", (int) 1 );
        graph.makeEdge( "y", "a", "cost", (byte) 1 );
        Dijkstra<Double> dijkstra = getDijkstra( graph, 0.0, "a", "z" );
        List<List<Node>> paths = dijkstra.getPathsAsNodes();
        assertTrue( paths.size() == 3 );
        assertTrue( dijkstra.getCost() == 1.0 );
    }

    @Test
    public void test6()
    {
        graph.makeEdgeChain( "a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,z", "cost",
            (double) 1 );
        graph.makeEdge( "a", "b2", "cost", (double) 4 );
        graph.makeEdge( "b2", "c", "cost", (int) -2 );
        Dijkstra<Double> dijkstra = new Dijkstra<Double>( 0.0, graph
            .getNode( "a" ), graph.getNode( "z" ),
            CommonEvaluators.doubleCostEvaluator( "cost" ),
            new org.neo4j.graphalgo.impl.util.DoubleAdder(),
            new org.neo4j.graphalgo.impl.util.DoubleComparator(),
            Direction.OUTGOING, MyRelTypes.R1 );
        List<List<Node>> paths = dijkstra.getPathsAsNodes();
        assertTrue( paths.size() == 2 );
    }

    @Test
    public void test7()
    {
        Relationship edgeAB = graph.makeEdge( "a", "b" );
        Relationship edgeBC = graph.makeEdge( "b", "c" );
        Relationship edgeCD = graph.makeEdge( "c", "d" );
        Relationship edgeDE = graph.makeEdge( "d", "e" );
        Relationship edgeAB2 = graph.makeEdge( "a", "b2" );
        Relationship edgeB2C = graph.makeEdge( "b2", "c" );
        Relationship edgeCD2 = graph.makeEdge( "c", "d2" );
        Relationship edgeD2E = graph.makeEdge( "d2", "e" );
        Dijkstra<Double> dijkstra = new Dijkstra<Double>(
            0.0,
            graph.getNode( "a" ),
            graph.getNode( "e" ),
            new CostEvaluator<Double>()
            {
                public Double getCost( Relationship relationship,
                    Direction direction )
                {
                    return 1.0;
                }
            }, new DoubleAdder(), new DoubleComparator(), Direction.OUTGOING,
            MyRelTypes.R1 );
        // path discovery flags
        boolean pathBD = false;
        boolean pathB2D = false;
        boolean pathBD2 = false;
        boolean pathB2D2 = false;
        List<List<PropertyContainer>> paths = dijkstra.getPaths();
        assertTrue( paths.size() == 4 );
        for ( List<PropertyContainer> path : paths )
        {
            assertTrue( path.size() == 9 );
            assertTrue( path.get( 0 ).equals( graph.getNode( "a" ) ) );
            assertTrue( path.get( 4 ).equals( graph.getNode( "c" ) ) );
            assertTrue( path.get( 8 ).equals( graph.getNode( "e" ) ) );
            // first choice
            if ( path.get( 2 ).equals( graph.getNode( "b" ) ) )
            {
                assertTrue( path.get( 1 ).equals( edgeAB ) );
                assertTrue( path.get( 3 ).equals( edgeBC ) );
            }
            else
            {
                assertTrue( path.get( 1 ).equals( edgeAB2 ) );
                assertTrue( path.get( 2 ).equals( graph.getNode( "b2" ) ) );
                assertTrue( path.get( 3 ).equals( edgeB2C ) );
            }
            // second choice
            if ( path.get( 6 ).equals( graph.getNode( "d" ) ) )
            {
                assertTrue( path.get( 5 ).equals( edgeCD ) );
                assertTrue( path.get( 7 ).equals( edgeDE ) );
            }
            else
            {
                assertTrue( path.get( 5 ).equals( edgeCD2 ) );
                assertTrue( path.get( 6 ).equals( graph.getNode( "d2" ) ) );
                assertTrue( path.get( 7 ).equals( edgeD2E ) );
            }
            // combinations
            if ( path.get( 2 ).equals( graph.getNode( "b" ) ) )
            {
                if ( path.get( 6 ).equals( graph.getNode( "d" ) ) )
                {
                    pathBD = true;
                }
                else if ( path.get( 6 ).equals( graph.getNode( "d2" ) ) )
                {
                    pathBD2 = true;
                }
            }
            else
            {
                if ( path.get( 6 ).equals( graph.getNode( "d" ) ) )
                {
                    pathB2D = true;
                }
                else if ( path.get( 6 ).equals( graph.getNode( "d2" ) ) )
                {
                    pathB2D2 = true;
                }
            }
        }
        assertTrue( pathBD );
        assertTrue( pathB2D );
        assertTrue( pathBD2 );
        assertTrue( pathB2D2 );
    }

    @Test
    public void test8()
    {
        Relationship edgeAB = graph.makeEdge( "a", "b" );
        Relationship edgeBC = graph.makeEdge( "b", "c" );
        Relationship edgeCD = graph.makeEdge( "c", "d" );
        Relationship edgeDE = graph.makeEdge( "d", "e" );
        Relationship edgeAB2 = graph.makeEdge( "a", "b2" );
        Relationship edgeB2C = graph.makeEdge( "b2", "c" );
        Relationship edgeCD2 = graph.makeEdge( "c", "d2" );
        Relationship edgeD2E = graph.makeEdge( "d2", "e" );
        Dijkstra<Double> dijkstra = new Dijkstra<Double>(
            0.0,
            graph.getNode( "a" ),
            graph.getNode( "e" ),
            new CostEvaluator<Double>()
            {
                public Double getCost( Relationship relationship,
                    Direction direction )
                {
                    return 1.0;
                }
            }, new DoubleAdder(), new DoubleComparator(), Direction.OUTGOING,
            MyRelTypes.R1 );
        // path discovery flags
        boolean pathBD = false;
        boolean pathB2D = false;
        boolean pathBD2 = false;
        boolean pathB2D2 = false;
        List<List<Relationship>> paths = dijkstra.getPathsAsRelationships();
        assertTrue( paths.size() == 4 );
        for ( List<Relationship> path : paths )
        {
            assertTrue( path.size() == 4 );
            // first choice
            if ( path.get( 0 ).equals( edgeAB ) )
            {
                assertTrue( path.get( 1 ).equals( edgeBC ) );
            }
            else
            {
                assertTrue( path.get( 0 ).equals( edgeAB2 ) );
                assertTrue( path.get( 1 ).equals( edgeB2C ) );
            }
            // second choice
            if ( path.get( 2 ).equals( edgeCD ) )
            {
                assertTrue( path.get( 3 ).equals( edgeDE ) );
            }
            else
            {
                assertTrue( path.get( 2 ).equals( edgeCD2 ) );
                assertTrue( path.get( 3 ).equals( edgeD2E ) );
            }
            // combinations
            if ( path.get( 0 ).equals( edgeAB ) )
            {
                if ( path.get( 2 ).equals( edgeCD ) )
                {
                    pathBD = true;
                }
                else if ( path.get( 2 ).equals( edgeCD2 ) )
                {
                    pathBD2 = true;
                }
            }
            else
            {
                if ( path.get( 2 ).equals( edgeCD ) )
                {
                    pathB2D = true;
                }
                else if ( path.get( 2 ).equals( edgeCD2 ) )
                {
                    pathB2D2 = true;
                }
            }
        }
        assertTrue( pathBD );
        assertTrue( pathB2D );
        assertTrue( pathBD2 );
        assertTrue( pathB2D2 );
    }
}
