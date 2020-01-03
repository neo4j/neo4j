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

import java.util.List;

import org.neo4j.graphalgo.CommonEvaluators;
import org.neo4j.graphalgo.impl.shortestpath.Dijkstra;
import org.neo4j.graphalgo.impl.util.DoubleAdder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DijkstraMultiplePathsTest extends Neo4jAlgoTestCase
{
    protected Dijkstra<Double> getDijkstra( SimpleGraphBuilder graph,
        Double startCost, String startNode, String endNode )
    {
        return new Dijkstra<>( startCost, graph.getNode( startNode ), graph.getNode( endNode ), CommonEvaluators.doubleCostEvaluator( "cost" ),
                new org.neo4j.graphalgo.impl.util.DoubleAdder(), Double::compareTo,
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
                    assertEquals( "Number of paths (" + node1 + "->" + node2 + "): " + nrPaths, 2, nrPaths );
                }
                assertEquals( 0.0, dijkstra.getCost(), 0.0 );
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
        graph.makeEdge( "a", "c", "cost", 1 );
        graph.makeEdge( "c", "d", "cost", (long) 1 );
        graph.makeEdge( "d", "e", "cost", (short) 1 );
        graph.makeEdge( "e", "f", "cost", (byte) 1 );
        graph.makeEdge( "f", "h", "cost", (float) 1 );
        graph.makeEdge( "e", "g", "cost", (double) 1 );
        graph.makeEdge( "g", "h", "cost", (double) 1 );
        Dijkstra<Double> dijkstra = getDijkstra( graph, 0.0, "a", "h" );
        assertEquals( 4, dijkstra.getPaths().size() );
        assertEquals( 4, dijkstra.getPathsAsNodes().size() );
        assertEquals( 4, dijkstra.getPathsAsRelationships().size() );
        assertEquals( 5.0, dijkstra.getCost(), 0.0 );
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
        graph.makeEdge( "f", "g", "cost", 1 );
        graph.makeEdge( "c", "d", "cost", (short) 1 );
        graph.makeEdge( "g", "h", "cost", (byte) 1 );
        graph.makeEdge( "d", "e", "cost", (float) 1 );
        graph.makeEdge( "h", "e", "cost", (double) 1 );
        Dijkstra<Double> dijkstra = getDijkstra( graph, 0.0, "a", "e" );
        assertEquals( 2, dijkstra.getPaths().size() );
        assertEquals( 2, dijkstra.getPathsAsNodes().size() );
        assertEquals( 2, dijkstra.getPathsAsRelationships().size() );
        assertEquals( 4.0, dijkstra.getCost(), 0.0 );
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
        graph.makeEdge( "d", "e", "cost", 0 );
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
        graph.makeEdge( "z", "b", "cost", 1 );
        graph.makeEdge( "zz", "z", "cost", (double) 0 );
        Dijkstra<Double> dijkstra = getDijkstra( graph, 0.0, "a", "zz" );
        assertEquals( 3, dijkstra.getPathsAsNodes().size() );
        assertEquals( 1.0, dijkstra.getCost(), 0.0 );
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
        graph.makeEdge( "z", "b", "cost", 1 );
        Dijkstra<Double> dijkstra = getDijkstra( graph, 0.0, "a", "z" );
        assertEquals( 3, dijkstra.getPathsAsNodes().size() );
        assertEquals( 1.0, dijkstra.getCost(), 0.0 );
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
        graph.makeEdge( "z", "b", "cost", 1 );
        graph.makeEdge( "y", "a", "cost", (byte) 1 );
        Dijkstra<Double> dijkstra = getDijkstra( graph, 0.0, "a", "z" );
        List<List<Node>> paths = dijkstra.getPathsAsNodes();
        assertEquals( 3, paths.size() );
        assertEquals( 1.0, dijkstra.getCost(), 0.0 );
    }

    @Test
    public void test6()
    {
        graph.makeEdgeChain( "a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,z", "cost",
            (double) 1 );
        graph.makeEdge( "a", "b2", "cost", (double) 4 );
        graph.makeEdge( "b2", "c", "cost", -2 );
        Dijkstra<Double> dijkstra = new Dijkstra<>( 0.0, graph.getNode( "a" ), graph.getNode( "z" ),
                CommonEvaluators.doubleCostEvaluator( "cost" ), new org.neo4j.graphalgo.impl.util.DoubleAdder(),
                Double::compareTo, Direction.OUTGOING, MyRelTypes.R1 );
        List<List<Node>> paths = dijkstra.getPathsAsNodes();
        assertEquals( 2, paths.size() );
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
        Dijkstra<Double> dijkstra =
                new Dijkstra<>( 0.0, graph.getNode( "a" ), graph.getNode( "e" ), ( relationship, direction ) -> 1.0,
                        new DoubleAdder(), Double::compareTo, Direction.OUTGOING, MyRelTypes.R1 );
        // path discovery flags
        boolean pathBD = false;
        boolean pathB2D = false;
        boolean pathBD2 = false;
        boolean pathB2D2 = false;
        List<List<PropertyContainer>> paths = dijkstra.getPaths();
        assertEquals( 4, paths.size() );
        for ( List<PropertyContainer> path : paths )
        {
            assertEquals( 9, path.size() );
            assertEquals( path.get( 0 ), graph.getNode( "a" ) );
            assertEquals( path.get( 4 ), graph.getNode( "c" ) );
            assertEquals( path.get( 8 ), graph.getNode( "e" ) );
            // first choice
            if ( path.get( 2 ).equals( graph.getNode( "b" ) ) )
            {
                assertEquals( path.get( 1 ), edgeAB );
                assertEquals( path.get( 3 ), edgeBC );
            }
            else
            {
                assertEquals( path.get( 1 ), edgeAB2 );
                assertEquals( path.get( 2 ), graph.getNode( "b2" ) );
                assertEquals( path.get( 3 ), edgeB2C );
            }
            // second choice
            if ( path.get( 6 ).equals( graph.getNode( "d" ) ) )
            {
                assertEquals( path.get( 5 ), edgeCD );
                assertEquals( path.get( 7 ), edgeDE );
            }
            else
            {
                assertEquals( path.get( 5 ), edgeCD2 );
                assertEquals( path.get( 6 ), graph.getNode( "d2" ) );
                assertEquals( path.get( 7 ), edgeD2E );
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
        Dijkstra<Double> dijkstra =
                new Dijkstra<>( 0.0, graph.getNode( "a" ), graph.getNode( "e" ), ( relationship, direction ) -> 1.0,
                        new DoubleAdder(), Double::compareTo, Direction.OUTGOING,
                        MyRelTypes.R1 );
        // path discovery flags
        boolean pathBD = false;
        boolean pathB2D = false;
        boolean pathBD2 = false;
        boolean pathB2D2 = false;
        List<List<Relationship>> paths = dijkstra.getPathsAsRelationships();
        assertEquals( 4, paths.size() );
        for ( List<Relationship> path : paths )
        {
            assertEquals( 4, path.size() );
            // first choice
            if ( path.get( 0 ).equals( edgeAB ) )
            {
                assertEquals( path.get( 1 ), edgeBC );
            }
            else
            {
                assertEquals( path.get( 0 ), edgeAB2 );
                assertEquals( path.get( 1 ), edgeB2C );
            }
            // second choice
            if ( path.get( 2 ).equals( edgeCD ) )
            {
                assertEquals( path.get( 3 ), edgeDE );
            }
            else
            {
                assertEquals( path.get( 2 ), edgeCD2 );
                assertEquals( path.get( 3 ), edgeD2E );
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

    /**
     * Should generate three paths. The three paths must have the prefix: a, b, and c. The three paths must have the sufix: f and g.
     * All the edges have cost 0.
     */
    @Test
    public void test9()
    {
        graph.makeEdge( "a", "b", "cost", (double) 0 );
        graph.makeEdge( "b", "c", "cost", (double) 0 );
        graph.makeEdge( "c", "d", "cost", (double) 0 );
        graph.makeEdge( "d", "e", "cost", (double) 0 );
        graph.makeEdge( "e", "f", "cost", (double) 0 );
        graph.makeEdge( "f", "g", "cost", (double) 0 );

        graph.makeEdge( "d", "j", "cost", (double) 0 );
        graph.makeEdge( "j", "k", "cost", (double) 0 );
        graph.makeEdge( "k", "f", "cost", (double) 0 );

        graph.makeEdge( "c", "h", "cost", (double) 0 );
        graph.makeEdge( "h", "i", "cost", (double) 0 );
        graph.makeEdge( "i", "e", "cost", (double) 0 );

        Dijkstra<Double> dijkstra =
                new Dijkstra<>( 0.0, graph.getNode( "a" ), graph.getNode( "g" ), ( relationship, direction ) -> .0,
                                new DoubleAdder(), Double::compareTo, Direction.OUTGOING, MyRelTypes.R1 );

        List<List<Node>> paths = dijkstra.getPathsAsNodes();

        assertEquals( paths.size(), 3 );
        String[] commonPrefix = {"a", "b", "c"};
        String[] commonSuffix = {"f", "g"};
        for ( List<Node> path : paths )
        {
            /**
             * Check if the prefixes are all correct.
             */
            for ( int j = 0; j < commonPrefix.length; j++ )
            {
                assertEquals( path.get( j ), graph.getNode( commonPrefix[j] ) );
            }

            int pathSize = path.size();

            /**
             * Check if the suffixes are all correct.
             */
            for ( int j = 0; j < commonSuffix.length; j++ )
            {
                assertEquals( path.get( pathSize - j - 1 ), graph.getNode( commonSuffix[commonSuffix.length - j - 1] ) );
            }
        }
    }
}
