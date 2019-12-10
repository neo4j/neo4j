/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import org.junit.jupiter.api.Test;

import java.util.List;

import org.neo4j.graphalgo.CommonEvaluators;
import org.neo4j.graphalgo.impl.shortestpath.Dijkstra;
import org.neo4j.graphalgo.impl.util.DoubleAdder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DijkstraMultiplePathsTest extends Neo4jAlgoTestCase
{
    private static Dijkstra<Double> getDijkstra( Transaction transaction, SimpleGraphBuilder graph, Double startCost, String startNode, String endNode )
    {
        return new Dijkstra<>( startCost, graph.getNode( transaction, startNode ), graph.getNode( transaction, endNode ),
                CommonEvaluators.doubleCostEvaluator( "cost" ), new org.neo4j.graphalgo.impl.util.DoubleAdder(), Double::compareTo,
                Direction.BOTH, MyRelTypes.R1 );
    }

    /**
     * A triangle with 0 cost should generate two paths between every pair of
     * nodes.
     */
    @Test
    void testTriangle()
    {
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdge( transaction, "a", "b", "cost", (double) 0 );
            graph.makeEdge( transaction, "b", "c", "cost", (double) 0 );
            graph.makeEdge( transaction, "c", "a", "cost", (double) 0 );
            Dijkstra<Double> dijkstra;
            String[] nodes = {"a", "b", "c"};
            for ( String node1 : nodes )
            {
                for ( String node2 : nodes )
                {
                    dijkstra = getDijkstra( transaction, graph, 0.0, node1, node2 );
                    int nrPaths = dijkstra.getPathsAsNodes().size();
                    if ( !node1.equals( node2 ) )
                    {
                        assertEquals( 2, nrPaths, "Number of paths (" + node1 + "->" + node2 + "): " + nrPaths );
                    }
                    assertEquals( 0.0, dijkstra.getCost(), 0.0 );
                }
            }
            transaction.commit();
        }
    }

    /**
     * From each direction 2 ways are possible so 4 ways should be the total.
     */
    @Test
    void test1()
    {
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdge( transaction, "a", "b", "cost", (double) 1 );
            graph.makeEdge( transaction, "b", "d", "cost", (float) 1 );
            graph.makeEdge( transaction, "a", "c", "cost", 1 );
            graph.makeEdge( transaction, "c", "d", "cost", (long) 1 );
            graph.makeEdge( transaction, "d", "e", "cost", (short) 1 );
            graph.makeEdge( transaction, "e", "f", "cost", (byte) 1 );
            graph.makeEdge( transaction, "f", "h", "cost", (float) 1 );
            graph.makeEdge( transaction, "e", "g", "cost", (double) 1 );
            graph.makeEdge( transaction, "g", "h", "cost", (double) 1 );
            Dijkstra<Double> dijkstra = getDijkstra( transaction, graph, 0.0, "a", "h" );
            assertEquals( 4, dijkstra.getPaths().size() );
            assertEquals( 4, dijkstra.getPathsAsNodes().size() );
            assertEquals( 4, dijkstra.getPathsAsRelationships().size() );
            assertEquals( 5.0, dijkstra.getCost(), 0.0 );
            transaction.commit();
        }
    }

    /**
     * Two different ways. This is supposed to test when the traversers meet in
     * several places.
     */
    @Test
    void test2()
    {
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdge( transaction, "a", "b", "cost", (double) 1 );
            graph.makeEdge( transaction, "a", "f", "cost", (float) 1 );
            graph.makeEdge( transaction, "b", "c", "cost", (long) 1 );
            graph.makeEdge( transaction, "f", "g", "cost", 1 );
            graph.makeEdge( transaction, "c", "d", "cost", (short) 1 );
            graph.makeEdge( transaction, "g", "h", "cost", (byte) 1 );
            graph.makeEdge( transaction, "d", "e", "cost", (float) 1 );
            graph.makeEdge( transaction, "h", "e", "cost", (double) 1 );
            Dijkstra<Double> dijkstra = getDijkstra( transaction, graph, 0.0, "a", "e" );
            assertEquals( 2, dijkstra.getPaths().size() );
            assertEquals( 2, dijkstra.getPathsAsNodes().size() );
            assertEquals( 2, dijkstra.getPathsAsRelationships().size() );
            assertEquals( 4.0, dijkstra.getCost(), 0.0 );
            transaction.commit();
        }
    }

    /**
     * One side finding several paths to one node previously visited by the
     * other side. The other side is kept busy with a chain of cost zero.
     */
    @Test
    void test3()
    {
        // "zero" side
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdge( transaction, "a", "b", "cost", (double) 0 );
            graph.makeEdge( transaction, "b", "c", "cost", (float) 0 );
            graph.makeEdge( transaction, "c", "d", "cost", (long) 0 );
            graph.makeEdge( transaction, "d", "e", "cost", 0 );
            graph.makeEdge( transaction, "e", "f", "cost", (byte) 0 );
            graph.makeEdge( transaction, "f", "g", "cost", (float) 0 );
            graph.makeEdge( transaction, "g", "h", "cost", (short) 0 );
            graph.makeEdge( transaction, "h", "i", "cost", (double) 0 );
            graph.makeEdge( transaction, "i", "j", "cost", (double) 0 );
            graph.makeEdge( transaction, "j", "k", "cost", (double) 0 );
            // "discovering" side
            graph.makeEdge( transaction, "z", "y", "cost", (double) 0 );
            graph.makeEdge( transaction, "y", "x", "cost", (double) 0 );
            graph.makeEdge( transaction, "x", "w", "cost", (double) 0 );
            graph.makeEdge( transaction, "w", "b", "cost", (double) 1 );
            graph.makeEdge( transaction, "x", "b", "cost", (float) 2 );
            graph.makeEdge( transaction, "y", "b", "cost", (long) 1 );
            graph.makeEdge( transaction, "z", "b", "cost", 1 );
            graph.makeEdge( transaction, "zz", "z", "cost", (double) 0 );
            Dijkstra<Double> dijkstra = getDijkstra( transaction, graph, 0.0, "a", "zz" );
            assertEquals( 3, dijkstra.getPathsAsNodes().size() );
            assertEquals( 1.0, dijkstra.getCost(), 0.0 );
            transaction.commit();
        }
    }

    /**
     * another variant of the test above, but the discovering is a bit mixed.
     */
    @Test
    void test4()
    {
        // "zero" side
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdge( transaction, "a", "b", "cost", (double) 0 );
            graph.makeEdge( transaction, "b", "c", "cost", (double) 0 );
            graph.makeEdge( transaction, "c", "d", "cost", (double) 0 );
            graph.makeEdge( transaction, "d", "e", "cost", (double) 0 );
            graph.makeEdge( transaction, "e", "f", "cost", (double) 0 );
            graph.makeEdge( transaction, "f", "g", "cost", (double) 0 );
            graph.makeEdge( transaction, "g", "h", "cost", (double) 0 );
            graph.makeEdge( transaction, "h", "i", "cost", (double) 0 );
            graph.makeEdge( transaction, "i", "j", "cost", (double) 0 );
            graph.makeEdge( transaction, "j", "k", "cost", (double) 0 );
            // "discovering" side
            graph.makeEdge( transaction, "z", "y", "cost", (double) 0 );
            graph.makeEdge( transaction, "y", "x", "cost", (double) 0 );
            graph.makeEdge( transaction, "x", "w", "cost", (double) 0 );
            graph.makeEdge( transaction, "w", "b", "cost", (double) 1 );
            graph.makeEdge( transaction, "x", "b", "cost", (float) 2 );
            graph.makeEdge( transaction, "y", "b", "cost", (long) 1 );
            graph.makeEdge( transaction, "z", "b", "cost", 1 );
            Dijkstra<Double> dijkstra = getDijkstra( transaction, graph, 0.0, "a", "z" );
            assertEquals( 3, dijkstra.getPathsAsNodes().size() );
            assertEquals( 1.0, dijkstra.getCost(), 0.0 );
            transaction.commit();
        }
    }

    /**
     * "Diamond" shape, with some weights to resemble the test case above.
     */
    @Test
    void test5()
    {
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdge( transaction, "a", "b", "cost", (double) 0 );
            graph.makeEdge( transaction, "z", "y", "cost", (float) 0 );
            graph.makeEdge( transaction, "y", "b", "cost", (long) 1 );
            graph.makeEdge( transaction, "z", "b", "cost", 1 );
            graph.makeEdge( transaction, "y", "a", "cost", (byte) 1 );
            Dijkstra<Double> dijkstra = getDijkstra( transaction, graph, 0.0, "a", "z" );
            List<List<Node>> paths = dijkstra.getPathsAsNodes();
            assertEquals( 3, paths.size() );
            assertEquals( 1.0, dijkstra.getCost(), 0.0 );
            transaction.commit();
        }
    }

    @Test
    void test6()
    {
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdgeChain( transaction, "a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,z", "cost", (double) 1 );
            graph.makeEdge( transaction, "a", "b2", "cost", (double) 4 );
            graph.makeEdge( transaction, "b2", "c", "cost", -2 );
            Dijkstra<Double> dijkstra =
                    new Dijkstra<>( 0.0, graph.getNode( transaction, "a" ), graph.getNode( transaction, "z" ),
                            CommonEvaluators.doubleCostEvaluator( "cost" ),
                            new DoubleAdder(), Double::compareTo, Direction.OUTGOING, MyRelTypes.R1 );
            List<List<Node>> paths = dijkstra.getPathsAsNodes();
            assertEquals( 2, paths.size() );
            transaction.commit();
        }
    }

    @Test
    void test7()
    {
        try ( Transaction transaction = graphDb.beginTx() )
        {
            Relationship edgeAB = graph.makeEdge( transaction, "a", "b" );
            Relationship edgeBC = graph.makeEdge( transaction, "b", "c" );
            Relationship edgeCD = graph.makeEdge( transaction, "c", "d" );
            Relationship edgeDE = graph.makeEdge( transaction, "d", "e" );
            Relationship edgeAB2 = graph.makeEdge( transaction, "a", "b2" );
            Relationship edgeB2C = graph.makeEdge( transaction, "b2", "c" );
            Relationship edgeCD2 = graph.makeEdge( transaction, "c", "d2" );
            Relationship edgeD2E = graph.makeEdge( transaction, "d2", "e" );
            Dijkstra<Double> dijkstra =
                    new Dijkstra<>( 0.0, graph.getNode( transaction, "a" ), graph.getNode( transaction, "e" ),
                            ( relationship, direction ) -> 1.0, new DoubleAdder(), Double::compareTo,
                            Direction.OUTGOING, MyRelTypes.R1 );
            // path discovery flags
            boolean pathBD = false;
            boolean pathB2D = false;
            boolean pathBD2 = false;
            boolean pathB2D2 = false;
            List<List<Entity>> paths = dijkstra.getPaths();
            assertEquals( 4, paths.size() );
            for ( List<Entity> path : paths )
            {
                assertEquals( 9, path.size() );
                assertEquals( path.get( 0 ), graph.getNode( transaction, "a" ) );
                assertEquals( path.get( 4 ), graph.getNode( transaction, "c" ) );
                assertEquals( path.get( 8 ), graph.getNode( transaction, "e" ) );
                // first choice
                if ( path.get( 2 ).equals( graph.getNode( transaction, "b" ) ) )
                {
                    assertEquals( path.get( 1 ), edgeAB );
                    assertEquals( path.get( 3 ), edgeBC );
                }
                else
                {
                    assertEquals( path.get( 1 ), edgeAB2 );
                    assertEquals( path.get( 2 ), graph.getNode( transaction, "b2" ) );
                    assertEquals( path.get( 3 ), edgeB2C );
                }
                // second choice
                if ( path.get( 6 ).equals( graph.getNode( transaction, "d" ) ) )
                {
                    assertEquals( path.get( 5 ), edgeCD );
                    assertEquals( path.get( 7 ), edgeDE );
                }
                else
                {
                    assertEquals( path.get( 5 ), edgeCD2 );
                    assertEquals( path.get( 6 ), graph.getNode( transaction, "d2" ) );
                    assertEquals( path.get( 7 ), edgeD2E );
                }
                // combinations
                if ( path.get( 2 ).equals( graph.getNode( transaction, "b" ) ) )
                {
                    if ( path.get( 6 ).equals( graph.getNode( transaction, "d" ) ) )
                    {
                        pathBD = true;
                    }
                    else if ( path.get( 6 ).equals( graph.getNode( transaction, "d2" ) ) )
                    {
                        pathBD2 = true;
                    }
                }
                else
                {
                    if ( path.get( 6 ).equals( graph.getNode( transaction, "d" ) ) )
                    {
                        pathB2D = true;
                    }
                    else if ( path.get( 6 ).equals( graph.getNode( transaction, "d2" ) ) )
                    {
                        pathB2D2 = true;
                    }
                }
            }
            assertTrue( pathBD );
            assertTrue( pathB2D );
            assertTrue( pathBD2 );
            assertTrue( pathB2D2 );
            transaction.commit();
        }
    }

    @Test
    void test8()
    {
        try ( Transaction transaction = graphDb.beginTx() )
        {
            Relationship edgeAB = graph.makeEdge( transaction, "a", "b" );
            Relationship edgeBC = graph.makeEdge( transaction, "b", "c" );
            Relationship edgeCD = graph.makeEdge( transaction, "c", "d" );
            Relationship edgeDE = graph.makeEdge( transaction, "d", "e" );
            Relationship edgeAB2 = graph.makeEdge( transaction, "a", "b2" );
            Relationship edgeB2C = graph.makeEdge( transaction, "b2", "c" );
            Relationship edgeCD2 = graph.makeEdge( transaction, "c", "d2" );
            Relationship edgeD2E = graph.makeEdge( transaction, "d2", "e" );
            Dijkstra<Double> dijkstra =
                    new Dijkstra<>( 0.0, graph.getNode( transaction, "a" ), graph.getNode( transaction, "e" ),
                            ( relationship, direction ) -> 1.0, new DoubleAdder(), Double::compareTo,
                            Direction.OUTGOING, MyRelTypes.R1 );
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
            transaction.commit();
        }
    }

    /**
     * Should generate three paths. The three paths must have the prefix: a, b, and c. The three paths must have the sufix: f and g.
     * All the edges have cost 0.
     */
    @Test
    public void test9()
    {
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdge( transaction, "a", "b", "cost", (double) 0 );
            graph.makeEdge( transaction, "b", "c", "cost", (double) 0 );
            graph.makeEdge( transaction, "c", "d", "cost", (double) 0 );
            graph.makeEdge( transaction, "d", "e", "cost", (double) 0 );
            graph.makeEdge( transaction, "e", "f", "cost", (double) 0 );
            graph.makeEdge( transaction, "f", "g", "cost", (double) 0 );

            graph.makeEdge( transaction, "d", "j", "cost", (double) 0 );
            graph.makeEdge( transaction, "j", "k", "cost", (double) 0 );
            graph.makeEdge( transaction, "k", "f", "cost", (double) 0 );

            graph.makeEdge( transaction, "c", "h", "cost", (double) 0 );
            graph.makeEdge( transaction, "h", "i", "cost", (double) 0 );
            graph.makeEdge( transaction, "i", "e", "cost", (double) 0 );

            Dijkstra<Double> dijkstra =
                    new Dijkstra<>( 0.0, graph.getNode( transaction, "a" ), graph.getNode( transaction, "g" ), ( relationship, direction ) -> .0,
                                    new DoubleAdder(), Double::compareTo, Direction.OUTGOING, MyRelTypes.R1 );

            List<List<Node>> paths = dijkstra.getPathsAsNodes();

            assertEquals( 3, paths.size() );
            String[] commonPrefix = {"a", "b", "c"};
            String[] commonSuffix = {"f", "g"};
            for ( List<Node> path : paths )
            {
                /**
                 * Check if the prefixes are all correct.
                 */
                for ( int j = 0; j < commonPrefix.length; j++ )
                {
                    assertEquals( path.get( j ), graph.getNode( transaction, commonPrefix[j] ) );
                }

                int pathSize = path.size();

                /**
                 * Check if the suffixes are all correct.
                 */
                for ( int j = 0; j < commonSuffix.length; j++ )
                {
                    assertEquals( path.get( pathSize - j - 1 ), graph.getNode( transaction, commonSuffix[commonSuffix.length - j - 1] ) );
                }
            }
            transaction.commit();
        }
    }
}
