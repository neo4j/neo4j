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
package org.neo4j.graphalgo.path;

import common.Neo4jAlgoTestCase;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.graphalgo.CommonEvaluators;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphalgo.impl.path.Dijkstra;
import org.neo4j.graphalgo.impl.util.PathInterestFactory;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.InitialBranchState;
import org.neo4j.kernel.impl.util.NoneStrictMath;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class DijkstraIncreasingWeightTest extends Neo4jAlgoTestCase
{
    @Test
    public void canFetchLongerPaths()
    {
        /*
         *    1-(b)-1
         *   /       \
         * (s) - 1 - (a) - 1 - (c) - 10 - (d) - 10 - (t)
         *
         */
        Node s = graph.makeNode( "s" );
        Node a = graph.makeNode( "a" );
        Node b = graph.makeNode( "b" );
        Node c = graph.makeNode( "c" );
        Node d = graph.makeNode( "d" );
        Node t = graph.makeNode( "t" );

        graph.makeEdge( "s", "a", "length", 1 );
        graph.makeEdge( "a", "c", "length", 1 );
        graph.makeEdge( "s", "b", "length", 1 );
        graph.makeEdge( "b", "a", "length", 1 );
        graph.makeEdge( "c", "d", "length", 10 );
        graph.makeEdge( "d", "t", "length", 10 );

        PathExpander expander = PathExpanders.allTypesAndDirections();
        Dijkstra algo = new Dijkstra( expander, CommonEvaluators.doubleCostEvaluator( "length" ),
                                      PathInterestFactory.all( NoneStrictMath.EPSILON ) );

        Iterator<WeightedPath> paths = algo.findAllPaths( s, t ).iterator();

        assertTrue( "Expected at least one path.", paths.hasNext() );
        assertPath( paths.next(), s, a, c, d, t );
        assertTrue( "Expected two paths", paths.hasNext() );
        assertPath( paths.next(), s, b, a, c, d, t );
    }

    @Test
    public void shouldReturnPathsInIncreasingOrderOfCost()
    {
        /*
         *
         *      ----- (e) - 1 - (f) ---
         *    /                         \
         *   /    ------- (a) --------   \
         *  1   /            \         \  2
         *  |  2              0         0 |
         *  | /                \         \|
         * (s) - 1 - (c) - 1 - (d) - 1 - (t)
         *   \                 /
         *    -- 1 - (b) - 1 -
         *
         */

        Node s = graph.makeNode( "s" );
        Node t = graph.makeNode( "t" );

        graph.makeEdgeChain( "s,e,f", "length", 1.0 );
        graph.makeEdge( "f", "t", "length", 2 );
        graph.makeEdge( "s","a", "length", 2 );
        graph.makeEdge( "a","t", "length", 0 );
        graph.makeEdge( "s", "c", "length", 1 );
        graph.makeEdge( "c","d", "length", 1 );
        graph.makeEdge( "s","b", "length", 1 );
        graph.makeEdge( "b","d", "length", 1 );
        graph.makeEdge( "d","a", "length", 0 );
        graph.makeEdge( "d","t", "length", 1 );

        PathExpander expander = PathExpanders.allTypesAndDirections();
        Dijkstra algo = new Dijkstra( expander, CommonEvaluators.doubleCostEvaluator( "length" ),
                                                PathInterestFactory.all( NoneStrictMath.EPSILON )  );

        Iterator<WeightedPath> paths = algo.findAllPaths( s, t ).iterator();

        for ( int i = 1; i <= 3; i++ )
        {
            assertTrue( "Expected at least " + i + " path(s)", paths.hasNext() );
            assertTrue( "Expected 3 paths of cost 2", NoneStrictMath.equals( paths.next().weight(), 2 ) );
        }
        for ( int i = 1; i <= 3; i++ )
        {
            assertTrue( "Expected at least " + i + " path(s)", paths.hasNext() );
            assertTrue( "Expected 3 paths of cost 3", NoneStrictMath.equals( paths.next().weight(), 3 ) );
        }
        assertTrue( "Expected at least 7 paths", paths.hasNext() );
        assertTrue( "Expected 1 path of cost 4", NoneStrictMath.equals( paths.next().weight(), 4 ) );
        assertFalse( "Expected exactly 7 paths", paths.hasNext() );
    }

    @Test( timeout = 5000 )
    public void testForLoops()
    {
        /*
         *
         *            (b)
         *           /  \         0
         *          0    0       / \            - 0 - (c1) - 0 -
         *           \  /        \/           /                 \
         * (s) - 1 - (a1) - 1 - (a2) - 1 - (a3)                (a4) - 1 - (t)
         *                                    \                 /
         *                                     - 0 - (c2) - 0 -
         *
         */

        try ( Transaction tx = graphDb.beginTx() )
        {
            Node s = graph.makeNode( "s" );
            Node t = graph.makeNode( "t" );

            // Blob loop
            graph.makeEdge( "s", "a1", "length", 1 );
            graph.makeEdge( "a1", "b", "length", 0 );
            graph.makeEdge( "b", "a1", "length", 0 );

            // Self loop
            graph.makeEdge( "a1", "a2", "length", 1 );
            graph.makeEdge( "a2", "a2", "length", 0 );

            // Diamond loop
            graph.makeEdge( "a2", "a3", "length", 1 );
            graph.makeEdge( "a3", "c1", "length", 0 );
            graph.makeEdge( "a3", "c2", "length", 0 );
            graph.makeEdge( "c1", "a4", "length", 0 );
            graph.makeEdge( "c1", "a4", "length", 0 );
            graph.makeEdge( "a4", "t", "length", 1 );

            PathExpander expander = PathExpanders.allTypesAndDirections();
            Dijkstra algo = new Dijkstra( expander, CommonEvaluators.doubleCostEvaluator( "length" ),
                                                    PathInterestFactory.all( NoneStrictMath.EPSILON ) );

            Iterator<WeightedPath> paths = algo.findAllPaths( s, t ).iterator();

            assertTrue( "Expected at least one path", paths.hasNext() );
            assertTrue( "Expected first path of length 6", paths.next().length() == 6 );
            assertTrue( "Expected at least two paths", paths.hasNext() );
            assertTrue( "Expected second path of length 6", paths.next().length() == 6 );
            assertFalse( "Expected exactly two paths", paths.hasNext() );

            tx.success();
        }
    }

    @Test
    public void testKShortestPaths()
    {
        /*
         *      ----- (e) - 3 - (f) ---
         *    /                         \
         *   /    ------- (a) --------   \
         *  3   /            \         \  3
         *  |  2              0         0 |
         *  | /                \         \|
         * (s) - 1 - (c) - 1 - (d) - 1 - (t)
         *   \                 /
         *    -- 1 - (b) - 1 -
         *
         */
        Node s = graph.makeNode( "s" );
        Node t = graph.makeNode( "t" );

        graph.makeEdge( "s", "a", "length", 2 );
        graph.makeEdge( "s", "b", "length", 1 );
        graph.makeEdge( "s", "c", "length", 1 );
        graph.makeEdge( "s", "e", "length", 3 );
        graph.makeEdge( "a", "t", "length", 0 );
        graph.makeEdge( "b", "d", "length", 1 );
        graph.makeEdge( "c", "d", "length", 1 );
        graph.makeEdge( "d", "a", "length", 0 );
        graph.makeEdge( "d", "t", "length", 1 );
        graph.makeEdge( "e", "f", "length", 3 );
        graph.makeEdge( "f", "t", "length", 3 );

        PathExpander expander = PathExpanders.allTypesAndDirections();
        PathFinder<WeightedPath> algo = new Dijkstra( expander,
                CommonEvaluators.doubleCostEvaluator( "length" ),
                PathInterestFactory.numberOfShortest( NoneStrictMath.EPSILON, 6 ) );

        Iterator<WeightedPath> paths = algo.findAllPaths( s, t ).iterator();

        int count = 0;
        while ( paths.hasNext() )
        {
            count++;
            WeightedPath path = paths.next();
            double expectedWeight;
            if ( count <= 3 )
            {
                expectedWeight = 2.0;
            }
            else
            {
                expectedWeight = 3.0;
            }
            assertTrue( "Expected path number " + count + " to have weight of " + expectedWeight,
                    NoneStrictMath.equals( path.weight(), expectedWeight ) );
        }
        assertTrue( "Expected exactly 6 returned paths", count == 6 );
    }

    @Test
    public void withState() throws Exception
    {
        /* Graph
         *
         * (a)-[1]->(b)-[2]->(c)-[5]->(d)
         */

        graph.makeEdgeChain( "a,b,c,d" );
        setWeight( "a", "b", 1 );
        setWeight( "b", "c", 2 );
        setWeight( "c", "d", 5 );

        InitialBranchState<Integer> state = new InitialBranchState.State<Integer>( 0, 0 );
        final Map<Node, Integer> encounteredState = new HashMap<Node, Integer>();
        PathExpander<Integer> expander = new PathExpander<Integer>()
        {
            @Override
            public Iterable<Relationship> expand( Path path, BranchState<Integer> state )
            {
                if ( path.length() > 0 )
                {
                    int newState = state.getState() + ((Number)path.lastRelationship().getProperty( "weight" )).intValue();
                    state.setState( newState );
                    encounteredState.put( path.endNode(), newState );
                }
                return path.endNode().getRelationships();
            }

            @Override
            public PathExpander<Integer> reverse()
            {
                return this;
            }
        };

        PathFinder<WeightedPath> finder = new Dijkstra( expander, state,
                CommonEvaluators.doubleCostEvaluator( "weight" ) );
        assertPaths(finder.findAllPaths( graph.getNode( "a" ), graph.getNode( "d" ) ),
                "a,b,c,d" );
        assertEquals( 1, encounteredState.get( graph.getNode( "b" ) ).intValue() );
        assertEquals( 3, encounteredState.get( graph.getNode( "c" ) ).intValue() );
        assertEquals( 8, encounteredState.get( graph.getNode( "d" ) ).intValue() );
    }

    private void setWeight( String start, String end, double weight )
    {
        Node startNode = graph.getNode( start );
        Node endNode = graph.getNode( end );
        for ( Relationship rel : startNode.getRelationships() )
        {
            if ( rel.getOtherNode( startNode ).equals( endNode ) )
            {
                rel.setProperty( "weight", weight );
                return;
            }
        }
        throw new RuntimeException( "No relationship between nodes " + start + " and " + end );
    }
}
