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
package org.neo4j.graphalgo.path;

import common.Neo4jAlgoTestCase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.graphalgo.BasicEvaluationContext;
import org.neo4j.graphalgo.CommonEvaluators;
import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.EvaluationContext;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphalgo.impl.path.Dijkstra;
import org.neo4j.graphalgo.impl.path.DijkstraBidirectional;
import org.neo4j.graphalgo.impl.util.PathInterestFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.MathUtil;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.internal.helpers.MathUtil.DEFAULT_EPSILON;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

class DijkstraTest extends Neo4jAlgoTestCase
{
    @ParameterizedTest
    @MethodSource( "params" )
    void pathToSelfReturnsZero( DijkstraFactory factory )
    {
        // GIVEN
        try ( Transaction transaction = graphDb.beginTx() )
        {
            Node start = graph.makeNode( transaction, "A" );

            // WHEN
            var context = new BasicEvaluationContext( transaction, graphDb );
            PathFinder<WeightedPath> finder = factory.dijkstra( context, PathExpanders.allTypesAndDirections() );
            WeightedPath path = finder.findSinglePath( start, start );
            // THEN
            assertNotNull( path );
            assertEquals( start, path.startNode() );
            assertEquals( start, path.endNode() );
            assertEquals( 0, path.length() );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void allPathsToSelfReturnsZero( DijkstraFactory factory )
    {
        // GIVEN
        try ( Transaction transaction = graphDb.beginTx() )
        {
            Node start = graph.makeNode( transaction, "A" );

            // WHEN
            var context = new BasicEvaluationContext( transaction, graphDb );
            PathFinder<WeightedPath> finder = factory.dijkstra( context, PathExpanders.allTypesAndDirections() );
            Iterable<WeightedPath> paths = finder.findAllPaths( start, start );

            // THEN
            for ( WeightedPath path : paths )
            {
                assertNotNull( path );
                assertEquals( start, path.startNode() );
                assertEquals( start, path.endNode() );
                assertEquals( 0, path.length() );
            }
            transaction.commit();
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void canFindOrphanGraph( DijkstraFactory factory )
    {
        /*
         *
         * (A)=1   (relationship to self)
         *
         * Should not find (A)-(A). Should find (A)
         */

        try ( Transaction transaction = graphDb.beginTx() )
        {
            Node nodeA = graph.makeNode( transaction, "A" );
            graph.makeEdge( transaction, "A", "A", "length", 1d );
            var context = new BasicEvaluationContext( transaction, graphDb );
            PathFinder<WeightedPath> finder = factory.dijkstra( context, PathExpanders.allTypesAndDirections() );
            assertPaths( finder.findAllPaths( nodeA, nodeA ), "A" );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void canFindNeighbour( DijkstraFactory factory )
    {
        /*
         * (A) - 1 -(B)
         */
        try ( Transaction transaction = graphDb.beginTx() )
        {
            Node nodeA = graph.makeNode( transaction, "A" );
            Node nodeB = graph.makeNode( transaction, "B" );
            graph.makeEdge( transaction, "A", "B", "length", 1 );
            var context = new BasicEvaluationContext( transaction, graphDb );
            PathFinder<WeightedPath> finder = factory.dijkstra( context, PathExpanders.allTypesAndDirections() );
            assertPaths( finder.findAllPaths( nodeA, nodeB ), "A,B" );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void canFindNeighbourMultipleCorrectPaths( DijkstraFactory factory )
    {
        /*
         *     - 1.0 -
         *   /        \
         * (A) - 1 - (B)
         */
        try ( Transaction transaction = graphDb.beginTx() )
        {
            Node nodeA = graph.makeNode( transaction, "A" );
            Node nodeB = graph.makeNode( transaction, "B" );
            graph.makeEdge( transaction, "A", "B", "length", 1.0 );
            graph.makeEdge( transaction, "A", "B", "length", 1 );

            var context = new BasicEvaluationContext( transaction, graphDb );
            PathFinder<WeightedPath> finder = factory.dijkstra( context, PathExpanders.allTypesAndDirections() );
            assertPaths( finder.findAllPaths( nodeA, nodeB ), "A,B", "A,B" );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void canFindNeighbourMultipleIncorrectPaths( DijkstraFactory factory )
    {
        /*
         *     - 2.0 -
         *   /        \
         * (A) - 1 - (B)
         */
        try ( Transaction transaction = graphDb.beginTx() )
        {
            Node nodeA = graph.makeNode( transaction, "A" );
            Node nodeB = graph.makeNode( transaction, "B" );
            graph.makeEdge( transaction, "A", "B", "length", 2.0 );
            graph.makeEdge( transaction, "A", "B", "length", 1 );

            var context = new BasicEvaluationContext( transaction, graphDb );
            PathFinder<WeightedPath> finder = factory.dijkstra( context, PathExpanders.allTypesAndDirections() );
            Iterator<WeightedPath> paths = finder.findAllPaths( nodeA, nodeB ).iterator();
            assertTrue( paths.hasNext(), "Expect at least one path" );
            WeightedPath path = paths.next();
            assertPath( path, nodeA, nodeB );
            assertEquals( 1, path.weight(), 0.0, "Expect weight 1" );
            assertFalse( paths.hasNext(), "Expected at most one path" );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void canKeepSearchingUntilFoundTrueShortest( DijkstraFactory factory )
    {
        /*
         *
         *  1 - (B) - 1 - (C) - 1 - (D) - 1 - (E) - 1
         *  |                                       |
         * (A) --- 1 --- (G) -- 2 -- (H) --- 1 --- (F)
         *
         */

        try ( Transaction transaction = graphDb.beginTx() )
        {
            Node a = graph.makeNode( transaction, "A" );
            Node b = graph.makeNode( transaction, "B" );
            Node c = graph.makeNode( transaction, "C" );
            Node d = graph.makeNode( transaction, "D" );
            Node e = graph.makeNode( transaction, "E" );
            Node f = graph.makeNode( transaction, "F" );
            Node g = graph.makeNode( transaction, "G" );
            Node h = graph.makeNode( transaction, "H" );

            graph.makeEdgeChain( transaction, "A,B,C,D,E,F", "length", 1 );
            graph.makeEdge( transaction, "A", "G", "length", 1 );
            graph.makeEdge( transaction, "G", "H", "length", 2 );
            graph.makeEdge( transaction, "H", "F", "length", 1 );

            var context = new BasicEvaluationContext( transaction, graphDb );
            PathFinder<WeightedPath> finder = factory.dijkstra( context, PathExpanders.allTypesAndDirections() );
            Iterator<WeightedPath> paths = finder.findAllPaths( a, f ).iterator();

            assertTrue( paths.hasNext(), "Expect at least one path" );
            WeightedPath path = paths.next();
            assertPath( path, a, g, h, f );
            assertEquals( 4, path.weight(), 0.0, "Expect weight 1" );
            assertFalse( paths.hasNext(), "Expected at most one path" );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void canGetPathsInTriangleGraph( DijkstraFactory factory )
    {
        /* NODE (NAME/INDEX)
         *
         * (A/0) ------- 2 -----> (B/1)
         *   \                     /
         *    - 10 -> (C/2) <- 3 -
         */
        try ( Transaction transaction = graphDb.beginTx() )
        {
            Node nodeA = graph.makeNode( transaction, "A" );
            Node nodeB = graph.makeNode( transaction, "B" );
            Node nodeC = graph.makeNode( transaction, "C" );
            graph.makeEdge( transaction, "A", "B", "length", 2d );
            graph.makeEdge( transaction, "B", "C", "length", 3L );
            graph.makeEdge( transaction, "A", "C", "length", (byte) 10 );

            var context = new BasicEvaluationContext( transaction, graphDb );
            PathFinder<WeightedPath> finder = factory.dijkstra( context, PathExpanders.allTypesAndDirections() );
            Iterator<WeightedPath> paths = finder.findAllPaths( nodeA, nodeC ).iterator();
            assertTrue( paths.hasNext(), "expected at least one path" );
            assertPath( paths.next(), nodeA, nodeB, nodeC );
            assertFalse( paths.hasNext(), "expected at most one path" );

            assertPath( finder.findSinglePath( nodeA, nodeC ), nodeA, nodeB, nodeC );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void canContinueGettingPathsByDiminishingCost( DijkstraFactory factory )
    {
        /*
         * NODE (NAME/INDEX)
         *
         * (A)-*2->(B)-*3->(C)-*1->(D)
         *  |        \             ^ ^
         *  |          ----*5-----/  |
         *   \                       |
         *     ---------*6-----------
         */

        try ( Transaction transaction = graphDb.beginTx() )
        {
            Node nodeA = graph.makeNode( transaction, "A" );
            graph.makeNode( transaction, "B" );
            graph.makeNode( transaction, "C" );
            Node nodeD = graph.makeNode( transaction, "D" );

            // Path "1"
            graph.makeEdge( transaction, "A", "B", "length", 2d );
            graph.makeEdge( transaction, "B", "C", "length", 3L );
            graph.makeEdge( transaction, "C", "D", "length", (byte) 1 ); // = 6

            // Path "2"
            graph.makeEdge( transaction, "B", "D", "length", (short) 5 ); // = 7

            // Path "3"
            graph.makeEdge( transaction, "A", "D", "length", (float) 6 ); // = 6

            var context = new BasicEvaluationContext( transaction, graphDb );
            PathFinder<WeightedPath> finder = factory.dijkstra( context, PathExpanders.allTypesAndDirections() );
            assertPaths( finder.findAllPaths( nodeA, nodeD ), "A,B,C,D", "A,D" );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void canGetMultiplePathsInTriangleGraph( DijkstraFactory factory )
    {
        /* NODE (NAME/INDEX)
         * ==> (two relationships)
         *
         * (A/0) ====== 1 =====> (B/1)
         *   \                    /
         *    - 5 -> (C/2) <- 2 -
         */
        try ( Transaction transaction = graphDb.beginTx() )
        {
            Node nodeA = graph.makeNode( transaction, "A" );
            Node nodeB = graph.makeNode( transaction, "B" );
            Node nodeC = graph.makeNode( transaction, "C" );
            Set<Relationship> expectedFirsts = new HashSet<>();
            expectedFirsts.add( graph.makeEdge( transaction, "A", "B", "length", 1d ) );
            expectedFirsts.add( graph.makeEdge( transaction, "A", "B", "length", 1 ) );
            Relationship expectedSecond = graph.makeEdge( transaction, "B", "C", "length", 2L );
            graph.makeEdge( transaction, "A", "C", "length", 5d );

            var context = new BasicEvaluationContext( transaction, graphDb );
            PathFinder<WeightedPath> finder = factory.dijkstra( context, PathExpanders.allTypesAndDirections() );
            Iterator<WeightedPath> paths = finder.findAllPaths( nodeA, nodeC ).iterator();
            for ( int i = 0; i < 2; i++ )
            {
                assertTrue( paths.hasNext(), "expected more paths" );
                Path path = paths.next();
                assertPath( path, nodeA, nodeB, nodeC );

                Iterator<Relationship> relationships = path.relationships().iterator();
                assertTrue( relationships.hasNext(), "found shorter path than expected" );
                assertTrue( expectedFirsts.remove( relationships.next() ), "path contained unexpected relationship" );
                assertTrue( relationships.hasNext(), "found shorter path than expected" );
                assertEquals( expectedSecond, relationships.next() );
                assertFalse( relationships.hasNext(), "found longer path than expected" );
            }
            assertFalse( paths.hasNext(), "expected at most two paths" );
            transaction.commit();
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void canGetMultiplePathsInASmallRoadNetwork( DijkstraFactory factory )
    {
        /*    NODE (NAME/INDEX)
         *
         *     --------------------- 25 -----------------------
         *    /                                                 \
         *  (A/0) - 2 - (B/1) - 2.5 - (D/3) - 3 - (E/4) - 5 - (F/5)
         *    |                        /           /           /
         *   2.5  ---------- 7.3 -----            /           /
         *    |  /                               /           /
         *  (C/2) ------------------ 5 ---------            /
         *    \                                            /
         *      ------------------ 12 --------------------
         *
         */
        try ( Transaction transaction = graphDb.beginTx() )
        {
            Node nodeA = graph.makeNode( transaction, "A" );
            Node nodeB = graph.makeNode( transaction, "B" );
            Node nodeC = graph.makeNode( transaction, "C" );
            Node nodeD = graph.makeNode( transaction, "D" );
            Node nodeE = graph.makeNode( transaction, "E" );
            Node nodeF = graph.makeNode( transaction, "F" );
            graph.makeEdge( transaction, "A", "B", "length", 2d );
            graph.makeEdge( transaction, "A", "C", "length", 2.5f );
            graph.makeEdge( transaction, "C", "D", "length", 7.3d );
            graph.makeEdge( transaction, "B", "D", "length", 2.5f );
            graph.makeEdge( transaction, "D", "E", "length", 3L );
            graph.makeEdge( transaction, "C", "E", "length", 5 );
            graph.makeEdge( transaction, "E", "F", "length", (byte) 5 );
            graph.makeEdge( transaction, "C", "F", "length", (short) 12 );
            graph.makeEdge( transaction, "A", "F", "length", (long) 25 );

            var context = new BasicEvaluationContext( transaction, graphDb );
            PathFinder<WeightedPath> finder = factory.dijkstra( context, PathExpanders.allTypesAndDirections() );
            // Try the search in both directions.
            for ( Node[] nodes : new Node[][]{{nodeA, nodeF}, {nodeF, nodeA}} )
            {
                int found = 0;
                Iterator<WeightedPath> paths = finder.findAllPaths( nodes[0], nodes[1] ).iterator();
                for ( int i = 0; i < 2; i++ )
                {
                    assertTrue( paths.hasNext(), "expected more paths" );
                    Path path = paths.next();
                    if ( path.length() != found && path.length() == 3 )
                    {
                        assertContains( path.nodes(), nodeA, nodeC, nodeE, nodeF );
                    }
                    else if ( path.length() != found && path.length() == 4 )
                    {
                        assertContains( path.nodes(), nodeA, nodeB, nodeD, nodeE, nodeF );
                    }
                    else
                    {
                        fail( "unexpected path length: " + path.length() );
                    }
                    found = path.length();
                }
                assertFalse( paths.hasNext(), "expected at most two paths" );
            }
            transaction.commit();
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldOnlyFindTheShortestPaths( DijkstraFactory factory )
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

        try ( Transaction transaction = graphDb.beginTx() )
        {
            Node s = graph.makeNode( transaction, "s" );
            Node t = graph.makeNode( transaction, "t" );
            Node a = graph.makeNode( transaction, "a" );
            Node b = graph.makeNode( transaction, "b" );
            Node c = graph.makeNode( transaction, "c" );

            graph.makeEdgeChain( transaction, "s,e,f", "length", 1.0 );
            graph.makeEdge( transaction, "f", "t", "length", 2 );
            graph.makeEdge( transaction, "s", "a", "length", 2 );
            graph.makeEdge( transaction, "a", "t", "length", 0 );
            graph.makeEdge( transaction, "s", "c", "length", 1 );
            graph.makeEdge( transaction, "c", "d", "length", 1 );
            graph.makeEdge( transaction, "s", "b", "length", 1 );
            graph.makeEdge( transaction, "b", "d", "length", 1 );
            graph.makeEdge( transaction, "d", "a", "length", 0 );
            graph.makeEdge( transaction, "d", "t", "length", 1 );

            var context = new BasicEvaluationContext( transaction, graphDb );
            PathFinder<WeightedPath> finder = factory.dijkstra( context, PathExpanders.allTypesAndDirections() );
            Iterator<WeightedPath> paths = finder.findAllPaths( s, t ).iterator();

            for ( int i = 1; i <= 3; i++ )
            {
                assertTrue( paths.hasNext(), "Expected at least " + i + " path(s)" );
                assertTrue( MathUtil.equals( paths.next().weight(), 2, DEFAULT_EPSILON ), "Expected 3 paths of cost 2" );
            }
            assertFalse( paths.hasNext(), "Expected exactly 3 paths" );
            transaction.commit();
        }
    }

    private static Relationship createGraph( Transaction transaction, boolean includeOnes )
    {
        /* Layout:
         *                       (y)
         *                        ^
         *                        [2]  _____[1]___
         *                          \ v           |
         * (start)--[1]->(a)--[9]-->(x)<-        (e)--[2]->(f)
         *                |         ^ ^^  \       ^
         *               [1]  ---[7][5][3] -[3]  [1]
         *                v  /       | /      \  /
         *               (b)--[1]-->(c)--[1]->(d)
         */

        Map<String, Object> propertiesForOnes = includeOnes ? map( "cost", (double) 1 ) : map();

        graph.makeEdge( transaction, "start", "a", "cost", (double) 1 );
        graph.makeEdge( transaction, "a", "x", "cost", (short) 9 );
        graph.makeEdge( transaction, "a", "b", propertiesForOnes );
        graph.makeEdge( transaction, "b", "x", "cost", (double) 7 );
        graph.makeEdge( transaction, "b", "c", propertiesForOnes );
        graph.makeEdge( transaction, "c", "x", "cost", 5 );
        Relationship shortCTOXRelationship = graph.makeEdge( transaction, "c", "x", "cost", (float) 3 );
        graph.makeEdge( transaction, "c", "d", propertiesForOnes );
        graph.makeEdge( transaction, "d", "x", "cost", (double) 3 );
        graph.makeEdge( transaction, "d", "e", propertiesForOnes );
        graph.makeEdge( transaction, "e", "x", propertiesForOnes );
        graph.makeEdge( transaction, "e", "f", "cost", (byte) 2 );
        graph.makeEdge( transaction, "x", "y", "cost", (double) 2 );
        return shortCTOXRelationship;
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void testSmallGraph( DijkstraFactory factory )
    {
        try ( Transaction transaction = graphDb.beginTx() )
        {
            Relationship shortCTOXRelationship = createGraph( transaction, true );
            var context = new BasicEvaluationContext( transaction, graphDb );
            PathFinder<WeightedPath> finder = factory.dijkstra( context, PathExpanders.forTypeAndDirection( MyRelTypes.R1, Direction.OUTGOING ),
                    CommonEvaluators.doubleCostEvaluator( "cost" ) );

            // Assert that there are two matching paths
            Node startNode = graph.getNode( transaction, "start" );
            Node endNode = graph.getNode( transaction, "x" );
            assertPaths( finder.findAllPaths( startNode, endNode ), "start,a,b,c,x", "start,a,b,c,d,e,x" );

            // Assert that for the shorter one it picked the correct relationship
            // of the two from (c) --> (x)
            for ( WeightedPath path : finder.findAllPaths( startNode, endNode ) )
            {
                if ( getPathDef( path ).equals( "start,a,b,c,x" ) )
                {
                    assertContainsRelationship( path, shortCTOXRelationship );
                }
            }
            transaction.commit();
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void testSmallGraphWithDefaults( DijkstraFactory factory )
    {
        try ( Transaction transaction = graphDb.beginTx() )
        {
            Relationship shortCTOXRelationship = createGraph( transaction, true );
            var context = new BasicEvaluationContext( transaction, graphDb );
            PathFinder<WeightedPath> finder = factory.dijkstra( context, PathExpanders.forTypeAndDirection( MyRelTypes.R1, Direction.OUTGOING ),
                    CommonEvaluators.doubleCostEvaluator( "cost", 1.0d ) );

            // Assert that there are two matching paths
            Node startNode = graph.getNode( transaction, "start" );
            Node endNode = graph.getNode( transaction, "x" );
            assertPaths( finder.findAllPaths( startNode, endNode ), "start,a,b,c,x", "start,a,b,c,d,e,x" );

            // Assert that for the shorter one it picked the correct relationship
            // of the two from (c) --> (x)
            for ( WeightedPath path : finder.findAllPaths( startNode, endNode ) )
            {
                if ( getPathDef( path ).equals( "start,a,b,c,x" ) )
                {
                    assertContainsRelationship( path, shortCTOXRelationship );
                }
            }
            transaction.commit();
        }
    }

    private static void assertContainsRelationship( WeightedPath path,
            Relationship relationship )
    {
        for ( Relationship rel : path.relationships() )
        {
            if ( rel.equals( relationship ) )
            {
                return;
            }
        }
        fail( path + " should've contained " + relationship );
    }

    private static Stream<Arguments> params()
    {
        return Stream.of(
                arguments( new DijkstraFactory()
                           {
                               @Override
                               public PathFinder<WeightedPath> dijkstra( EvaluationContext context, PathExpander<Double> expander )
                               {
                                   return new Dijkstra( expander, CommonEvaluators.doubleCostEvaluator( "length" ), DEFAULT_EPSILON,
                                           PathInterestFactory.allShortest( DEFAULT_EPSILON ) );
                               }

                               @Override
                               public PathFinder<WeightedPath> dijkstra( EvaluationContext context, PathExpander<Double> expander,
                                       CostEvaluator<Double> costEvaluator )
                               {
                                   return new Dijkstra( expander, costEvaluator, DEFAULT_EPSILON, PathInterestFactory.allShortest( DEFAULT_EPSILON ) );
                               }
                           }
                ),
                // DijkstraBidirectional
                arguments( new DijkstraFactory()
                           {
                               @Override
                               public PathFinder<WeightedPath> dijkstra( EvaluationContext context, PathExpander<Double> expander )
                               {
                                   return new DijkstraBidirectional( context, expander,
                                           CommonEvaluators.doubleCostEvaluator( "length" ), DEFAULT_EPSILON );
                               }

                               @Override
                               public PathFinder<WeightedPath> dijkstra( EvaluationContext context, PathExpander<Double> expander,
                                       CostEvaluator<Double> costEvaluator )
                               {
                                   return new DijkstraBidirectional( context, expander, costEvaluator, DEFAULT_EPSILON );
                               }
                           }
                ) );
    }

    private interface DijkstraFactory
    {
        PathFinder<WeightedPath> dijkstra( EvaluationContext context, PathExpander<Double> expander );
        PathFinder<WeightedPath> dijkstra( EvaluationContext context, PathExpander<Double> expander, CostEvaluator<Double> costEvaluator );
    }
}
