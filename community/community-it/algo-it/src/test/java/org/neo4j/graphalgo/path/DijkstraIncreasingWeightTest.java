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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Iterator;

import org.neo4j.graphalgo.CommonEvaluators;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphalgo.impl.path.Dijkstra;
import org.neo4j.graphalgo.impl.util.PathInterestFactory;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.MathUtil;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.helpers.MathUtil.DEFAULT_EPSILON;

class DijkstraIncreasingWeightTest extends Neo4jAlgoTestCase
{
    @Test
    void canFetchLongerPaths()
    {
        /*
         *    1-(b)-1
         *   /       \
         * (s) - 1 - (a) - 1 - (c) - 10 - (d) - 10 - (t)
         *
         */
        try ( Transaction transaction = graphDb.beginTx() )
        {
            Node s = graph.makeNode( transaction, "s" );
            Node a = graph.makeNode( transaction, "a" );
            Node b = graph.makeNode( transaction, "b" );
            Node c = graph.makeNode( transaction, "c" );
            Node d = graph.makeNode( transaction, "d" );
            Node t = graph.makeNode( transaction, "t" );

            graph.makeEdge( transaction, "s", "a", "length", 1 );
            graph.makeEdge( transaction, "a", "c", "length", 1 );
            graph.makeEdge( transaction, "s", "b", "length", 1 );
            graph.makeEdge( transaction, "b", "a", "length", 1 );
            graph.makeEdge( transaction, "c", "d", "length", 10 );
            graph.makeEdge( transaction, "d", "t", "length", 10 );

            PathExpander<Double> expander = PathExpanders.allTypesAndDirections();
            Dijkstra algo = new Dijkstra( expander, CommonEvaluators.doubleCostEvaluator( "length" ), DEFAULT_EPSILON,
                    PathInterestFactory.all( DEFAULT_EPSILON ) );

            Iterator<WeightedPath> paths = algo.findAllPaths( s, t ).iterator();

            assertTrue( paths.hasNext(), "Expected at least one path." );
            assertPath( paths.next(), s, a, c, d, t );
            assertTrue( paths.hasNext(), "Expected two paths" );
            assertPath( paths.next(), s, b, a, c, d, t );
            transaction.commit();
        }
    }

    @Test
    void shouldReturnPathsInIncreasingOrderOfCost()
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

            PathExpander<Double> expander = PathExpanders.allTypesAndDirections();
            Dijkstra algo = new Dijkstra( expander, CommonEvaluators.doubleCostEvaluator( "length" ), DEFAULT_EPSILON,
                    PathInterestFactory.all( DEFAULT_EPSILON ) );

            Iterator<WeightedPath> paths = algo.findAllPaths( s, t ).iterator();

            for ( int i = 1; i <= 3; i++ )
            {
                assertTrue( paths.hasNext(), "Expected at least " + i + " path(s)" );
                assertTrue( MathUtil.equals( paths.next().weight(), 2, DEFAULT_EPSILON ), "Expected 3 paths of cost 2" );
            }
            for ( int i = 1; i <= 3; i++ )
            {
                assertTrue( paths.hasNext(), "Expected at least " + i + " path(s)" );
                assertTrue( MathUtil.equals( paths.next().weight(), 3, DEFAULT_EPSILON ), "Expected 3 paths of cost 3" );
            }
            assertTrue( paths.hasNext(), "Expected at least 7 paths" );
            assertTrue( MathUtil.equals( paths.next().weight(), 4, DEFAULT_EPSILON ), "Expected 1 path of cost 4" );
            assertFalse( paths.hasNext(), "Expected exactly 7 paths" );
            transaction.commit();
        }
    }

    @Test
    @Timeout( 5 )
    void testForLoops()
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
        try ( Transaction transaction = graphDb.beginTx() )
        {
            Node s = graph.makeNode( transaction, "s" );
            Node t = graph.makeNode( transaction, "t" );

            // Blob loop
            graph.makeEdge( transaction, "s", "a1", "length", 1 );
            graph.makeEdge( transaction, "a1", "b", "length", 0 );
            graph.makeEdge( transaction, "b", "a1", "length", 0 );

            // Self loop
            graph.makeEdge( transaction, "a1", "a2", "length", 1 );
            graph.makeEdge( transaction, "a2", "a2", "length", 0 );

            // Diamond loop
            graph.makeEdge( transaction, "a2", "a3", "length", 1 );
            graph.makeEdge( transaction, "a3", "c1", "length", 0 );
            graph.makeEdge( transaction, "a3", "c2", "length", 0 );
            graph.makeEdge( transaction, "c1", "a4", "length", 0 );
            graph.makeEdge( transaction, "c1", "a4", "length", 0 );
            graph.makeEdge( transaction, "a4", "t", "length", 1 );

            PathExpander<Double> expander = PathExpanders.allTypesAndDirections();
            Dijkstra algo = new Dijkstra( expander, CommonEvaluators.doubleCostEvaluator( "length" ), DEFAULT_EPSILON,
                    PathInterestFactory.all( DEFAULT_EPSILON ) );

            Iterator<WeightedPath> paths = algo.findAllPaths( s, t ).iterator();

            assertTrue( paths.hasNext(), "Expected at least one path" );
            assertEquals( 6, paths.next().length(), "Expected first path of length 6" );
            assertTrue( paths.hasNext(), "Expected at least two paths" );
            assertEquals( 6, paths.next().length(), "Expected second path of length 6" );
            assertFalse( paths.hasNext(), "Expected exactly two paths" );

            transaction.commit();
        }
    }

    @Test
    void testKShortestPaths()
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
        try ( Transaction transaction = graphDb.beginTx() )
        {
            Node s = graph.makeNode( transaction, "s" );
            Node t = graph.makeNode( transaction, "t" );

            graph.makeEdge( transaction, "s", "a", "length", 2 );
            graph.makeEdge( transaction, "s", "b", "length", 1 );
            graph.makeEdge( transaction, "s", "c", "length", 1 );
            graph.makeEdge( transaction, "s", "e", "length", 3 );
            graph.makeEdge( transaction, "a", "t", "length", 0 );
            graph.makeEdge( transaction, "b", "d", "length", 1 );
            graph.makeEdge( transaction, "c", "d", "length", 1 );
            graph.makeEdge( transaction, "d", "a", "length", 0 );
            graph.makeEdge( transaction, "d", "t", "length", 1 );
            graph.makeEdge( transaction, "e", "f", "length", 3 );
            graph.makeEdge( transaction, "f", "t", "length", 3 );

            PathExpander<Double> expander = PathExpanders.allTypesAndDirections();
            PathFinder<WeightedPath> algo = new Dijkstra( expander, CommonEvaluators.doubleCostEvaluator( "length" ), DEFAULT_EPSILON,
                    PathInterestFactory.numberOfShortest( DEFAULT_EPSILON, 6 ) );

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
                assertTrue( MathUtil.equals( path.weight(), expectedWeight, DEFAULT_EPSILON ),
                        "Expected path number " + count + " to have weight of " + expectedWeight );
            }
            assertEquals( 6, count, "Expected exactly 6 returned paths" );
            transaction.commit();
        }
    }
}
