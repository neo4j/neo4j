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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphalgo.EstimateEvaluator;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphalgo.impl.path.TraversalAStar;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.InitialBranchState;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.MapUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.neo4j.graphalgo.CommonEvaluators.doubleCostEvaluator;
import static org.neo4j.graphalgo.GraphAlgoFactory.aStar;
import static org.neo4j.graphdb.Direction.OUTGOING;

@RunWith( Parameterized.class )
public class TestAStar extends Neo4jAlgoTestCase
{

    @Test
    public void pathToSelfReturnsZero()
    {
        // GIVEN
        Node start = graph.makeNode( "start", "x", 0d, "y", 0d );

        // WHEN
        WeightedPath path = finder.findSinglePath( start, start );
        // THEN
        assertNotNull( path );
        assertEquals( start, path.startNode() );
        assertEquals( start, path.endNode() );
        assertEquals( 0, path.length() );
    }

    @Test
    public void allPathsToSelfReturnsZero()
    {
        // GIVEN
        Node start = graph.makeNode( "start", "x", 0d, "y", 0d );

        // WHEN
        ResourceIterable<WeightedPath> paths = Iterables.asResourceIterable(finder.findAllPaths( start, start ) );

        // THEN
        for ( WeightedPath path : paths )
        {
            assertNotNull( path );
            assertEquals( start, path.startNode() );
            assertEquals( start, path.endNode() );
            assertEquals( 0, path.length() );
        }
    }

    @Test
    public void wikipediaExample()
    {
        /* GIVEN
         *
         * (start)---2--->(d)
         *    \             \
         *    1.5            .\
         *     v               3
         *    (a)-\             v
         *         -2-\         (e)
         *             ->(b)     \
         *               /        \
         *           /--           2
         *      /-3-                v
         *     v        --4------->(end)
         *    (c)------/
         */
        Node start = graph.makeNode( "start", "x", 0d,   "y", 0d );
        graph.makeNode( "a", "x", 0.3d, "y", 1d );
        graph.makeNode( "b", "x", 2d, "y", 2d );
        graph.makeNode( "c", "x", 0d, "y", 3d );
        graph.makeNode( "d", "x", 2d, "y", 0d );
        graph.makeNode( "e", "x", 3d, "y", 1.5d );
        Node end = graph.makeNode( "end", "x", 3.3d, "y", 2.8d );
        graph.makeEdge( "start", "a", "length", 1.5d );
        graph.makeEdge( "a", "b", "length", 2f );
        graph.makeEdge( "b", "c", "length", 3 );
        graph.makeEdge( "c", "end", "length", 4L );
        graph.makeEdge( "start", "d", "length", (short)2 );
        graph.makeEdge( "d", "e", "length", (byte)3 );
        graph.makeEdge( "e", "end", "length", 2 );

        // WHEN
        WeightedPath path = finder.findSinglePath( start, end );
        // THEN
        assertPathDef( path, "start", "d", "e", "end" );
    }

    /**
     * <pre>
     *   01234567
     *  +-------->x  A - C: 10
     * 0|A      C    A - B:  2 (x2)
     * 1|  B         B - C:  3
     *  V
     *  y
     * </pre>
     */
    @Test
    public void testSimplest()
    {
        Node nodeA = graph.makeNode( "A", "x", 0d, "y", 0d );
        Node nodeB = graph.makeNode( "B", "x", 2d, "y", 1d );
        Node nodeC = graph.makeNode( "C", "x", 7d, "y", 0d );
        Relationship relAB = graph.makeEdge( "A", "B", "length", 2d );
        Relationship relAB2 = graph.makeEdge( "A", "B", "length", 2 );
        Relationship relBC = graph.makeEdge( "B", "C", "length", 3f );
        Relationship relAC = graph.makeEdge( "A", "C", "length", (short)10 );

        int counter = 0;
        Iterable<WeightedPath> allPaths = finder.findAllPaths( nodeA, nodeC );
        for ( WeightedPath path : allPaths )
        {
            assertEquals( (Double)5d, (Double)path.weight() );
            assertPath( path, nodeA, nodeB, nodeC );
            counter++;
        }
        assertEquals( 1, counter );
    }

    @SuppressWarnings( { "rawtypes", "unchecked" } )
    @Test
    public void canUseBranchState()
    {
        // This test doesn't use the predefined finder, which only means an unnecessary instantiation
        // if such an object. And this test will be run twice (once for each finder type in data()).
        /**
         * <pre>
         *   012345    A - B:  2
         *  +------>y  A - B:  2
         * 0|A         B - C:  3
         * 1|          A - C:  10
         * 2| B
         * 3|
         * 4|
         * 5|
         * 6|
         * 7|C
         *  V
         *  x
         *
         * </pre>
         */
        Node nodeA = graph.makeNode( "A", "x", 0d, "y", 0d );
        Node nodeB = graph.makeNode( "B", "x", 2d, "y", 1d );
        Node nodeC = graph.makeNode( "C", "x", 7d, "y", 0d );
        graph.makeEdge( "A", "B", "length", 2d );
        graph.makeEdge( "A", "B", "length", 2d );
        graph.makeEdge( "B", "C", "length", 3d );
        graph.makeEdge( "A", "C", "length", 10d );

        final Map<Node, Double> seenBranchStates = new HashMap<>();
        PathExpander<Double> expander = new PathExpander<Double>()
        {
            @Override
            public Iterable<Relationship> expand( Path path, BranchState<Double> state )
            {
                double newState = state.getState();
                if ( path.length() > 0 )
                {
                    newState += (Double) path.lastRelationship().getProperty( "length" );
                    state.setState( newState );
                }
                seenBranchStates.put( path.endNode(), newState );

                return path.endNode().getRelationships( OUTGOING );
            }

            @Override
            public PathExpander<Double> reverse()
            {
                throw new UnsupportedOperationException();
            }
        };

        double initialStateValue = 0D;
        PathFinder<WeightedPath> traversalFinder = new TraversalAStar( expander,
                new InitialBranchState.State( initialStateValue, initialStateValue ),
                doubleCostEvaluator( "length" ), ESTIMATE_EVALUATOR );
        WeightedPath path = traversalFinder.findSinglePath( nodeA, nodeC );
        assertEquals( (Double) 5.0D, (Double) path.weight() );
        assertPathDef( path, "A", "B", "C" );
        assertEquals( MapUtil.<Node,Double>genericMap( nodeA, 0D, nodeB, 2D ), seenBranchStates );
    }

    @Test
    public void betterTentativePath()
    {
        // GIVEN
        EstimateEvaluator<Double> estimator = ( node, goal ) -> (Double) node.getProperty( "estimate" );
        PathFinder<WeightedPath> finder = aStar( PathExpanders.allTypesAndDirections(),
                doubleCostEvaluator( "weight", 0d ), estimator );

        final Node node1 = graph.makeNode( "1", "estimate", 0.003d );
        final Node node2 = graph.makeNode( "2", "estimate", 0.002d );
        final Node node3 = graph.makeNode( "3", "estimate", 0.001d );
        final Node node4 = graph.makeNode( "4", "estimate", 0d );
        graph.makeEdge( "1", "3", "weight", 0.253d );
        graph.makeEdge( "1", "2", "weight", 0.018d );
        graph.makeEdge( "2", "4", "weight", 0.210d );
        graph.makeEdge( "2", "3", "weight", 0.180d );
        graph.makeEdge( "2", "3", "weight", 0.024d );
        graph.makeEdge( "3", "4", "weight", 0.135d );
        graph.makeEdge( "3", "4", "weight", 0.013d );

        // WHEN
        WeightedPath best14 = finder.findSinglePath( node1, node4 );
        // THEN
        assertPath( best14, node1, node2, node3, node4 );
    }

    static EstimateEvaluator<Double> ESTIMATE_EVALUATOR = ( node, goal ) ->
    {
        double dx = (Double) node.getProperty( "x" )
                    - (Double) goal.getProperty( "x" );
        double dy = (Double) node.getProperty( "y" )
                    - (Double) goal.getProperty( "y" );
        return Math.sqrt( Math.pow( dx, 2 ) + Math.pow( dy, 2 ) );
    };

    @Parameters
    public static Collection<Object[]> data()
    {
        return Arrays.asList( new Object[][]
        {
            {
                GraphAlgoFactory.aStar( PathExpanders.allTypesAndDirections(), doubleCostEvaluator( "length" ), ESTIMATE_EVALUATOR )
            },
            {
                new TraversalAStar( PathExpanders.allTypesAndDirections(), doubleCostEvaluator( "length" ), ESTIMATE_EVALUATOR )
            }
        } );
    }

    private final PathFinder<WeightedPath> finder;

    public TestAStar( PathFinder<WeightedPath> finder )
    {
        this.finder = finder;
    }
}
