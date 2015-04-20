/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.HashMap;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;

import org.neo4j.graphalgo.CommonEvaluators;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.InitialBranchState;

import common.Neo4jAlgoTestCase;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import static org.neo4j.graphalgo.GraphAlgoFactory.dijkstra;
import static org.neo4j.helpers.collection.MapUtil.map;

public class TestDijkstra extends Neo4jAlgoTestCase
{
    private Relationship createGraph( boolean includeOnes )
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

        graph.makeEdge( "start", "a", "cost", (double) 1 );
        graph.makeEdge( "a", "x", "cost", (short) 9 );
        graph.makeEdge( "a", "b", propertiesForOnes );
        graph.makeEdge( "b", "x", "cost", (double) 7 );
        graph.makeEdge( "b", "c", propertiesForOnes );
        graph.makeEdge( "c", "x", "cost", (int) 5 );
        Relationship shortCTOXRelationship = graph.makeEdge( "c", "x", "cost", (float) 3 );
        graph.makeEdge( "c", "d", propertiesForOnes );
        graph.makeEdge( "d", "x", "cost", (double) 3 );
        graph.makeEdge( "d", "e", propertiesForOnes );
        graph.makeEdge( "e", "x", propertiesForOnes );
        graph.makeEdge( "e", "f", "cost", (byte) 2 );
        graph.makeEdge( "x", "y", "cost", (double) 2 );
        return shortCTOXRelationship;
    }

    @Test
    public void testSmallGraph()
    {
        Relationship shortCTOXRelationship = createGraph( true );

        PathFinder<WeightedPath> finder = GraphAlgoFactory.dijkstra(
                PathExpanders.forTypeAndDirection( MyRelTypes.R1, Direction.OUTGOING ), "cost" );
        PathFinder<WeightedPath> finder2 = GraphAlgoFactory.dijkstra(
                PathExpanders.forTypeAndDirection( MyRelTypes.R1, Direction.OUTGOING ),
                CommonEvaluators.doubleCostEvaluator( "cost" ) );

        // Assert that there are two matching paths
        Node startNode = graph.getNode( "start" );
        Node endNode = graph.getNode( "x" );
        assertPaths( finder.findAllPaths( startNode, endNode ),
                "start,a,b,c,x", "start,a,b,c,d,e,x" );
        assertPaths( finder2.findAllPaths( startNode, endNode ),
                "start,a,b,c,x", "start,a,b,c,d,e,x" );

        // Assert that for the shorter one it picked the correct relationship
        // of the two from (c) --> (x)
        for ( WeightedPath path : finder.findAllPaths( startNode, endNode ) )
        {
            if ( getPathDef( path ).equals( "start,a,b,c,x" ) )
            {
                assertContainsRelationship( path, shortCTOXRelationship );
            }
        }
    }

    @Test
    public void testSmallGraphWithDefaults()
    {
        Relationship shortCTOXRelationship = createGraph( true );

        PathFinder<WeightedPath> finder = GraphAlgoFactory.dijkstra(
                PathExpanders.forTypeAndDirection( MyRelTypes.R1, Direction.OUTGOING ),
                CommonEvaluators.doubleCostEvaluator( "cost", 1.0d ) );

        // Assert that there are two matching paths
        Node startNode = graph.getNode( "start" );
        Node endNode = graph.getNode( "x" );
        assertPaths( finder.findAllPaths( startNode, endNode ),
                "start,a,b,c,x", "start,a,b,c,d,e,x" );

        // Assert that for the shorter one it picked the correct relationship
        // of the two from (c) --> (x)
        for ( WeightedPath path : finder.findAllPaths( startNode, endNode ) )
        {
            if ( getPathDef( path ).equals( "start,a,b,c,x" ) )
            {
                assertContainsRelationship( path, shortCTOXRelationship );
            }
        }
    }

    private void assertContainsRelationship( WeightedPath path,
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

    @Ignore( "See issue #627" )
    @Test
    public void determineLongestPath() throws Exception
    {
        /*
         *      --------
         *     |        \
         *     |      --(0)-[-0.1]->(1)
         *     |      |   \          |
         *  [-0.1]    |  [-0.1]    [-0.1]
         *     |      |     \        v
         *     |   [-1.0]    ------>(2)
         *     |       \             |
         *     v        --v          |
         *    (4)<-[-0.1]-(3)<-[-1.0]-
         *
         *    Shortest path: 0->1->2->3->4
         */

        RelationshipType type = DynamicRelationshipType.withName( "EDGE" );
        graph.setCurrentRelType( type );

        graph.makeEdgeChain( "0,1,2,3,4" );
        graph.makeEdge( "0", "2" );
        graph.makeEdge( "0", "3" );
        graph.makeEdge( "0", "4" );

        setWeight( "0", "1", -0.1 );
        setWeight( "1", "2", -0.1 );
        setWeight( "2", "3", -1.0 );
        setWeight( "3", "4", -0.1 );
        setWeight( "0", "2", -0.1 );
        setWeight( "0", "3", -1.0 );
        setWeight( "0", "4", -0.1 );

        Node node0 = graph.getNode( "0" );
        Node node1 = graph.getNode( "1" );
        Node node2 = graph.getNode( "2" );
        Node node3 = graph.getNode( "3" );
        Node node4 = graph.getNode( "4" );

        PathFinder<WeightedPath> pathFinder = GraphAlgoFactory.dijkstra(
                PathExpanders.forTypeAndDirection( type, Direction.OUTGOING ), "weight" );
        WeightedPath wPath = pathFinder.findSinglePath( node0, node4 );

        assertPath( wPath, node0, node1, node2, node3, node4 );
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

        assertPaths( dijkstra( expander, state, "weight" ).findAllPaths( graph.getNode( "a" ), graph.getNode( "d" ) ),
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
