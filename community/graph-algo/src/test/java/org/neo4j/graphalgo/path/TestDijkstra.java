/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.map;

import java.util.Map;

import org.junit.Test;
import org.neo4j.graphalgo.CommonEvaluators;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.Traversal;

import common.Neo4jAlgoTestCase;

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
        graph.makeEdge( "a", "x", "cost", (double) 9 );
        graph.makeEdge( "a", "b", propertiesForOnes );
        graph.makeEdge( "b", "x", "cost", (double) 7 );
        graph.makeEdge( "b", "c", propertiesForOnes );
        graph.makeEdge( "c", "x", "cost", (double) 5 );
        Relationship shortCTOXRelationship = graph.makeEdge( "c", "x", "cost", (double) 3 );
        graph.makeEdge( "c", "d", propertiesForOnes );
        graph.makeEdge( "d", "x", "cost", (double) 3 );
        graph.makeEdge( "d", "e", propertiesForOnes );
        graph.makeEdge( "e", "x", propertiesForOnes );
        graph.makeEdge( "e", "f", "cost", (double) 2 );
        graph.makeEdge( "x", "y", "cost", (double) 2 );
        return shortCTOXRelationship;
    }
    
    @Test
    public void testSmallGraph()
    {
        Relationship shortCTOXRelationship = createGraph( true );
        
        PathFinder<WeightedPath> finder = GraphAlgoFactory.dijkstra(
                Traversal.expanderForTypes( MyRelTypes.R1, Direction.OUTGOING ), "cost" );
        PathFinder<WeightedPath> finder2 = GraphAlgoFactory.dijkstra(
                Traversal.expanderForTypes( MyRelTypes.R1, Direction.OUTGOING ),
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
                Traversal.expanderForTypes( MyRelTypes.R1, Direction.OUTGOING ),
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
}
