/*
 * Copyright 2008 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.shortestPath;

import java.util.List;

import org.neo4j.graphalgo.shortestpath.CostEvaluator;
import org.neo4j.graphalgo.shortestpath.FloydWarshall;
import org.neo4j.graphalgo.testUtil.Neo4jAlgoTestCase;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class FloydWarshallTest extends Neo4jAlgoTestCase
{
    /**
     * Test case for paths of length 0 and 1, and an impossible path
     */
    public void testMinimal()
    {
        graph.makeEdge( "a", "b", "cost", (double) 1 );
        graph.makeEdge( "a", "c", "cost", (double) 1 );
        graph.makeEdge( "a", "d", "cost", (double) 1 );
        graph.makeEdge( "a", "e", "cost", (double) 1 );
        graph.makeEdge( "b", "c", "cost", (double) 1 );
        graph.makeEdge( "c", "d", "cost", (double) 1 );
        graph.makeEdge( "d", "e", "cost", (double) 1 );
        graph.makeEdge( "e", "b", "cost", (double) 1 );
        FloydWarshall<Double> floydWarshall = new FloydWarshall<Double>( 0.0,
            Double.MAX_VALUE, Direction.OUTGOING,
            new org.neo4j.graphalgo.shortestpath.std.DoubleEvaluator( "cost" ),
            new org.neo4j.graphalgo.shortestpath.std.DoubleAdder(),
            new org.neo4j.graphalgo.shortestpath.std.DoubleComparator(), graph
                .getAllNodes(), graph.getAllEdges() );
        assertTrue( floydWarshall.getCost( graph.getNode( "a" ), graph
            .getNode( "a" ) ) == 0.0 );
        assertTrue( floydWarshall.getCost( graph.getNode( "a" ), graph
            .getNode( "b" ) ) == 1.0 );
        assertTrue( floydWarshall.getCost( graph.getNode( "b" ), graph
            .getNode( "a" ) ) == Double.MAX_VALUE );
    }

    /**
     * Test case for extracting paths
     */
    public void testPath()
    {
        graph.makeEdge( "a", "b", "cost", (double) 1 );
        graph.makeEdge( "b", "c", "cost", (double) 1 );
        graph.makeEdge( "c", "d", "cost", (double) 1 );
        graph.makeEdge( "d", "e", "cost", (double) 1 );
        graph.makeEdge( "e", "f", "cost", (double) 1 );
        FloydWarshall<Double> floydWarshall = new FloydWarshall<Double>( 0.0,
            Double.MAX_VALUE, Direction.OUTGOING,
            new org.neo4j.graphalgo.shortestpath.std.DoubleEvaluator( "cost" ),
            new org.neo4j.graphalgo.shortestpath.std.DoubleAdder(),
            new org.neo4j.graphalgo.shortestpath.std.DoubleComparator(), graph
                .getAllNodes(), graph.getAllEdges() );
        List<Node> path = floydWarshall.getPath( graph.getNode( "a" ), graph
            .getNode( "f" ) );
        assertTrue( path.size() == 6 );
        assertTrue( path.get( 0 ).equals( graph.getNode( "a" ) ) );
        assertTrue( path.get( 1 ).equals( graph.getNode( "b" ) ) );
        assertTrue( path.get( 2 ).equals( graph.getNode( "c" ) ) );
        assertTrue( path.get( 3 ).equals( graph.getNode( "d" ) ) );
        assertTrue( path.get( 4 ).equals( graph.getNode( "e" ) ) );
        assertTrue( path.get( 5 ).equals( graph.getNode( "f" ) ) );
    }

    public void testDirection()
    {
        graph.makeEdge( "a", "b" );
        graph.makeEdge( "b", "c" );
        graph.makeEdge( "c", "d" );
        graph.makeEdge( "d", "a" );
        graph.makeEdge( "s", "a" );
        graph.makeEdge( "b", "s" );
        graph.makeEdge( "e", "c" );
        graph.makeEdge( "d", "e" );
        new FloydWarshall<Double>( 0.0, Double.MAX_VALUE, Direction.OUTGOING,
            new CostEvaluator<Double>()
            {
                public Double getCost( Relationship relationship,
                    boolean backwards )
                {
                    assertFalse( backwards );
                    return 1.0;
                }
            }, new org.neo4j.graphalgo.shortestpath.std.DoubleAdder(),
            new org.neo4j.graphalgo.shortestpath.std.DoubleComparator(), graph
                .getAllNodes(), graph.getAllEdges() ).calculate();
        new FloydWarshall<Double>( 0.0, Double.MAX_VALUE, Direction.INCOMING,
            new CostEvaluator<Double>()
            {
                public Double getCost( Relationship relationship,
                    boolean backwards )
                {
                    assertTrue( backwards );
                    return 1.0;
                }
            }, new org.neo4j.graphalgo.shortestpath.std.DoubleAdder(),
            new org.neo4j.graphalgo.shortestpath.std.DoubleComparator(), graph
                .getAllNodes(), graph.getAllEdges() ).calculate();
    }
}
