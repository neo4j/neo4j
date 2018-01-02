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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;
import org.neo4j.graphalgo.CommonEvaluators;
import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.impl.shortestpath.FloydWarshall;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import common.Neo4jAlgoTestCase;

public class FloydWarshallTest extends Neo4jAlgoTestCase
{
    /**
     * Test case for paths of length 0 and 1, and an impossible path
     */
    @Test
    public void testMinimal()
    {
        graph.makeEdge( "a", "b", "cost", (double) 1 );
        graph.makeEdge( "a", "c", "cost", (float) 1 );
        graph.makeEdge( "a", "d", "cost", (long) 1 );
        graph.makeEdge( "a", "e", "cost", (int) 1 );
        graph.makeEdge( "b", "c", "cost", (double) 1 );
        graph.makeEdge( "c", "d", "cost", (byte) 1 );
        graph.makeEdge( "d", "e", "cost", (short) 1 );
        graph.makeEdge( "e", "b", "cost", (byte) 1 );
        FloydWarshall<Double> floydWarshall = new FloydWarshall<Double>( 0.0,
            Double.MAX_VALUE, Direction.OUTGOING,
            CommonEvaluators.doubleCostEvaluator( "cost" ),
            new org.neo4j.graphalgo.impl.util.DoubleAdder(),
            new org.neo4j.graphalgo.impl.util.DoubleComparator(), graph
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
    @Test
    public void testPath()
    {
        graph.makeEdge( "a", "b", "cost", (double) 1 );
        graph.makeEdge( "b", "c", "cost", (float) 1 );
        graph.makeEdge( "c", "d", "cost", (int) 1 );
        graph.makeEdge( "d", "e", "cost", (long) 1 );
        graph.makeEdge( "e", "f", "cost", (byte) 1 );
        FloydWarshall<Double> floydWarshall = new FloydWarshall<Double>( 0.0,
            Double.MAX_VALUE, Direction.OUTGOING,
            CommonEvaluators.doubleCostEvaluator( "cost" ),
            new org.neo4j.graphalgo.impl.util.DoubleAdder(),
            new org.neo4j.graphalgo.impl.util.DoubleComparator(), graph
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

    @Test
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
                    Direction direction )
                {
                    assertEquals( Direction.OUTGOING, direction );
                    return 1.0;
                }
            }, new org.neo4j.graphalgo.impl.util.DoubleAdder(),
            new org.neo4j.graphalgo.impl.util.DoubleComparator(), graph
                .getAllNodes(), graph.getAllEdges() ).calculate();
        new FloydWarshall<Double>( 0.0, Double.MAX_VALUE, Direction.INCOMING,
            new CostEvaluator<Double>()
            {
                public Double getCost( Relationship relationship,
                    Direction direction )
                {
                    assertEquals( Direction.INCOMING, direction );
                    return 1.0;
                }
            }, new org.neo4j.graphalgo.impl.util.DoubleAdder(),
            new org.neo4j.graphalgo.impl.util.DoubleComparator(), graph
                .getAllNodes(), graph.getAllEdges() ).calculate();
    }
}
