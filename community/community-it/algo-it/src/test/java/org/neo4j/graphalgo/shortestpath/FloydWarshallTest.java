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
import org.junit.jupiter.api.Test;

import java.util.List;

import org.neo4j.graphalgo.CommonEvaluators;
import org.neo4j.graphalgo.impl.shortestpath.FloydWarshall;
import org.neo4j.graphalgo.impl.util.DoubleAdder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FloydWarshallTest extends Neo4jAlgoTestCase
{
    /**
     * Test case for paths of length 0 and 1, and an impossible path
     */
    @Test
    void testMinimal()
    {
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdge( transaction, "a", "b", "cost", (double) 1 );
            graph.makeEdge( transaction, "a", "c", "cost", (float) 1 );
            graph.makeEdge( transaction, "a", "d", "cost", (long) 1 );
            graph.makeEdge( transaction, "a", "e", "cost", 1 );
            graph.makeEdge( transaction, "b", "c", "cost", (double) 1 );
            graph.makeEdge( transaction, "c", "d", "cost", (byte) 1 );
            graph.makeEdge( transaction, "d", "e", "cost", (short) 1 );
            graph.makeEdge( transaction, "e", "b", "cost", (byte) 1 );
            FloydWarshall<Double> floydWarshall =
                    new FloydWarshall<>( 0.0, Double.MAX_VALUE, Direction.OUTGOING, CommonEvaluators.doubleCostEvaluator( "cost" ), new DoubleAdder(),
                            Double::compareTo, graph.getAllNodes(), graph.getAllEdges() );
            assertEquals( 0.0, floydWarshall.getCost( graph.getNode( transaction, "a" ), graph.getNode( transaction, "a" ) ), 0.0 );
            assertEquals( 1.0, floydWarshall.getCost( graph.getNode( transaction, "a" ), graph.getNode( transaction, "b" ) ), 0.0 );
            assertEquals( floydWarshall.getCost( graph.getNode( transaction, "b" ), graph.getNode( transaction, "a" ) ), Double.MAX_VALUE, 0.0 );
            transaction.commit();
        }
    }

    /**
     * Test case for extracting paths
     */
    @Test
    void testPath()
    {
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdge( transaction, "a", "b", "cost", (double) 1 );
            graph.makeEdge( transaction, "b", "c", "cost", (float) 1 );
            graph.makeEdge( transaction, "c", "d", "cost", 1 );
            graph.makeEdge( transaction, "d", "e", "cost", (long) 1 );
            graph.makeEdge( transaction, "e", "f", "cost", (byte) 1 );
            FloydWarshall<Double> floydWarshall =
                    new FloydWarshall<>( 0.0, Double.MAX_VALUE, Direction.OUTGOING, CommonEvaluators.doubleCostEvaluator( "cost" ), new DoubleAdder(),
                            Double::compareTo, graph.getAllNodes(), graph.getAllEdges() );
            List<Node> path = floydWarshall.getPath( graph.getNode( transaction, "a" ), graph.getNode( transaction, "f" ) );
            assertEquals( 6, path.size() );
            assertEquals( path.get( 0 ), graph.getNode( transaction, "a" ) );
            assertEquals( path.get( 1 ), graph.getNode( transaction, "b" ) );
            assertEquals( path.get( 2 ), graph.getNode( transaction, "c" ) );
            assertEquals( path.get( 3 ), graph.getNode( transaction, "d" ) );
            assertEquals( path.get( 4 ), graph.getNode( transaction, "e" ) );
            assertEquals( path.get( 5 ), graph.getNode( transaction, "f" ) );
            transaction.commit();
        }
    }

    @Test
    void testDirection()
    {
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdge( transaction, "a", "b" );
            graph.makeEdge( transaction, "b", "c" );
            graph.makeEdge( transaction, "c", "d" );
            graph.makeEdge( transaction, "d", "a" );
            graph.makeEdge( transaction, "s", "a" );
            graph.makeEdge( transaction, "b", "s" );
            graph.makeEdge( transaction, "e", "c" );
            graph.makeEdge( transaction, "d", "e" );
            new FloydWarshall<>( 0.0, Double.MAX_VALUE, Direction.OUTGOING, ( relationship, direction ) ->
            {
                assertEquals( Direction.OUTGOING, direction );
                return 1.0;
            }, new DoubleAdder(), Double::compareTo, graph.getAllNodes(), graph.getAllEdges() ).calculate();
            new FloydWarshall<>( 0.0, Double.MAX_VALUE, Direction.INCOMING, ( relationship, direction ) ->
            {
                assertEquals( Direction.INCOMING, direction );
                return 1.0;
            }, new DoubleAdder(), Double::compareTo, graph.getAllNodes(), graph.getAllEdges() ).calculate();
            transaction.commit();
        }
    }
}
