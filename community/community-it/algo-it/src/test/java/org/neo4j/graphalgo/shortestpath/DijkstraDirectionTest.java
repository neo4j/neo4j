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

import java.util.HashMap;

import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.impl.shortestpath.Dijkstra;
import org.neo4j.graphalgo.impl.util.DoubleAdder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * This set of tests is mainly made to test the "backwards" argument to the
 * CostEvaluator sent to a Dijkstra.
 * @see CostEvaluator
 */
class DijkstraDirectionTest extends Neo4jAlgoTestCase
{
    @Test
    void testDijkstraDirection1()
    {
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdge( transaction, "s", "e" );
            Dijkstra<Double> dijkstra =
                    new Dijkstra<>( (double) 0, graph.getNode( transaction, "s" ), graph.getNode( transaction, "e" ), ( relationship, direction ) ->
                    {
                        assertEquals( Direction.OUTGOING, direction );
                        return 1.0;
                    }, new DoubleAdder(), Double::compareTo, Direction.OUTGOING, MyRelTypes.R1 );
            dijkstra.getCost();
            dijkstra = new Dijkstra<>( (double) 0, graph.getNode( transaction, "s" ), graph.getNode( transaction, "e" ), ( relationship, direction ) ->
            {
                assertEquals( Direction.INCOMING, direction );
                return 1.0;
            }, new DoubleAdder(), Double::compareTo, Direction.INCOMING, MyRelTypes.R1 );
            dijkstra.getCost();
            transaction.commit();
        }
    }

    @Test
    void testDijkstraDirection2()
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
            Dijkstra<Double> dijkstra = new Dijkstra<>( (double) 0, graph.getNode( transaction, "s" ), graph.getNode( transaction, "e" ),
                    ( relationship, direction ) ->
            {
                assertEquals( Direction.OUTGOING, direction );
                return 1.0;
            }, new DoubleAdder(), Double::compareTo, Direction.OUTGOING, MyRelTypes.R1 );
            dijkstra.getCost();
            dijkstra = new Dijkstra<>( (double) 0, graph.getNode( transaction, "s" ), graph.getNode( transaction, "e" ), ( relationship, direction ) ->
            {
                assertEquals( Direction.INCOMING, direction );
                return 1.0;
            }, new DoubleAdder(), Double::compareTo, Direction.INCOMING, MyRelTypes.R1 );
            dijkstra.getCost();
            transaction.commit();
        }
    }

    // This saves the first direction observed
    @Test
    void testDijkstraDirection3()
    {
        try ( Transaction transaction = graphDb.beginTx() )
        {
            Relationship r1 = graph.makeEdge( transaction, "start", "b" );
            Relationship r2 = graph.makeEdge( transaction, "c", "b" );
            Relationship r3 = graph.makeEdge( transaction, "c", "d" );
            Relationship r4 = graph.makeEdge( transaction, "e", "d" );
            Relationship r5 = graph.makeEdge( transaction, "e", "f" );
            Relationship r6 = graph.makeEdge( transaction, "g", "f" );
            Relationship r7 = graph.makeEdge( transaction, "g", "end" );
            HashMap<Relationship,Direction> dirs = new HashMap<>();
            Dijkstra<Double> dijkstra = new Dijkstra<>( (double) 0, graph.getNode( transaction, "start" ), graph.getNode( transaction, "end" ),
                    new DirectionSavingCostEvaluator( dirs ), new DoubleAdder(), Double::compareTo, Direction.BOTH, MyRelTypes.R1 );
            dijkstra.getCost();
            assertEquals( Direction.OUTGOING, dirs.get( r1 ) );
            assertEquals( Direction.INCOMING, dirs.get( r2 ) );
            assertEquals( Direction.OUTGOING, dirs.get( r3 ) );
            assertEquals( Direction.INCOMING, dirs.get( r4 ) );
            assertEquals( Direction.OUTGOING, dirs.get( r5 ) );
            assertEquals( Direction.INCOMING, dirs.get( r6 ) );
            assertEquals( Direction.OUTGOING, dirs.get( r7 ) );
            transaction.commit();
        }
    }

    static class DirectionSavingCostEvaluator implements CostEvaluator<Double>
    {
        HashMap<Relationship, Direction> dirs;

        DirectionSavingCostEvaluator( HashMap<Relationship,Direction> dirs )
        {
            super();
            this.dirs = dirs;
        }

        @Override
        public Double getCost( Relationship relationship, Direction direction )
        {
            if ( !dirs.containsKey( relationship ) )
            {
                dirs.put( relationship, direction );
            }
            return 1.0;
        }
    }
}
