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

import java.util.HashMap;

import org.junit.Test;
import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.impl.shortestpath.Dijkstra;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;

import common.Neo4jAlgoTestCase;

/**
 * This set of tests is mainly made to test the "backwards" argument to the
 * CostEvaluator sent to a Dijkstra.
 * @author Patrik Larsson
 * @see CostEvaluator
 */
public class DijkstraDirectionTest extends Neo4jAlgoTestCase
{
    @Test
    public void testDijkstraDirection1()
    {
        graph.makeEdge( "s", "e" );
        Dijkstra<Double> dijkstra = new Dijkstra<Double>(
            (double) 0,
            graph.getNode( "s" ),
            graph.getNode( "e" ),
            new CostEvaluator<Double>()
            {
                public Double getCost( Relationship relationship,
                            Direction direction )
                {
                    assertEquals( Direction.OUTGOING, direction );
                    return 1.0;
                }
            }, new org.neo4j.graphalgo.impl.util.DoubleAdder(),
            new org.neo4j.graphalgo.impl.util.DoubleComparator(),
            Direction.OUTGOING, MyRelTypes.R1 );
        dijkstra.getCost();
        dijkstra = new Dijkstra<Double>( (double) 0, graph.getNode( "s" ),
            graph.getNode( "e" ), new CostEvaluator<Double>()
            {
                public Double getCost( Relationship relationship,
                            Direction direction )
                {
                    assertEquals( Direction.INCOMING, direction );
                    return 1.0;
                }
            }, new org.neo4j.graphalgo.impl.util.DoubleAdder(),
            new org.neo4j.graphalgo.impl.util.DoubleComparator(),
            Direction.INCOMING, MyRelTypes.R1 );
        dijkstra.getCost();
    }

    @Test
    public void testDijkstraDirection2()
    {
        graph.makeEdge( "a", "b" );
        graph.makeEdge( "b", "c" );
        graph.makeEdge( "c", "d" );
        graph.makeEdge( "d", "a" );
        graph.makeEdge( "s", "a" );
        graph.makeEdge( "b", "s" );
        graph.makeEdge( "e", "c" );
        graph.makeEdge( "d", "e" );
        Dijkstra<Double> dijkstra = new Dijkstra<Double>(
            (double) 0,
            graph.getNode( "s" ),
            graph.getNode( "e" ),
            new CostEvaluator<Double>()
            {
                public Double getCost( Relationship relationship,
                            Direction direction )
                {
                    assertEquals( Direction.OUTGOING, direction );
                    return 1.0;
                }
            }, new org.neo4j.graphalgo.impl.util.DoubleAdder(),
            new org.neo4j.graphalgo.impl.util.DoubleComparator(),
            Direction.OUTGOING, MyRelTypes.R1 );
        dijkstra.getCost();
        dijkstra = new Dijkstra<Double>( (double) 0, graph.getNode( "s" ),
            graph.getNode( "e" ), new CostEvaluator<Double>()
            {
                public Double getCost( Relationship relationship,
                            Direction direction )
                {
                    assertEquals( Direction.INCOMING, direction );
                    return 1.0;
                }
            }, new org.neo4j.graphalgo.impl.util.DoubleAdder(),
            new org.neo4j.graphalgo.impl.util.DoubleComparator(),
            Direction.INCOMING, MyRelTypes.R1 );
        dijkstra.getCost();
    }

    // This saves the first direction observed
    class directionSavingCostEvaluator implements CostEvaluator<Double>
    {
        HashMap<Relationship, Direction> dirs;

        public directionSavingCostEvaluator(
                HashMap<Relationship, Direction> dirs )
        {
            super();
            this.dirs = dirs;
        }

        public Double getCost( Relationship relationship, Direction direction )
        {
            if ( !dirs.containsKey( relationship ) )
            {
                dirs.put( relationship, direction );
            }
            return 1.0;
        }
    }

    @Test
    public void testDijkstraDirection3()
    {
        Relationship r1 = graph.makeEdge( "start", "b" );
        Relationship r2 = graph.makeEdge( "c", "b" );
        Relationship r3 = graph.makeEdge( "c", "d" );
        Relationship r4 = graph.makeEdge( "e", "d" );
        Relationship r5 = graph.makeEdge( "e", "f" );
        Relationship r6 = graph.makeEdge( "g", "f" );
        Relationship r7 = graph.makeEdge( "g", "end" );
        HashMap<Relationship, Direction> dirs = new HashMap<Relationship, Direction>();
        Dijkstra<Double> dijkstra = new Dijkstra<Double>( (double) 0, graph
            .getNode( "start" ), graph.getNode( "end" ),
            new directionSavingCostEvaluator( dirs ),
            new org.neo4j.graphalgo.impl.util.DoubleAdder(),
            new org.neo4j.graphalgo.impl.util.DoubleComparator(),
            Direction.BOTH, MyRelTypes.R1 );
        dijkstra.getCost();
        assertEquals( Direction.OUTGOING, dirs.get( r1 ) );
        assertEquals( Direction.INCOMING, dirs.get( r2 ) );
        assertEquals( Direction.OUTGOING, dirs.get( r3 ) );
        assertEquals( Direction.INCOMING, dirs.get( r4 ) );
        assertEquals( Direction.OUTGOING, dirs.get( r5 ) );
        assertEquals( Direction.INCOMING, dirs.get( r6 ) );
        assertEquals( Direction.OUTGOING, dirs.get( r7 ) );
    }
}
