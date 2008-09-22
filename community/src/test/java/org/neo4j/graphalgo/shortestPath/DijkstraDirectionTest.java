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

import java.util.HashMap;
import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Relationship;
import org.neo4j.graphalgo.shortestpath.CostEvaluator;
import org.neo4j.graphalgo.shortestpath.Dijkstra;
import org.neo4j.graphalgo.testUtil.NeoAlgoTestCase;

/**
 * This set of tests is mainly made to test the "backwards" argument to the
 * CostEvaluator sent to a Dijkstra.
 * @author Patrik Larsson
 * @see CostEvaluator
 */
public class DijkstraDirectionTest extends NeoAlgoTestCase
{
    public DijkstraDirectionTest( String arg0 )
    {
        super( arg0 );
    }

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
                    boolean backwards )
                {
                    assertFalse( backwards );
                    return 1.0;
                }
            }, new org.neo4j.graphalgo.shortestpath.std.DoubleAdder(),
            new org.neo4j.graphalgo.shortestpath.std.DoubleComparator(),
            Direction.OUTGOING, MyRelTypes.R1 );
        dijkstra.getCost();
        dijkstra = new Dijkstra<Double>( (double) 0, graph.getNode( "s" ),
            graph.getNode( "e" ), new CostEvaluator<Double>()
            {
                public Double getCost( Relationship relationship,
                    boolean backwards )
                {
                    assertTrue( backwards );
                    return 1.0;
                }
            }, new org.neo4j.graphalgo.shortestpath.std.DoubleAdder(),
            new org.neo4j.graphalgo.shortestpath.std.DoubleComparator(),
            Direction.INCOMING, MyRelTypes.R1 );
        dijkstra.getCost();
    }

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
                    boolean backwards )
                {
                    assertFalse( backwards );
                    return 1.0;
                }
            }, new org.neo4j.graphalgo.shortestpath.std.DoubleAdder(),
            new org.neo4j.graphalgo.shortestpath.std.DoubleComparator(),
            Direction.OUTGOING, MyRelTypes.R1 );
        dijkstra.getCost();
        dijkstra = new Dijkstra<Double>( (double) 0, graph.getNode( "s" ),
            graph.getNode( "e" ), new CostEvaluator<Double>()
            {
                public Double getCost( Relationship relationship,
                    boolean backwards )
                {
                    assertTrue( backwards );
                    return 1.0;
                }
            }, new org.neo4j.graphalgo.shortestpath.std.DoubleAdder(),
            new org.neo4j.graphalgo.shortestpath.std.DoubleComparator(),
            Direction.INCOMING, MyRelTypes.R1 );
        dijkstra.getCost();
    }

    // This saves the first direction observed
    class directionSavingCostEvaluator implements CostEvaluator<Double>
    {
        HashMap<Relationship,Boolean> dirs;

        public directionSavingCostEvaluator( HashMap<Relationship,Boolean> dirs )
        {
            super();
            this.dirs = dirs;
        }

        public Double getCost( Relationship relationship, boolean backwards )
        {
            if ( !dirs.containsKey( relationship ) )
            {
                dirs.put( relationship, backwards );
            }
            return 1.0;
        }
    }

    public void testDijkstraDirection3()
    {
        Relationship r1 = graph.makeEdge( "start", "b" );
        Relationship r2 = graph.makeEdge( "c", "b" );
        Relationship r3 = graph.makeEdge( "c", "d" );
        Relationship r4 = graph.makeEdge( "e", "d" );
        Relationship r5 = graph.makeEdge( "e", "f" );
        Relationship r6 = graph.makeEdge( "g", "f" );
        Relationship r7 = graph.makeEdge( "g", "end" );
        HashMap<Relationship,Boolean> dirs = new HashMap<Relationship,Boolean>();
        Dijkstra<Double> dijkstra = new Dijkstra<Double>( (double) 0, graph
            .getNode( "start" ), graph.getNode( "end" ),
            new directionSavingCostEvaluator( dirs ),
            new org.neo4j.graphalgo.shortestpath.std.DoubleAdder(),
            new org.neo4j.graphalgo.shortestpath.std.DoubleComparator(),
            Direction.BOTH, MyRelTypes.R1 );
        dijkstra.getCost();
        assertFalse( dirs.get( r1 ) );
        assertTrue( dirs.get( r2 ) );
        assertFalse( dirs.get( r3 ) );
        assertTrue( dirs.get( r4 ) );
        assertFalse( dirs.get( r5 ) );
        assertTrue( dirs.get( r6 ) );
        assertFalse( dirs.get( r7 ) );
    }
}
