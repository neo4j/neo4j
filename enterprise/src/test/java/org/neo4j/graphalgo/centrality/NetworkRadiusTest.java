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
package org.neo4j.graphalgo.centrality;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.neo4j.graphalgo.shortestpath.CostEvaluator;
import org.neo4j.graphalgo.shortestpath.SingleSourceShortestPath;
import org.neo4j.graphalgo.shortestpath.SingleSourceShortestPathDijkstra;
import org.neo4j.graphalgo.shortestpath.std.DoubleComparator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;

import common.Neo4jAlgoTestCase;

public class NetworkRadiusTest extends Neo4jAlgoTestCase
{
    protected SingleSourceShortestPath<Double> getSingleSourceShortestPath()
    {
        return new SingleSourceShortestPathDijkstra<Double>( 0.0, null,
            new CostEvaluator<Double>()
            {
                public Double getCost( Relationship relationship,
                    boolean backwards )
                {
                    return 1.0;
                }
            }, new org.neo4j.graphalgo.shortestpath.std.DoubleAdder(),
            new org.neo4j.graphalgo.shortestpath.std.DoubleComparator(),
            Direction.BOTH, MyRelTypes.R1 );
    }

    @Test
    public void testBox()
    {
        graph.makeEdgeChain( "a,b,c,d,a" );
        NetworkRadius<Double> radius = new NetworkRadius<Double>(
            getSingleSourceShortestPath(), 0.0, graph.getAllNodes(),
            new DoubleComparator() );
        assertTrue( radius.getCentrality( null ) == 2.0 );
    }

    @Test
    public void testPlusShape()
    {
        graph.makeEdgeChain( "a,b,c" );
        graph.makeEdgeChain( "d,b,e" );
        NetworkRadius<Double> radius = new NetworkRadius<Double>(
            getSingleSourceShortestPath(), 0.0, graph.getAllNodes(),
            new DoubleComparator() );
        assertTrue( radius.getCentrality( null ) == 1.0 );
    }

    @Test
    public void testChain()
    {
        graph.makeEdgeChain( "a,b,c,d,e" );
        NetworkRadius<Double> radius = new NetworkRadius<Double>(
            getSingleSourceShortestPath(), 0.0, graph.getAllNodes(),
            new DoubleComparator() );
        assertTrue( radius.getCentrality( null ) == 2.0 );
    }
}
