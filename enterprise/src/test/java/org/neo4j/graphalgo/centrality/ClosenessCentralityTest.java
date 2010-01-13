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

import org.neo4j.graphalgo.shortestpath.CostEvaluator;
import org.neo4j.graphalgo.shortestpath.SingleSourceShortestPath;
import org.neo4j.graphalgo.shortestpath.SingleSourceShortestPathDijkstra;
import org.neo4j.graphalgo.shortestpath.std.DoubleAdder;
import org.neo4j.graphalgo.testUtil.NeoAlgoTestCase;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;

public class ClosenessCentralityTest extends NeoAlgoTestCase
{
    public ClosenessCentralityTest( String name )
    {
        super( name );
    }

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

    // protected SingleSourceShortestPath<Integer> getSingleSourceShortestPath()
    // {
    // return new SingleSourceShortestPathBFS( null, MyRelTypes.R1,
    // Direction.BOTH );
    // }
    ClosenessCentrality<Double> getCentralityAlgorithm()
    {
        return new ClosenessCentrality<Double>( getSingleSourceShortestPath(),
            new DoubleAdder(), 0.0, graph.getAllNodes(),
            new CostDivider<Double>()
            {
                public Double divideByCost( Double d, Double c )
                {
                    return d / c;
                }

                public Double divideCost( Double c, Double d )
                {
                    return c / d;
                }
            } );
    }

    protected void assertCentrality(
        ClosenessCentrality<Double> closenessCentrality, String nodeId,
        Double value )
    {
        assertTrue( closenessCentrality.getCentrality( graph.getNode( nodeId ) )
            .equals( value ) );
    }

    public void testBox()
    {
        graph.makeEdgeChain( "a,b,c,d,a" );
        ClosenessCentrality<Double> closenessCentrality = getCentralityAlgorithm();
        assertCentrality( closenessCentrality, "a", 1.0 / 4 );
        assertCentrality( closenessCentrality, "b", 1.0 / 4 );
        assertCentrality( closenessCentrality, "c", 1.0 / 4 );
        assertCentrality( closenessCentrality, "d", 1.0 / 4 );
    }

    public void testPlusShape()
    {
        graph.makeEdgeChain( "a,b,c" );
        graph.makeEdgeChain( "d,b,e" );
        ClosenessCentrality<Double> closenessCentrality = getCentralityAlgorithm();
        assertCentrality( closenessCentrality, "a", 1.0 / 7 );
        assertCentrality( closenessCentrality, "b", 1.0 / 4 );
        assertCentrality( closenessCentrality, "c", 1.0 / 7 );
        assertCentrality( closenessCentrality, "d", 1.0 / 7 );
        assertCentrality( closenessCentrality, "e", 1.0 / 7 );
    }

    public void testChain()
    {
        graph.makeEdgeChain( "a,b,c,d,e" );
        ClosenessCentrality<Double> closenessCentrality = getCentralityAlgorithm();
        assertCentrality( closenessCentrality, "a", 1.0 / 10 );
        assertCentrality( closenessCentrality, "b", 1.0 / 7 );
        assertCentrality( closenessCentrality, "c", 1.0 / 6 );
        assertCentrality( closenessCentrality, "d", 1.0 / 7 );
        assertCentrality( closenessCentrality, "e", 1.0 / 10 );
    }
}
