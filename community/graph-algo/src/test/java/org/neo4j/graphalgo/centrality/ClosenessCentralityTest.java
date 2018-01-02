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
package org.neo4j.graphalgo.centrality;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.impl.centrality.ClosenessCentrality;
import org.neo4j.graphalgo.impl.centrality.CostDivider;
import org.neo4j.graphalgo.impl.shortestpath.SingleSourceShortestPath;
import org.neo4j.graphalgo.impl.shortestpath.SingleSourceShortestPathDijkstra;
import org.neo4j.graphalgo.impl.util.DoubleAdder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;

import common.Neo4jAlgoTestCase;

public class ClosenessCentralityTest extends Neo4jAlgoTestCase
{
    protected SingleSourceShortestPath<Double> getSingleSourceShortestPath()
    {
        return new SingleSourceShortestPathDijkstra<Double>( 0.0, null,
            new CostEvaluator<Double>()
            {
                public Double getCost( Relationship relationship,
                            Direction direction )
                {
                    return 1.0;
                }
            }, new org.neo4j.graphalgo.impl.util.DoubleAdder(),
            new org.neo4j.graphalgo.impl.util.DoubleComparator(),
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

    @Test
    public void testBox()
    {
        /*
         * Layout
         *
         * (a)-(b)
         *  |   |
         * (d)-(c)
         */
        graph.makeEdgeChain( "a,b,c,d,a" );
        ClosenessCentrality<Double> closenessCentrality = getCentralityAlgorithm();
        assertCentrality( closenessCentrality, "a", 1.0 / 4 );
        assertCentrality( closenessCentrality, "b", 1.0 / 4 );
        assertCentrality( closenessCentrality, "c", 1.0 / 4 );
        assertCentrality( closenessCentrality, "d", 1.0 / 4 );
    }

    @Test
    public void testPlusShape()
    {
        /*
         * Layout
         *     (d)
         *      |
         * (a)-(b)-(c)
         *      |
         *     (e)
         */
        graph.makeEdgeChain( "a,b,c" );
        graph.makeEdgeChain( "d,b,e" );
        ClosenessCentrality<Double> closenessCentrality = getCentralityAlgorithm();
        assertCentrality( closenessCentrality, "a", 1.0 / 7 );
        assertCentrality( closenessCentrality, "b", 1.0 / 4 );
        assertCentrality( closenessCentrality, "c", 1.0 / 7 );
        assertCentrality( closenessCentrality, "d", 1.0 / 7 );
        assertCentrality( closenessCentrality, "e", 1.0 / 7 );
    }

    @Test
    public void testChain()
    {
        /*
         * Layout
         *
         * (a) - (b) - (c) - (d) - (e)
         */
        graph.makeEdgeChain( "a,b,c,d,e" );
        ClosenessCentrality<Double> closenessCentrality = getCentralityAlgorithm();
        assertCentrality( closenessCentrality, "a", 1.0 / 10 );
        assertCentrality( closenessCentrality, "b", 1.0 / 7 );
        assertCentrality( closenessCentrality, "c", 1.0 / 6 );
        assertCentrality( closenessCentrality, "d", 1.0 / 7 );
        assertCentrality( closenessCentrality, "e", 1.0 / 10 );
    }

    @Test
    public void isolatedNode()
    {
        /*
         * Layout
         *
         * (o)
         *
         * (a) -- (b)
         *  \-(c)-/
         */
        graph.makeNode( "o" );
        graph.makeEdgeChain( "a,b,c,a" );
        ClosenessCentrality<Double> closenessCentrality = getCentralityAlgorithm();
        Double o = closenessCentrality.getCentrality( graph.getNode( "o" ) );
        Double a = closenessCentrality.getCentrality( graph.getNode( "a" ) );
        Double b = closenessCentrality.getCentrality( graph.getNode( "b" ) );
        Double c = closenessCentrality.getCentrality( graph.getNode( "c" ) );

        assertCentrality( closenessCentrality, "o", 0d );
        assertCentrality( closenessCentrality, "a", 0.5 );
        assertCentrality( closenessCentrality, "b", 0.5 );
        assertCentrality( closenessCentrality, "c", 0.5 );
    }

    @Test
    public void isolatedCommunities()
    {
        /*
         * Layout
         *
         *  (a) -- (b)
         *   \-(c)-/
         *
         *  (d) -- (e)
         *   \-(f)-/
         */
        graph.makeEdgeChain( "a,b,c,a" );
        graph.makeEdgeChain( "d,e,f,d" );
        ClosenessCentrality<Double> closenessCentrality = getCentralityAlgorithm();

        assertCentrality( closenessCentrality, "a", 0.5 );
        assertCentrality( closenessCentrality, "b", 0.5 );
        assertCentrality( closenessCentrality, "c", 0.5 );

        assertCentrality( closenessCentrality, "d", 0.5 );
        assertCentrality( closenessCentrality, "e", 0.5 );
        assertCentrality( closenessCentrality, "f", 0.5 );
    }
}
