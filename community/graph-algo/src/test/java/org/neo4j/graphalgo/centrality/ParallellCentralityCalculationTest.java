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
import org.neo4j.graphalgo.impl.centrality.BetweennessCentrality;
import org.neo4j.graphalgo.impl.centrality.ClosenessCentrality;
import org.neo4j.graphalgo.impl.centrality.CostDivider;
import org.neo4j.graphalgo.impl.centrality.ParallellCentralityCalculation;
import org.neo4j.graphalgo.impl.centrality.ShortestPathBasedCentrality;
import org.neo4j.graphalgo.impl.centrality.StressCentrality;
import org.neo4j.graphalgo.impl.shortestpath.SingleSourceShortestPath;
import org.neo4j.graphalgo.impl.shortestpath.SingleSourceShortestPathDijkstra;
import org.neo4j.graphalgo.impl.util.DoubleAdder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;

import common.Neo4jAlgoTestCase;

public class ParallellCentralityCalculationTest extends Neo4jAlgoTestCase
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

    protected void assertCentrality(
        ShortestPathBasedCentrality<Double,Double> centrality, String nodeId,
        Double value )
    {
        assertTrue( centrality.getCentrality( graph.getNode( nodeId ) ).equals(
            value ) );
    }

    @Test
    public void testPlusShape()
    {
        // Make graph
        graph.makeEdgeChain( "a,b,c" );
        graph.makeEdgeChain( "d,b,e" );
        SingleSourceShortestPath<Double> singleSourceShortestPath = getSingleSourceShortestPath();
        ParallellCentralityCalculation<Double> pcc = new ParallellCentralityCalculation<Double>(
            singleSourceShortestPath, graph.getAllNodes() );
        BetweennessCentrality<Double> betweennessCentrality = new BetweennessCentrality<Double>(
            singleSourceShortestPath, graph.getAllNodes() );
        StressCentrality<Double> stressCentrality = new StressCentrality<Double>(
            singleSourceShortestPath, graph.getAllNodes() );
        ClosenessCentrality<Double> closenessCentrality = new ClosenessCentrality<Double>(
            singleSourceShortestPath, new DoubleAdder(), 0.0, graph
                .getAllNodes(), new CostDivider<Double>()
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
        pcc.addCalculation( betweennessCentrality );
        pcc.addCalculation( stressCentrality );
        pcc.addCalculation( closenessCentrality );
        pcc.calculate();

        assertCentrality( betweennessCentrality, "a", 0.0 );
        assertCentrality( betweennessCentrality, "b", 6.0 );
        assertCentrality( betweennessCentrality, "c", 0.0 );
        assertCentrality( betweennessCentrality, "d", 0.0 );
        assertCentrality( betweennessCentrality, "e", 0.0 );
        assertCentrality( stressCentrality, "a", 0.0 );
        assertCentrality( stressCentrality, "b", 6.0 );
        assertCentrality( stressCentrality, "c", 0.0 );
        assertCentrality( stressCentrality, "d", 0.0 );
        assertCentrality( stressCentrality, "e", 0.0 );
        assertCentrality( closenessCentrality, "a", 1.0 / 7 );
        assertCentrality( closenessCentrality, "b", 1.0 / 4 );
        assertCentrality( closenessCentrality, "c", 1.0 / 7 );
        assertCentrality( closenessCentrality, "d", 1.0 / 7 );
        assertCentrality( closenessCentrality, "e", 1.0 / 7 );
    }
}
