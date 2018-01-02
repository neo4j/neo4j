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

import org.junit.Test;

import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.impl.centrality.Eccentricity;
import org.neo4j.graphalgo.impl.shortestpath.SingleSourceShortestPath;
import org.neo4j.graphalgo.impl.shortestpath.SingleSourceShortestPathDijkstra;
import org.neo4j.graphalgo.impl.util.DoubleComparator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;

import common.Neo4jAlgoTestCase;
import static org.junit.Assert.assertTrue;

public class EccentricityTest extends Neo4jAlgoTestCase
{
    protected SingleSourceShortestPath<Double> getSingleSourceShortestPath()
    {
        return new SingleSourceShortestPathDijkstra<Double>( 0.0, null,
            new CostEvaluator<Double>()
            {
                @Override
                public Double getCost( Relationship relationship,
                            Direction direction )
                {
                    return 1.0;
                }
            }, new org.neo4j.graphalgo.impl.util.DoubleAdder(),
            new org.neo4j.graphalgo.impl.util.DoubleComparator(),
            Direction.BOTH, MyRelTypes.R1 );
    }

    Eccentricity<Double> getCentralityAlgorithm()
    {
        return new Eccentricity<Double>( getSingleSourceShortestPath(), 0.0,
            graph.getAllNodes(), new DoubleComparator() );
    }

    protected void assertCentrality( Eccentricity<Double> centrality,
        String nodeId, Double value )
    {
        assertTrue( centrality.getCentrality( graph.getNode( nodeId ) ).equals(
            value ) );
    }

    @Test
    public void testBox()
    {
        graph.makeEdgeChain( "a,b,c,d,a" );
        Eccentricity<Double> centrality = getCentralityAlgorithm();
        assertCentrality( centrality, "a", 2.0 );
        assertCentrality( centrality, "b", 2.0 );
        assertCentrality( centrality, "c", 2.0 );
        assertCentrality( centrality, "d", 2.0 );
    }

    @Test
    public void testPlusShape()
    {
        graph.makeEdgeChain( "a,b,c" );
        graph.makeEdgeChain( "d,b,e" );
        Eccentricity<Double> centrality = getCentralityAlgorithm();
        assertCentrality( centrality, "a", 2.0 );
        assertCentrality( centrality, "b", 1.0 );
        assertCentrality( centrality, "c", 2.0 );
        assertCentrality( centrality, "d", 2.0 );
        assertCentrality( centrality, "e", 2.0 );
    }

    @Test
    public void testChain()
    {
        graph.makeEdgeChain( "a,b,c,d,e" );
        Eccentricity<Double> centrality = getCentralityAlgorithm();
        assertCentrality( centrality, "a", 4.0 );
        assertCentrality( centrality, "b", 3.0 );
        assertCentrality( centrality, "c", 2.0 );
        assertCentrality( centrality, "d", 3.0 );
        assertCentrality( centrality, "e", 4.0 );
    }
}
