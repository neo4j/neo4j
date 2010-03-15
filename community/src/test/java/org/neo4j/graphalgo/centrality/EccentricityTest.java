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
import org.neo4j.graphalgo.shortestpath.std.DoubleComparator;
import org.neo4j.graphalgo.testUtil.Neo4jAlgoTestCase;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;

public class EccentricityTest extends Neo4jAlgoTestCase
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

    public void testBox()
    {
        graph.makeEdgeChain( "a,b,c,d,a" );
        Eccentricity<Double> centrality = getCentralityAlgorithm();
        assertCentrality( centrality, "a", 2.0 );
        assertCentrality( centrality, "b", 2.0 );
        assertCentrality( centrality, "c", 2.0 );
        assertCentrality( centrality, "d", 2.0 );
    }

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
