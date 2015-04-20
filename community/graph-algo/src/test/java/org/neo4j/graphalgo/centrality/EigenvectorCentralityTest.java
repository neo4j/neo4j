/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.neo4j.graphalgo.CommonEvaluators;
import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.impl.centrality.EigenvectorCentrality;
import org.neo4j.graphalgo.impl.centrality.EigenvectorCentralityPower;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import common.Neo4jAlgoTestCase;

public abstract class EigenvectorCentralityTest extends Neo4jAlgoTestCase
{
    protected void assertCentrality(
        EigenvectorCentralityPower eigenvectorCentralityPower, String nodeId,
        Double value )
    {
        assertEquals( value,
                      eigenvectorCentralityPower.getCentrality( graph.getNode( nodeId ) ) );
    }

    /**
     * @param eigenvectorCentralityPower
     * @param nodeId
     *            Id of the node
     * @param value
     *            The correct value
     * @param precision
     *            Precision factor (ex. 0.01)
     */
    protected void assertApproximateCentrality(
        EigenvectorCentrality eigenvectorCentrality, String nodeId,
        Double value, Double precision )
    {
        Double centrality = eigenvectorCentrality.getCentrality( graph
            .getNode( nodeId ) );
        assertEquals( value, centrality, precision );
    }

    public abstract EigenvectorCentrality getEigenvectorCentrality(
        Direction relationDirection, CostEvaluator<Double> costEvaluator,
        Set<Node> nodeSet, Set<Relationship> relationshipSet, double precision );

    @Test
    public void testRun()
    {
        graph.makeEdgeChain( "a,b,c,d" );
        graph.makeEdges( "b,a,c,a" );
        EigenvectorCentrality eigenvectorCentrality = getEigenvectorCentrality(
            Direction.OUTGOING, new CostEvaluator<Double>()
            {
                public Double getCost( Relationship relationship,
                            Direction direction )
                {
                    return 1.0;
                }
            }, graph.getAllNodes(), graph.getAllEdges(), 0.01 );
        // eigenvectorCentrality.setMaxIterations( 100 );
        assertApproximateCentrality( eigenvectorCentrality, "a", 0.693, 0.01 );
        assertApproximateCentrality( eigenvectorCentrality, "b", 0.523, 0.01 );
        assertApproximateCentrality( eigenvectorCentrality, "c", 0.395, 0.01 );
        assertApproximateCentrality( eigenvectorCentrality, "d", 0.298, 0.01 );
    }

    /**
     * Same as above, but inverted direction.
     */
    @Test
    public void testDirection()
    {
        graph.makeEdgeChain( "d,c,b,a" );
        graph.makeEdges( "a,b,a,c" );
        EigenvectorCentrality eigenvectorCentrality = getEigenvectorCentrality(
            Direction.INCOMING, new CostEvaluator<Double>()
            {
                public Double getCost( Relationship relationship,
                            Direction direction )
                {
                    return 1.0;
                }
            }, graph.getAllNodes(), graph.getAllEdges(), 0.01 );
        // eigenvectorCentrality.setMaxIterations( 100 );
        assertApproximateCentrality( eigenvectorCentrality, "a", 0.693, 0.01 );
        assertApproximateCentrality( eigenvectorCentrality, "b", 0.523, 0.01 );
        assertApproximateCentrality( eigenvectorCentrality, "c", 0.395, 0.01 );
        assertApproximateCentrality( eigenvectorCentrality, "d", 0.298, 0.01 );
    }

    /**
     * Some weighted relationships.
     */
    @Test
    public void testWeight()
    {
        graph.makeEdgeChain( "a,b", "cost", 1.0 );
        graph.makeEdgeChain( "b,c", "cost", 1.0 );
        graph.makeEdgeChain( "c,d", "cost", 1.0 );
        graph.makeEdgeChain( "c,b", "cost", 0.1 );
        graph.makeEdgeChain( "c,a", "cost", 0.1 );
        EigenvectorCentrality eigenvectorCentrality = getEigenvectorCentrality(
            Direction.OUTGOING, CommonEvaluators.doubleCostEvaluator( "cost" ), graph
                .getAllNodes(), graph.getAllEdges(), 0.01 );
        // eigenvectorCentrality.setMaxIterations( 100 );
        assertApproximateCentrality( eigenvectorCentrality, "a", 0.0851, 0.01 );
        assertApproximateCentrality( eigenvectorCentrality, "b", 0.244, 0.01 );
        assertApproximateCentrality( eigenvectorCentrality, "c", 0.456, 0.01 );
        assertApproximateCentrality( eigenvectorCentrality, "d", 0.852, 0.01 );
    }

    /**
     * Same network as above, but with direction BOTH and weights in different
     * directions are given by a map.
     */
    @Test
    public void testWeightAndDirection()
    {
        graph.makeEdgeChain( "a,b" );
        graph.makeEdgeChain( "b,c" );
        graph.makeEdgeChain( "c,d" );
        graph.makeEdgeChain( "c,a" );
        final Map<String,Double> costs = new HashMap<String,Double>();
        costs.put( "a,b", 1.0 );
        costs.put( "b,c", 1.0 );
        costs.put( "c,d", 1.0 );
        costs.put( "c,b", 0.1 );
        costs.put( "c,a", 0.1 );
        EigenvectorCentrality eigenvectorCentrality = getEigenvectorCentrality(
            Direction.BOTH, new CostEvaluator<Double>()
            {
                public Double getCost( Relationship relationship,
                            Direction direction )
                {
                    String start = graph
                        .getNodeId( relationship.getStartNode() );
                    String end = graph.getNodeId( relationship.getEndNode() );
                    if ( direction == Direction.INCOMING )
                    {
                        // swap
                        String tmp = end;
                        end = start;
                        start = tmp;
                    }
                    Double value = costs.get( start + "," + end );
                    if ( value == null )
                    {
                        return 0.0;
                    }
                    return value;
                }
            }, graph.getAllNodes(), graph.getAllEdges(), 0.01 );
        // eigenvectorCentrality.setMaxIterations( 100 );
        assertApproximateCentrality( eigenvectorCentrality, "a", 0.0851, 0.01 );
        assertApproximateCentrality( eigenvectorCentrality, "b", 0.244, 0.01 );
        assertApproximateCentrality( eigenvectorCentrality, "c", 0.456, 0.01 );
        assertApproximateCentrality( eigenvectorCentrality, "d", 0.852, 0.01 );
    }
}
