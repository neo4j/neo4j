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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.impl.centrality.BetweennessCentrality;
import org.neo4j.graphalgo.impl.shortestpath.SingleSourceShortestPath;
import org.neo4j.graphalgo.impl.shortestpath.SingleSourceShortestPathDijkstra;
import org.neo4j.graphalgo.impl.shortestpath.Util;
import org.neo4j.graphalgo.impl.shortestpath.Util.PathCounter;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import common.Neo4jAlgoTestCase;

public class BetweennessCentralityTest extends Neo4jAlgoTestCase
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
            Direction.BOTH, MyRelTypes.R1, MyRelTypes.R2, MyRelTypes.R3 );
    }

    protected void assertCentrality(
        BetweennessCentrality<Double> betweennessCentrality, String nodeId,
        Double value )
    {
        assertTrue( betweennessCentrality
            .getCentrality( graph.getNode( nodeId ) ).equals( value ) );
    }

    @Test
    public void testBox()
    {
        graph.makeEdgeChain( "a,b,c,d,a" );
        BetweennessCentrality<Double> betweennessCentrality = new BetweennessCentrality<Double>(
            getSingleSourceShortestPath(), graph.getAllNodes() );
        betweennessCentrality.calculate();
        assertCentrality( betweennessCentrality, "a", 0.5 );
        assertCentrality( betweennessCentrality, "b", 0.5 );
        assertCentrality( betweennessCentrality, "c", 0.5 );
        assertCentrality( betweennessCentrality, "d", 0.5 );
    }

    @Test
    public void testPlusShape()
    {
        graph.makeEdgeChain( "a,b,c" );
        graph.setCurrentRelType( MyRelTypes.R3 );
        graph.makeEdgeChain( "d,b,e" );
        BetweennessCentrality<Double> betweennessCentrality = new BetweennessCentrality<Double>(
            getSingleSourceShortestPath(), graph.getAllNodes() );
        betweennessCentrality.calculate();
        assertCentrality( betweennessCentrality, "a", 0.0 );
        assertCentrality( betweennessCentrality, "b", 6.0 );
        assertCentrality( betweennessCentrality, "c", 0.0 );
        assertCentrality( betweennessCentrality, "d", 0.0 );
        assertCentrality( betweennessCentrality, "e", 0.0 );
    }

    @Test
    public void testChain()
    {
        graph.makeEdgeChain( "a,b,c,d,e" );
        BetweennessCentrality<Double> betweennessCentrality = new BetweennessCentrality<Double>(
            getSingleSourceShortestPath(), graph.getAllNodes() );
        betweennessCentrality.calculate();
        assertCentrality( betweennessCentrality, "a", 0.0 );
        assertCentrality( betweennessCentrality, "b", 3.0 );
        assertCentrality( betweennessCentrality, "c", 4.0 );
        assertCentrality( betweennessCentrality, "d", 3.0 );
        assertCentrality( betweennessCentrality, "e", 0.0 );
    }
    

    @Test
    public void testXlike()
    {
        graph.makeEdgeChain( "a,c,a");
        graph.makeEdgeChain( "b,c,b");
        graph.makeEdgeChain( "b,d,b");
        graph.makeEdgeChain( "c,d,c");
        graph.makeEdgeChain( "d,e,d");
        graph.makeEdgeChain( "d,f,d");
        BetweennessCentrality<Double> betweennessCentrality = new BetweennessCentrality<Double>(
            getSingleSourceShortestPath(), graph.getAllNodes() );
        betweennessCentrality.calculate();
        assertCentrality( betweennessCentrality, "a", 0.0 );
        assertCentrality( betweennessCentrality, "b", 0.0 );
        assertCentrality( betweennessCentrality, "c", 4.0 );
        assertCentrality( betweennessCentrality, "d", 7.0 );
        assertCentrality( betweennessCentrality, "e", 0.0 );
        assertCentrality( betweennessCentrality, "f", 0.0 );
    }

    @Test
    public void testDependencyUpdating()
    {
        graph.makeEdgeChain( "a,b,d,e,f,h" );
        graph.makeEdgeChain( "a,c,d" );
        graph.makeEdgeChain( "e,g,h" );
        new DependencyTest( getSingleSourceShortestPath(), graph.getAllNodes() )
            .test();
    }

    class DependencyTest extends BetweennessCentrality<Double>
    {
        public DependencyTest(
            SingleSourceShortestPath<Double> singleSourceShortestPath,
            Set<Node> nodeSet )
        {
            super( singleSourceShortestPath, nodeSet );
        }

        public void test()
        {
            // avoid starting the real calculation by mistake
            this.doneCalculation = true;
            // set things up
            Node startNode = graph.getNode( "a" );
            singleSourceShortestPath.reset();
            singleSourceShortestPath.setStartNode( startNode );
            Map<Node,List<Relationship>> predecessors = singleSourceShortestPath
                .getPredecessors();
            Map<Node,List<Relationship>> successors = Util
                .reversedPredecessors( predecessors );
            PathCounter counter = new Util.PathCounter( predecessors );
            // Recursively update the node dependencies
            getAndUpdateNodeDependency( startNode, true, successors, counter,
                new HashMap<Node,Double>() );
            Double adjustment = 0.5; // since direction is BOTH
            assertCentrality( this, "a", 0.0 * adjustment );
            assertCentrality( this, "b", 2.5 * adjustment );
            assertCentrality( this, "c", 2.5 * adjustment );
            assertCentrality( this, "d", 4.0 * adjustment );
            assertCentrality( this, "e", 3.0 * adjustment );
            assertCentrality( this, "f", 0.5 * adjustment );
            assertCentrality( this, "g", 0.5 * adjustment );
            assertCentrality( this, "h", 0.0 * adjustment );
        }
    }
}
