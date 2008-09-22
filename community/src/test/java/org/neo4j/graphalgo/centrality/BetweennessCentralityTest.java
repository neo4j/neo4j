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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.graphalgo.centrality.BetweennessCentrality;
import org.neo4j.graphalgo.shortestpath.CostEvaluator;
import org.neo4j.graphalgo.shortestpath.SingleSourceShortestPath;
import org.neo4j.graphalgo.shortestpath.SingleSourceShortestPathDijkstra;
import org.neo4j.graphalgo.shortestpath.Util;
import org.neo4j.graphalgo.shortestpath.Util.PathCounter;
import org.neo4j.graphalgo.testUtil.NeoAlgoTestCase;

public class BetweennessCentralityTest extends NeoAlgoTestCase
{
    public BetweennessCentralityTest( String arg0 )
    {
        super( arg0 );
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
            Direction.BOTH, MyRelTypes.R1, MyRelTypes.R2, MyRelTypes.R3 );
    }

    protected void assertCentrality(
        BetweennessCentrality<Double> betweennessCentrality, String nodeId,
        Double value )
    {
        assertTrue( betweennessCentrality
            .getCentrality( graph.getNode( nodeId ) ).equals( value ) );
    }

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
