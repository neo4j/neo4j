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
package org.neo4j.graphalgo;

import org.neo4j.api.core.Direction;
import org.neo4j.graphalgo.benchmark.graphGeneration.SubGraph;
import org.neo4j.graphalgo.testUtil.NeoAlgoTestCase;

public class SubGraphTest extends NeoAlgoTestCase
{
    public SubGraphTest( String name )
    {
        super( name );
    }

    public void generateGraph()
    {
        graph.makeEdgeChain( "a,b,c" );
        graph.makeEdgeChain( "a,d,e" );
        graph.makeEdgeChain( "a,g,h" );
        graph.makeEdgeChain( "b,d,g,b" );
        graph.makeEdgeChain( "c,e,h,c" );
    }

    public void testHighDepth()
    {
        generateGraph();
        SubGraph subGraph = new SubGraph();
        subGraph.addSubGraphFromCentralNode( graph.getNode( "a" ), 5,
            MyRelTypes.R1, Direction.BOTH, false );
        assertTrue( graph.getAllNodes().equals( subGraph.getNodes() ) );
        assertTrue( graph.getAllEdges().equals( subGraph.getEdges() ) );
    }

    public void testLowDepth()
    {
        generateGraph();
        SubGraph subGraph = new SubGraph();
        subGraph.addSubGraphFromCentralNode( graph.getNode( "a" ), 1,
            MyRelTypes.R1, Direction.BOTH, true );
        assertFalse( graph.getAllNodes().equals( subGraph.getNodes() ) );
        assertFalse( graph.getAllEdges().equals( subGraph.getEdges() ) );
    }

    public void testBorder()
    {
        generateGraph();
        SubGraph subGraph = new SubGraph();
        subGraph.addSubGraphFromCentralNode( graph.getNode( "a" ), 2,
            MyRelTypes.R1, Direction.BOTH, false );
        assertTrue( graph.getAllNodes().equals( subGraph.getNodes() ) );
        assertFalse( graph.getAllEdges().equals( subGraph.getEdges() ) );
        subGraph.clear();
        subGraph.addSubGraphFromCentralNode( graph.getNode( "a" ), 2,
            MyRelTypes.R1, Direction.BOTH, true );
        assertTrue( graph.getAllNodes().equals( subGraph.getNodes() ) );
        assertTrue( graph.getAllEdges().equals( subGraph.getEdges() ) );
    }

    public void testMultipleAdds()
    {
        generateGraph();
        SubGraph subGraph = new SubGraph();
        subGraph.addSubGraphFromCentralNode( graph.getNode( "c" ), 1,
            MyRelTypes.R1, Direction.BOTH, true );
        subGraph.addSubGraphFromCentralNode( graph.getNode( "e" ), 1,
            MyRelTypes.R1, Direction.BOTH, true );
        subGraph.addSubGraphFromCentralNode( graph.getNode( "h" ), 1,
            MyRelTypes.R1, Direction.BOTH, true );
        subGraph.addSubGraphFromCentralNode( graph.getNode( "a" ), 1,
            MyRelTypes.R1, Direction.BOTH, true );
        assertTrue( graph.getAllNodes().equals( subGraph.getNodes() ) );
        assertTrue( graph.getAllEdges().equals( subGraph.getEdges() ) );
    }
}
