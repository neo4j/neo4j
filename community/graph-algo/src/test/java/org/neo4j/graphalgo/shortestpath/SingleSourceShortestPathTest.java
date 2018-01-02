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
package org.neo4j.graphalgo.shortestpath;

import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;
import org.neo4j.graphalgo.impl.shortestpath.SingleSourceShortestPath;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import common.Neo4jAlgoTestCase;

public abstract class SingleSourceShortestPathTest extends Neo4jAlgoTestCase
{
    protected abstract SingleSourceShortestPath<Integer> getSingleSourceAlgorithm(
        Node startNode );

    protected abstract SingleSourceShortestPath<Integer> getSingleSourceAlgorithm(
        Node startNode, Direction direction, RelationshipType... relTypes );

    @Test
    public void testRun()
    {
        // make the graph
        graph.makeEdgeChain( "a,b1,c1,d1,e1,f1,g1" );
        graph.makeEdgeChain( "a,b2,c2,d2,e2,f2,g2" );
        graph.makeEdgeChain( "a,b3,c3,d3,e3,f3,g3" );
        graph.makeEdgeChain( "b1,b2,b3,b1" );
        graph.makeEdgeChain( "d1,d2,d3,d1" );
        graph.makeEdgeChain( "f1,f2,f3,f1" );
        // make the computation
        SingleSourceShortestPath<Integer> singleSource = getSingleSourceAlgorithm( graph
            .getNode( "a" ) );
        // check a few distances
        assertTrue( singleSource.getCost( graph.getNode( "a" ) ) == 0 );
        assertTrue( singleSource.getCost( graph.getNode( "b2" ) ) == 1 );
        assertTrue( singleSource.getCost( graph.getNode( "c3" ) ) == 2 );
        assertTrue( singleSource.getCost( graph.getNode( "d1" ) ) == 3 );
        assertTrue( singleSource.getCost( graph.getNode( "e2" ) ) == 4 );
        assertTrue( singleSource.getCost( graph.getNode( "f3" ) ) == 5 );
        assertTrue( singleSource.getCost( graph.getNode( "g1" ) ) == 6 );
        // check one path
        List<Node> path = singleSource.getPathAsNodes( graph.getNode( "g2" ) );
        assertTrue( path.size() == 7 );
        assertTrue( path.get( 0 ).equals( graph.getNode( "a" ) ) );
        assertTrue( path.get( 1 ).equals( graph.getNode( "b2" ) ) );
        assertTrue( path.get( 2 ).equals( graph.getNode( "c2" ) ) );
        assertTrue( path.get( 3 ).equals( graph.getNode( "d2" ) ) );
        assertTrue( path.get( 4 ).equals( graph.getNode( "e2" ) ) );
        assertTrue( path.get( 5 ).equals( graph.getNode( "f2" ) ) );
        assertTrue( path.get( 6 ).equals( graph.getNode( "g2" ) ) );
        // check it as relationships
        List<Relationship> rpath = singleSource.getPathAsRelationships( graph
            .getNode( "g2" ) );
        assertTrue( rpath.size() == 6 );
        assertTrue( rpath.get( 0 ).equals( graph.getRelationship( "a", "b2" ) ) );
        assertTrue( rpath.get( 1 ).equals( graph.getRelationship( "b2", "c2" ) ) );
        assertTrue( rpath.get( 2 ).equals( graph.getRelationship( "c2", "d2" ) ) );
        assertTrue( rpath.get( 3 ).equals( graph.getRelationship( "d2", "e2" ) ) );
        assertTrue( rpath.get( 4 ).equals( graph.getRelationship( "e2", "f2" ) ) );
        assertTrue( rpath.get( 5 ).equals( graph.getRelationship( "f2", "g2" ) ) );
        // check it as both
        List<PropertyContainer> cpath = singleSource.getPath( graph
            .getNode( "g2" ) );
        assertTrue( cpath.size() == 13 );
        assertTrue( cpath.get( 0 ).equals( graph.getNode( "a" ) ) );
        assertTrue( cpath.get( 2 ).equals( graph.getNode( "b2" ) ) );
        assertTrue( cpath.get( 4 ).equals( graph.getNode( "c2" ) ) );
        assertTrue( cpath.get( 6 ).equals( graph.getNode( "d2" ) ) );
        assertTrue( cpath.get( 8 ).equals( graph.getNode( "e2" ) ) );
        assertTrue( cpath.get( 10 ).equals( graph.getNode( "f2" ) ) );
        assertTrue( cpath.get( 12 ).equals( graph.getNode( "g2" ) ) );
        assertTrue( cpath.get( 1 ).equals( graph.getRelationship( "a", "b2" ) ) );
        assertTrue( cpath.get( 3 ).equals( graph.getRelationship( "b2", "c2" ) ) );
        assertTrue( cpath.get( 5 ).equals( graph.getRelationship( "c2", "d2" ) ) );
        assertTrue( cpath.get( 7 ).equals( graph.getRelationship( "d2", "e2" ) ) );
        assertTrue( cpath.get( 9 ).equals( graph.getRelationship( "e2", "f2" ) ) );
        assertTrue( cpath.get( 11 )
            .equals( graph.getRelationship( "f2", "g2" ) ) );
        graph.clear();
    }

    @Test
    public void testMultipleRelTypes()
    {
        graph.setCurrentRelType( MyRelTypes.R1 );
        graph.makeEdgeChain( "a,b,c,d,e" );
        graph.setCurrentRelType( MyRelTypes.R2 );
        graph.makeEdges( "a,c" ); // first shortcut
        graph.setCurrentRelType( MyRelTypes.R3 );
        graph.makeEdges( "c,e" ); // second shortcut
        SingleSourceShortestPath<Integer> singleSource;
        // one path
        singleSource = getSingleSourceAlgorithm( graph.getNode( "a" ),
            Direction.BOTH, MyRelTypes.R1 );
        assertTrue( singleSource.getCost( graph.getNode( "e" ) ) == 4 );
        // one shortcut
        singleSource = getSingleSourceAlgorithm( graph.getNode( "a" ),
            Direction.BOTH, MyRelTypes.R1, MyRelTypes.R2 );
        assertTrue( singleSource.getCost( graph.getNode( "e" ) ) == 3 );
        // other shortcut
        singleSource = getSingleSourceAlgorithm( graph.getNode( "a" ),
            Direction.BOTH, MyRelTypes.R1, MyRelTypes.R3 );
        assertTrue( singleSource.getCost( graph.getNode( "e" ) ) == 3 );
        // both shortcuts
        singleSource = getSingleSourceAlgorithm( graph.getNode( "a" ),
            Direction.BOTH, MyRelTypes.R1, MyRelTypes.R2, MyRelTypes.R3 );
        assertTrue( singleSource.getCost( graph.getNode( "e" ) ) == 2 );
    }
}
