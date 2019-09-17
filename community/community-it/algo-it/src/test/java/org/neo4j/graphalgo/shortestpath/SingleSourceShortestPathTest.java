/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import common.Neo4jAlgoTestCase;
import org.junit.jupiter.api.Test;

import java.util.List;

import org.neo4j.graphalgo.impl.shortestpath.SingleSourceShortestPath;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class SingleSourceShortestPathTest extends Neo4jAlgoTestCase
{
    protected abstract SingleSourceShortestPath<Integer> getSingleSourceAlgorithm(
        Node startNode );

    protected abstract SingleSourceShortestPath<Integer> getSingleSourceAlgorithm(
        Node startNode, Direction direction, RelationshipType... relTypes );

    @Test
    void testRun()
    {
        try ( Transaction transaction = graphDb.beginTx() )
        {
            // make the graph
            graph.makeEdgeChain( transaction, "a,b1,c1,d1,e1,f1,g1" );
            graph.makeEdgeChain( transaction, "a,b2,c2,d2,e2,f2,g2" );
            graph.makeEdgeChain( transaction, "a,b3,c3,d3,e3,f3,g3" );
            graph.makeEdgeChain( transaction, "b1,b2,b3,b1" );
            graph.makeEdgeChain( transaction, "d1,d2,d3,d1" );
            graph.makeEdgeChain( transaction, "f1,f2,f3,f1" );
            // make the computation
            SingleSourceShortestPath<Integer> singleSource = getSingleSourceAlgorithm( graph.getNode( transaction, "a" ) );
            // check a few distances
            assertEquals( 0, (int) singleSource.getCost( graph.getNode( transaction, "a" ) ) );
            assertEquals( 1, (int) singleSource.getCost( graph.getNode( transaction, "b2" ) ) );
            assertEquals( 2, (int) singleSource.getCost( graph.getNode( transaction, "c3" ) ) );
            assertEquals( 3, (int) singleSource.getCost( graph.getNode( transaction, "d1" ) ) );
            assertEquals( 4, (int) singleSource.getCost( graph.getNode( transaction, "e2" ) ) );
            assertEquals( 5, (int) singleSource.getCost( graph.getNode( transaction, "f3" ) ) );
            assertEquals( 6, (int) singleSource.getCost( graph.getNode( transaction, "g1" ) ) );
            // check one path
            List<Node> path = singleSource.getPathAsNodes( graph.getNode( transaction, "g2" ) );
            assertEquals( 7, path.size() );
            assertEquals( path.get( 0 ), graph.getNode( transaction, "a" ) );
            assertEquals( path.get( 1 ), graph.getNode( transaction, "b2" ) );
            assertEquals( path.get( 2 ), graph.getNode( transaction, "c2" ) );
            assertEquals( path.get( 3 ), graph.getNode( transaction, "d2" ) );
            assertEquals( path.get( 4 ), graph.getNode( transaction, "e2" ) );
            assertEquals( path.get( 5 ), graph.getNode( transaction, "f2" ) );
            assertEquals( path.get( 6 ), graph.getNode( transaction, "g2" ) );
            // check it as relationships
            List<Relationship> rpath = singleSource.getPathAsRelationships( graph.getNode( transaction, "g2" ) );
            assertEquals( 6, rpath.size() );
            assertEquals( rpath.get( 0 ), graph.getRelationship( transaction,  "a", "b2" ) );
            assertEquals( rpath.get( 1 ), graph.getRelationship( transaction,  "b2", "c2" ) );
            assertEquals( rpath.get( 2 ), graph.getRelationship( transaction,  "c2", "d2" ) );
            assertEquals( rpath.get( 3 ), graph.getRelationship( transaction,  "d2", "e2" ) );
            assertEquals( rpath.get( 4 ), graph.getRelationship( transaction,  "e2", "f2" ) );
            assertEquals( rpath.get( 5 ), graph.getRelationship( transaction,  "f2", "g2" ) );
            // check it as both
            List<Entity> cpath = singleSource.getPath( graph.getNode( transaction, "g2" ) );
            assertEquals( 13, cpath.size() );
            assertEquals( cpath.get( 0 ), graph.getNode( transaction, "a" ) );
            assertEquals( cpath.get( 2 ), graph.getNode( transaction, "b2" ) );
            assertEquals( cpath.get( 4 ), graph.getNode( transaction, "c2" ) );
            assertEquals( cpath.get( 6 ), graph.getNode( transaction, "d2" ) );
            assertEquals( cpath.get( 8 ), graph.getNode( transaction, "e2" ) );
            assertEquals( cpath.get( 10 ), graph.getNode( transaction, "f2" ) );
            assertEquals( cpath.get( 12 ), graph.getNode( transaction, "g2" ) );
            assertEquals( cpath.get( 1 ), graph.getRelationship( transaction,  "a", "b2" ) );
            assertEquals( cpath.get( 3 ), graph.getRelationship( transaction,  "b2", "c2" ) );
            assertEquals( cpath.get( 5 ), graph.getRelationship( transaction,  "c2", "d2" ) );
            assertEquals( cpath.get( 7 ), graph.getRelationship( transaction,  "d2", "e2" ) );
            assertEquals( cpath.get( 9 ), graph.getRelationship( transaction,  "e2", "f2" ) );
            assertEquals( cpath.get( 11 ), graph.getRelationship( transaction,  "f2", "g2" ) );
            transaction.commit();
        }
    }

    @Test
    void testMultipleRelTypes()
    {
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.setCurrentRelType( MyRelTypes.R1 );
            graph.makeEdgeChain( transaction, "a,b,c,d,e" );
            graph.setCurrentRelType( MyRelTypes.R2 );
            graph.makeEdges( transaction, "a,c" ); // first shortcut
            graph.setCurrentRelType( MyRelTypes.R3 );
            graph.makeEdges( transaction, "c,e" ); // second shortcut
            SingleSourceShortestPath<Integer> singleSource;
            // one path
            singleSource = getSingleSourceAlgorithm( graph.getNode( transaction, "a" ), Direction.BOTH, MyRelTypes.R1 );
            assertEquals( 4, (int) singleSource.getCost( graph.getNode( transaction, "e" ) ) );
            // one shortcut
            singleSource = getSingleSourceAlgorithm( graph.getNode( transaction, "a" ), Direction.BOTH, MyRelTypes.R1, MyRelTypes.R2 );
            assertEquals( 3, (int) singleSource.getCost( graph.getNode( transaction, "e" ) ) );
            // other shortcut
            singleSource = getSingleSourceAlgorithm( graph.getNode( transaction, "a" ), Direction.BOTH, MyRelTypes.R1, MyRelTypes.R3 );
            assertEquals( 3, (int) singleSource.getCost( graph.getNode( transaction, "e" ) ) );
            // both shortcuts
            singleSource = getSingleSourceAlgorithm( graph.getNode( transaction, "a" ), Direction.BOTH, MyRelTypes.R1, MyRelTypes.R2, MyRelTypes.R3 );
            assertEquals( 2, (int) singleSource.getCost( graph.getNode( transaction, "e" ) ) );
            transaction.commit();
        }
    }
}
