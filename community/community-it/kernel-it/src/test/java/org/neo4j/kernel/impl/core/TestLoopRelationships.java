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
package org.neo4j.kernel.impl.core;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.impl.MyRelTypes.TEST;

class TestLoopRelationships extends AbstractNeo4jTestCase
{
    @Test
    void canCreateRelationshipBetweenTwoNodesWithLoopsThenDeleteOneOfTheNodesAndItsRelationships()
    {
        Node source = createNode();
        Node target = createNode();
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            source.createRelationshipTo( source, TEST );
            target.createRelationshipTo( target, TEST );
            source.createRelationshipTo( target, TEST );
            transaction.commit();
        }

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            for ( Relationship rel : target.getRelationships() )
            {
                rel.delete();
            }
            target.delete();
            transaction.commit();
        }
    }

    @Test
    void canDeleteNodeAfterDeletingItsRelationshipsIfThoseRelationshipsIncludeLoops()
    {
        Node node = createNode();

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            txCreateLoop( node );
            txCreateRel( node );
            txCreateLoop( node );

            for ( Relationship rel : node.getRelationships() )
            {
                rel.delete();
            }
            node.delete();
            transaction.commit();
        }
    }

    private void txCreateRel( Node node )
    {
        node.createRelationshipTo( getGraphDb().createNode(), TEST );
    }

    private void txCreateLoop( Node node )
    {
        node.createRelationshipTo( node, TEST );
    }

    @Test
    void canAddLoopRelationship()
    {
        Node node = createNode();
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.createRelationshipTo( node, TEST );
            transaction.commit();
        }

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            for ( Direction dir : Direction.values() )
            {
                int count = 0;
                for ( Relationship rel : node.getRelationships( dir ) )
                {
                    count++;
                    assertEquals( node, rel.getStartNode(), "start node" );
                    assertEquals( node, rel.getEndNode(), "end node" );
                    assertEquals( node, rel.getOtherNode( node ), "other node" );
                }
                assertEquals( 1, count, dir.name() + " relationship count" );
            }
            transaction.commit();
        }
    }

    @Test
    void canAddManyLoopRelationships()
    {
        testAddManyLoopRelationships( 2 );
        testAddManyLoopRelationships( 3 );
        testAddManyLoopRelationships( 5 );
    }

    private void testAddManyLoopRelationships( int count )
    {
        for ( boolean[] loop : permutations( count ) )
        {
            Relationship[] relationships = new Relationship[count];
            Node root = createNode();
            try ( Transaction transaction = getGraphDb().beginTx() )
            {
                for ( int i = 0; i < count; i++ )
                {
                    if ( loop[i] )
                    {
                        relationships[i] = root.createRelationshipTo( root, TEST );
                    }
                    else
                    {
                        relationships[i] = root.createRelationshipTo( getGraphDb().createNode(), TEST );
                    }
                }
                transaction.commit();
            }
            verifyRelationships( Arrays.toString( loop ), root, loop, relationships );
        }
    }

    @Test
    void canAddLoopRelationshipAndOtherRelationships()
    {
        testAddLoopRelationshipAndOtherRelationships( 2 );
        testAddLoopRelationshipAndOtherRelationships( 3 );
        testAddLoopRelationshipAndOtherRelationships( 5 );
    }

    private void testAddLoopRelationshipAndOtherRelationships( int size )
    {
        for ( int i = 0; i < size; i++ )
        {
            Node root = createNode();
            Relationship[] relationships = createRelationships( size, i, root );
            verifyRelationships( String.format( "loop on %s of %s", i, size ),
                    root, i, relationships );
        }
    }

    @Test
    void canAddAndRemoveLoopRelationshipAndOtherRelationships()
    {
        testAddAndRemoveLoopRelationshipAndOtherRelationships( 2 );
        testAddAndRemoveLoopRelationshipAndOtherRelationships( 3 );
        testAddAndRemoveLoopRelationshipAndOtherRelationships( 5 );
    }

    @Test
    void getSingleRelationshipOnNodeWithOneLoopOnly()
    {
        Node node = createNode();
        Relationship singleRelationship;
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            singleRelationship = node.createRelationshipTo( node, TEST );
            assertEquals( singleRelationship, node.getSingleRelationship( TEST, Direction.OUTGOING ) );
            assertEquals( singleRelationship, node.getSingleRelationship( TEST, Direction.INCOMING ) );
            assertEquals( singleRelationship, node.getSingleRelationship( TEST, Direction.BOTH ) );
            transaction.commit();
        }

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            assertEquals( singleRelationship, node.getSingleRelationship( TEST, Direction.OUTGOING ) );
            assertEquals( singleRelationship, node.getSingleRelationship( TEST, Direction.INCOMING ) );
            assertEquals( singleRelationship, node.getSingleRelationship( TEST, Direction.BOTH ) );
            transaction.commit();
        }
    }

    @Test
    void cannotDeleteNodeWithLoopStillAttached()
    {
        // Given
        GraphDatabaseService db = getGraphDb();
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            node.createRelationshipTo( node, RelationshipType.withName( "MAYOR_OF" ) );
            tx.commit();
        }

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            // And given a transaction deleting just the node
            node.delete();

            var e = assertThrows( ConstraintViolationException.class, transaction::commit );
            assertThat( e.getMessage(), equalTo( "Cannot delete node<" + node.getId() + ">, because it still has relationships. " +
                    "To delete this node, you must first delete its relationships." ) );
        }
    }

    @Test
    void getOtherNodeFunctionsCorrectly()
    {
        Node node = createNode();
        Relationship relationship;
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            relationship = node.createRelationshipTo( node, TEST );
            transaction.commit();
        }

        // This loop messes up the readability of the test case, but avoids duplicated
        // assertion code. Same assertions withing the transaction as after it has committed.
        for ( int i = 0; i < 2; i++ )
        {
            try ( Transaction transaction = getGraphDb().beginTx() )
            {
                assertEquals( node, relationship.getOtherNode( node ) );
                assertEquals( asList( node, node ), asList( relationship.getNodes() ) );
                assertThrows( NotFoundException.class, () -> relationship.getOtherNode( getGraphDb().createNode() ) );
                transaction.commit();
            }
        }
    }

    @Test
    void getNewlyCreatedLoopRelationshipFromCache()
    {
        Node node = createNode();
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node.createRelationshipTo( getGraphDb().createNode(), TEST );
            transaction.commit();
        }
        Relationship relationship;
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            relationship = node.createRelationshipTo( node, TEST );
            transaction.commit();
        }
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            assertEquals( relationship, node.getSingleRelationship( TEST, Direction.INCOMING ) );
            transaction.commit();
        }
    }

    private void testAddAndRemoveLoopRelationshipAndOtherRelationships( int size )
    {
        for ( boolean[] delete : permutations( size ) )
        {
            for ( int i = 0; i < size; i++ )
            {
                Node root = createNode();
                Relationship[] relationships = createRelationships( size, i, root );
                try ( Transaction transaction = getGraphDb().beginTx() )
                {
                    for ( int j = 0; j < size; j++ )
                    {
                        if ( delete[j] )
                        {
                            relationships[j].delete();
                            relationships[j] = null;
                        }
                    }
                    transaction.commit();
                }
                verifyRelationships( String.format( "loop on %s of %s, delete %s", i, size, Arrays.toString( delete ) ), root, i, relationships );
            }
        }
    }

    private static Iterable<boolean[]> permutations( final int size )
    {
        final int max = 1 << size;
        return () -> new PrefetchingIterator<>()
        {
            int pos;

            @Override
            protected boolean[] fetchNextOrNull()
            {
                if ( pos < max )
                {
                    int cur = pos++;
                    boolean[] result = new boolean[size];
                    for ( int i = 0; i < size; i++ )
                    {
                        result[i] = (cur & 1) == 1;
                        cur >>= 1;
                    }
                    return result;
                }
                return null;
            }
        };
    }

    private Relationship[] createRelationships( int count, int loop, Node root )
    {
        Node[] nodes = new Node[count];
        for ( int i = 0; i < count; i++ )
        {
            if ( loop == i )
            {
                nodes[i] = root;
            }
            else
            {
                nodes[i] = createNode();
            }
        }

        Relationship[] relationships = new Relationship[count];
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            for ( int i = 0; i < count; i++ )
            {
                relationships[i] = root.createRelationshipTo( nodes[i], TEST );
            }
            transaction.commit();
        }
        return relationships;
    }

    private void verifyRelationships( String message, Node root, int loop,
        Relationship... relationships )
    {
        boolean[] loops = new boolean[relationships.length];
        for ( int i = 0; i < relationships.length; i++ )
        {
            loops[i] = i == loop;
        }
        verifyRelationships( message, root, loops, relationships );
    }

    private void verifyRelationships( String message, Node root,
        boolean[] loop, Relationship... relationships )
    {
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            for ( Direction dir : Direction.values() )
            {
                Set<Relationship> expected = new HashSet<>();
                for ( int i = 0; i < relationships.length; i++ )
                {
                    if ( relationships[i] != null && (dir != Direction.INCOMING || loop[i]) )
                    {
                        expected.add( relationships[i] );
                    }
                }

                for ( Relationship rel : root.getRelationships( dir ) )
                {
                    assertTrue( expected.remove( rel ), message + ": unexpected relationship: " + rel );
                }
                assertTrue( expected.isEmpty(), message + ": expected relationships not seen " + expected );
            }
            transaction.commit();
        }
    }
}
