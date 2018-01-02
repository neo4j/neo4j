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
package org.neo4j.kernel.impl.core;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.junit.Test;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.collection.PrefetchingIterator;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.kernel.impl.MyRelTypes.TEST;

public class TestLoopRelationships extends AbstractNeo4jTestCase
{
    @Test
    public void canCreateRelationshipBetweenTwoNodesWithLoopsThenDeleteOneOfTheNodesAndItsRelationships()
            throws Exception
    {
        Node source = getGraphDb().createNode(), target = getGraphDb().createNode();
        source.createRelationshipTo( source, TEST );
        target.createRelationshipTo( target, TEST );
        source.createRelationshipTo( target, TEST );

        newTransaction();

        for ( Relationship rel : target.getRelationships() )
        {
            rel.delete();
        }
        target.delete();
    }

    @Test
    public void canDeleteNodeAfterDeletingItsRelationshipsIfThoseRelationshipsIncludeLoops() throws Exception
    {
        Node node = getGraphDb().createNode();

        txCreateLoop( node );
        txCreateRel( node );
        txCreateLoop( node );

        for ( Relationship rel : node.getRelationships() )
        {
            rel.delete();
        }
        node.delete();

        commit();
    }

    private void txCreateRel( Node node )
    {
        node.createRelationshipTo( getGraphDb().createNode(), TEST );
        newTransaction();
    }

    private void txCreateLoop( Node node )
    {
        node.createRelationshipTo( node, TEST );
        newTransaction();
    }

    @Test
    public void canAddLoopRelationship()
    {
        Node node = getGraphDb().createNode();
        node.createRelationshipTo( node, TEST );

        newTransaction();

        for ( Direction dir : Direction.values() )
        {
            int count = 0;
            for ( Relationship rel : node.getRelationships( dir ) )
            {
                count++;
                assertEquals( "start node", node, rel.getStartNode() );
                assertEquals( "end node", node, rel.getEndNode() );
                assertEquals( "other node", node, rel.getOtherNode( node ) );
            }
            assertEquals( dir.name() + " relationship count", 1, count );
        }
    }

    @Test
    public void canAddManyLoopRelationships()
    {
        testAddManyLoopRelationships( 2 );
        testAddManyLoopRelationships( 3 );
        testAddManyLoopRelationships( 5 );
    }

    private void testAddManyLoopRelationships( int count )
    {
        for ( boolean[] loop : permutations( count ) )
        {
            Node root = getGraphDb().createNode();
            Relationship[] relationships = new Relationship[count];
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
            newTransaction();
            verifyRelationships( Arrays.toString( loop ), root, loop,
                    relationships );
        }
    }

    @Test
    public void canAddLoopRelationshipAndOtherRelationships()
    {
        testAddLoopRelationshipAndOtherRelationships( 2 );
        testAddLoopRelationshipAndOtherRelationships( 3 );
        testAddLoopRelationshipAndOtherRelationships( 5 );
    }

    private void testAddLoopRelationshipAndOtherRelationships( int size )
    {
        for ( int i = 0; i < size; i++ )
        {
            Node root = getGraphDb().createNode();
            Relationship[] relationships = createRelationships( size, i, root );
            verifyRelationships( String.format( "loop on %s of %s", i, size ),
                    root, i, relationships );
        }
    }

    @Test
    public void canAddAndRemoveLoopRelationshipAndOtherRelationships()
    {
        testAddAndRemoveLoopRelationshipAndOtherRelationships( 2 );
        testAddAndRemoveLoopRelationshipAndOtherRelationships( 3 );
        testAddAndRemoveLoopRelationshipAndOtherRelationships( 5 );
    }

    @Test
    public void getSingleRelationshipOnNodeWithOneLoopOnly() throws Exception
    {
        Node node = getGraphDb().createNode();
        Relationship singleRelationship = node.createRelationshipTo( node, TEST );
        assertEquals( singleRelationship, node.getSingleRelationship( TEST, Direction.OUTGOING ) );
        assertEquals( singleRelationship, node.getSingleRelationship( TEST, Direction.INCOMING ) );
        assertEquals( singleRelationship, node.getSingleRelationship( TEST, Direction.BOTH ) );
        commit();

        newTransaction();
        assertEquals( singleRelationship, node.getSingleRelationship( TEST, Direction.OUTGOING ) );
        assertEquals( singleRelationship, node.getSingleRelationship( TEST, Direction.INCOMING ) );
        assertEquals( singleRelationship, node.getSingleRelationship( TEST, Direction.BOTH ) );
        finish();
    }

    @Test
    public void cannotDeleteNodeWithLoopStillAttached() throws Exception
    {
        Node node = getGraphDb().createNode();
        node.createRelationshipTo( node, TEST );
        newTransaction();
        node.delete();
        try
        {
            commit();
            fail( "Shouldn't be able to delete a node which still has a loop relationship attached" );
        }
        catch ( TransactionFailureException e )
        { // Good
        }
    }

    @Test
    public void getOtherNodeFunctionsCorrectly() throws Exception
    {
        Node node = getGraphDb().createNode();
        Relationship relationship = node.createRelationshipTo( node, TEST );

        // This loop messes up the readability of the test case, but avoids duplicated
        // assertion code. Same assertions withing the transaction as after it has committed.
        for ( int i = 0; i < 2; i++ )
        {
            assertEquals( node, relationship.getOtherNode( node ) );
            assertEquals( asList( node, node ), asList( relationship.getNodes() ) );
            try
            {
                relationship.getOtherNode( getGraphDb().createNode() );
                fail( "Should throw exception if another node is passed into loop.getOtherNode" );
            }
            catch ( NotFoundException e )
            { // Good
            }
            newTransaction();
        }
    }

    @Test
    public void getNewlyCreatedLoopRelationshipFromCache() throws Exception
    {
        Node node = getGraphDb().createNode();
        node.createRelationshipTo( getGraphDb().createNode(), TEST );
        newTransaction();
        Relationship relationship = node.createRelationshipTo( node, TEST );
        newTransaction();
        assertEquals( relationship, node.getSingleRelationship( TEST, Direction.INCOMING ) );
    }

    private void testAddAndRemoveLoopRelationshipAndOtherRelationships( int size )
    {
        for ( boolean[] delete : permutations( size ) )
        {
            for ( int i = 0; i < size; i++ )
            {
                Node root = getGraphDb().createNode();
                Relationship[] relationships = createRelationships( size, i,
                        root );
                for ( int j = 0; j < size; j++ )
                {
                    if ( delete[j] )
                    {
                        relationships[j].delete();
                        relationships[j] = null;
                    }
                    newTransaction();
                }
                verifyRelationships( String.format(
                                "loop on %s of %s, delete %s", i, size,
                                Arrays.toString( delete ) ), root,
                        i, relationships );
            }
        }
    }

    private String print( Relationship[] relationships )
    {
        StringBuilder b = new StringBuilder();
        for ( Relationship rel : relationships )
        {
            b.append( rel.getStartNode() + "--" + rel + "->" + rel.getEndNode() );
        }
        return b.toString();
    }

    private static Iterable<boolean[]> permutations( final int size )
    {
        final int max = 1 << size;
        return new Iterable<boolean[]>()
        {
            @Override
            public Iterator<boolean[]> iterator()
            {
                return new PrefetchingIterator<boolean[]>()
                {
                    int pos = 0;

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
                nodes[i] = getGraphDb().createNode();
            }
        }

        newTransaction();

        Relationship[] relationships = new Relationship[count];
        for ( int i = 0; i < count; i++ )
        {
            relationships[i] = root.createRelationshipTo( nodes[i], TEST );
            newTransaction();
        }
        return relationships;
    }

    private void verifyRelationships( String message, Node root, int loop,
            Relationship... relationships )
    {
        boolean[] loops = new boolean[relationships.length];
        for ( int i = 0; i < relationships.length; i++ )
        {
            loops[i] = (i == loop);
        }
        verifyRelationships( message, root, loops, relationships );
    }

    private void verifyRelationships( String message, Node root,
            boolean[] loop, Relationship... relationships )
    {
        for ( Direction dir : Direction.values() )
        {
            Set<Relationship> expected = new HashSet<Relationship>();
            for ( int i = 0; i < relationships.length; i++ )
            {
                if ( relationships[i] != null
                        && (dir != Direction.INCOMING || loop[i]) )
                {
                    expected.add( relationships[i] );
                }
            }

            for ( Relationship rel : root.getRelationships( dir ) )
            {
                assertTrue( message + ": unexpected relationship: " + rel,
                        expected.remove( rel ) );
            }
            assertTrue( message + ": expected relationships not seen "
                            + expected,
                    expected.isEmpty() );
        }
    }
}
