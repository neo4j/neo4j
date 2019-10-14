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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

@TestDirectoryExtension
class TestNeo4jCacheAndPersistence extends AbstractNeo4jTestCase
{
    @Inject
    private TestDirectory testDirectory;

    private static final String key1 = "key1";
    private static final String key2 = "key2";
    private static final String arrayKey = "arrayKey";
    private static final Integer int1 = 1;
    private static final Integer int2 = 2;
    private static final String string1 = "1";
    private static final String string2 = "2";
    private final int[] array = {1, 2, 3, 4, 5, 6, 7};
    private long node1Id = -1;
    private long node2Id = -1;

    @BeforeEach
    void createTestingGraph()
    {
        Node node1 = createNode();
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            Node node2 = transaction.createNode();
            node1 = transaction.getNodeById( node1.getId() );
            Relationship rel = node1.createRelationshipTo( node2, MyRelTypes.TEST );
            node1Id = node1.getId();
            node2Id = node2.getId();
            node1.setProperty( key1, int1 );
            node1.setProperty( key2, string1 );
            node2.setProperty( key1, int2 );
            node2.setProperty( key2, string2 );
            rel.setProperty( key1, int1 );
            rel.setProperty( key2, string1 );
            node1.setProperty( arrayKey, array );
            node2.setProperty( arrayKey, array );
            rel.setProperty( arrayKey, array );
            transaction.commit();
        }
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            assertEquals( 1, transaction.getNodeById( node1.getId() ).getProperty( key1 ) );
            transaction.commit();
        }
    }

    @AfterEach
    void deleteTestingGraph()
    {
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            Node node1 = transaction.getNodeById( node1Id );
            Node node2 = transaction.getNodeById( node2Id );
            node1.getSingleRelationship( MyRelTypes.TEST, Direction.BOTH ).delete();
            node1.delete();
            node2.delete();
            transaction.commit();
        }
    }

    @Test
    void testAddProperty()
    {
        String key3 = "key3";

        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            Node node1 = transaction.getNodeById( node1Id );
            Node node2 = transaction.getNodeById( node2Id );
            Relationship rel = node1.getSingleRelationship( MyRelTypes.TEST, Direction.BOTH );
            // add new property
            node2.setProperty( key3, int1 );
            rel.setProperty( key3, int2 );
            assertTrue( node1.hasProperty( key1 ) );
            assertTrue( node2.hasProperty( key1 ) );
            assertTrue( node1.hasProperty( key2 ) );
            assertTrue( node2.hasProperty( key2 ) );
            assertTrue( node1.hasProperty( arrayKey ) );
            assertTrue( node2.hasProperty( arrayKey ) );
            assertTrue( rel.hasProperty( arrayKey ) );
            assertFalse( node1.hasProperty( key3 ) );
            assertTrue( node2.hasProperty( key3 ) );
            assertEquals( int1, node1.getProperty( key1 ) );
            assertEquals( int2, node2.getProperty( key1 ) );
            assertEquals( string1, node1.getProperty( key2 ) );
            assertEquals( string2, node2.getProperty( key2 ) );
            assertEquals( int1, rel.getProperty( key1 ) );
            assertEquals( string1, rel.getProperty( key2 ) );
            assertEquals( int2, rel.getProperty( key3 ) );
            transaction.commit();
        }
    }

    @Test
    void testNodeRemoveProperty()
    {
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            Node node1 = transaction.getNodeById( node1Id );
            Node node2 = transaction.getNodeById( node2Id );
            Relationship rel = node1.getSingleRelationship( MyRelTypes.TEST, Direction.BOTH );

            // test remove property
            assertEquals( 1, node1.removeProperty( key1 ) );
            assertEquals( 2, node2.removeProperty( key1 ) );
            assertEquals( 1, rel.removeProperty( key1 ) );
            assertEquals( string1, node1.removeProperty( key2 ) );
            assertEquals( string2, node2.removeProperty( key2 ) );
            assertEquals( string1, rel.removeProperty( key2 ) );
            assertNotNull( node1.removeProperty( arrayKey ) );
            assertNotNull( node2.removeProperty( arrayKey ) );
            assertNotNull( rel.removeProperty( arrayKey ) );
            transaction.commit();
        }
    }

    @Test
    void testNodeChangeProperty()
    {
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            Node node1 = transaction.getNodeById( node1Id );
            Node node2 = transaction.getNodeById( node2Id );
            Relationship rel = node1.getSingleRelationship( MyRelTypes.TEST, Direction.BOTH );

            // test change property
            node1.setProperty( key1, int2 );
            node2.setProperty( key1, int1 );
            rel.setProperty( key1, int2 );
            int[] newIntArray = {3, 2, 1};
            node1.setProperty( arrayKey, newIntArray );
            node2.setProperty( arrayKey, newIntArray );
            rel.setProperty( arrayKey, newIntArray );
            transaction.commit();
        }
    }

    @Test
    void testNodeGetProperties()
    {
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            Node node1 = transaction.getNodeById( node1Id );

            assertFalse( node1.hasProperty( null ) );
            Iterator<String> keys = node1.getPropertyKeys().iterator();
            keys.next();
            keys.next();
            assertTrue( node1.hasProperty( key1 ) );
            assertTrue( node1.hasProperty( key2 ) );
            transaction.commit();
        }
    }

    private Relationship[] getRelationshipArray(
        Iterable<Relationship> relsIterable )
    {
        ArrayList<Relationship> relList = new ArrayList<>();
        for ( Relationship rel : relsIterable )
        {
            relList.add( rel );
        }
        return relList.toArray( new Relationship[0] );
    }

    @Test
    void testDirectedRelationship1()
    {
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            Node node1 = transaction.getNodeById( node1Id );
            Relationship rel = node1.getSingleRelationship( MyRelTypes.TEST, Direction.BOTH );
            Node[] nodes = rel.getNodes();
            assertEquals( 2, nodes.length );

            Node node2 = transaction.getNodeById( node2Id );
            assertTrue( nodes[0].equals( node1 ) && nodes[1].equals( node2 ) );
            assertEquals( node1, rel.getStartNode() );
            assertEquals( node2, rel.getEndNode() );

            Relationship[] relArray = getRelationshipArray( node1.getRelationships( OUTGOING, MyRelTypes.TEST ) );
            assertEquals( 1, relArray.length );
            assertEquals( rel, relArray[0] );
            relArray = getRelationshipArray( node2.getRelationships( INCOMING, MyRelTypes.TEST ) );
            assertEquals( 1, relArray.length );
            assertEquals( rel, relArray[0] );
            transaction.commit();
        }
    }

    @Test
    void testRelCountInSameTx()
    {
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            Node node1 = transaction.createNode();
            Node node2 = transaction.createNode();
            Relationship rel = node1.createRelationshipTo( node2, MyRelTypes.TEST );
            assertEquals( 1, getRelationshipArray( node1.getRelationships() ).length );
            assertEquals( 1, getRelationshipArray( node2.getRelationships() ).length );
            rel.delete();
            assertEquals( 0, getRelationshipArray( node1.getRelationships() ).length );
            assertEquals( 0, getRelationshipArray( node2.getRelationships() ).length );
            node1.delete();
            node2.delete();
            transaction.commit();
        }
    }

    @Test
    void testGetDirectedRelationship()
    {
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            Node node1 = transaction.getNodeById( node1Id );
            Relationship rel = node1.getSingleRelationship( MyRelTypes.TEST, OUTGOING );
            assertEquals( int1, rel.getProperty( key1 ) );
            transaction.commit();
        }
    }

    @Test
    void testSameTxWithArray()
    {
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            Node nodeA = transaction.createNode();
            Node nodeB = transaction.createNode();
            Relationship relA = nodeA.createRelationshipTo( nodeB, MyRelTypes.TEST );
            nodeA.setProperty( arrayKey, array );
            relA.setProperty( arrayKey, array );
            assertNotNull( nodeA.getProperty( arrayKey ) );
            assertNotNull( relA.getProperty( arrayKey ) );
            relA.delete();
            nodeA.delete();
            nodeB.delete();
            transaction.commit();
        }
    }

    @Test
    void testAddCacheCleared()
    {
        Node nodeA = createNode();
        Node nodeB = createNode();
        Relationship rel;
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            nodeA = transaction.getNodeById( nodeA.getId() );
            nodeB = transaction.getNodeById( nodeB.getId() );

            nodeA.setProperty( "1", 1 );
            rel = nodeA.createRelationshipTo( nodeB, MyRelTypes.TEST );
            rel.setProperty( "1", 1 );
            transaction.commit();
        }
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            nodeA = transaction.getNodeById( nodeA.getId() );
            nodeB = transaction.getNodeById( nodeB.getId() );
            rel = transaction.getRelationshipById( rel.getId() );

            nodeA.createRelationshipTo( nodeB, MyRelTypes.TEST );
            int count = 0;
            for ( Relationship relToB : nodeA.getRelationships( MyRelTypes.TEST ) )
            {
                count++;
            }
            assertEquals( 2, count );
            nodeA.setProperty( "2", 2 );
            assertEquals( 1, nodeA.getProperty( "1" ) );
            rel.setProperty( "2", 2 );
            assertEquals( 1, rel.getProperty( "1" ) );
            // trigger empty load
            transaction.getNodeById( nodeA.getId() );
            transaction.getRelationshipById( rel.getId() );
            transaction.commit();
        }
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            nodeA = transaction.getNodeById( nodeA.getId() );
            rel = transaction.getRelationshipById( rel.getId() );

            int count = 0;
            for ( Relationship relToB : nodeA.getRelationships( MyRelTypes.TEST ) )
            {
                count++;
            }
            assertEquals( 2, count );
            assertEquals( 1, nodeA.getProperty( "1" ) );
            assertEquals( 1, rel.getProperty( "1" ) );
            assertEquals( 2, nodeA.getProperty( "2" ) );
            assertEquals( 2, rel.getProperty( "2" ) );
            transaction.commit();
        }
    }

    @Test
    void testNodeMultiRemoveProperty()
    {
        Node node = createNode();
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node = transaction.getNodeById( node.getId() );
            node.setProperty( "key0", "0" );
            node.setProperty( "key1", "1" );
            node.setProperty( "key2", "2" );
            node.setProperty( "key3", "3" );
            node.setProperty( "key4", "4" );
            transaction.commit();
        }
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node = transaction.getNodeById( node.getId() );

            node.removeProperty( "key3" );
            node.removeProperty( "key2" );
            node.removeProperty( "key3" );
            transaction.commit();
        }
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node = transaction.getNodeById( node.getId() );

            assertEquals( "0", node.getProperty( "key0" ) );
            assertEquals( "1", node.getProperty( "key1" ) );
            assertEquals( "4", node.getProperty( "key4" ) );
            assertFalse( node.hasProperty( "key2" ) );
            assertFalse( node.hasProperty( "key3" ) );
            node.delete();
            transaction.commit();
        }
    }

    @Test
    void testRelMultiRemoveProperty()
    {
        Node node1 = createNode();
        Node node2 = createNode();
        Relationship rel;
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node1 = transaction.getNodeById( node1.getId() );
            node2 = transaction.getNodeById( node2.getId() );

            rel = node1.createRelationshipTo( node2, MyRelTypes.TEST );
            rel.setProperty( "key0", "0" );
            rel.setProperty( "key1", "1" );
            rel.setProperty( "key2", "2" );
            rel.setProperty( "key3", "3" );
            rel.setProperty( "key4", "4" );
            transaction.commit();
        }
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            rel = transaction.getRelationshipById( rel.getId() );

            rel.removeProperty( "key3" );
            rel.removeProperty( "key2" );
            rel.removeProperty( "key3" );
            transaction.commit();
        }
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            node1 = transaction.getNodeById( node1.getId() );
            node2 = transaction.getNodeById( node2.getId() );
            rel = transaction.getRelationshipById( rel.getId() );

            assertEquals( "0", rel.getProperty( "key0" ) );
            assertEquals( "1", rel.getProperty( "key1" ) );
            assertEquals( "4", rel.getProperty( "key4" ) );
            assertFalse( rel.hasProperty( "key2" ) );
            assertFalse( rel.hasProperty( "key3" ) );
            rel.delete();
            node1.delete();
            node2.delete();
            transaction.commit();
        }
    }

    @Test
    void testLowGrabSize()
    {
        Node node1;
        Node node2;
        try ( Transaction tx = getGraphDb().beginTx() )
        {
            node1 = tx.createNode();
            node2 = tx.createNode();
            node1.createRelationshipTo( node2, MyRelTypes.TEST );
            node2.createRelationshipTo( node1, MyRelTypes.TEST2 );
            node1.createRelationshipTo( node2, MyRelTypes.TEST_TRAVERSAL );
            tx.commit();
        }

        try ( Transaction tx = getGraphDb().beginTx() )
        {
            node1 = tx.getNodeById( node1.getId() );
            node2 = tx.getNodeById( node2.getId() );

            RelationshipType[] types = {MyRelTypes.TEST, MyRelTypes.TEST2, MyRelTypes.TEST_TRAVERSAL};

            assertEquals( 3, Iterables.count( node1.getRelationships( types ) ) );

            assertEquals( 3, Iterables.count( node1.getRelationships() ) );

            assertEquals( 3, Iterables.count( node2.getRelationships( types ) ) );

            assertEquals( 3, Iterables.count( node2.getRelationships() ) );

            assertEquals( 2, Iterables.count( node1.getRelationships( OUTGOING ) ) );

            assertEquals( 1, Iterables.count( node1.getRelationships( INCOMING ) ) );

            assertEquals( 1, Iterables.count( node2.getRelationships( OUTGOING ) ) );

            assertEquals( 2, Iterables.count( node2.getRelationships( INCOMING ) ) );

            tx.commit();
        }
    }

    @Test
    void testAnotherLowGrabSize()
    {
        testLowGrabSize( false );
    }

    @Test
    void testAnotherLowGrabSizeWithLoops()
    {
        testLowGrabSize( true );
    }

    private void testLowGrabSize( boolean includeLoops )
    {
        Collection<Relationship> outgoingOriginal = new HashSet<>();
        Collection<Relationship> incomingOriginal = new HashSet<>();
        Collection<Relationship> loopsOriginal = new HashSet<>();
        Node node1 = createNode();
        Node node2 = createNode();
        Node node3 = createNode();
        int total = 0;
        int totalOneDirection = 0;
        try ( Transaction tx = getGraphDb().beginTx() )
        {
            node1 = tx.getNodeById( node1.getId() );
            node2 = tx.getNodeById( node2.getId() );
            node3 = tx.getNodeById( node3.getId() );

            for ( int i = 0; i < 33; i++ )
            {
                if ( includeLoops )
                {
                    loopsOriginal.add( node2.createRelationshipTo( node2, MyRelTypes.TEST ) );
                    total++;
                    totalOneDirection++;
                }

                if ( i % 2 == 0 )
                {
                    incomingOriginal.add( node1.createRelationshipTo( node2, MyRelTypes.TEST ) );
                    outgoingOriginal.add( node2.createRelationshipTo( node3, MyRelTypes.TEST ) );
                }
                else
                {
                    outgoingOriginal.add( node2.createRelationshipTo( node1, MyRelTypes.TEST ) );
                    incomingOriginal.add( node3.createRelationshipTo( node2, MyRelTypes.TEST ) );
                }
                total += 2;
                totalOneDirection++;
            }
            tx.commit();
        }

        try ( Transaction tx = getGraphDb().beginTx() )
        {
            node2 = tx.getNodeById( node2.getId() );
            Set<Relationship> rels = new HashSet<>();

            Collection<Relationship> outgoing = new HashSet<>( outgoingOriginal );
            Collection<Relationship> incoming = new HashSet<>( incomingOriginal );
            Collection<Relationship> loops = new HashSet<>( loopsOriginal );
            for ( Relationship rel : node2.getRelationships( MyRelTypes.TEST ) )
            {
                assertTrue( rels.add( rel ) );
                if ( rel.getStartNode().equals( node2 ) && rel.getEndNode().equals( node2 ) )
                {
                    assertTrue( loops.remove( rel ) );
                }
                else if ( rel.getStartNode().equals( node2 ) )
                {
                    assertTrue( outgoing.remove( rel ) );
                }
                else
                {
                    assertTrue( incoming.remove( rel ) );
                }
            }
            assertEquals( total, rels.size() );
            assertEquals( 0, loops.size() );
            assertEquals( 0, incoming.size() );
            assertEquals( 0, outgoing.size() );
            rels.clear();

            outgoing = new HashSet<>( outgoingOriginal );
            incoming = new HashSet<>( incomingOriginal );
            loops = new HashSet<>( loopsOriginal );
            for ( Relationship rel : node2.getRelationships( Direction.OUTGOING ) )
            {
                assertTrue( rels.add( rel ) );
                if ( rel.getStartNode().equals( node2 ) && rel.getEndNode().equals( node2 ) )
                {
                    assertTrue( loops.remove( rel ) );
                }
                else if ( rel.getStartNode().equals( node2 ) )
                {
                    assertTrue( outgoing.remove( rel ) );
                }
                else
                {
                    fail( "There should be no incoming relationships " + rel );
                }
            }
            assertEquals( totalOneDirection, rels.size() );
            assertEquals( 0, loops.size() );
            assertEquals( 0, outgoing.size() );
            rels.clear();

            outgoing = new HashSet<>( outgoingOriginal );
            incoming = new HashSet<>( incomingOriginal );
            loops = new HashSet<>( loopsOriginal );
            for ( Relationship rel : node2.getRelationships( Direction.INCOMING ) )
            {
                assertTrue( rels.add( rel ) );
                if ( rel.getStartNode().equals( node2 ) && rel.getEndNode().equals( node2 ) )
                {
                    assertTrue( loops.remove( rel ) );
                }
                else if ( rel.getEndNode().equals( node2 ) )
                {
                    assertTrue( incoming.remove( rel ) );
                }
                else
                {
                    fail( "There should be no outgoing relationships " + rel );
                }
            }
            assertEquals( totalOneDirection, rels.size() );
            assertEquals( 0, loops.size() );
            assertEquals( 0, incoming.size() );
            rels.clear();

            tx.commit();
        }
    }
}
