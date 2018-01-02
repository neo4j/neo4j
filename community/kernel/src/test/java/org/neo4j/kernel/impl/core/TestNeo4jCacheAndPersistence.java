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

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.helpers.collection.Iterables.toList;
import static org.neo4j.helpers.collection.IteratorUtil.count;

public class TestNeo4jCacheAndPersistence extends AbstractNeo4jTestCase
{
    private long node1Id = -1;
    private long node2Id = -1;
    private final String key1 = "key1";
    private final String key2 = "key2";
    private final String arrayKey = "arrayKey";
    private final Integer int1 = new Integer( 1 );
    private final Integer int2 = new Integer( 2 );
    private final String string1 = new String( "1" );
    private final String string2 = new String( "2" );
    private final int[] array = new int[] { 1, 2, 3, 4, 5, 6, 7 };

    @Before
    public void createTestingGraph()
    {
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
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
 //       assertTrue( node1.getProperty( key1 ).equals( 1 ) );
        Transaction tx = getTransaction();
        tx.success();
        tx.close();
        tx = getGraphDb().beginTx();
//        node1.getPropertyKeys().iterator().next();
        assertTrue( node1.getProperty( key1 ).equals( 1 ) );
        setTransaction( tx );
    }

    @After
    public void deleteTestingGraph()
    {
        Node node1 = getGraphDb().getNodeById( node1Id );
        Node node2 = getGraphDb().getNodeById( node2Id );
        node1.getSingleRelationship( MyRelTypes.TEST, Direction.BOTH ).delete();
        node1.delete();
        node2.delete();
    }

    @Test
    public void testAddProperty()
    {
        String key3 = "key3";

        Node node1 = getGraphDb().getNodeById( node1Id );
        Node node2 = getGraphDb().getNodeById( node2Id );
        Relationship rel = node1.getSingleRelationship( MyRelTypes.TEST,
            Direction.BOTH );
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
        assertTrue( !node1.hasProperty( key3 ) );
        assertTrue( node2.hasProperty( key3 ) );
        assertEquals( int1, node1.getProperty( key1 ) );
        assertEquals( int2, node2.getProperty( key1 ) );
        assertEquals( string1, node1.getProperty( key2 ) );
        assertEquals( string2, node2.getProperty( key2 ) );
        assertEquals( int1, rel.getProperty( key1 ) );
        assertEquals( string1, rel.getProperty( key2 ) );
        assertEquals( int2, rel.getProperty( key3 ) );
    }

    @Test
    public void testNodeRemoveProperty()
    {
        Node node1 = getGraphDb().getNodeById( node1Id );
        Node node2 = getGraphDb().getNodeById( node2Id );
        Relationship rel = node1.getSingleRelationship( MyRelTypes.TEST,
            Direction.BOTH );

        // test remove property
        assertEquals( 1, node1.removeProperty( key1 ) );
        assertEquals( 2, node2.removeProperty( key1 ) );
        assertEquals( 1, rel.removeProperty( key1 ) );
        assertEquals( string1, node1.removeProperty( key2 ) );
        assertEquals( string2, node2.removeProperty( key2 ) );
        assertEquals( string1, rel.removeProperty( key2 ) );
        assertTrue( node1.removeProperty( arrayKey ) != null );
        assertTrue( node2.removeProperty( arrayKey ) != null );
        assertTrue( rel.removeProperty( arrayKey ) != null );
    }

    @Test
    public void testNodeChangeProperty()
    {
        Node node1 = getGraphDb().getNodeById( node1Id );
        Node node2 = getGraphDb().getNodeById( node2Id );
        Relationship rel = node1.getSingleRelationship( MyRelTypes.TEST,
            Direction.BOTH );

        // test change property
        node1.setProperty( key1, int2 );
        node2.setProperty( key1, int1 );
        rel.setProperty( key1, int2 );
        int[] newIntArray = new int[] { 3, 2, 1 };
        node1.setProperty( arrayKey, newIntArray );
        node2.setProperty( arrayKey, newIntArray );
        rel.setProperty( arrayKey, newIntArray );
    }

    @Test
    public void testNodeGetProperties()
    {
        Node node1 = getGraphDb().getNodeById( node1Id );

        assertTrue( !node1.hasProperty( null ) );
        Iterator<String> keys = node1.getPropertyKeys().iterator();
        keys.next();
        keys.next();
        assertTrue( node1.hasProperty( key1 ) );
        assertTrue( node1.hasProperty( key2 ) );
    }

    private Relationship[] getRelationshipArray(
        Iterable<Relationship> relsIterable )
    {
        ArrayList<Relationship> relList = new ArrayList<>();
        for ( Relationship rel : relsIterable )
        {
            relList.add( rel );
        }
        return relList.toArray( new Relationship[relList.size()] );
    }

    @Test
    public void testDirectedRelationship1()
    {
        Node node1 = getGraphDb().getNodeById( node1Id );
        Relationship rel = node1.getSingleRelationship( MyRelTypes.TEST,
            Direction.BOTH );
        Node nodes[] = rel.getNodes();
        assertEquals( 2, nodes.length );

        Node node2 = getGraphDb().getNodeById( node2Id );
        assertTrue( nodes[0].equals( node1 ) && nodes[1].equals( node2 ) );
        assertEquals( node1, rel.getStartNode() );
        assertEquals( node2, rel.getEndNode() );

        Relationship relArray[] = getRelationshipArray( node1.getRelationships(
            MyRelTypes.TEST, Direction.OUTGOING ) );
        assertEquals( 1, relArray.length );
        assertEquals( rel, relArray[0] );
        relArray = getRelationshipArray( node2.getRelationships(
            MyRelTypes.TEST, Direction.INCOMING ) );
        assertEquals( 1, relArray.length );
        assertEquals( rel, relArray[0] );
    }

    @Test
    public void testRelCountInSameTx()
    {
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        Relationship rel = node1.createRelationshipTo( node2, MyRelTypes.TEST );
        assertEquals( 1,
            getRelationshipArray( node1.getRelationships() ).length );
        assertEquals( 1,
            getRelationshipArray( node2.getRelationships() ).length );
        rel.delete();
        assertEquals( 0,
            getRelationshipArray( node1.getRelationships() ).length );
        assertEquals( 0,
            getRelationshipArray( node2.getRelationships() ).length );
        node1.delete();
        node2.delete();
    }

    @Test
    public void testGetDirectedRelationship()
    {
        Node node1 = getGraphDb().getNodeById( node1Id );
        Relationship rel = node1.getSingleRelationship( MyRelTypes.TEST,
            Direction.OUTGOING );
        assertEquals( int1, rel.getProperty( key1 ) );
    }

    @Test
    public void testSameTxWithArray()
    {
        commit();
        newTransaction();

        Node nodeA = getGraphDb().createNode();
        Node nodeB = getGraphDb().createNode();
        Relationship relA = nodeA.createRelationshipTo( nodeB, MyRelTypes.TEST );
        nodeA.setProperty( arrayKey, array );
        relA.setProperty( arrayKey, array );
        assertTrue( nodeA.getProperty( arrayKey ) != null );
        assertTrue( relA.getProperty( arrayKey ) != null );
        relA.delete();
        nodeA.delete();
        nodeB.delete();

    }

    @Test
    public void testAddCacheCleared()
    {
        Node nodeA = getGraphDb().createNode();
        nodeA.setProperty( "1", 1 );
        Node nodeB = getGraphDb().createNode();
        Relationship rel = nodeA.createRelationshipTo( nodeB, MyRelTypes.TEST );
        rel.setProperty( "1", 1 );
        commit();
        newTransaction();
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
        getGraphDb().getNodeById( nodeA.getId() );
        getGraphDb().getRelationshipById( rel.getId() );
        // apply COW maps
        commit();
        newTransaction();
        count = 0;
        for ( Relationship relToB : nodeA.getRelationships( MyRelTypes.TEST ) )
        {
            count++;
        }
        assertEquals( 2, count );
        assertEquals( 1, nodeA.getProperty( "1" ) );
        assertEquals( 1, rel.getProperty( "1" ) );
        assertEquals( 2, nodeA.getProperty( "2" ) );
        assertEquals( 2, rel.getProperty( "2" ) );
    }

    @Test
    public void testNodeMultiRemoveProperty()
    {
        Node node = getGraphDb().createNode();
        node.setProperty( "key0", "0" );
        node.setProperty( "key1", "1" );
        node.setProperty( "key2", "2" );
        node.setProperty( "key3", "3" );
        node.setProperty( "key4", "4" );
        newTransaction();
        node.removeProperty( "key3" );
        node.removeProperty( "key2" );
        node.removeProperty( "key3" );
        newTransaction();
        assertEquals( "0", node.getProperty( "key0" ) );
        assertEquals( "1", node.getProperty( "key1" ) );
        assertEquals( "4", node.getProperty( "key4" ) );
        assertTrue( !node.hasProperty( "key2" ) );
        assertTrue( !node.hasProperty( "key3" ) );
        node.delete();
    }

    @Test
    public void testRelMultiRemoveProperty()
    {
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        Relationship rel = node1.createRelationshipTo( node2, MyRelTypes.TEST );
        rel.setProperty( "key0", "0" );
        rel.setProperty( "key1", "1" );
        rel.setProperty( "key2", "2" );
        rel.setProperty( "key3", "3" );
        rel.setProperty( "key4", "4" );
        newTransaction();
        rel.removeProperty( "key3" );
        rel.removeProperty( "key2" );
        rel.removeProperty( "key3" );
        newTransaction();
        assertEquals( "0", rel.getProperty( "key0" ) );
        assertEquals( "1", rel.getProperty( "key1" ) );
        assertEquals( "4", rel.getProperty( "key4" ) );
        assertTrue( !rel.hasProperty( "key2" ) );
        assertTrue( !rel.hasProperty( "key3" ) );
        rel.delete();
        node1.delete();
        node2.delete();
    }

    @Ignore( "Can't depend on this behaviour since the introduction of dense nodes, at least the implementation of it currently" )
    @Test
    public void testRelationshipCachingIterator()
    {
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        Relationship rels[] = new Relationship[100];
        for ( int i = 0; i < rels.length; i++ )
        {
            if ( i < 50 )
            {
                rels[i] = node1.createRelationshipTo( node2, MyRelTypes.TEST );
            }
            else
            {
                rels[i] = node2.createRelationshipTo( node1, MyRelTypes.TEST );
            }
        }
        newTransaction();
        Iterable<Relationship> relIterable = toList( node1.getRelationships() );
        Set<Relationship> relSet = new HashSet<>();
        for ( Relationship rel : rels )
        {
            rel.delete();
            relSet.add( rel );
        }
        newTransaction();
        assertEquals( relSet, new HashSet<>( IteratorUtil.asCollection( relIterable ) ) );
        node1.delete();
        node2.delete();
    }

    @Test
    public void testLowGrabSize()
    {
        Map<String,String> config = new HashMap<>();
        config.put( "relationship_grab_size", "1" );
        File storeDir = getStorePath( "neo2" );
        deleteFileOrDirectory( storeDir );
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().setConfig( config ).newGraphDatabase();

        Node node1, node2;
        try ( Transaction tx = graphDb.beginTx() )
        {
            node1 = graphDb.createNode();
            node2 = graphDb.createNode();
            node1.createRelationshipTo( node2, MyRelTypes.TEST );
            node2.createRelationshipTo( node1, MyRelTypes.TEST2 );
            node1.createRelationshipTo( node2, MyRelTypes.TEST_TRAVERSAL );
            tx.success();
        }

        try ( Transaction tx = graphDb.beginTx() )
        {
            RelationshipType types[] = new RelationshipType[] {
                MyRelTypes.TEST, MyRelTypes.TEST2, MyRelTypes.TEST_TRAVERSAL };

            assertEquals( 3, count( node1.getRelationships( types ) ) );

            assertEquals( 3, count( node1.getRelationships() ) );

            assertEquals( 3, count( node2.getRelationships( types ) ) );

            assertEquals( 3, count( node2.getRelationships() ) );

            assertEquals( 2, count( node1.getRelationships( OUTGOING ) ) );

            assertEquals( 1, count( node1.getRelationships( INCOMING ) ) );

            assertEquals( 1, count( node2.getRelationships( OUTGOING ) ) );

            assertEquals( 2, count( node2.getRelationships( INCOMING ) ) );

            tx.success();
        }
        graphDb.shutdown();
    }

    @Test
    public void testAnotherLowGrabSize()
    {
        testLowGrabSize( false );
    }

    @Test
    public void testAnotherLowGrabSizeWithLoops()
    {
        testLowGrabSize( true );
    }

    private void testLowGrabSize( boolean includeLoops )
    {
        Map<String, String> config = new HashMap<>();
        config.put( "relationship_grab_size", "2" );
        File storeDir = getStorePath( "neo2" );
        deleteFileOrDirectory( storeDir );
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().setConfig( config ).newGraphDatabase();
        Transaction tx = graphDb.beginTx();
        Node node1 = graphDb.createNode();
        Node node2 = graphDb.createNode();
        Node node3 = graphDb.createNode();

        // These are expected relationships for node2
        Collection<Relationship> outgoingOriginal = new HashSet<>();
        Collection<Relationship> incomingOriginal = new HashSet<>();
        Collection<Relationship> loopsOriginal = new HashSet<>();

        int total = 0;
        int totalOneDirection = 0;
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
        tx.success();
        tx.close();

        tx = graphDb.beginTx();
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
                fail( "There should be no incomming relationships " + rel );
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

        tx.success();
        tx.close();
        graphDb.shutdown();
    }
}
