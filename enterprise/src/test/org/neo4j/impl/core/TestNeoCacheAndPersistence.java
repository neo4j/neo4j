/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.core;

import java.util.ArrayList;
import java.util.Iterator;

import javax.transaction.TransactionManager;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.Transaction;
import org.neo4j.impl.AbstractNeoTestCase;
import org.neo4j.impl.MyRelTypes;

public class TestNeoCacheAndPersistence extends AbstractNeoTestCase
{
    public TestNeoCacheAndPersistence( String testName )
    {
        super( testName );
    }

    private int node1Id = -1;
    private int node2Id = -1;
    private String key1 = "key1";
    private String key2 = "key2";
    private String arrayKey = "arrayKey";
    private Integer int1 = new Integer( 1 );
    private Integer int2 = new Integer( 2 );
    private String string1 = new String( "1" );
    private String string2 = new String( "2" );
    private int[] array = new int[] { 1, 2, 3, 4, 5, 6, 7 };

    public void setUp()
    {
        super.setUp();
        Node node1 = getNeo().createNode();
        Node node2 = getNeo().createNode();
        Relationship rel = node1.createRelationshipTo( node2, MyRelTypes.TEST );
        node1Id = (int) node1.getId();
        node2Id = (int) node2.getId();
        node1.setProperty( key1, int1 );
        node1.setProperty( key2, string1 );
        node2.setProperty( key1, int2 );
        node2.setProperty( key2, string2 );
        rel.setProperty( key1, int1 );
        rel.setProperty( key2, string1 );
        node1.setProperty( arrayKey, array );
        node2.setProperty( arrayKey, array );
        rel.setProperty( arrayKey, array );
        Transaction tx = getTransaction();
        tx.success();
        tx.finish();
        NodeManager nodeManager = ((EmbeddedNeo) getNeo()).getConfig()
            .getNeoModule().getNodeManager();
        nodeManager.clearCache();
        tx = getNeo().beginTx();
        setTransaction( tx );
    }

    public void tearDown()
    {
        Node node1 = getNeo().getNodeById( node1Id );
        Node node2 = getNeo().getNodeById( node2Id );
        node1.getSingleRelationship( MyRelTypes.TEST, Direction.BOTH ).delete();
        node1.delete();
        node2.delete();
        super.tearDown();
    }

    public void testAddProperty()
    {
        String key3 = "key3";

        Node node1 = getNeo().getNodeById( node1Id );
        Node node2 = getNeo().getNodeById( node2Id );
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

    public void testNodeRemoveProperty()
    {
        Node node1 = getNeo().getNodeById( node1Id );
        Node node2 = getNeo().getNodeById( node2Id );
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

    public void testNodeChangeProperty()
    {
        Node node1 = getNeo().getNodeById( node1Id );
        Node node2 = getNeo().getNodeById( node2Id );
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

    public void testNodeGetProperties()
    {
        Node node1 = getNeo().getNodeById( node1Id );

        assertTrue( !node1.hasProperty( null ) );
        Iterator<Object> values = node1.getPropertyValues().iterator();
        values.next();
        values.next();
        Iterator<String> keys = node1.getPropertyKeys().iterator();
        keys.next();
        keys.next();
        assertTrue( node1.hasProperty( key1 ) );
        assertTrue( node1.hasProperty( key2 ) );
    }

    private Relationship[] getRelationshipArray(
        Iterable<Relationship> relsIterable )
    {
        ArrayList<Relationship> relList = new ArrayList<Relationship>();
        for ( Relationship rel : relsIterable )
        {
            relList.add( rel );
        }
        return relList.toArray( new Relationship[relList.size()] );
    }

    public void testDirectedRelationship1()
    {
        Node node1 = getNeo().getNodeById( node1Id );
        Relationship rel = node1.getSingleRelationship( MyRelTypes.TEST,
            Direction.BOTH );
        Node nodes[] = rel.getNodes();
        assertEquals( 2, nodes.length );

        Node node2 = getNeo().getNodeById( node2Id );
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

    public void testRelCountInSameTx()
    {
        Node node1 = getNeo().createNode();
        Node node2 = getNeo().createNode();
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

    public void testGetDirectedRelationship()
    {
        Node node1 = getNeo().getNodeById( node1Id );
        Relationship rel = node1.getSingleRelationship( MyRelTypes.TEST,
            Direction.OUTGOING );
        assertEquals( int1, rel.getProperty( key1 ) );
    }

    public void testSameTxWithArray()
    {
        getTransaction().success();
        getTransaction().finish();
        newTransaction();

        Node nodeA = getNeo().createNode();
        Node nodeB = getNeo().createNode();
        Relationship relA = nodeA.createRelationshipTo( nodeB, MyRelTypes.TEST );
        nodeA.setProperty( arrayKey, array );
        relA.setProperty( arrayKey, array );
        NodeManager nodeManager = ((EmbeddedNeo) getNeo()).getConfig()
            .getNeoModule().getNodeManager();
        nodeManager.clearCache();
        assertTrue( nodeA.getProperty( arrayKey ) != null );
        assertTrue( relA.getProperty( arrayKey ) != null );
        relA.delete();
        nodeA.delete();
        nodeB.delete();

    }
    
    public void testAddCacheCleared()
    {
        Node nodeA = getNeo().createNode();
        nodeA.setProperty( "1", 1 );
        Node nodeB = getNeo().createNode();
        Relationship rel = nodeA.createRelationshipTo( nodeB, MyRelTypes.TEST );
        rel.setProperty( "1", 1 );
        getTransaction().success();
        getTransaction().finish();
        newTransaction();
        NodeManager nodeManager = ((EmbeddedNeo) 
            getNeo()).getConfig().getNeoModule().getNodeManager();
        nodeManager.clearCache();
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
        nodeManager.clearCache();
        // trigger empty load
        getNeo().getNodeById( nodeA.getId() );
        getNeo().getRelationshipById( rel.getId() );
        // apply COW maps
        getTransaction().success();
        getTransaction().finish();
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
    
    public void testTxCacheLoadIsolation()
    {
        Node node = getNeo().createNode();
        node.setProperty( "someproptest", "testing" );
        Node node1 = getNeo().createNode();
        node1.setProperty( "someotherproptest", 2 );
        commit();
        EmbeddedNeo eNeo = (EmbeddedNeo) getNeo();
        TransactionManager txManager = 
            eNeo.getConfig().getTxModule().getTxManager();
        NodeManager nodeManager = 
            eNeo.getConfig().getNeoModule().getNodeManager();
        try
        {
            txManager.begin();
            node.setProperty( "someotherproptest", "testing2" );
            Relationship rel = node.createRelationshipTo( node1, 
                MyRelTypes.TEST );
            javax.transaction.Transaction txA = txManager.suspend();
            txManager.begin();
            assertEquals( "testing", node.getProperty( "someproptest" ) );
            assertTrue( !node.hasProperty( "someotherproptest" ) );
            assertTrue( !node.hasRelationship() );
            nodeManager.clearCache();
            assertEquals( "testing", node.getProperty( "someproptest" ) );
            assertTrue( !node.hasProperty( "someotherproptest" ) );
            javax.transaction.Transaction txB = txManager.suspend();
            txManager.resume( txA );
            assertEquals( "testing", node.getProperty( "someproptest" ) );
            assertTrue( node.hasProperty( "someotherproptest" ) );
            assertTrue( node.hasRelationship() );
            nodeManager.clearCache();
            assertEquals( "testing", node.getProperty( "someproptest" ) );
            assertTrue( node.hasProperty( "someotherproptest" ) );
            assertTrue( node.hasRelationship() );
            txManager.suspend();
            txManager.resume( txB );
            assertEquals( "testing", node.getProperty( "someproptest" ) );
            assertTrue( !node.hasProperty( "someotherproptest" ) );
            assertTrue( !node.hasRelationship() );
            txManager.rollback();
            txManager.resume( txA );
            node.delete();
            node1.delete();
            rel.delete();
            txManager.commit();
            newTransaction();
        }
        catch ( Exception e )
        {
            fail( "" + e );
        }
    }
}