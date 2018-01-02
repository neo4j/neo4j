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

import java.lang.Thread.State;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.GraphTransactionRule;
import org.neo4j.test.ImpermanentDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class NodeTest
{
    @ClassRule
    public static DatabaseRule db = new ImpermanentDatabaseRule();

    @Rule
    public GraphTransactionRule tx = new GraphTransactionRule( db );

    @Test
    public void givenNodeWithRelationshipWhenDeleteNodeThenThrowExceptionOnCommit() throws Exception
    {
        // Given
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        node1.createRelationshipTo( node2, DynamicRelationshipType.withName( "KNOWS" ) );

        tx.success();

        // When
        tx.begin();
        node1.delete();

        try
        {
            tx.success();

        // Then
            Assert.fail();
        }
        catch ( TransactionFailureException e )
        {
            Assert.assertNotEquals( e.getCause().getMessage().indexOf( "still has relationships" ), -1 );
        }
    }

    @Test
    public void testNodeCreateAndDelete()
    {
        Node node = getGraphDb().createNode();
        long nodeId = node.getId();
        getGraphDb().getNodeById( nodeId );
        node.delete();

        tx.success();
        tx.begin();
        try
        {
            getGraphDb().getNodeById( nodeId );
            fail( "Node[" + nodeId + "] should be deleted." );
        }
        catch ( NotFoundException e )
        {
        }
    }

    @Test
    public void testDeletedNode()
    {
        // do some evil stuff
        Node node = getGraphDb().createNode();
        node.delete();
        try
        {
            node.setProperty( "key1", new Integer( 1 ) );
            fail( "Adding stuff to deleted node should throw exception" );
        }
        catch ( Exception e )
        { // good
        }
    }

    @Test
    public void testNodeAddProperty()
    {
        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        try
        {
            node1.setProperty( null, null );
            fail( "Null argument should result in exception." );
        }
        catch ( IllegalArgumentException e )
        {
        }
        String key1 = "key1";
        String key2 = "key2";
        String key3 = "key3";
        Integer int1 = new Integer( 1 );
        Integer int2 = new Integer( 2 );
        String string1 = new String( "1" );
        String string2 = new String( "2" );

        // add property
        node1.setProperty( key1, int1 );
        node2.setProperty( key1, string1 );
        node1.setProperty( key2, string2 );
        node2.setProperty( key2, int2 );
        assertTrue( node1.hasProperty( key1 ) );
        assertTrue( node2.hasProperty( key1 ) );
        assertTrue( node1.hasProperty( key2 ) );
        assertTrue( node2.hasProperty( key2 ) );
        assertTrue( !node1.hasProperty( key3 ) );
        assertTrue( !node2.hasProperty( key3 ) );
        assertEquals( int1, node1.getProperty( key1 ) );
        assertEquals( string1, node2.getProperty( key1 ) );
        assertEquals( string2, node1.getProperty( key2 ) );
        assertEquals( int2, node2.getProperty( key2 ) );

//        getTransaction().failure();
    }

    @Test
    public void testNodeRemoveProperty()
    {
        String key1 = "key1";
        String key2 = "key2";
        Integer int1 = new Integer( 1 );
        Integer int2 = new Integer( 2 );
        String string1 = new String( "1" );
        String string2 = new String( "2" );

        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();

        try
        {
            if ( node1.removeProperty( key1 ) != null )
            {
                fail( "Remove of non existing property should return null" );
            }
        }
        catch ( NotFoundException e )
        {
        }
        try
        {
            node1.removeProperty( null );
            fail( "Remove null property should throw exception." );
        }
        catch ( IllegalArgumentException e )
        {
        }

        node1.setProperty( key1, int1 );
        node2.setProperty( key1, string1 );
        node1.setProperty( key2, string2 );
        node2.setProperty( key2, int2 );
        try
        {
            node1.removeProperty( null );
            fail( "Null argument should result in exception." );
        }
        catch ( IllegalArgumentException e )
        {
        }

        // test remove property
        assertEquals( int1, node1.removeProperty( key1 ) );
        assertEquals( string1, node2.removeProperty( key1 ) );
        // test remove of non existing property
        try
        {
            if ( node2.removeProperty( key1 ) != null )
            {
                fail( "Remove of non existing property return null." );
            }
        }
        catch ( NotFoundException e )
        {
            // must mark as rollback only
        }
 //       getTransaction().failure();
    }

    @Test
    public void testNodeChangeProperty()
    {
        String key1 = "key1";
        String key2 = "key2";
        String key3 = "key3";
        Integer int1 = new Integer( 1 );
        Integer int2 = new Integer( 2 );
        String string1 = new String( "1" );
        String string2 = new String( "2" );
        Boolean bool1 = new Boolean( true );
        Boolean bool2 = new Boolean( false );

        Node node1 = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        node1.setProperty( key1, int1 );
        node2.setProperty( key1, string1 );
        node1.setProperty( key2, string2 );
        node2.setProperty( key2, int2 );

        try
        {
            node1.setProperty( null, null );
            fail( "Null argument should result in exception." );
        }
        catch ( IllegalArgumentException e )
        {
        }
        catch ( NotFoundException e )
        {
            fail( "wrong exception" );
        }

        // test change property
        node1.setProperty( key1, int2 );
        node2.setProperty( key1, string2 );
        assertEquals( string2, node2.getProperty( key1 ) );
        node1.setProperty( key3, bool1 );
        node1.setProperty( key3, bool2 );
        assertEquals( string2, node2.getProperty( key1 ) );
        node1.delete();
        node2.delete();
    }

    @Test
    public void testNodeChangeProperty2()
    {
        String key1 = "key1";
        Integer int1 = new Integer( 1 );
        Integer int2 = new Integer( 2 );
        String string1 = new String( "1" );
        String string2 = new String( "2" );
        Boolean bool1 = new Boolean( true );
        Boolean bool2 = new Boolean( false );
        Node node1 = getGraphDb().createNode();
        node1.setProperty( key1, int1 );
        node1.setProperty( key1, int2 );
        assertEquals( int2, node1.getProperty( key1 ) );
        node1.removeProperty( key1 );
        node1.setProperty( key1, string1 );
        node1.setProperty( key1, string2 );
        assertEquals( string2, node1.getProperty( key1 ) );
        node1.removeProperty( key1 );
        node1.setProperty( key1, bool1 );
        node1.setProperty( key1, bool2 );
        assertEquals( bool2, node1.getProperty( key1 ) );
        node1.removeProperty( key1 );
        node1.delete();
    }

    @Test
    public void testNodeGetProperties()
    {
        String key1 = "key1";
        String key2 = "key2";
        String key3 = "key3";
        Integer int1 = new Integer( 1 );
        Integer int2 = new Integer( 2 );
        String string = new String( "3" );

        Node node1 = getGraphDb().createNode();
        try
        {
            node1.getProperty( key1 );
            fail( "get non existing property din't throw exception" );
        }
        catch ( NotFoundException e )
        {
        }
        try
        {
            node1.getProperty( null );
            fail( "get of null key din't throw exception" );
        }
        catch ( IllegalArgumentException e )
        {
        }
        assertTrue( !node1.hasProperty( key1 ) );
        assertTrue( !node1.hasProperty( null ) );
        node1.setProperty( key1, int1 );
        node1.setProperty( key2, int2 );
        node1.setProperty( key3, string );
        Iterator<String> keys = node1.getPropertyKeys().iterator();
        keys.next();
        keys.next();
        keys.next();
        Map<String, Object> properties = node1.getAllProperties();
        assertTrue( properties.get( key1 ).equals( int1 ) );
        assertTrue( properties.get( key2 ).equals( int2 ) );
        assertTrue( properties.get( key3 ).equals( string ) );
        properties = node1.getProperties( key1, key2 );
        assertTrue( properties.get( key1 ).equals( int1 ) );
        assertTrue( properties.get( key2 ).equals( int2 ) );
        assertFalse( properties.containsKey( key3 ) );

        properties = node1.getProperties();
        assertTrue( properties.isEmpty() );

        try
        {
            String[] names = null;
            node1.getProperties( names );
            fail();
        }
        catch ( NullPointerException e )
        {
            // Ok
        }

        try
        {
            String[] names = new String[]{null};
            node1.getProperties( names );
            fail();
        }
        catch ( NullPointerException e )
        {
            // Ok
        }

        try
        {
            node1.removeProperty( key3 );
        }
        catch ( NotFoundException e )
        {
            fail( "Remove of property failed." );
        }
        assertTrue( !node1.hasProperty( key3 ) );
        assertTrue( !node1.hasProperty( null ) );
        node1.delete();
    }

    @Test
    public void testAddPropertyThenDelete()
    {
        Node node = getGraphDb().createNode();
        node.setProperty( "test", "test" );

        tx.success();
        tx.begin();
        node.setProperty( "test2", "test2" );
        node.delete();

        tx.success();
    }
    
    @Test
    public void testChangeProperty()
    {
        Node node = getGraphDb().createNode();
        node.setProperty( "test", "test1" );
        tx.success();
        tx.begin();
        node.setProperty( "test", "test2" );
        node.removeProperty( "test" );
        node.setProperty( "test", "test3" );
        assertEquals( "test3", node.getProperty( "test" ) );
        node.removeProperty( "test" );
        node.setProperty( "test", "test4" );
        tx.success();
        tx.begin();
        assertEquals( "test4", node.getProperty( "test" ) );
    }
    
    @Test
    public void testChangeProperty2()
    {
        Node node = getGraphDb().createNode();
        node.setProperty( "test", "test1" );
        tx.success();
        tx.begin();
        node.removeProperty( "test" );
        node.setProperty( "test", "test3" );
        assertEquals( "test3", node.getProperty( "test" ) );
        tx.success();
        tx.begin();
        assertEquals( "test3", node.getProperty( "test" ) );
        node.removeProperty( "test" );
        node.setProperty( "test", "test4" );
        tx.success();
        tx.begin();
        assertEquals( "test4", node.getProperty( "test" ) );
    }
    
    private GraphDatabaseService getGraphDb()
    {
        return db.getGraphDatabaseService();
    }
}
