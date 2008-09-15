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

import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.NotFoundException;
import org.neo4j.api.core.Transaction;
import org.neo4j.impl.AbstractNeoTestCase;

public class TestNode extends AbstractNeoTestCase
{
    public TestNode( String testName )
    {
        super( testName );
    }

    public void testNodeCreateAndDelete()
    {
        int nodeId = -1;
        Node node = getNeo().createNode();
        nodeId = (int) node.getId();
        getNeo().getNodeById( nodeId );
        node.delete();
        Transaction tx = getTransaction();
        tx.success();
        tx.finish();
        setTransaction( getNeo().beginTx() );
        try
        {
            getNeo().getNodeById( nodeId );
            fail( "Node[" + nodeId + "] should be deleted." );
        }
        catch ( NotFoundException e )
        {
        }
    }

    public void testDeletedNode()
    {
        // do some evil stuff
        Node node = getNeo().createNode();
        node.delete();
        Logger log = Logger
            .getLogger( "org.neo4j.impl.core.NeoConstraintsListener" );
        Level level = log.getLevel();
        log.setLevel( Level.OFF );
        try
        {
            node.setProperty( "key1", new Integer( 1 ) );
            fail( "Adding stuff to deleted node should throw exception" );
        }
        catch ( Exception e )
        { // good
        }
        log.setLevel( level );
    }

    public void testNodeAddProperty()
    {
        Node node1 = getNeo().createNode();
        Node node2 = getNeo().createNode();
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

        getTransaction().failure();
    }

    public void testNodeRemoveProperty()
    {
        String key1 = "key1";
        String key2 = "key2";
        Integer int1 = new Integer( 1 );
        Integer int2 = new Integer( 2 );
        String string1 = new String( "1" );
        String string2 = new String( "2" );

        Node node1 = getNeo().createNode();
        Node node2 = getNeo().createNode();

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
        // test remove of non exsisting property
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
        getTransaction().failure();
    }

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

        Node node1 = getNeo().createNode();
        Node node2 = getNeo().createNode();
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

    public void testNodeChangeProperty2()
    {
        String key1 = "key1";
        Integer int1 = new Integer( 1 );
        Integer int2 = new Integer( 2 );
        String string1 = new String( "1" );
        String string2 = new String( "2" );
        Boolean bool1 = new Boolean( true );
        Boolean bool2 = new Boolean( false );
        Node node1 = getNeo().createNode();
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

    public void testNodeGetProperties()
    {
        String key1 = "key1";
        String key2 = "key2";
        String key3 = "key3";
        Integer int1 = new Integer( 1 );
        Integer int2 = new Integer( 2 );
        String string = new String( "3" );

        Node node1 = getNeo().createNode();
        assertTrue( !node1.getPropertyValues().iterator().hasNext() );
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
        Iterator<Object> values = node1.getPropertyValues().iterator();
        values.next();
        values.next();
        values.next();
        Iterator<String> keys = node1.getPropertyKeys().iterator();
        keys.next();
        keys.next();
        keys.next();
        assertTrue( node1.hasProperty( key1 ) );
        assertTrue( node1.hasProperty( key2 ) );
        assertTrue( node1.hasProperty( key3 ) );
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

    public void testAddPropertyThenDelete()
    {
        Node node = getNeo().createNode();
        node.setProperty( "test", "test" );
        Transaction tx = getTransaction();
        tx.success();
        tx.finish();
        tx = getNeo().beginTx();
        node.setProperty( "test2", "test2" );
        node.delete();
        tx.success();
        tx.finish();
        setTransaction( getNeo().beginTx() );
    }
    
    public void testChangeProperty()
    {
        Node node = getNeo().createNode();
        node.setProperty( "test", "test1" );
        newTransaction();
        node.setProperty( "test", "test2" );
        node.removeProperty( "test" );
        node.setProperty( "test", "test3" );
        assertEquals( "test3", node.getProperty( "test" ) );
        node.removeProperty( "test" );
        node.setProperty( "test", "test4" );
        newTransaction();
        assertEquals( "test4", node.getProperty( "test" ) );
    }
    
    public void testChangeProperty2()
    {
        Node node = getNeo().createNode();
        node.setProperty( "test", "test1" );
        newTransaction();
        node.removeProperty( "test" );
        node.setProperty( "test", "test3" );
        assertEquals( "test3", node.getProperty( "test" ) );
        newTransaction();
        assertEquals( "test3", node.getProperty( "test" ) );
        node.removeProperty( "test" );
        node.setProperty( "test", "test4" );
        newTransaction();
        assertEquals( "test4", node.getProperty( "test" ) );
    }
}