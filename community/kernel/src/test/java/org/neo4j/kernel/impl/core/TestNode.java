/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Test;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.helpers.Exceptions.launderedException;

public class TestNode extends AbstractNeo4jTestCase
{
    @Test
    public void testNodeCreateAndDelete()
    {
        Node node = getGraphDb().createNode();
        long nodeId = node.getId();
        getGraphDb().getNodeById( nodeId );
        node.delete();
        Transaction tx = getTransaction();
        tx.success();
        //noinspection deprecation
        tx.finish();
        setTransaction( getGraphDb().beginTx() );
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
        Logger log = Logger
            .getLogger( "org.neo4j.kernel.impl.core.NeoConstraintsListener" );
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

        getTransaction().failure();
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
        getTransaction().failure();
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

    @Test
    public void testAddPropertyThenDelete()
    {
        Node node = getGraphDb().createNode();
        node.setProperty( "test", "test" );
        Transaction tx = getTransaction();
        tx.success();
        //noinspection deprecation
        tx.finish();
        tx = getGraphDb().beginTx();
        node.setProperty( "test2", "test2" );
        node.delete();
        tx.success();
        //noinspection deprecation
        tx.finish();
        setTransaction( getGraphDb().beginTx() );
    }
    
    @Test
    public void testChangeProperty()
    {
        Node node = getGraphDb().createNode();
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
    
    @Test
    public void testChangeProperty2()
    {
        Node node = getGraphDb().createNode();
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
    
    @Test
    public void testNodeLockingProblem() throws InterruptedException
    {
        testLockProblem( getGraphDb().createNode() );
    }

    @Test
    public void testRelationshipLockingProblem() throws InterruptedException
    {
        Node node = getGraphDb().createNode();
        Node node2 = getGraphDb().createNode();
        testLockProblem( node.createRelationshipTo( node2,
                DynamicRelationshipType.withName( "lock-rel" ) ) );
    }
    
    private void testLockProblem( final PropertyContainer entity ) throws InterruptedException
    {
        entity.setProperty( "key", "value" );
        final AtomicBoolean gotTheLock = new AtomicBoolean();
        Thread thread = new Thread()
        {
            @Override
            public void run()
            {
                try( Transaction tx = getGraphDb().beginTx() )
                {
                    tx.acquireWriteLock( entity );
                    gotTheLock.set( true );
                    tx.success();
                }
                catch ( Exception e )
                {
                    e.printStackTrace();
                    throw launderedException( e );
                }
            }
        };
        thread.start();
        long endTime = System.currentTimeMillis() + 5000;
        WAIT: while ( thread.getState() != State.TERMINATED )
        {
            if ( thread.getState() == Thread.State.WAITING )
            {
                for ( StackTraceElement el : thread.getStackTrace() )
                {
                    // if we are in WAITING state in acquireWriteLock we know that we are waiting for the lock
                    if ( el.getClassName().equals( "org.neo4j.kernel.impl.transaction.RWLock" ) )
                        if ( el.getMethodName().equals( "acquireWriteLock" ) ) break WAIT;
                }
            }
            Thread.sleep( 1 );
            if ( System.currentTimeMillis() > endTime ) break;
        }
        boolean gotLock = gotTheLock.get();
        newTransaction();
        assertFalse( gotLock );
        thread.join();
    }
}
