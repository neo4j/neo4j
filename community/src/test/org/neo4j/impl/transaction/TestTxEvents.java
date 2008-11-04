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
package org.neo4j.impl.transaction;

import java.util.logging.Level;
import java.util.logging.Logger;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.neo4j.api.core.Node;
import org.neo4j.impl.AbstractNeoTestCase;
import org.neo4j.impl.MyRelTypes;
import org.neo4j.impl.event.Event;
import org.neo4j.impl.event.EventData;
import org.neo4j.impl.event.EventManager;
import org.neo4j.impl.event.ReActiveEventListener;

public class TestTxEvents extends AbstractNeoTestCase
{
    private EventManager eventManager;
    private TxManager txManager;

    public TestTxEvents( String name )
    {
        super( name );
    }

    public static Test suite()
    {
        return new TestSuite( TestTxEvents.class );
    }

    public void setUp()
    {
        super.setUp();
        getTransaction().finish();
        eventManager = getEmbeddedNeo().getConfig().getEventModule()
            .getEventManager();
        txManager = (TxManager) getEmbeddedNeo().getConfig().getTxModule()
            .getTxManager();
    }

    // Small inner class that listens to a specific event (specified in the
    // constructor) and turns on a flag if it's been received
    private static class EventConsumer implements ReActiveEventListener
    {
        private boolean received = false;
        private Event eventType = null;
        private Integer eventIdentifier = null;

        EventConsumer( Event eventType )
        {
            this.eventType = eventType;
        }

        public void reActiveEventReceived( Event event, EventData data )
        {
            if ( event == this.eventType )
            {
                this.received = true;
                this.eventIdentifier = (Integer) data.getData();
            }
        }

        boolean received()
        {
            return this.received;
        }

        Integer getEventIdentifier()
        {
            return this.eventIdentifier;
        }
    }

    private int getEventIdentifier()
    {
        return ((TransactionImpl) txManager.getTransaction())
            .getEventIdentifier();
    }

    private Integer performOperation( Event operation )
    {
        Integer eventIdentifier = null;
        try
        {
            if ( operation == Event.TX_BEGIN )
            {
                txManager.begin();
                eventIdentifier = getEventIdentifier();
            }
            else if ( operation == Event.TX_ROLLBACK )
            {
                txManager.begin();
                eventIdentifier = getEventIdentifier();
                txManager.rollback();
            }
            else if ( operation == Event.TX_COMMIT )
            {
                txManager.begin();
                eventIdentifier = getEventIdentifier();
                txManager.commit();
            }
            else
            {
                fail( "Unknown operation to test: " + operation );
            }
        }
        catch ( Exception e )
        {
            fail( "Failed executing operation for: " + operation );
        }
        return eventIdentifier;
    }

    // operation = { TX_BEGIN, TX_ROLLBACK, TX_COMMIT }
    private void testTxOperation( Event operation )
    {
        EventConsumer eventHook = new EventConsumer( operation );
        try
        {
            eventManager.registerReActiveEventListener( eventHook, operation );
            Integer eventIdentifier = performOperation( operation );
            Thread.sleep( 500 ); // should be enough to propagate the event
            assertTrue( operation + " event not generated", eventHook
                .received() );
            assertEquals( "Event generated, but with wrong event identifier",
                eventHook.getEventIdentifier(), eventIdentifier );
        }
        catch ( Exception e )
        {
            fail( "Exception raised during testing of TX_BEGIN event: " + e );
        }
        finally
        {
            try
            {
                eventManager.unregisterReActiveEventListener( eventHook,
                    operation );
                txManager.rollback();
            }
            catch ( Exception e )
            {
            } // Just ignore
        }
    }

    public void testTxBegin()
    {
        this.testTxOperation( Event.TX_BEGIN );
    }

    public void testTxRollback()
    {
        this.testTxOperation( Event.TX_ROLLBACK );
    }

    public void testTxCommit()
    {
        this.testTxOperation( Event.TX_COMMIT );
    }

    public void testSelfMarkedRollback()
    {
        EventConsumer eventHook = new EventConsumer( Event.TX_ROLLBACK );
        try
        {
            eventManager.registerReActiveEventListener( eventHook,
                Event.TX_ROLLBACK );
            txManager.begin();
            Integer eventIdentifier = getEventIdentifier();
            Node node = getNeo().createNode();
            txManager.setRollbackOnly();
            try
            {
                txManager.commit();
                fail( "Marked rollback tx should throw exception on commit" );
            }
            catch ( javax.transaction.RollbackException e )
            { // good
            }
            Thread.sleep( 500 ); // should be enough to propagate the event
            assertTrue( Event.TX_ROLLBACK + " event not generated", eventHook
                .received() );
            assertEquals( "Event generated, but with wrong event identifier",
                eventHook.getEventIdentifier(), eventIdentifier );
            txManager.begin();
            try
            {
                getNeo().getNodeById( (int) node.getId() );
                fail( "Node exist but tx should have rolled back" );
            }
            catch ( org.neo4j.api.core.NotFoundException e )
            { // good
            }
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            fail( "Exception raised during testing of TX_BEGIN event: " + e );
        }
        finally
        {
            try
            {
                eventManager.unregisterReActiveEventListener( eventHook,
                    Event.TX_ROLLBACK );
                txManager.rollback();
            }
            catch ( Exception e )
            {
            } // Just ignore
        }
    }

    public void testMarkedRollback()
    {
        Logger log = Logger
            .getLogger( "org.neo4j.impl.core.NeoConstraintsListener" );
        Level level = log.getLevel();
        log.setLevel( Level.OFF );
        EventConsumer eventHook = new EventConsumer( Event.TX_ROLLBACK );
        try
        {
            eventManager.registerReActiveEventListener( eventHook,
                Event.TX_ROLLBACK );
            txManager.begin();
            Integer eventIdentifier = getEventIdentifier();
            Node node1 = getNeo().createNode();
            Node node2 = getNeo().createNode();
            node1.createRelationshipTo( node2, MyRelTypes.TEST );
            node1.delete();
            try
            {
                txManager.commit();
                fail( "tx should throw exception on commit" );
            }
            catch ( Exception e )
            { // good
            }
            Thread.sleep( 500 ); // should be enough to propagate the event
            assertTrue( Event.TX_ROLLBACK + " event not generated", eventHook
                .received() );
            assertEquals( "Event generated, but with wrong event identifier",
                eventHook.getEventIdentifier(), eventIdentifier );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            fail( "Exception raised during testing of TX_BEGIN event: " + e );
        }
        finally
        {
            try
            {
                eventManager.unregisterReActiveEventListener( eventHook,
                    Event.TX_ROLLBACK );
                txManager.rollback();
            }
            catch ( Exception e )
            {
            } // Just ignore
            log.setLevel( level );
        }
    }
}