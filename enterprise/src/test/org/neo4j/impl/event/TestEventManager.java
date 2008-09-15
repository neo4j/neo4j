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
package org.neo4j.impl.event;

import java.util.ArrayList;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.neo4j.impl.event.Event;
import org.neo4j.impl.event.EventData;
import org.neo4j.impl.event.EventListenerAlreadyRegisteredException;
import org.neo4j.impl.event.EventListenerNotRegisteredException;
import org.neo4j.impl.event.EventManager;
import org.neo4j.impl.event.EventModule;
import org.neo4j.impl.event.ProActiveEventListener;
import org.neo4j.impl.event.ReActiveEventListener;

public class TestEventManager extends TestCase
{
    private EventManager evtMgr;
    private EventModule module = new EventModule();

    public void setUp()
    {
        module.init();
        module.start();
        evtMgr = module.getEventManager();
    }

    public void tearDown()
    {
        module.stop();
        module.destroy();
    }

    private class MyProActiveEventListener implements ProActiveEventListener
    {
        private boolean state = true;

        public boolean proActiveEventReceived( Event event, EventData data )
        {
            return state;
        }

        void setState( boolean mode )
        {
            state = mode;
        }
    }

    private class MyReActiveEventListener implements ReActiveEventListener
    {
        private boolean eventReceived = false;
        private Thread originatingThread = null;

        public void reActiveEventReceived( Event event, EventData data )
        {
            this.eventReceived = true;
            this.originatingThread = data.getOriginatingThread();
        }

        void reset()
        {
            this.eventReceived = false;
            this.originatingThread = null;
        }

        boolean isEventReceived()
        {
            return this.eventReceived;
        }

        Thread getOriginatingThread()
        {
            return this.originatingThread;
        }
    }

    public TestEventManager( String testName )
    {
        super( testName );
    }

    public static Test suite()
    {
        return new TestSuite( TestEventManager.class );
    }

    public void testProActiveEventListener()
        throws EventListenerAlreadyRegisteredException,
        EventListenerNotRegisteredException
    {
        // register test
        try
        {
            evtMgr.registerProActiveEventListener( null, Event.TEST_EVENT );
            fail( "null listener should throw exception." );
        }
        catch ( EventListenerNotRegisteredException e )
        {
        }
        MyProActiveEventListener myEvtListener1 = new MyProActiveEventListener();
        try
        {
            evtMgr.registerProActiveEventListener( myEvtListener1, null );
            fail( "null event should throw exception." );
        }
        catch ( EventListenerNotRegisteredException e )
        {
        }
        evtMgr
            .registerProActiveEventListener( myEvtListener1, Event.TEST_EVENT );
        try
        {
            evtMgr.registerProActiveEventListener( myEvtListener1,
                Event.TEST_EVENT );
            fail( "registration of same listener, should throw exception. " );
        }
        catch ( EventListenerAlreadyRegisteredException e )
        {
        }

        // unregister test
        MyProActiveEventListener myEvtListener2 = new MyProActiveEventListener();
        MyProActiveEventListener myEvtListener3 = new MyProActiveEventListener();
        evtMgr
            .registerProActiveEventListener( myEvtListener2, Event.TEST_EVENT );
        evtMgr
            .registerProActiveEventListener( myEvtListener3, Event.TEST_EVENT );
        evtMgr.unregisterProActiveEventListener( myEvtListener3,
            Event.TEST_EVENT );
        try
        {
            evtMgr.unregisterProActiveEventListener( myEvtListener3,
                Event.TEST_EVENT );
            fail( "unregister of non registered listener "
                + "should throw exception" );
        }
        catch ( EventListenerNotRegisteredException e )
        {
        }
        evtMgr
            .registerProActiveEventListener( myEvtListener3, Event.TEST_EVENT );

        // send test
        assertTrue( evtMgr.generateProActiveEvent( Event.TEST_EVENT,
            new EventData( null ) ) );
        myEvtListener3.setState( false );
        assertTrue( !evtMgr.generateProActiveEvent( Event.TEST_EVENT,
            new EventData( null ) ) );
        myEvtListener2.setState( false );
        myEvtListener3.setState( true );
        assertTrue( !evtMgr.generateProActiveEvent( Event.TEST_EVENT,
            new EventData( null ) ) );
        myEvtListener2.setState( true );
        assertTrue( evtMgr.generateProActiveEvent( Event.TEST_EVENT,
            new EventData( null ) ) );
        evtMgr.unregisterProActiveEventListener( myEvtListener1,
            Event.TEST_EVENT );
        evtMgr.unregisterProActiveEventListener( myEvtListener2,
            Event.TEST_EVENT );
        evtMgr.unregisterProActiveEventListener( myEvtListener3,
            Event.TEST_EVENT );
        assertTrue( evtMgr.generateProActiveEvent( Event.TEST_EVENT,
            new EventData( null ) ) );
    }

    public void testReActiveEventListener()
        throws EventListenerAlreadyRegisteredException,
        EventListenerNotRegisteredException
    {
        // register test
        try
        {
            evtMgr.registerReActiveEventListener( null, Event.TEST_EVENT );
            fail( "null listener should throw exception." );
        }
        catch ( EventListenerNotRegisteredException e )
        {
        }
        MyReActiveEventListener myEvtListener1 = new MyReActiveEventListener();
        try
        {
            evtMgr.registerReActiveEventListener( myEvtListener1, null );
            fail( "null event should throw exception." );
        }
        catch ( EventListenerNotRegisteredException e )
        {
        }
        evtMgr.registerReActiveEventListener( myEvtListener1, Event.TEST_EVENT );
        try
        {
            evtMgr.registerReActiveEventListener( myEvtListener1,
                Event.TEST_EVENT );
            fail( "registration of same listener, should throw exception. " );
        }
        catch ( EventListenerAlreadyRegisteredException e )
        {
        }

        // unregister test
        MyReActiveEventListener myEvtListener2 = new MyReActiveEventListener();
        MyReActiveEventListener myEvtListener3 = new MyReActiveEventListener();
        evtMgr.registerReActiveEventListener( myEvtListener2, Event.TEST_EVENT );
        evtMgr.registerReActiveEventListener( myEvtListener3, Event.TEST_EVENT );
        evtMgr.unregisterReActiveEventListener( myEvtListener3,
            Event.TEST_EVENT );
        try
        {
            evtMgr.unregisterReActiveEventListener( myEvtListener3,
                Event.TEST_EVENT );
            fail( "unregister of non registered listener "
                + "should throw exception" );
        }
        catch ( EventListenerNotRegisteredException e )
        {
        }
        evtMgr.registerReActiveEventListener( myEvtListener3, Event.TEST_EVENT );

        // send test
        evtMgr.generateReActiveEvent( Event.TEST_EVENT, new EventData( null ) );
        waitForReceived( new MyReActiveEventListener[] { myEvtListener1,
            myEvtListener2, myEvtListener3 } );
        evtMgr.unregisterReActiveEventListener( myEvtListener1,
            Event.TEST_EVENT );
        evtMgr.unregisterReActiveEventListener( myEvtListener2,
            Event.TEST_EVENT );
        evtMgr.unregisterReActiveEventListener( myEvtListener3,
            Event.TEST_EVENT );
    }

    public void testStartStopEventManager()
    {
        module.stop();
        module.start();
    }

    private void waitForReceived( MyReActiveEventListener[] listeners )
    {
        ArrayList<MyReActiveEventListener> listenerList = new ArrayList<MyReActiveEventListener>();
        for ( int i = 0; i < listeners.length; i++ )
        {
            listenerList.add( listeners[i] );
        }
        int iterations = 0;
        while ( listenerList.size() > 0 && iterations < 10 )
        {
            for ( int i = 0; i < listenerList.size(); i++ )
            {
                MyReActiveEventListener myListener = listenerList.get( i );
                if ( myListener.isEventReceived() )
                {
                    listenerList.remove( i );
                    if ( listenerList.size() == 0 )
                    {
                        return;
                    }
                }
            }

            try
            {
                Thread.sleep( 100 );
            }
            catch ( InterruptedException ie )
            {
            }
            iterations++;
        }
        fail( "Reactive events not received." );
    }

    public void testEventData()
    {
        Object data = new Object();
        EventData eventData = new EventData( data );
        assertTrue( eventData.getData() == data );
    }

    public void testReActiveEventOriginatingThread()
    {
        try
        {
            MyReActiveEventListener listener = new MyReActiveEventListener();
            evtMgr.registerReActiveEventListener( listener, Event.TEST_EVENT );
            evtMgr.generateReActiveEvent( Event.TEST_EVENT,
                new EventData( null ) );
            waitForReceived( new MyReActiveEventListener[] { listener } );
            assertEquals( "Wrong originating thread for event", Thread
                .currentThread(), listener.getOriginatingThread() );
        }
        catch ( Exception e )
        {
            fail( "Unknown exception: " + e );
        }
    }
}