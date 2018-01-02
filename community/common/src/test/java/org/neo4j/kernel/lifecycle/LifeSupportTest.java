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
package org.neo4j.kernel.lifecycle;

import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

/**
 * Test LifeSupport lifecycle transitions
 */
public class LifeSupportTest
{
    @Test
    public void testOkLifecycle()
        throws LifecycleException
    {
        LifeSupport lifeSupport = newLifeSupport();

        LifecycleMock instance1 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance1 );
        LifecycleMock instance2 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance2 );
        LifecycleMock instance3 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance3 );

        lifeSupport.init();
        assertEquals( LifecycleStatus.STOPPED, lifeSupport.getStatus());
        assertEquals( LifecycleStatus.STOPPED, instance1.getStatus());
        assertEquals( LifecycleStatus.STOPPED, instance2.getStatus());
        assertEquals( LifecycleStatus.STOPPED, instance3.getStatus());

        lifeSupport.start();
        assertEquals( LifecycleStatus.STARTED, lifeSupport.getStatus());
        assertEquals( LifecycleStatus.STARTED, instance1.getStatus());
        assertEquals( LifecycleStatus.STARTED, instance2.getStatus() );
        assertEquals( LifecycleStatus.STARTED, instance3.getStatus());

        lifeSupport.stop();
        assertEquals( LifecycleStatus.STOPPED, lifeSupport.getStatus());
        assertEquals( LifecycleStatus.STOPPED, instance1.getStatus());
        assertEquals( LifecycleStatus.STOPPED, instance2.getStatus());
        assertEquals( LifecycleStatus.STOPPED, instance3.getStatus());

        lifeSupport.start();
        assertEquals( LifecycleStatus.STARTED, lifeSupport.getStatus());
        assertEquals( LifecycleStatus.STARTED, instance1.getStatus());
        assertEquals( LifecycleStatus.STARTED, instance2.getStatus());
        assertEquals( LifecycleStatus.STARTED, instance3.getStatus());

        lifeSupport.shutdown();
        assertEquals( LifecycleStatus.SHUTDOWN, lifeSupport.getStatus());
        assertEquals( LifecycleStatus.SHUTDOWN, instance1.getStatus());
        assertEquals( LifecycleStatus.SHUTDOWN, instance2.getStatus());
        assertEquals( LifecycleStatus.SHUTDOWN, instance3.getStatus());
    }

    @Test()
    public void testFailingInit()
    {
        LifeSupport lifeSupport = newLifeSupport();

        LifecycleMock instance1 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance1 );
        Exception initThrowable = new Exception();
        LifecycleMock instance2 = new LifecycleMock( initThrowable, null, null, null );
        lifeSupport.add( instance2 );
        LifecycleMock instance3 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance3 );

        try
        {
            lifeSupport.init();
            fail();
        }
        catch( LifecycleException throwable )
        {
            assertEquals( initThrowable, throwable.getCause() );
        }
        assertEquals( LifecycleStatus.SHUTDOWN, lifeSupport.getStatus());
        assertEquals( LifecycleStatus.SHUTDOWN , instance1.getStatus());
        assertEquals( LifecycleStatus.NONE , instance2.getStatus() );
        assertEquals( LifecycleStatus.NONE , instance3.getStatus() );
    }

    @Test()
    public void testFailingStart()
    {
        LifeSupport lifeSupport = newLifeSupport();

        LifecycleMock instance1 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance1 );
        Exception startThrowable = new Exception();
        LifecycleMock instance2 = new LifecycleMock( null, startThrowable, null, null );
        lifeSupport.add( instance2 );
        LifecycleMock instance3 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance3 );

        try
        {
            lifeSupport.start();
            fail();
        }
        catch( LifecycleException throwable )
        {
            assertEquals( startThrowable, throwable.getCause());
        }
        assertEquals( LifecycleStatus.STOPPED, lifeSupport.getStatus());
        assertEquals( LifecycleStatus.STOPPED , instance1.getStatus());
        assertEquals( LifecycleStatus.STOPPED , instance2.getStatus() );
        assertEquals( LifecycleStatus.STOPPED , instance3.getStatus() );
    }

    @Test()
    public void testFailingStartAndFailingStop()
    {
        LifeSupport lifeSupport = newLifeSupport();

        Exception stopThrowable = new Exception();
        LifecycleMock instance1 = new LifecycleMock( null, null, stopThrowable, null );
        lifeSupport.add( instance1 );
        Exception startThrowable = new Exception();
        LifecycleMock instance2 = new LifecycleMock( null, startThrowable, null, null );
        lifeSupport.add( instance2 );
        LifecycleMock instance3 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance3 );

        try
        {
            lifeSupport.start();
            fail();
        }
        catch( LifecycleException throwable )
        {
            assertEquals( startThrowable, throwable.getCause() );
            assertEquals( 1, throwable.getSuppressed().length );
            assertThat(throwable.getSuppressed()[0], instanceOf(LifecycleException.class));
            assertEquals( stopThrowable, throwable.getSuppressed()[0].getCause() );
        }

        assertEquals( LifecycleStatus.STOPPED, lifeSupport.getStatus() );
        assertEquals( LifecycleStatus.STOPPED , instance1.getStatus());
        assertEquals( LifecycleStatus.STOPPED , instance2.getStatus() );
        assertEquals( LifecycleStatus.STOPPED , instance3.getStatus() );
    }

    @Test()
    public void testFailingStop()
        throws LifecycleException
    {
        LifeSupport lifeSupport = newLifeSupport();

        LifecycleMock instance1 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance1 );
        Exception stopThrowable = new Exception();
        LifecycleMock instance2 = new LifecycleMock( null, null, stopThrowable, null );
        lifeSupport.add( instance2 );
        LifecycleMock instance3 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance3 );

        lifeSupport.start();

        try
        {
            lifeSupport.stop();
            fail();
        }
        catch( LifecycleException throwable )
        {
            assertEquals( stopThrowable, throwable.getCause());
        }
        assertEquals( LifecycleStatus.STOPPED, lifeSupport.getStatus());
        assertEquals( LifecycleStatus.STOPPED , instance1.getStatus());
        assertEquals( LifecycleStatus.STOPPED , instance2.getStatus() );
        assertEquals( LifecycleStatus.STOPPED , instance3.getStatus() );
    }

    @Test()
    public void testFailingShutdown()
        throws LifecycleException
    {
        LifeSupport lifeSupport = newLifeSupport();

        LifecycleMock instance1 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance1 );
        Exception shutdownThrowable = new Exception();
        LifecycleMock instance2 = new LifecycleMock( null, null, null, shutdownThrowable );
        lifeSupport.add( instance2 );
        LifecycleMock instance3 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance3 );

        lifeSupport.start();

        try
        {
            lifeSupport.shutdown();
            fail();
        }
        catch( LifecycleException throwable )
        {
            assertEquals( shutdownThrowable, throwable.getCause());
        }
        assertEquals( LifecycleStatus.SHUTDOWN, lifeSupport.getStatus());
        assertEquals( LifecycleStatus.SHUTDOWN , instance1.getStatus());
        assertEquals( LifecycleStatus.SHUTDOWN , instance2.getStatus() );
        assertEquals( LifecycleStatus.SHUTDOWN , instance3.getStatus() );
    }

    @Test
    public void testRestartOk()
        throws LifecycleException
    {
        LifeSupport lifeSupport = newLifeSupport();

        LifecycleMock instance1 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance1 );
        LifecycleMock instance2 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance2 );
        LifecycleMock instance3 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance3 );

        lifeSupport.init();
        assertEquals( LifecycleStatus.STOPPED, lifeSupport.getStatus());
        assertEquals( LifecycleStatus.STOPPED, instance1.getStatus());
        assertEquals( LifecycleStatus.STOPPED, instance2.getStatus());
        assertEquals( LifecycleStatus.STOPPED, instance3.getStatus());

        lifeSupport.start();
        assertEquals( LifecycleStatus.STARTED, lifeSupport.getStatus());
        assertEquals( LifecycleStatus.STARTED, instance1.getStatus());
        assertEquals( LifecycleStatus.STARTED, instance2.getStatus() );
        assertEquals( LifecycleStatus.STARTED, instance3.getStatus());

        lifeSupport.stop();
        assertEquals( LifecycleStatus.STOPPED, lifeSupport.getStatus());
        assertEquals( LifecycleStatus.STOPPED, instance1.getStatus());
        assertEquals( LifecycleStatus.STOPPED, instance2.getStatus());
        assertEquals( LifecycleStatus.STOPPED, instance3.getStatus());

        lifeSupport.start();
        assertEquals( LifecycleStatus.STARTED, lifeSupport.getStatus());
        assertEquals( LifecycleStatus.STARTED, instance1.getStatus());
        assertEquals( LifecycleStatus.STARTED, instance2.getStatus());
        assertEquals( LifecycleStatus.STARTED, instance3.getStatus());

        lifeSupport.shutdown();
        assertEquals( LifecycleStatus.SHUTDOWN, lifeSupport.getStatus());
        assertEquals( LifecycleStatus.SHUTDOWN, instance1.getStatus());
        assertEquals( LifecycleStatus.SHUTDOWN, instance2.getStatus());
        assertEquals( LifecycleStatus.SHUTDOWN, instance3.getStatus());
    }

    @Test
    public void testAddInstanceWhenInitInitsInstance()
        throws LifecycleException
    {
        LifeSupport support = newLifeSupport();

        support.init();

        LifecycleMock instance1 = new LifecycleMock( null, null, null, null );

        support.add( instance1 );

        assertEquals( LifecycleStatus.STOPPED, instance1.getStatus() );

        assertEquals( LifecycleStatus.NONE, instance1.transitions.get( 0 ) );
        assertEquals( 2, instance1.transitions.size() );
    }

    @Test
    public void testAddInstanceWhenStartedStartsInstance()
        throws LifecycleException
    {
        LifeSupport support = newLifeSupport();

        support.init();
        support.start();

        LifecycleMock instance1 = new LifecycleMock( null, null, null, null );

        support.add( instance1 );

        assertEquals( LifecycleStatus.STARTED, instance1.getStatus() );

        assertEquals( LifecycleStatus.NONE, instance1.transitions.get( 0 ) );
        assertEquals( LifecycleStatus.STOPPED, instance1.transitions.get( 1 ) );

        assertEquals( 3, instance1.transitions.size() );
    }

    @Test
    public void testAddInstanceWhenStoppedInitsInstance()
        throws LifecycleException
    {
        LifeSupport support = newLifeSupport();

        support.init();
        support.start();
        support.stop();

        LifecycleMock instance1 = new LifecycleMock( null, null, null, null );

        support.add( instance1 );

        assertEquals( LifecycleStatus.STOPPED, instance1.getStatus() );

        assertEquals( LifecycleStatus.NONE, instance1.transitions.get( 0 ) );
        assertEquals( LifecycleStatus.STOPPED, instance1.transitions.get( 1 ) );

        assertEquals( 2, instance1.transitions.size() );
    }

    @Test
    public void testAddInstanceWhenShutdownDoesNotAffectInstance()
        throws LifecycleException
    {
        LifeSupport support = newLifeSupport();

        support.init();
        support.start();
        support.stop();
        support.shutdown();

        LifecycleMock instance1 = new LifecycleMock( null, null, null, null );

        support.add( instance1 );

        assertEquals( LifecycleStatus.NONE, instance1.getStatus() );

        assertEquals( 1, instance1.transitions.size() );
    }

    @Test
    public void testInitFailsShutdownWorks() throws Throwable
    {
        //given
        LifeSupport lifeSupport = newLifeSupport();
        Lifecycle lifecycle = mock( Lifecycle.class);
        RuntimeException runtimeException = new RuntimeException();

        //when
        doThrow( runtimeException ).when( lifecycle ).init();
        lifeSupport.add( lifecycle );
        try
        {
            lifeSupport.init();
            fail("Expected exception");
        }
        catch ( LifecycleException e )
        {
            assertEquals( runtimeException, e.getCause() );
            assertEquals( 0, e.getSuppressed().length );
        }

    }

    @Test
    public void testInitFailsShutdownFails() throws Throwable
    {
        LifeSupport lifeSupport = newLifeSupport();
        Lifecycle lifecycle1 = mock( Lifecycle.class);
        Lifecycle lifecycle2 = mock( Lifecycle.class);
        RuntimeException initRuntimeException = new RuntimeException();
        RuntimeException shutdownRuntimeException = new RuntimeException();
        doThrow( initRuntimeException ).when( lifecycle2 ).init();
        doThrow( shutdownRuntimeException ).when( lifecycle1 ).shutdown();
        lifeSupport.add( lifecycle1 );
        lifeSupport.add( lifecycle2 );
        try
        {
            lifeSupport.init();
            fail("Expected exception");
        }
        catch ( LifecycleException e )
        {
            assertEquals( initRuntimeException, e.getCause() );
            assertEquals( 1, e.getSuppressed().length );
            assertEquals( shutdownRuntimeException, e.getSuppressed()[0].getCause() );
            assertThat( e.getSuppressed()[0], instanceOf(LifecycleException.class));
        }

    }

    @Test
    public void testStartFailsStopWorks() throws Throwable
    {
        LifeSupport lifeSupport = newLifeSupport();
        Lifecycle lifecycle = mock( Lifecycle.class);
        RuntimeException runtimeException = new RuntimeException();
        doThrow( runtimeException ).when( lifecycle ).start();
        lifeSupport.add( lifecycle );
        try
        {
            lifeSupport.start();
            fail("Expected exception");
        }
        catch ( LifecycleException e )
        {
            assertEquals( runtimeException, e.getCause() );
            assertEquals( 0, e.getSuppressed().length );
        }

    }

    @Test
    public void testStartFailsStopFails() throws Throwable
    {
        LifeSupport lifeSupport = newLifeSupport();
        Lifecycle lifecycle1 = mock( Lifecycle.class);
        Lifecycle lifecycle2 = mock( Lifecycle.class);
        RuntimeException startRuntimeException = new RuntimeException();
        RuntimeException stopRuntimeException = new RuntimeException();
        doThrow( startRuntimeException ).when( lifecycle2 ).start();
        doThrow( stopRuntimeException ).when( lifecycle1 ).stop();
        lifeSupport.add( lifecycle1 );
        lifeSupport.add( lifecycle2 );
        try
        {
            lifeSupport.start();
            fail("Expected exception");
        }
        catch ( LifecycleException e )
        {
            assertEquals( startRuntimeException, e.getCause() );
            assertEquals( 1, e.getSuppressed().length );
            assertEquals( stopRuntimeException, e.getSuppressed()[0].getCause() );
            assertThat( e.getSuppressed()[0], instanceOf(LifecycleException.class));
        }

    }

    @Test
    public void testStopFailsShutdownWorks() throws Throwable
    {
        LifeSupport lifeSupport = newLifeSupport();
        Lifecycle lifecycle = mock( Lifecycle.class);
        RuntimeException runtimeException = new RuntimeException();
        doThrow( runtimeException ).when( lifecycle ).stop();
        lifeSupport.add( lifecycle );
        lifeSupport.start();
        try
        {
            lifeSupport.stop();
            fail("Expected exception");
        }
        catch ( LifecycleException e )
        {
            assertEquals( runtimeException, e.getCause() );
            assertEquals( 0, e.getSuppressed().length );
        }

    }

    @Test
    public void testStopFailsShutdownFails() throws Throwable
    {
        LifeSupport lifeSupport = newLifeSupport();
        Lifecycle lifecycle1 = mock( Lifecycle.class);
        Lifecycle lifecycle2 = mock( Lifecycle.class);
        RuntimeException stopRuntimeException = new RuntimeException();
        RuntimeException shutdownRuntimeException = new RuntimeException();
        doThrow( stopRuntimeException ).when( lifecycle2 ).stop();
        doThrow( shutdownRuntimeException ).when( lifecycle1 ).shutdown();
        lifeSupport.add( lifecycle1 );
        lifeSupport.add( lifecycle2 );
        lifeSupport.start();
        try
        {
            lifeSupport.shutdown();
            fail("Expected exception");
        }
        catch ( LifecycleException e )
        {
            assertEquals( stopRuntimeException, e.getCause() );
            assertEquals( 1, e.getSuppressed().length );
            assertEquals( shutdownRuntimeException, e.getSuppressed()[0].getCause() );
            assertThat( e.getSuppressed()[0], instanceOf(LifecycleException.class));
        }

    }


    public class LifecycleMock
        implements Lifecycle
    {
        Throwable initThrowable;
        Throwable startThrowable;
        Throwable stopThrowable;
        Throwable shutdownThrowable;

        List<LifecycleStatus> transitions;

        public LifecycleMock( Throwable initThrowable,
                              Throwable startThrowable,
                              Throwable stopThrowable,
                              Throwable shutdownThrowable
        )
        {
            this.initThrowable = initThrowable;
            this.startThrowable = startThrowable;
            this.stopThrowable = stopThrowable;
            this.shutdownThrowable = shutdownThrowable;

            transitions = new LinkedList<LifecycleStatus>();
            transitions.add( LifecycleStatus.NONE );
        }

        @Override
        public void init()
            throws Throwable
        {
            if (initThrowable != null)
                throw initThrowable;

            transitions.add(LifecycleStatus.STOPPED);
        }

        @Override
        public void start()
            throws Throwable
        {
            if (startThrowable != null)
                throw startThrowable;

            transitions.add(LifecycleStatus.STARTED);
        }

        @Override
        public void stop()
            throws Throwable
        {
            transitions.add(LifecycleStatus.STOPPED);

            if (stopThrowable != null)
                throw stopThrowable;
        }

        @Override
        public void shutdown()
            throws Throwable
        {
            transitions.add(LifecycleStatus.SHUTDOWN);

            if (shutdownThrowable != null)
                throw shutdownThrowable;
        }

        public LifecycleStatus getStatus()
        {
            return transitions.get( transitions.size() - 1 );
        }
    }

    private LifeSupport newLifeSupport()
    {
        return new LifeSupport();
    }
}
