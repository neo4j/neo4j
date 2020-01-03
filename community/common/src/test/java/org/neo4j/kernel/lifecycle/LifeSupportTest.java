/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.lifecycle;

import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class LifeSupportTest
{
    @Test
    void testOkLifecycle() throws LifecycleException
    {
        LifeSupport lifeSupport = newLifeSupport();

        LifecycleMock instance1 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance1 );
        LifecycleMock instance2 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance2 );
        LifecycleMock instance3 = new LifecycleMock( null, null, null, null );
        lifeSupport.add( instance3 );

        lifeSupport.init();
        assertEquals( LifecycleStatus.STOPPED, lifeSupport.getStatus() );
        assertEquals( LifecycleStatus.STOPPED, instance1.getStatus() );
        assertEquals( LifecycleStatus.STOPPED, instance2.getStatus() );
        assertEquals( LifecycleStatus.STOPPED, instance3.getStatus() );

        lifeSupport.start();
        assertEquals( LifecycleStatus.STARTED, lifeSupport.getStatus() );
        assertEquals( LifecycleStatus.STARTED, instance1.getStatus() );
        assertEquals( LifecycleStatus.STARTED, instance2.getStatus() );
        assertEquals( LifecycleStatus.STARTED, instance3.getStatus() );

        lifeSupport.stop();
        assertEquals( LifecycleStatus.STOPPED, lifeSupport.getStatus() );
        assertEquals( LifecycleStatus.STOPPED, instance1.getStatus() );
        assertEquals( LifecycleStatus.STOPPED, instance2.getStatus() );
        assertEquals( LifecycleStatus.STOPPED, instance3.getStatus() );

        lifeSupport.start();
        assertEquals( LifecycleStatus.STARTED, lifeSupport.getStatus() );
        assertEquals( LifecycleStatus.STARTED, instance1.getStatus() );
        assertEquals( LifecycleStatus.STARTED, instance2.getStatus() );
        assertEquals( LifecycleStatus.STARTED, instance3.getStatus() );

        lifeSupport.shutdown();
        assertEquals( LifecycleStatus.SHUTDOWN, lifeSupport.getStatus() );
        assertEquals( LifecycleStatus.SHUTDOWN, instance1.getStatus() );
        assertEquals( LifecycleStatus.SHUTDOWN, instance2.getStatus() );
        assertEquals( LifecycleStatus.SHUTDOWN, instance3.getStatus() );
    }

    @Test
    void testFailingInit()
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
            fail( "Failure was expected" );
        }
        catch ( LifecycleException throwable )
        {
            assertEquals( initThrowable, throwable.getCause() );
        }
        assertEquals( LifecycleStatus.SHUTDOWN, lifeSupport.getStatus() );
        assertEquals( LifecycleStatus.SHUTDOWN, instance1.getStatus() );
        assertEquals( LifecycleStatus.SHUTDOWN, instance2.getStatus() );
        assertEquals( LifecycleStatus.NONE, instance3.getStatus() );
    }

    @Test
    void testFailingStart()
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
            fail( "Failure was expected" );
        }
        catch ( LifecycleException throwable )
        {
            assertEquals( startThrowable, throwable.getCause() );
        }
        assertEquals( LifecycleStatus.STOPPED, lifeSupport.getStatus() );
        assertEquals( LifecycleStatus.STOPPED, instance1.getStatus() );
        assertEquals( LifecycleStatus.STOPPED, instance2.getStatus() );
        assertEquals( LifecycleStatus.STOPPED, instance3.getStatus() );
    }

    @Test
    void testFailingStartAndFailingStop()
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
            fail( "Failure was expected" );
        }
        catch ( LifecycleException throwable )
        {
            assertEquals( startThrowable, throwable.getCause() );
            assertEquals( 1, throwable.getSuppressed().length );
            assertThat( throwable.getSuppressed()[0], instanceOf( LifecycleException.class ) );
            assertEquals( stopThrowable, throwable.getSuppressed()[0].getCause() );
        }

        assertEquals( LifecycleStatus.STOPPED, lifeSupport.getStatus() );
        assertEquals( LifecycleStatus.STOPPED, instance1.getStatus() );
        assertEquals( LifecycleStatus.STOPPED, instance2.getStatus() );
        assertEquals( LifecycleStatus.STOPPED, instance3.getStatus() );
    }

    @Test
    void testFailingStop() throws LifecycleException
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
            fail( "Failure was expected" );
        }
        catch ( LifecycleException throwable )
        {
            assertEquals( stopThrowable, throwable.getCause() );
        }
        assertEquals( LifecycleStatus.STOPPED, lifeSupport.getStatus() );
        assertEquals( LifecycleStatus.STOPPED, instance1.getStatus() );
        assertEquals( LifecycleStatus.STOPPED, instance2.getStatus() );
        assertEquals( LifecycleStatus.STOPPED, instance3.getStatus() );
    }

    @Test
    void testFailingShutdown() throws LifecycleException
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
            fail( "Failure was expected" );
        }
        catch ( LifecycleException throwable )
        {
            assertEquals( shutdownThrowable, throwable.getCause() );
        }
        assertEquals( LifecycleStatus.SHUTDOWN, lifeSupport.getStatus() );
        assertEquals( LifecycleStatus.SHUTDOWN, instance1.getStatus() );
        assertEquals( LifecycleStatus.SHUTDOWN, instance2.getStatus() );
        assertEquals( LifecycleStatus.SHUTDOWN, instance3.getStatus() );
    }

    @Test
    void testAddInstanceWhenInitInitsInstance() throws LifecycleException
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
    void testAddInstanceWhenStartedStartsInstance() throws LifecycleException
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
    void testAddInstanceWhenStoppedInitsInstance() throws LifecycleException
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
    void testAddInstanceWhenShutdownDoesNotAffectInstance() throws LifecycleException
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
    void testInitFailsShutdownWorks() throws Throwable
    {
        //given
        LifeSupport lifeSupport = newLifeSupport();
        Lifecycle lifecycle = mock( Lifecycle.class );
        RuntimeException runtimeException = new RuntimeException();

        //when
        doThrow( runtimeException ).when( lifecycle ).init();
        lifeSupport.add( lifecycle );
        try
        {
            lifeSupport.init();
            fail( "Expected exception" );
        }
        catch ( LifecycleException e )
        {
            assertEquals( runtimeException, e.getCause() );
            assertEquals( 0, e.getSuppressed().length );
        }

    }

    @Test
    void testInitFailsShutdownFails() throws Throwable
    {
        LifeSupport lifeSupport = newLifeSupport();
        Lifecycle lifecycle1 = mock( Lifecycle.class );
        Lifecycle lifecycle2 = mock( Lifecycle.class );
        RuntimeException initRuntimeException = new RuntimeException();
        RuntimeException shutdownRuntimeException = new RuntimeException();
        doThrow( initRuntimeException ).when( lifecycle2 ).init();
        doThrow( shutdownRuntimeException ).when( lifecycle1 ).shutdown();
        lifeSupport.add( lifecycle1 );
        lifeSupport.add( lifecycle2 );
        try
        {
            lifeSupport.init();
            fail( "Expected exception" );
        }
        catch ( LifecycleException e )
        {
            assertEquals( initRuntimeException, e.getCause() );
            assertEquals( 1, e.getSuppressed().length );
            assertEquals( shutdownRuntimeException, e.getSuppressed()[0].getCause() );
            assertThat( e.getSuppressed()[0], instanceOf( LifecycleException.class ) );
        }

    }

    @Test
    void testStartFailsStopWorks() throws Throwable
    {
        LifeSupport lifeSupport = newLifeSupport();
        Lifecycle lifecycle = mock( Lifecycle.class );
        RuntimeException runtimeException = new RuntimeException();
        doThrow( runtimeException ).when( lifecycle ).start();
        lifeSupport.add( lifecycle );
        try
        {
            lifeSupport.start();
            fail( "Expected exception" );
        }
        catch ( LifecycleException e )
        {
            assertEquals( runtimeException, e.getCause() );
            assertEquals( 0, e.getSuppressed().length );
        }

    }

    @Test
    void testStartFailsStopFails() throws Throwable
    {
        LifeSupport lifeSupport = newLifeSupport();
        Lifecycle lifecycle1 = mock( Lifecycle.class );
        Lifecycle lifecycle2 = mock( Lifecycle.class );
        RuntimeException startRuntimeException = new RuntimeException();
        RuntimeException stopRuntimeException = new RuntimeException();
        doThrow( startRuntimeException ).when( lifecycle2 ).start();
        doThrow( stopRuntimeException ).when( lifecycle1 ).stop();
        lifeSupport.add( lifecycle1 );
        lifeSupport.add( lifecycle2 );
        try
        {
            lifeSupport.start();
            fail( "Expected exception" );
        }
        catch ( LifecycleException e )
        {
            assertEquals( startRuntimeException, e.getCause() );
            assertEquals( 1, e.getSuppressed().length );
            assertEquals( stopRuntimeException, e.getSuppressed()[0].getCause() );
            assertThat( e.getSuppressed()[0], instanceOf( LifecycleException.class ) );
        }

    }

    @Test
    void testStopFailsShutdownWorks() throws Throwable
    {
        LifeSupport lifeSupport = newLifeSupport();
        Lifecycle lifecycle = mock( Lifecycle.class );
        RuntimeException runtimeException = new RuntimeException();
        doThrow( runtimeException ).when( lifecycle ).stop();
        lifeSupport.add( lifecycle );
        lifeSupport.start();
        try
        {
            lifeSupport.stop();
            fail( "Expected exception" );
        }
        catch ( LifecycleException e )
        {
            assertEquals( runtimeException, e.getCause() );
            assertEquals( 0, e.getSuppressed().length );
        }

    }

    @Test
    void testStopFailsShutdownFails() throws Throwable
    {
        LifeSupport lifeSupport = newLifeSupport();
        Lifecycle lifecycle1 = mock( Lifecycle.class );
        Lifecycle lifecycle2 = mock( Lifecycle.class );
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
            fail( "Expected exception" );
        }
        catch ( LifecycleException e )
        {
            assertEquals( stopRuntimeException, e.getCause() );
            assertEquals( 1, e.getSuppressed().length );
            assertEquals( shutdownRuntimeException, e.getSuppressed()[0].getCause() );
            assertThat( e.getSuppressed()[0], instanceOf( LifecycleException.class ) );
        }

    }

    @Test
    void tryToStopComponentOnStartFailure() throws Throwable
    {
        LifeSupport lifeSupport = newLifeSupport();
        Lifecycle component = mock( Lifecycle.class );
        doThrow( new RuntimeException( "Start exceptions" ) ).when( component ).start();
        doThrow( new RuntimeException( "Stop exceptions" ) ).when( component ).stop();
        lifeSupport.add( component );

        try
        {
            lifeSupport.start();
            fail( "Failure was expected" );
        }
        catch ( Exception e )
        {
            String message = getExceptionStackTrace( e );
            assertThat( message, containsString(
                    "Exception during graceful attempt to stop partially started component. " +
                            "Please use non suppressed exception to see original component failure." ) );
        }

        assertEquals( LifecycleStatus.STOPPED, lifeSupport.getStatus() );
        verify( component ).stop();
    }

    @Test
    void tryToShutdownComponentOnInitFailure() throws Throwable
    {
        LifeSupport lifeSupport = newLifeSupport();
        Lifecycle component = mock( Lifecycle.class );
        doThrow( new RuntimeException( "Init exceptions" ) ).when( component ).init();
        doThrow( new RuntimeException( "Shutdown exceptions" ) ).when( component ).shutdown();
        lifeSupport.add( component );

        try
        {
            lifeSupport.init();
            fail( "Failure was expected" );
        }
        catch ( Exception e )
        {
            String message = getExceptionStackTrace( e );
            assertThat( message, containsString(
                    "Exception during graceful attempt to shutdown partially initialized component. " +
                            "Please use non suppressed exception to see original component failure." ) );
        }

        assertEquals( LifecycleStatus.SHUTDOWN, lifeSupport.getStatus() );
        verify( component ).shutdown();
    }

    @Test
    void addLastComponentBeforeChain()
    {
        LifeSupport lifeSupport = newLifeSupport();
        Lifecycle lastComponent = mock( Lifecycle.class );
        Lifecycle notLastComponent1 = mock( Lifecycle.class );
        Lifecycle notLastComponent2 = mock( Lifecycle.class );
        Lifecycle notLastComponent3 = mock( Lifecycle.class );
        Lifecycle notLastComponent4 = mock( Lifecycle.class );
        lifeSupport.setLast( lastComponent );
        lifeSupport.add( notLastComponent1 );
        lifeSupport.add( notLastComponent2 );
        lifeSupport.add( notLastComponent3 );
        lifeSupport.add( notLastComponent4 );

        lifeSupport.start();

        List<Lifecycle> lifecycleInstances = lifeSupport.getLifecycleInstances();
        assertSame( notLastComponent1, lifecycleInstances.get( 0 ) );
        assertSame( notLastComponent2, lifecycleInstances.get( 1 ) );
        assertSame( notLastComponent3, lifecycleInstances.get( 2 ) );
        assertSame( notLastComponent4, lifecycleInstances.get( 3 ) );
        assertSame( lastComponent, lifecycleInstances.get( 4 ) );
        assertThat( lifecycleInstances, hasSize( 5 ) );
    }

    @Test
    void addLastComponentSomewhereInAChain()
    {
        LifeSupport lifeSupport = newLifeSupport();
        Lifecycle notLastComponent1 = mock( Lifecycle.class );
        Lifecycle notLastComponent2 = mock( Lifecycle.class );
        Lifecycle lastComponent = mock( Lifecycle.class );
        Lifecycle notLastComponent3 = mock( Lifecycle.class );
        Lifecycle notLastComponent4 = mock( Lifecycle.class );
        lifeSupport.add( notLastComponent1 );
        lifeSupport.add( notLastComponent2 );
        lifeSupport.setLast( lastComponent );
        lifeSupport.add( notLastComponent3 );
        lifeSupport.add( notLastComponent4 );

        lifeSupport.start();

        List<Lifecycle> lifecycleInstances = lifeSupport.getLifecycleInstances();
        assertSame( notLastComponent1, lifecycleInstances.get( 0 ) );
        assertSame( notLastComponent2, lifecycleInstances.get( 1 ) );
        assertSame( notLastComponent3, lifecycleInstances.get( 2 ) );
        assertSame( notLastComponent4, lifecycleInstances.get( 3 ) );
        assertSame( lastComponent, lifecycleInstances.get( 4 ) );
        assertThat( lifecycleInstances, hasSize( 5 ) );
    }

    @Test
    void addOnlyLastComponent()
    {
        LifeSupport lifeSupport = newLifeSupport();
        Lifecycle lastComponent = mock( Lifecycle.class );
        lifeSupport.setLast( lastComponent );
        lifeSupport.start();
        List<Lifecycle> lifecycleInstances = lifeSupport.getLifecycleInstances();

        assertSame( lastComponent, lifecycleInstances.get( 0 ) );
        assertThat( lifecycleInstances, hasSize( 1 ) );
    }

    @Test
    void failToAddSeveralLastComponents()
    {
        LifeSupport lifeSupport = newLifeSupport();
        Lifecycle lastComponent = mock( Lifecycle.class );
        Lifecycle anotherLastComponent = mock( Lifecycle.class );
        lifeSupport.setLast( lastComponent );
        assertThrows( IllegalStateException.class, () -> lifeSupport.setLast( anotherLastComponent ) );
    }

    static class LifecycleMock implements Lifecycle
    {

        Throwable initThrowable;
        Throwable startThrowable;
        Throwable stopThrowable;
        Throwable shutdownThrowable;

        List<LifecycleStatus> transitions;

        LifecycleMock( Throwable initThrowable, Throwable startThrowable, Throwable stopThrowable,
                Throwable shutdownThrowable )
        {
            this.initThrowable = initThrowable;
            this.startThrowable = startThrowable;
            this.stopThrowable = stopThrowable;
            this.shutdownThrowable = shutdownThrowable;

            transitions = new ArrayList<>();
            transitions.add( LifecycleStatus.NONE );
        }

        @Override
        public void init() throws Throwable
        {
            if ( initThrowable != null )
            {
                throw initThrowable;
            }

            transitions.add( LifecycleStatus.STOPPED );
        }

        @Override
        public void start() throws Throwable
        {
            if ( startThrowable != null )
            {
                throw startThrowable;
            }

            transitions.add( LifecycleStatus.STARTED );
        }

        @Override
        public void stop() throws Throwable
        {
            transitions.add( LifecycleStatus.STOPPED );

            if ( stopThrowable != null )
            {
                throw stopThrowable;
            }
        }

        @Override
        public void shutdown() throws Throwable
        {
            transitions.add( LifecycleStatus.SHUTDOWN );

            if ( shutdownThrowable != null )
            {
                throw shutdownThrowable;
            }
        }

        LifecycleStatus getStatus()
        {
            return transitions.get( transitions.size() - 1 );
        }
    }

    private LifeSupport newLifeSupport()
    {
        return new LifeSupport();
    }

    private String getExceptionStackTrace( Exception e )
    {
        StringWriter stringWriter = new StringWriter();
        e.printStackTrace( new PrintWriter( stringWriter ) );
        return stringWriter.toString();
    }
}
