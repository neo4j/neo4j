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
package org.neo4j.bolt.runtime;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.BoltKernelExtension;
import org.neo4j.bolt.logging.BoltMessageLogger;
import org.neo4j.bolt.logging.BoltMessageLogging;
import org.neo4j.bolt.v1.runtime.BoltConnectionAuthFatality;
import org.neo4j.bolt.v1.runtime.BoltConnectionFatality;
import org.neo4j.bolt.v1.runtime.BoltProtocolBreachFatality;
import org.neo4j.bolt.v1.runtime.BoltStateMachine;
import org.neo4j.bolt.v1.runtime.Job;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.SimpleLogService;
import org.neo4j.logging.AssertableLogProvider;

import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultBoltConnectionTest
{
    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private final LogService logService = new SimpleLogService( logProvider );
    private final OutOfBandStrategy outOfBandStrategy = mock( OutOfBandStrategy.class );
    private final BoltConnectionListener connectionListener = mock( BoltConnectionListener.class );
    private final BoltConnectionQueueMonitor queueMonitor = mock( BoltConnectionQueueMonitor.class );
    private final EmbeddedChannel channel = new EmbeddedChannel();
    private final BoltMessageLogger messageLogger = BoltMessageLogging.none().newLogger( channel );

    private BoltChannel boltChannel;
    private BoltStateMachine stateMachine;

    @Before
    public void setup() throws Throwable
    {
        ChannelHandlerContext handlerContext = mock( ChannelHandlerContext.class );
        when( handlerContext.channel() ).thenReturn( channel );
        boltChannel = BoltChannel.open( handlerContext, messageLogger );
        stateMachine = mock( BoltStateMachine.class ); // MachineRoom.newMachineWithOwner( BoltStateMachine.State.READY, "neo4j" );
        when( stateMachine.owner() ).thenReturn( "neo4j" );
    }

    @Test
    public void idShouldReturnBoltChannelId()
    {
        BoltConnection connection = newConnection();

        assertEquals( boltChannel.id(), connection.id() );
    }

    @Test
    public void localAddressShouldReturnBoltServerAddress()
    {
        BoltConnection connection = newConnection();

        assertEquals( boltChannel.serverAddress(), connection.localAddress() );
    }

    @Test
    public void remoteAddressShouldReturnBoltClientAddress()
    {
        BoltConnection connection = newConnection();

        assertEquals( boltChannel.clientAddress(), connection.remoteAddress() );
    }

    @Test
    public void channelShouldReturnBoltRawChannel()
    {
        BoltConnection connection = newConnection();

        assertEquals( boltChannel.rawChannel(), connection.channel() );
    }

    @Test
    public void principalShouldReturnBoltStateMachineOwner()
    {
        BoltConnection connection = newConnection();

        String principal = connection.principal();

        verify( stateMachine ).owner();
        assertEquals( stateMachine.owner(), principal );
    }

    @Test
    public void isOutOfBandIsDecidedByStrategy()
    {
        BoltConnection connection = newConnection();

        connection.isOutOfBand();

        verify( outOfBandStrategy ).isOutOfBand( connection );
    }

    @Test
    public void hasPendingJobsShouldReportFalseWhenInitialised()
    {
        BoltConnection connection = newConnection();

        assertFalse( connection.hasPendingJobs() );
    }

    @Test
    public void startShouldNotifyListener()
    {
        BoltConnection connection = newConnection();

        connection.start();

        verify( connectionListener ).created( connection );
    }

    @Test
    public void stopShouldNotifyListenerOnTheNextBatch()
    {
        BoltConnection connection = newConnection();
        connection.start();

        connection.stop();
        connection.processNextBatch();

        verify( connectionListener ).destroyed( connection );
    }

    @Test
    public void enqueuedShouldNotifyQueueMonitor()
    {
        Job job = machine -> doNothing();
        BoltConnection connection = newConnection();

        connection.enqueue( job );

        verify( queueMonitor ).enqueued( connection, job );
    }

    @Test
    public void enqueuedShouldQueueJob()
    {
        Job job = machine -> doNothing();
        BoltConnection connection = newConnection();

        connection.enqueue( job );

        assertTrue( connection.hasPendingJobs() );
    }

    @Test
    public void processNextBatchShouldDoNothingIfQueueIsEmptyAndConnectionNotClosed()
    {
        BoltConnection connection = newConnection();

        connection.processNextBatch();

        verify( queueMonitor, never() ).drained( same( connection ), anyCollection() );
    }

    @Test
    public void processNextBatchShouldNotifyQueueMonitorAboutDrain()
    {
        List<Job> drainedJobs = new ArrayList<>();
        Job job = machine -> doNothing();
        BoltConnection connection = newConnection();
        doAnswer( inv -> drainedJobs.addAll( (Collection<Job>)inv.getArgument( 1 ) ) ).when( queueMonitor ).drained( same( connection ), anyCollection() );

        connection.enqueue( job );
        connection.processNextBatch();

        verify( queueMonitor ).drained( same( connection ), anyCollection() );
        assertTrue( drainedJobs.contains( job ) );
    }

    @Test
    public void processNextBatchShouldDrainMaxBatchSizeItemsOnEachCall()
    {
        List<Job> drainedJobs = new ArrayList<>();
        List<Job> pushedJobs = new ArrayList<>();
        BoltConnection connection = newConnection( 10 );
        doAnswer( inv -> drainedJobs.addAll( (Collection<Job>)inv.getArgument( 1 ) ) ).when( queueMonitor ).drained( same( connection ), anyCollection() );

        for ( int i = 0; i < 15; i++ )
        {
            final int x = i;
            Job newJob = machine -> doNothing( x );
            pushedJobs.add( newJob );
            connection.enqueue( newJob );
        }

        connection.processNextBatch();

        verify( queueMonitor ).drained( same( connection ), anyCollection() );
        assertEquals( 10, drainedJobs.size() );
        assertTrue( drainedJobs.containsAll( pushedJobs.subList( 0, 10 ) ) );

        drainedJobs.clear();
        connection.processNextBatch();

        verify( queueMonitor, times( 2 ) ).drained( same( connection ), anyCollection() );
        assertEquals( 5, drainedJobs.size() );
        assertTrue( drainedJobs.containsAll( pushedJobs.subList( 10, 15 ) ) );
    }

    @Test
    public void interruptShouldInterruptStateMachine()
    {
        BoltConnection connection = newConnection();

        connection.interrupt();

        verify( stateMachine ).interrupt();
    }

    @Test
    public void stopShouldFirstTerminateStateMachine()
    {
        BoltConnection connection = newConnection();

        connection.stop();

        verify( stateMachine ).terminate();
    }

    @Test
    public void stopShouldCloseStateMachineInFirstProcessNextBatch()
    {
        BoltConnection connection = newConnection();

        connection.stop();

        verify( stateMachine ).terminate();
        verify( stateMachine, never() ).close();

        connection.processNextBatch();

        verify( stateMachine ).terminate();
        verify( stateMachine ).close();
    }

    @Test
    public void processNextBatchShouldCloseConnectionOnFatalAuthenticationError()
    {
        BoltConnection connection = newConnection();

        connection.enqueue( machine ->
        {
            throw new BoltConnectionAuthFatality( "auth failure" );
        } );

        connection.processNextBatch();

        verify( stateMachine ).close();
        logProvider.assertNone(
                AssertableLogProvider.inLog( containsString( BoltKernelExtension.class.getPackage().getName() ) ).error( any( String.class ), any( Throwable.class ) ) );
    }

    @Test
    public void processNextBatchShouldCloseConnectionAndLogOnFatalBoltError()
    {
        BoltConnectionFatality exception = new BoltProtocolBreachFatality( "fatal bolt error" );
        BoltConnection connection = newConnection();

        connection.enqueue( machine ->
        {
            throw exception;
        } );

        connection.processNextBatch();

        verify( stateMachine ).close();
        logProvider.assertExactly(
                AssertableLogProvider.inLog( containsString( BoltKernelExtension.class.getPackage().getName() ) ).error( containsString( "Protocol breach detected in bolt session" ),
                        is( exception ) ) );
    }

    @Test
    public void processNextBatchShouldCloseConnectionAndLogOnUnexpectedException()
    {
        RuntimeException exception = new RuntimeException( "unexpected exception" );
        BoltConnection connection = newConnection();

        connection.enqueue( machine ->
        {
            throw exception;
        } );

        connection.processNextBatch();

        verify( stateMachine ).close();
        logProvider.assertExactly(
                AssertableLogProvider.inLog( containsString( BoltKernelExtension.class.getPackage().getName() ) ).error( containsString( "Unexpected error detected in bolt session" ),
                        is( exception ) ) );
    }

    private DefaultBoltConnection newConnection()
    {
        return newConnection( 10 );
    }

    private DefaultBoltConnection newConnection( int maxBatchSize )
    {
        return new DefaultBoltConnection( boltChannel, stateMachine, logService, outOfBandStrategy, connectionListener, queueMonitor, maxBatchSize );
    }

    private static void doNothing()
    {

    }

    private static void doNothing( int i )
    {

    }

}
