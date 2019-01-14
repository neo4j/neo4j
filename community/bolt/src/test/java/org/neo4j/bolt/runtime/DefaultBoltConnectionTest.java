/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.bolt.runtime;

import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.BoltKernelExtension;
import org.neo4j.bolt.logging.BoltMessageLogger;
import org.neo4j.bolt.logging.BoltMessageLogging;
import org.neo4j.bolt.security.auth.AuthenticationException;
import org.neo4j.bolt.testing.Jobs;
import org.neo4j.bolt.v1.packstream.PackOutput;
import org.neo4j.bolt.v1.runtime.BoltConnectionAuthFatality;
import org.neo4j.bolt.v1.runtime.BoltConnectionFatality;
import org.neo4j.bolt.v1.runtime.BoltProtocolBreachFatality;
import org.neo4j.bolt.v1.runtime.BoltStateMachine;
import org.neo4j.bolt.v1.runtime.Job;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.SimpleLogService;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.rule.concurrent.OtherThreadRule;

import static org.hamcrest.CoreMatchers.any;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.startsWith;
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
    private final String connector = "default";
    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private final LogService logService = new SimpleLogService( logProvider );
    private final BoltConnectionLifetimeListener connectionListener = mock( BoltConnectionLifetimeListener.class );
    private final BoltConnectionQueueMonitor queueMonitor = mock( BoltConnectionQueueMonitor.class );
    private final EmbeddedChannel channel = new EmbeddedChannel();
    private final BoltMessageLogger messageLogger = BoltMessageLogging.none().newLogger( channel );

    private BoltChannel boltChannel;
    private BoltStateMachine stateMachine;

    @Rule
    public OtherThreadRule<Boolean> otherThread = new OtherThreadRule<>();

    @Before
    public void setup()
    {
        boltChannel = BoltChannel.open( connector, channel, messageLogger );
        stateMachine = mock( BoltStateMachine.class ); // MachineRoom.newMachineWithOwner( BoltStateMachine.State.READY, "neo4j" );
        when( stateMachine.owner() ).thenReturn( "neo4j" );
        when( stateMachine.shouldStickOnThread() ).thenReturn( false );
        when( stateMachine.hasOpenStatement() ).thenReturn( false );
    }

    @After
    public void cleanup()
    {
        channel.finishAndReleaseAll();
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

        verify( connectionListener ).closed( connection );
    }

    @Test
    public void enqueuedShouldNotifyQueueMonitor()
    {
        Job job = Jobs.noop();
        BoltConnection connection = newConnection();

        connection.enqueue( job );

        verify( queueMonitor ).enqueued( connection, job );
    }

    @Test
    public void enqueuedShouldQueueJob()
    {
        Job job = Jobs.noop();
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
        Job job = Jobs.noop();
        BoltConnection connection = newConnection();
        doAnswer( inv -> drainedJobs.addAll( inv.getArgument( 1 ) ) ).when( queueMonitor ).drained( same( connection ), anyCollection() );

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
        doAnswer( inv -> drainedJobs.addAll( inv.getArgument( 1 ) ) ).when( queueMonitor ).drained( same( connection ), anyCollection() );

        for ( int i = 0; i < 15; i++ )
        {
            Job newJob = Jobs.noop();
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
        verify( queueMonitor ).enqueued( ArgumentMatchers.eq( connection ), ArgumentMatchers.any( Job.class ) );
    }

    @Test
    public void stopShouldCloseStateMachineOnProcessNextBatch()
    {
        BoltConnection connection = newConnection();

        connection.stop();

        connection.processNextBatch();

        verify( queueMonitor ).enqueued( ArgumentMatchers.eq( connection ), ArgumentMatchers.any( Job.class ) );
        verify( stateMachine ).terminate();
        verify( stateMachine ).close();
    }

    @Test
    public void stopShouldCloseStateMachineIfEnqueueEndsWithRejectedExecutionException()
    {
        BoltConnection connection = newConnection();

        doAnswer( i ->
        {
            connection.handleSchedulingError( new RejectedExecutionException() );
            return null;
        } ).when( queueMonitor ).enqueued( ArgumentMatchers.eq( connection ), ArgumentMatchers.any( Job.class ) );

        connection.stop();

        verify( stateMachine ).terminate();
        verify( stateMachine ).close();
    }

    @Test
    public void shouldLogBoltConnectionAuthFatalityError()
    {
        BoltConnection connection = newConnection();
        connection.enqueue( machine ->
        {
            throw new BoltConnectionAuthFatality( new AuthenticationException( Status.Security.Unauthorized, "inner error" ) );
        } );
        connection.processNextBatch();
        verify( stateMachine ).close();
        logProvider.assertExactly( AssertableLogProvider.inLog( containsString( BoltKernelExtension.class.getPackage().getName() ) ).warn(
                containsString( "inner error" ) ) );
    }

    @Test
    public void processNextBatchShouldCloseConnectionOnFatalAuthenticationError()
    {
        BoltConnection connection = newConnection();

        connection.enqueue( machine ->
        {
            throw new BoltConnectionAuthFatality( "auth failure", new RuntimeException( "inner error" ) );
        } );

        connection.processNextBatch();

        verify( stateMachine ).close();
        logProvider.assertNone( AssertableLogProvider.inLog( containsString( BoltKernelExtension.class.getPackage().getName() ) ).warn( any( String.class ) ) );
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
        logProvider.assertExactly( AssertableLogProvider.inLog( containsString( BoltKernelExtension.class.getPackage().getName() ) ).error(
                containsString( "Protocol breach detected in bolt session" ), is( exception ) ) );
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
        logProvider.assertExactly( AssertableLogProvider.inLog( containsString( BoltKernelExtension.class.getPackage().getName() ) ).error(
                containsString( "Unexpected error detected in bolt session" ), is( exception ) ) );
    }

    @Test
    public void processNextBatchShouldThrowAssertionErrorIfStatementOpen() throws Exception
    {
        BoltConnection connection = newConnection( 1 );
        connection.enqueue( Jobs.noop() );
        connection.enqueue( Jobs.noop() );

        // force to a message waiting loop
        when( stateMachine.hasOpenStatement() ).thenReturn( true );

        connection.processNextBatch();

        logProvider.assertExactly(
                AssertableLogProvider.inLog( DefaultBoltConnection.class.getName() ).error( startsWith( "Unexpected error" ), isA( AssertionError.class ) ) );
    }

    @Test
    public void processNextBatchShouldNotThrowAssertionErrorIfStatementOpenButStopping() throws Exception
    {
        BoltConnection connection = newConnection( 1 );
        connection.enqueue( Jobs.noop() );
        connection.enqueue( Jobs.noop() );

        // force to a message waiting loop
        when( stateMachine.hasOpenStatement() ).thenReturn( true );

        connection.stop();
        connection.processNextBatch();

        logProvider.assertNone(
                AssertableLogProvider.inLog( DefaultBoltConnection.class.getName() ).error( startsWith( "Unexpected error" ), isA( AssertionError.class ) ) );
    }

    @Test
    public void processNextBatchShouldReturnWhenConnectionIsStopped() throws Exception
    {
        BoltConnection connection = newConnection( 1 );
        connection.enqueue( Jobs.noop() );
        connection.enqueue( Jobs.noop() );

        // force to a message waiting loop
        when( stateMachine.shouldStickOnThread() ).thenReturn( true );

        Future<Boolean> future = otherThread.execute( state -> connection.processNextBatch() );

        connection.stop();

        otherThread.get().awaitFuture( future );

        verify( stateMachine ).close();
    }

    private DefaultBoltConnection newConnection()
    {
        return newConnection( 10 );
    }

    private DefaultBoltConnection newConnection( int maxBatchSize )
    {
        return new DefaultBoltConnection( boltChannel, mock( PackOutput.class ), stateMachine, logService, connectionListener, queueMonitor, maxBatchSize );
    }

}
