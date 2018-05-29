/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.bolt.v1.runtime.concurrent;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.neo4j.bolt.v1.runtime.BoltConnectionAuthFatality;
import org.neo4j.bolt.v1.runtime.BoltProtocolBreachFatality;
import org.neo4j.bolt.v1.runtime.BoltStateMachine;
import org.neo4j.bolt.v1.runtime.BoltWorkerQueueMonitor;
import org.neo4j.bolt.v1.runtime.Job;
import org.neo4j.cypher.internal.frontend.v3_2.ast.functions.Collect;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.logging.AssertableLogProvider;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class RunnableBoltWorkerTest
{

    private AssertableLogProvider internalLog;
    private AssertableLogProvider userLog;
    private LogService logService;
    private BoltStateMachine machine;

    @Before
    public void setup()
    {
        internalLog = new AssertableLogProvider();
        userLog = new AssertableLogProvider();
        logService = mock( LogService.class );
        when( logService.getUserLogProvider() ).thenReturn( userLog );
        when( logService.getUserLog( RunnableBoltWorker.class ) )
                .thenReturn( userLog.getLog( RunnableBoltWorker.class ) );
        when( logService.getInternalLogProvider() ).thenReturn( internalLog );
        when( logService.getInternalLog( RunnableBoltWorker.class ) )
                .thenReturn( internalLog.getLog( RunnableBoltWorker.class ) );
        machine = mock( BoltStateMachine.class );
        when( machine.key() ).thenReturn( "test-session" );
    }

    @Test
    public void shouldExecuteWorkWhenRun() throws Throwable
    {
        // Given
        RunnableBoltWorker worker = new RunnableBoltWorker( machine, NullLogService.getInstance() );
        worker.enqueue( s -> s.run( "Hello, world!", null, null ) );
        worker.enqueue( s -> worker.halt() );

        // When
        worker.run();

        // Then
        verify( machine ).run( "Hello, world!", null, null );
        verify( machine ).terminate();
        verify( machine ).close();
        verifyNoMoreInteractions( machine );
    }

    @Test
    public void errorThrownDuringExecutionShouldCauseSessionClose() throws Throwable
    {
        // Given
        RunnableBoltWorker worker = new RunnableBoltWorker( machine, NullLogService.getInstance() );
        worker.enqueue( s ->
        {
            throw new RuntimeException( "It didn't work out." );
        } );

        // When
        worker.run();

        // Then
        verify( machine ).close();
    }

    @Test
    public void authExceptionShouldNotBeLoggedHere() throws Throwable
    {
        // Given
        RunnableBoltWorker worker = new RunnableBoltWorker( machine, logService );
        worker.enqueue( s ->
        {
            throw new BoltConnectionAuthFatality( "fatality" );
        } );

        // When
        worker.run();

        // Then
        verify( machine ).close();
        internalLog.assertNone( inLog( RunnableBoltWorker.class ).any() );
        userLog.assertNone( inLog( RunnableBoltWorker.class ).any() );
    }

    @Test
    public void protocolBreachesShouldBeLoggedWithStackTraces() throws Throwable
    {
        // Given
        BoltProtocolBreachFatality error = new BoltProtocolBreachFatality( "protocol breach fatality" );
        RunnableBoltWorker worker = new RunnableBoltWorker( machine, logService );
        worker.enqueue( s ->
        {
            throw error;
        } );

        // When
        worker.run();

        // Then
        verify( machine ).close();
        internalLog.assertExactly( inLog( RunnableBoltWorker.class )
                .error( equalTo( "Bolt protocol breach in session 'test-session'" ), equalTo( error ) ) );
        userLog.assertNone( inLog( RunnableBoltWorker.class ).any() );
    }

    @Test
    public void haltShouldTerminateButNotCloseTheStateMachine()
    {
        RunnableBoltWorker worker = new RunnableBoltWorker( machine, logService );

        worker.halt();

        verify( machine ).terminate();
        verify( machine, never() ).close();
    }

    @Test
    public void workerCanBeHaltedMultipleTimes()
    {
        RunnableBoltWorker worker = new RunnableBoltWorker( machine, logService );

        worker.halt();
        worker.halt();
        worker.halt();

        verify( machine, times( 3 ) ).terminate();
        verify( machine, never() ).close();

        worker.run();

        verify( machine ).close();
    }

    @Test
    public void stateMachineIsClosedOnExit()
    {
        RunnableBoltWorker worker = new RunnableBoltWorker( machine, logService );

        worker.enqueue( machine1 ->
        {
            machine1.run( "RETURN 1", null, null );
            worker.enqueue( machine2 ->
            {
                machine2.run( "RETURN 1", null, null );
                worker.enqueue( machine3 ->
                {
                    worker.halt();
                    worker.enqueue( machine4 -> fail( "Should not be executed" ) );
                } );
            } );
        } );

        worker.run();

        verify( machine ).close();
    }

    @Test
    public void stateMachineNotClosedOnHalt()
    {
        RunnableBoltWorker worker = new RunnableBoltWorker( machine, logService );

        worker.halt();

        verify( machine, never() ).close();
    }

    @Test
    public void stateMachineInterrupted()
    {
        RunnableBoltWorker worker = new RunnableBoltWorker( machine, logService );

        worker.interrupt();

        verify( machine ).interrupt();
    }

    @Test
    public void stateMachineCloseFailureIsLogged()
    {
        RunnableBoltWorker worker = new RunnableBoltWorker( machine, logService );

        RuntimeException closeError = new RuntimeException( "Oh!" );
        doThrow( closeError ).when( machine ).close();

        worker.enqueue( s -> worker.halt() );
        worker.run();

        internalLog.assertExactly( inLog( RunnableBoltWorker.class ).error(
                equalTo( "Unable to close Bolt session 'test-session'" ),
                equalTo( closeError ) ) );
    }

    @Test
    public void haltIsRespected()
    {
        RunnableBoltWorker worker = new RunnableBoltWorker( machine, logService );

        worker.enqueue( machine1 ->
                worker.enqueue( machine2 ->
                        worker.enqueue( machine3 ->
                        {
                            worker.halt();
                            verify( machine ).terminate();
                            worker.enqueue( machine4 -> fail( "Should not be executed" ) );
                        } ) ) );

        worker.run();

        verify( machine ).close();
    }

    @Test
    public void runDoesNothingAfterHalt()
    {
        RunnableBoltWorker worker = new RunnableBoltWorker( machine, logService );
        MutableBoolean jobWasExecuted = new MutableBoolean();
        worker.enqueue( machine1 ->
        {
            jobWasExecuted.setTrue();
            fail( "Should not be executed" );
        } );

        worker.halt();
        worker.run();

        assertFalse( jobWasExecuted.booleanValue() );
        verify( machine ).close();
    }

    @Test
    public void shouldValidateTransaction() throws Exception
    {
        RunnableBoltWorker worker = new RunnableBoltWorker( machine, logService );
        Future workerFuture = Executors.newSingleThreadExecutor().submit( worker );

        Thread.sleep( Duration.ofSeconds( RunnableBoltWorker.workQueuePollDuration ).toMillis() );

        worker.halt();
        workerFuture.get();

        verify( machine, atLeastOnce() ).validateTransaction();
    }

    @Test
    public void shouldNotNotifyMonitorWhenNothingEnqueued() throws Exception
    {
        BoltWorkerQueueMonitor monitor = mock( BoltWorkerQueueMonitor.class );
        RunnableBoltWorker worker = new RunnableBoltWorker( machine, logService, monitor );

        verify( monitor, never() ).enqueued( any( Job.class ) );
        verify( monitor, never() ).dequeued( any( Job.class ) );
        verify( monitor, never() ).drained( any( Collection.class ) );
    }

    @Test
    public void shouldNotifyMonitorWhenQueued() throws Exception
    {
        BoltWorkerQueueMonitor monitor = mock( BoltWorkerQueueMonitor.class );
        RunnableBoltWorker worker = new RunnableBoltWorker( machine, logService, monitor );
        Job job = s -> s.run( "Hello world", null, null );

        worker.enqueue( job );

        verify( monitor ).enqueued( job );
    }

    @Test
    public void shouldNotifyMonitorWhenDequeued() throws Exception
    {
        BoltWorkerQueueMonitor monitor = mock( BoltWorkerQueueMonitor.class );
        RunnableBoltWorker worker = new RunnableBoltWorker( machine, logService, monitor );
        Job job = s -> s.run( "Hello world", null, null );

        worker.enqueue( job );
        worker.enqueue( s -> worker.halt() );
        worker.run();

        verify( monitor ).enqueued( job );
        verify( monitor ).dequeued( job );
    }

    @Test
    public void shouldNotifyMonitorWhenDrained() throws Exception
    {
        List<Job> drainedJobs = new ArrayList<>();
        BoltWorkerQueueMonitor monitor = newMonitor( drainedJobs );
        RunnableBoltWorker worker = new RunnableBoltWorker( machine, logService, monitor );
        Job job1 = s -> s.run( "Hello world 1", null, null );
        Job job2 = s -> s.run( "Hello world 1", null, null );
        Job job3 = s -> s.run( "Hello world 1", null, null );
        Job haltJob = s -> worker.halt();

        worker.enqueue( job1 );
        worker.enqueue( job2 );
        worker.enqueue( job3 );
        worker.enqueue( haltJob );
        worker.run();

        verify( monitor ).enqueued( job1 );
        verify( monitor ).enqueued( job2 );
        verify( monitor ).enqueued( job3 );
        verify( monitor ).dequeued( job1 );
        verify( monitor ).drained( anyCollection() );

        assertThat( drainedJobs, hasSize( 3 ) );
        assertThat( drainedJobs, contains( job2, job3, haltJob ) );
    }

    private static BoltWorkerQueueMonitor newMonitor( final List<Job> drained )
    {
        BoltWorkerQueueMonitor monitor = mock( BoltWorkerQueueMonitor.class );

        doAnswer( invocation ->
        {
            final Collection<Job> jobs = invocation.getArgumentAt( 0, Collection.class );
            drained.addAll( jobs );
            return null;
        } ).when( monitor ).drained( anyListOf( Job.class ) );

        return monitor;
    }

}
