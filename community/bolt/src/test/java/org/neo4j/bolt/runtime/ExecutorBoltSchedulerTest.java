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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.bolt.BoltKernelExtension;
import org.neo4j.bolt.testing.Jobs;
import org.neo4j.function.Predicates;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.SimpleLogService;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLog;
import org.neo4j.scheduler.JobScheduler;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.test.matchers.CommonMatchers.matchesExceptionMessage;

public class ExecutorBoltSchedulerTest
{
    private static final String CONNECTOR_KEY = "connector-id";

    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private final LogService logService = new SimpleLogService( logProvider );
    private final Config config = Config.defaults();
    private final ExecutorFactory executorFactory = new CachedThreadPoolExecutorFactory( NullLog.getInstance() );
    private final JobScheduler jobScheduler = mock( JobScheduler.class );
    private final ExecutorBoltScheduler boltScheduler =
            new ExecutorBoltScheduler( CONNECTOR_KEY, executorFactory, jobScheduler, logService, 0, 10, Duration.ofMinutes( 1 ), 0, ForkJoinPool.commonPool() );

    @Before
    public void setup()
    {
        when( jobScheduler.threadFactory( any() ) ).thenReturn( Executors.defaultThreadFactory() );
    }

    @After
    public void cleanup() throws Throwable
    {
        boltScheduler.stop();
    }

    @Test
    public void initShouldCreateThreadPool() throws Throwable
    {
        ExecutorFactory mockExecutorFactory = mock( ExecutorFactory.class );
        when( mockExecutorFactory.create( anyInt(), anyInt(), any(), anyInt(), anyBoolean(), any() ) ).thenReturn( Executors.newCachedThreadPool() );
        ExecutorBoltScheduler scheduler =
                new ExecutorBoltScheduler( CONNECTOR_KEY, mockExecutorFactory, jobScheduler, logService, 0, 10, Duration.ofMinutes( 1 ), 0,
                        ForkJoinPool.commonPool() );

        scheduler.start();

        verify( jobScheduler ).threadFactory( JobScheduler.Groups.boltWorker );
        verify( mockExecutorFactory, times( 1 ) ).create( anyInt(), anyInt(), any( Duration.class ), anyInt(), anyBoolean(), any( ThreadFactory.class ) );
    }

    @Test
    public void shutdownShouldTerminateThreadPool() throws Throwable
    {
        ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
        ExecutorFactory mockExecutorFactory = mock( ExecutorFactory.class );
        when( mockExecutorFactory.create( anyInt(), anyInt(), any(), anyInt(), anyBoolean(), any() ) ).thenReturn( cachedThreadPool );
        ExecutorBoltScheduler scheduler =
                new ExecutorBoltScheduler( CONNECTOR_KEY, mockExecutorFactory, jobScheduler, logService, 0, 10, Duration.ofMinutes( 1 ), 0,
                        ForkJoinPool.commonPool() );

        scheduler.start();
        scheduler.stop();

        assertTrue( cachedThreadPool.isShutdown() );
    }

    @Test
    public void createdShouldAddConnectionToActiveConnections() throws Throwable
    {
        String id = UUID.randomUUID().toString();
        BoltConnection connection = newConnection( id );

        boltScheduler.start();
        boltScheduler.created( connection );

        verify( connection ).id();
        assertTrue( boltScheduler.isRegistered( connection ) );
    }

    @Test
    public void destroyedShouldRemoveConnectionFromActiveConnections() throws Throwable
    {
        String id = UUID.randomUUID().toString();
        BoltConnection connection = newConnection( id );

        boltScheduler.start();
        boltScheduler.created( connection );
        boltScheduler.closed( connection );

        assertFalse( boltScheduler.isRegistered( connection ) );
    }

    @Test
    public void enqueuedShouldScheduleJob() throws Throwable
    {
        String id = UUID.randomUUID().toString();
        AtomicBoolean exitCondition = new AtomicBoolean();
        BoltConnection connection = newConnection( id );
        when( connection.processNextBatch() ).thenAnswer( inv -> awaitExit( exitCondition ) );

        boltScheduler.start();
        boltScheduler.created( connection );
        boltScheduler.enqueued( connection, Jobs.noop() );

        Predicates.await( () -> boltScheduler.isActive( connection ), 1, MINUTES );
        exitCondition.set( true );
        Predicates.await( () -> !boltScheduler.isActive( connection ), 1, MINUTES );

        verify( connection ).processNextBatch();
    }

    @Test
    public void enqueuedShouldNotScheduleJobWhenActiveWorkItemExists() throws Throwable
    {
        String id = UUID.randomUUID().toString();
        BoltConnection connection = newConnection( id );
        AtomicBoolean exitCondition = new AtomicBoolean();
        when( connection.processNextBatch() ).thenAnswer( inv -> awaitExit( exitCondition ) );

        boltScheduler.start();
        boltScheduler.created( connection );
        boltScheduler.enqueued( connection, Jobs.noop() );

        Predicates.await( () -> boltScheduler.isActive( connection ), 1, MINUTES );
        boltScheduler.enqueued( connection, Jobs.noop() );
        exitCondition.set( true );
        Predicates.await( () -> !boltScheduler.isActive( connection ), 1, MINUTES );

        verify( connection ).processNextBatch();
    }

    @Test
    public void failingJobShouldLogAndStopConnection() throws Throwable
    {
        AtomicBoolean stopped = new AtomicBoolean();
        String id = UUID.randomUUID().toString();
        BoltConnection connection = newConnection( id );
        doThrow( new RuntimeException( "some unexpected error" ) ).when( connection ).processNextBatch();
        doAnswer( inv -> stopped.getAndSet( true ) ).when( connection ).stop();

        boltScheduler.start();
        boltScheduler.created( connection );
        boltScheduler.enqueued( connection, Jobs.noop() );

        Predicates.await( () -> stopped.get(), 1, MINUTES );

        assertFalse( boltScheduler.isActive( connection ) );
        verify( connection ).processNextBatch();
        verify( connection ).stop();

        logProvider.assertExactly( AssertableLogProvider.inLog( containsString( BoltKernelExtension.class.getPackage().getName() ) ).error(
                containsString( "Unexpected error during job scheduling for session" ),
                matchesExceptionMessage( containsString( "some unexpected error" ) ) ) );
    }

    @Test
    public void successfulJobsShouldTriggerSchedulingOfPendingJobs() throws Throwable
    {
        AtomicInteger counter = new AtomicInteger();
        String id = UUID.randomUUID().toString();
        BoltConnection connection = newConnection( id );
        when( connection.processNextBatch() ).thenAnswer( inv -> counter.incrementAndGet() > 0 );
        when( connection.hasPendingJobs() ).thenReturn( true ).thenReturn( false );

        boltScheduler.start();
        boltScheduler.created( connection );
        boltScheduler.enqueued( connection, Jobs.noop() );

        Predicates.await( () -> counter.get() > 1, 1, MINUTES );

        verify( connection, times( 2 ) ).processNextBatch();
    }

    @Test
    public void destroyedShouldCancelActiveWorkItem() throws Throwable
    {
        AtomicInteger processNextBatchCount = new AtomicInteger();
        String id = UUID.randomUUID().toString();
        BoltConnection connection = newConnection( id );
        AtomicBoolean exitCondition = new AtomicBoolean();
        when( connection.processNextBatch() ).thenAnswer( inv ->
        {
            processNextBatchCount.incrementAndGet();
            return awaitExit( exitCondition );
        } );

        boltScheduler.start();
        boltScheduler.created( connection );
        boltScheduler.enqueued( connection, Jobs.noop() );

        Predicates.await( () -> processNextBatchCount.get() > 0, 1, MINUTES );

        boltScheduler.closed( connection );

        Predicates.await( () -> !boltScheduler.isActive( connection ), 1, MINUTES );

        assertFalse( boltScheduler.isActive( connection ) );
        assertEquals( 1, processNextBatchCount.get() );

        exitCondition.set( true );
    }

    @Test
    public void createdWorkerThreadsShouldContainConnectorName() throws Exception
    {
        AtomicInteger executeBatchCompletionCount = new AtomicInteger();
        AtomicReference<Thread> poolThread = new AtomicReference<>();
        AtomicReference<String> poolThreadName = new AtomicReference<>();

        String id = UUID.randomUUID().toString();
        BoltConnection connection = newConnection( id );
        when( connection.hasPendingJobs() ).thenAnswer( inv ->
        {
            executeBatchCompletionCount.incrementAndGet();
            return false;
        } );
        when( connection.processNextBatch() ).thenAnswer( inv ->
        {
            poolThread.set( Thread.currentThread() );
            poolThreadName.set( Thread.currentThread().getName() );
            return true;
        } );

        boltScheduler.start();
        boltScheduler.created( connection );
        boltScheduler.enqueued( connection, Jobs.noop() );

        Predicates.await( () -> executeBatchCompletionCount.get() > 0, 1, MINUTES );

        assertThat( poolThread.get().getName(), not( equalTo( poolThreadName.get() ) ) );
        assertThat( poolThread.get().getName(), containsString( String.format( "[%s]", CONNECTOR_KEY ) ) );
        assertThat( poolThread.get().getName(), not( containsString( String.format( "[%s]", connection.remoteAddress() ) ) ) );
    }

    @Test
    public void createdWorkerThreadsShouldContainConnectorNameAndRemoteAddressInTheirNamesWhenActive() throws Exception
    {
        final AtomicReference<String> capturedThreadName = new AtomicReference<>();

        AtomicInteger processNextBatchCount = new AtomicInteger();
        String id = UUID.randomUUID().toString();
        BoltConnection connection = newConnection( id );
        AtomicBoolean exitCondition = new AtomicBoolean();
        when( connection.processNextBatch() ).thenAnswer( inv ->
        {
            capturedThreadName.set( Thread.currentThread().getName() );
            processNextBatchCount.incrementAndGet();
            return awaitExit( exitCondition );
        } );

        boltScheduler.start();
        boltScheduler.created( connection );
        boltScheduler.enqueued( connection, Jobs.noop() );

        Predicates.await( () -> processNextBatchCount.get() > 0, 1, MINUTES );

        assertThat( capturedThreadName.get(), containsString( String.format( "[%s]", CONNECTOR_KEY ) ) );
        assertThat( capturedThreadName.get(), containsString( String.format( "[%s]", connection.remoteAddress() ) ) );

        exitCondition.set( true );
    }

    private BoltConnection newConnection( String id )
    {
        BoltConnection result = mock( BoltConnection.class );
        when( result.id() ).thenReturn( id );
        when( result.remoteAddress() ).thenReturn( new InetSocketAddress( "localhost", 32_000 ) );
        return result;
    }

    private static boolean awaitExit( AtomicBoolean exitCondition )
    {
        Predicates.awaitForever( () -> Thread.currentThread().isInterrupted() || exitCondition.get(), 500, MILLISECONDS );
        return true;
    }
}
