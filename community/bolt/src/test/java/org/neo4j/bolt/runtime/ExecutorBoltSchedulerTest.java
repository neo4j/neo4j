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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.bolt.BoltKernelExtension;
import org.neo4j.function.Predicates;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.SimpleLogService;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.scheduler.JobScheduler;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
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
    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private final LogService logService = new SimpleLogService( logProvider );
    private final Config config = Config.defaults();
    private final ExecutorFactory executorFactory = mock( ExecutorFactory.class );
    private final JobScheduler jobScheduler = mock( JobScheduler.class );
    private final ExecutorBoltScheduler boltScheduler = new ExecutorBoltScheduler( config, executorFactory, jobScheduler, logService );

    @Before
    public void setup()
    {
        when( jobScheduler.threadFactory( any() ) ).thenReturn( Executors.defaultThreadFactory() );
        when( executorFactory.create( anyInt(), anyInt(), any( Duration.class ), any( ThreadFactory.class ) ) ).thenReturn( Executors.newCachedThreadPool() );
    }

    @After
    public void cleanup() throws Throwable
    {
        boltScheduler.shutdown();
    }

    @Test
    public void initShouldCreateBothThreadPools() throws Throwable
    {
        boltScheduler.init();

        verify( jobScheduler ).threadFactory( JobScheduler.Groups.boltStdWorker );
        verify( jobScheduler ).threadFactory( JobScheduler.Groups.boltOobWorker );
        verify( executorFactory, times( 2 ) ).create( anyInt(), anyInt(), any( Duration.class ), any( ThreadFactory.class ) );
    }

    @Test
    public void shutdownShouldTerminateBothThreadPools() throws Throwable
    {
        boltScheduler.init();
        boltScheduler.shutdown();

        verify( executorFactory, times( 2 ) ).destroy( any() );
    }

    @Test
    public void createdShouldAddConnectionToActiveConnections() throws Throwable
    {
        String id = UUID.randomUUID().toString();
        BoltConnection connection = newConnection( id );

        boltScheduler.init();
        boltScheduler.created( connection );

        verify( connection ).id();
        assertTrue( boltScheduler.isRegistered( connection ) );
    }

    @Test
    public void destroyedShouldRemoveConnectionFromActiveConnections() throws Throwable
    {
        String id = UUID.randomUUID().toString();
        BoltConnection connection = newConnection( id );

        boltScheduler.init();
        boltScheduler.created( connection );
        boltScheduler.destroyed( connection );

        assertFalse( boltScheduler.isRegistered( connection ) );
    }

    @Test
    public void enqueuedShouldScheduleJob() throws Throwable
    {
        String id = UUID.randomUUID().toString();
        AtomicBoolean exitCondition = new AtomicBoolean();
        BoltConnection connection = newConnection( id );
        doAnswer( inv -> awaitExit( exitCondition ) ).when( connection ).processNextBatch();

        boltScheduler.init();
        boltScheduler.created( connection );
        boltScheduler.enqueued( connection, machine -> nothing() );

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
        doAnswer( inv -> awaitExit( exitCondition ) ).when( connection ).processNextBatch();

        boltScheduler.init();
        boltScheduler.created( connection );
        boltScheduler.enqueued( connection, machine -> nothing() );

        Predicates.await( () -> boltScheduler.isActive( connection ), 1, MINUTES );
        boltScheduler.enqueued( connection, machine -> nothing() );
        exitCondition.set( true );
        Predicates.await( () -> !boltScheduler.isActive( connection ), 1, MINUTES );

        verify( connection ).processNextBatch();
    }

    @Test
    public void failingJobShouldLogAndStopConnection() throws Throwable
    {
        String id = UUID.randomUUID().toString();
        BoltConnection connection = newConnection( id );
        doThrow( new RuntimeException( "some unexpected error" ) ).when( connection ).processNextBatch();

        boltScheduler.init();
        boltScheduler.created( connection );
        boltScheduler.enqueued( connection, machine -> nothing() );

        Predicates.await( () -> !boltScheduler.isActive( connection ), 1, MINUTES );
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
        String id = UUID.randomUUID().toString();
        BoltConnection connection = newConnection( id );
        AtomicInteger counter = new AtomicInteger( 0 );
        doAnswer( inv -> counter.incrementAndGet() ).when( connection ).processNextBatch();
        when( connection.hasPendingJobs() ).thenReturn( true ).thenReturn( false );

        boltScheduler.init();
        boltScheduler.created( connection );
        boltScheduler.enqueued( connection, machine -> nothing() );

        Predicates.await( () -> counter.get() > 1, 1, MINUTES );

        assertFalse( boltScheduler.isActive( connection ) );
        verify( connection, times( 2 ) ).processNextBatch();
    }

    @Test
    public void destroyedShouldCancelActiveWorkItem() throws Throwable
    {
        String id = UUID.randomUUID().toString();
        BoltConnection connection = newConnection( id );
        AtomicBoolean exitCondition = new AtomicBoolean();
        doAnswer( inv -> awaitExit( exitCondition ) ).when( connection ).processNextBatch();

        boltScheduler.init();
        boltScheduler.created( connection );
        boltScheduler.enqueued( connection, machine -> nothing() );

        Predicates.await( () -> boltScheduler.isActive( connection ), 1, MINUTES );

        boltScheduler.destroyed( connection );

        Predicates.await( () -> !boltScheduler.isActive( connection ), 1, MINUTES );

        assertFalse( boltScheduler.isActive( connection ) );
        verify( connection ).processNextBatch();
    }

    private BoltConnection newConnection( String id )
    {
        BoltConnection result = mock( BoltConnection.class );
        when( result.id() ).thenReturn( id );
        return result;
    }

    private static void nothing()
    {

    }

    private static Object awaitExit( AtomicBoolean exitCondition )
    {
        Predicates.awaitForever( () -> Thread.currentThread().isInterrupted() || exitCondition.get(), 500, MILLISECONDS );
        return null;
    }

}
