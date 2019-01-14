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
import org.mockito.stubbing.Answer;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.bolt.testing.Jobs;
import org.neo4j.function.Predicates;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.SimpleLogService;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.scheduler.JobScheduler;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExecutorBoltSchedulerConcurrencyTest
{
    private static final String CONNECTOR_KEY = "connector-id";
    private static final int maxPoolSize = 5;

    private final CountDownLatch beforeExecuteEvent = new CountDownLatch( 1 );
    private final CountDownLatch beforeExecuteBarrier = new CountDownLatch( maxPoolSize );
    private final CountDownLatch afterExecuteEvent = new CountDownLatch( 1 );
    private final CountDownLatch afterExecuteBarrier = new CountDownLatch( maxPoolSize );

    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private final LogService logService = new SimpleLogService( logProvider, logProvider );
    private final ExecutorFactory executorFactory = new NotifyingThreadPoolFactory();
    private final JobScheduler jobScheduler = mock( JobScheduler.class );
    private final ExecutorBoltScheduler boltScheduler =
            new ExecutorBoltScheduler( CONNECTOR_KEY, executorFactory, jobScheduler, logService, maxPoolSize, maxPoolSize, Duration.ofMinutes( 1 ), 0,
                    ForkJoinPool.commonPool() );

    @Before
    public void setup() throws Throwable
    {
        when( jobScheduler.threadFactory( any() ) ).thenReturn( Executors.defaultThreadFactory() );

        boltScheduler.start();
    }

    @After
    public void cleanup() throws Throwable
    {
        boltScheduler.stop();
    }

    @Test
    public void shouldInvokeHandleSchedulingErrorIfNoThreadsAvailable() throws Throwable
    {
        AtomicInteger handleSchedulingErrorCounter = new AtomicInteger( 0 );
        BoltConnection newConnection = newConnection( UUID.randomUUID().toString() );
        doAnswer( newCountingAnswer( handleSchedulingErrorCounter ) ).when( newConnection ).handleSchedulingError( any() );

        blockAllThreads();

        // register connection
        boltScheduler.created( newConnection );

        // send a job and wait for it to enter handleSchedulingError and block there
        CompletableFuture.runAsync( () -> boltScheduler.enqueued( newConnection, Jobs.noop() ) );
        Predicates.awaitForever( () -> handleSchedulingErrorCounter.get() > 0, 500, MILLISECONDS );

        // verify that handleSchedulingError is called once
        assertEquals( 1, handleSchedulingErrorCounter.get() );

        // allow all threads to complete
        afterExecuteEvent.countDown();
        afterExecuteBarrier.await();
    }

    @Test
    public void shouldNotScheduleNewJobIfHandlingSchedulingError() throws Throwable
    {
        AtomicInteger handleSchedulingErrorCounter = new AtomicInteger( 0 );
        AtomicBoolean exitCondition = new AtomicBoolean();
        BoltConnection newConnection = newConnection( UUID.randomUUID().toString() );
        doAnswer( newBlockingAnswer( handleSchedulingErrorCounter, exitCondition ) ).when( newConnection ).handleSchedulingError( any() );

        blockAllThreads();

        // register connection
        boltScheduler.created( newConnection );

        // send a job and wait for it to enter handleSchedulingError and block there
        CompletableFuture.runAsync( () -> boltScheduler.enqueued( newConnection, Jobs.noop() ) );
        Predicates.awaitForever( () -> handleSchedulingErrorCounter.get() > 0, 500, MILLISECONDS );

        // allow all threads to complete
        afterExecuteEvent.countDown();
        afterExecuteBarrier.await();

        // post a job
        boltScheduler.enqueued( newConnection, Jobs.noop() );

        // exit handleSchedulingError
        exitCondition.set( true );

        // verify that handleSchedulingError is called once and processNextBatch never.
        assertEquals( 1, handleSchedulingErrorCounter.get() );
        verify( newConnection, never() ).processNextBatch();
    }

    private void blockAllThreads() throws InterruptedException
    {
        for ( int i = 0; i < maxPoolSize; i++ )
        {
            BoltConnection connection = newConnection( UUID.randomUUID().toString() );
            boltScheduler.created( connection );
            boltScheduler.enqueued( connection, Jobs.noop() );
        }

        beforeExecuteEvent.countDown();
        beforeExecuteBarrier.await();
    }

    private <T> Answer<T> newCountingAnswer( AtomicInteger counter )
    {
        return invocationOnMock ->
        {
            counter.incrementAndGet();
            return null;
        };
    }

    private <T> Answer<T> newBlockingAnswer( AtomicInteger counter, AtomicBoolean exitCondition )
    {
        return invocationOnMock ->
        {
            counter.incrementAndGet();
            Predicates.awaitForever( () -> Thread.currentThread().isInterrupted() || exitCondition.get(), 500, MILLISECONDS );
            return null;
        };
    }

    private BoltConnection newConnection( String id )
    {
        BoltConnection result = mock( BoltConnection.class );
        when( result.id() ).thenReturn( id );
        when( result.remoteAddress() ).thenReturn( new InetSocketAddress( "localhost", 32_000 ) );
        return result;
    }

    private class NotifyingThreadPoolFactory implements ExecutorFactory
    {
        @Override
        public ExecutorService create( int corePoolSize, int maxPoolSize, Duration keepAlive, int queueSize, boolean startCoreThreads,
                ThreadFactory threadFactory )
        {
            return new NotifyingThreadPoolExecutor( corePoolSize, maxPoolSize, keepAlive, new SynchronousQueue<>(), threadFactory,
                    new ThreadPoolExecutor.AbortPolicy() );
        }
    }

    private class NotifyingThreadPoolExecutor extends ThreadPoolExecutor
    {

        private NotifyingThreadPoolExecutor( int corePoolSize, int maxPoolSize, Duration keepAlive, BlockingQueue<Runnable> workQueue,
                ThreadFactory threadFactory, RejectedExecutionHandler rejectionHandler )
        {
            super( corePoolSize, maxPoolSize, keepAlive.toMillis(), MILLISECONDS, workQueue, threadFactory, rejectionHandler );
        }

        @Override
        protected void beforeExecute( Thread t, Runnable r )
        {
            try
            {
                beforeExecuteEvent.await();
                super.beforeExecute( t, r );
                beforeExecuteBarrier.countDown();
            }
            catch ( Throwable ex )
            {
                throw new RuntimeException( ex );
            }
        }

        @Override
        protected void afterExecute( Runnable r, Throwable t )
        {
            try
            {
                afterExecuteEvent.await();
                super.afterExecute( r, t );
                afterExecuteBarrier.countDown();
            }
            catch ( Throwable ex )
            {
                throw new RuntimeException( ex );
            }
        }
    }
}
