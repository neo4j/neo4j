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
package org.neo4j.kernel.impl.api.index.sampling;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.test.DoubleLatch;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class IndexSamplingJobTrackerTest
{
    private final IndexSamplingConfig config = mock( IndexSamplingConfig.class );

    @Test
    public void shouldNotRunASampleJobWhichIsAlreadyRunning() throws Throwable
    {
        // given
        when( config.jobLimit() ).thenReturn( 2 );
        JobScheduler jobScheduler = new Neo4jJobScheduler();
        jobScheduler.init();
        IndexSamplingJobTracker jobTracker = new IndexSamplingJobTracker( config, jobScheduler );
        final DoubleLatch latch = new DoubleLatch();

        // when
        final AtomicInteger count = new AtomicInteger( 0 );

        assertTrue( jobTracker.canExecuteMoreSamplingJobs() );
        IndexSamplingJob job = new IndexSamplingJob()
        {
            private final IndexDescriptor descriptor = new IndexDescriptor( 1, 2 );

            @Override
            public void run()
            {
                count.incrementAndGet();

                latch.awaitStart();
                latch.finish();
            }

            @Override
            public IndexDescriptor descriptor()
            {
                return descriptor;
            }
        };

        jobTracker.scheduleSamplingJob( job );
        jobTracker.scheduleSamplingJob( job );

        latch.start();
        latch.awaitFinish();

        assertEquals( 1, count.get() );
    }

    @Test
    public void shouldNotAcceptMoreJobsThanAllowed() throws Throwable
    {
        // given
        when( config.jobLimit() ).thenReturn( 1 );
        JobScheduler jobScheduler = new Neo4jJobScheduler();
        jobScheduler.init();

        final IndexSamplingJobTracker jobTracker = new IndexSamplingJobTracker( config, jobScheduler );
        final DoubleLatch latch = new DoubleLatch();
        final DoubleLatch waitingLatch = new DoubleLatch();

        // when
        assertTrue( jobTracker.canExecuteMoreSamplingJobs() );

        jobTracker.scheduleSamplingJob( new IndexSamplingJob()
        {
            private final IndexDescriptor descriptor = new IndexDescriptor( 1, 2 );

            @Override
            public void run()
            {
                latch.start();
                latch.awaitFinish();
            }

            @Override
            public IndexDescriptor descriptor()
            {
                return descriptor;
            }
        } );

        // then
        latch.awaitStart();

        assertFalse( jobTracker.canExecuteMoreSamplingJobs() );

        final AtomicBoolean waiting = new AtomicBoolean( false );
        new Thread( new Runnable()
        {
            @Override
            public void run()
            {
                waiting.set( true );
                waitingLatch.start();
                jobTracker.waitUntilCanExecuteMoreSamplingJobs();
                waiting.set( false );
                waitingLatch.finish();
            }
        }).start();

        waitingLatch.awaitStart();

        assertTrue( waiting.get() );

        latch.finish();

        waitingLatch.awaitFinish();

        assertFalse( waiting.get() );

        // eventually we accept new jobs
        while( ! jobTracker.canExecuteMoreSamplingJobs() )
        {
            Thread.yield();
        }
    }

    @Test( timeout = 5_000 )
    public void shouldAcceptNewJobWhenRunningJobFinishes() throws Throwable
    {
        // Given
        when( config.jobLimit() ).thenReturn( 1 );

        JobScheduler jobScheduler = new Neo4jJobScheduler();
        jobScheduler.init();

        final IndexSamplingJobTracker jobTracker = new IndexSamplingJobTracker( config, jobScheduler );

        final DoubleLatch latch = new DoubleLatch();
        final AtomicBoolean lastJobExecuted = new AtomicBoolean();

        jobTracker.scheduleSamplingJob( new IndexSamplingJob()
        {
            @Override
            public IndexDescriptor descriptor()
            {
                return new IndexDescriptor( 1, 1 );
            }

            @Override
            public void run()
            {
                latch.awaitStart();
            }
        } );

        // When
        Executors.newSingleThreadExecutor().execute( new Runnable()
        {
            @Override
            public void run()
            {
                jobTracker.waitUntilCanExecuteMoreSamplingJobs();
                jobTracker.scheduleSamplingJob( new IndexSamplingJob()
                {
                    @Override
                    public IndexDescriptor descriptor()
                    {
                        return new IndexDescriptor( 2, 2 );
                    }

                    @Override
                    public void run()
                    {
                        lastJobExecuted.set( true );
                        latch.finish();
                    }
                } );
            }
        } );

        assertFalse( jobTracker.canExecuteMoreSamplingJobs() );
        latch.start();
        latch.awaitFinish();

        // Then
        assertTrue( lastJobExecuted.get() );
    }

    @Test( timeout = 5_000 )
    public void shouldDoNothingWhenUsedAfterBeingStopped()
    {
        // Given
        JobScheduler scheduler = mock( JobScheduler.class );
        IndexSamplingJobTracker jobTracker = new IndexSamplingJobTracker( config, scheduler );
        jobTracker.stopAndAwaitAllJobs();

        // When
        jobTracker.scheduleSamplingJob( mock( IndexSamplingJob.class ) );

        // Then
        verifyZeroInteractions( scheduler );
    }

    @Test( timeout = 5_000 )
    public void shouldNotAllowNewJobsAfterBeingStopped()
    {
        // Given
        IndexSamplingJobTracker jobTracker = new IndexSamplingJobTracker( config, mock( JobScheduler.class ) );

        // When
        jobTracker.stopAndAwaitAllJobs();

        // Then
        assertFalse( jobTracker.canExecuteMoreSamplingJobs() );
    }

    @Test( timeout = 5_000 )
    public void shouldStopAndWaitForAllJobsToFinish() throws Throwable
    {
        // Given
        when( config.jobLimit() ).thenReturn( 2 );

        JobScheduler jobScheduler = new Neo4jJobScheduler();
        jobScheduler.init();

        final IndexSamplingJobTracker jobTracker = new IndexSamplingJobTracker( config, jobScheduler );
        final CountDownLatch latch1 = new CountDownLatch( 1 );
        final CountDownLatch latch2 = new CountDownLatch( 1 );

        WaitingIndexSamplingJob job1 = new WaitingIndexSamplingJob( new IndexDescriptor( 1, 1 ), latch1 );
        WaitingIndexSamplingJob job2 = new WaitingIndexSamplingJob( new IndexDescriptor( 2, 2 ), latch1 );

        jobTracker.scheduleSamplingJob( job1 );
        jobTracker.scheduleSamplingJob( job2 );

        Future<?> stopping = Executors.newSingleThreadExecutor().submit( new Runnable()
        {
            @Override
            public void run()
            {
                latch2.countDown();
                jobTracker.stopAndAwaitAllJobs();
            }
        } );

        // When
        latch2.await();
        assertFalse( stopping.isDone() );
        latch1.countDown();
        stopping.get( 10, SECONDS );

        // Then
        assertTrue( stopping.isDone() );
        assertNull( stopping.get() );
        assertTrue( job1.executed );
        assertTrue( job2.executed );
    }

    @Test( timeout = 5_000 )
    public void shouldWaitForAllJobsToFinish() throws Throwable
    {
        // Given
        when( config.jobLimit() ).thenReturn( 2 );

        JobScheduler jobScheduler = new Neo4jJobScheduler();
        jobScheduler.init();

        final IndexSamplingJobTracker jobTracker = new IndexSamplingJobTracker( config, jobScheduler );
        final CountDownLatch latch1 = new CountDownLatch( 1 );
        final CountDownLatch latch2 = new CountDownLatch( 1 );

        WaitingIndexSamplingJob job1 = new WaitingIndexSamplingJob( new IndexDescriptor( 1, 1 ), latch1 );
        WaitingIndexSamplingJob job2 = new WaitingIndexSamplingJob( new IndexDescriptor( 2, 2 ), latch1 );

        jobTracker.scheduleSamplingJob( job1 );
        jobTracker.scheduleSamplingJob( job2 );

        Future<?> stopping = Executors.newSingleThreadExecutor().submit( new Runnable()
        {
            @Override
            public void run()
            {
                latch2.countDown();
                try {
                    jobTracker.awaitAllJobs( 10, TimeUnit.SECONDS );
                } catch (InterruptedException e)
                {
                    throw new RuntimeException( e );
                }
            }
        } );

        // When
        latch2.await();
        assertFalse( stopping.isDone() );
        latch1.countDown();
        stopping.get( 10, SECONDS );

        // Then
        assertTrue( stopping.isDone() );
        assertNull( stopping.get() );
        assertTrue( job1.executed );
        assertTrue( job2.executed );
    }

    private static class WaitingIndexSamplingJob implements IndexSamplingJob
    {
        final IndexDescriptor descriptor;
        final CountDownLatch latch;

        volatile boolean executed;

        WaitingIndexSamplingJob( IndexDescriptor descriptor, CountDownLatch latch )
        {
            this.descriptor = descriptor;
            this.latch = latch;
        }

        @Override
        public IndexDescriptor descriptor()
        {
            return descriptor;
        }

        @Override
        public void run()
        {
            try
            {
                latch.await();
                executed = true;
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                throw new RuntimeException( e );
            }
        }
    }
}
