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

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.internal.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.DoubleLatch;

import static java.lang.Thread.currentThread;
import static java.lang.Thread.yield;
import static java.time.Duration.ofMillis;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forLabel;
import static org.neo4j.kernel.api.schema.index.IndexDescriptorFactory.forSchema;

public class IndexSamplingJobTrackerTest
{
    private final IndexSamplingConfig config = mock( IndexSamplingConfig.class );
    LabelSchemaDescriptor descriptor11 = forLabel( 1, 1 );
    LabelSchemaDescriptor descriptor12 = forLabel( 1, 2 );
    LabelSchemaDescriptor descriptor22 = forLabel( 2, 2 );
    IndexDescriptor index11 = forSchema( descriptor11 );
    IndexDescriptor index12 = forSchema( descriptor12 );
    IndexDescriptor index22 = forSchema( descriptor22 );
    long indexId11;
    long indexId12 = 1;
    long indexId22 = 2;

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
            @Override
            public void run()
            {
                count.incrementAndGet();

                latch.waitForAllToStart();
                latch.finish();
            }

            @Override
            public long indexId()
            {
                return indexId12;
            }
        };

        jobTracker.scheduleSamplingJob( job );
        jobTracker.scheduleSamplingJob( job );

        latch.startAndWaitForAllToStart();
        latch.waitForAllToFinish();

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
            @Override
            public void run()
            {
                latch.startAndWaitForAllToStart();
                latch.waitForAllToFinish();
            }

            @Override
            public long indexId()
            {
                return indexId12;
            }
        } );

        // then
        latch.waitForAllToStart();

        assertFalse( jobTracker.canExecuteMoreSamplingJobs() );

        final AtomicBoolean waiting = new AtomicBoolean( false );
        new Thread( () -> {
            waiting.set( true );
            waitingLatch.startAndWaitForAllToStart();
            jobTracker.waitUntilCanExecuteMoreSamplingJobs();
            waiting.set( false );
            waitingLatch.finish();
        } ).start();

        waitingLatch.waitForAllToStart();

        assertTrue( waiting.get() );

        latch.finish();

        waitingLatch.waitForAllToFinish();

        assertFalse( waiting.get() );

        // eventually we accept new jobs
        while ( !jobTracker.canExecuteMoreSamplingJobs() )
        {
            yield();
        }
    }

    @Test
    public void shouldAcceptNewJobWhenRunningJobFinishes()
    {
        assertTimeout( ofMillis( 5_000 ), () -> {
            //  Given

            when( config.jobLimit() ).thenReturn( 1 );

            JobScheduler jobScheduler = new Neo4jJobScheduler();
            jobScheduler.init();

            final IndexSamplingJobTracker jobTracker = new IndexSamplingJobTracker( config, jobScheduler );

            final DoubleLatch latch = new DoubleLatch();
            final AtomicBoolean lastJobExecuted = new AtomicBoolean();

            jobTracker.scheduleSamplingJob( new IndexSamplingJob()
            {
                @Override
                public long indexId()
                {
                    return indexId11;
                }

                @Override
                public void run()
                {
                    latch.waitForAllToStart();
                }
            } );

            // When
            newSingleThreadExecutor().execute( () -> {
                jobTracker.waitUntilCanExecuteMoreSamplingJobs();
                jobTracker.scheduleSamplingJob( new IndexSamplingJob()
                {
                    @Override
                    public long indexId()
                    {
                        return indexId22;
                    }

                    @Override
                    public void run()
                    {
                        lastJobExecuted.set( true );
                        latch.finish();
                    }
                } );
            } );

            assertFalse( jobTracker.canExecuteMoreSamplingJobs() );
            latch.startAndWaitForAllToStart();
            latch.waitForAllToFinish();

            // Then
            assertTrue( lastJobExecuted.get() );
        } );
    }

    @Test
    public void shouldDoNothingWhenUsedAfterBeingStopped()
    {
        assertTimeout( Duration.ofMillis( 5_000 ), () -> {
            // Given
            JobScheduler scheduler = mock( JobScheduler.class );
            IndexSamplingJobTracker jobTracker = new IndexSamplingJobTracker( config, scheduler );
            jobTracker.stopAndAwaitAllJobs();

            // When
            jobTracker.scheduleSamplingJob( mock( IndexSamplingJob.class ) );

            // Then
            verifyZeroInteractions( scheduler );
        } );
    }

    @Test
    public void shouldNotAllowNewJobsAfterBeingStopped()
    {
        assertTimeout( Duration.ofMillis( 5_000 ), () -> {
            // Given
            IndexSamplingJobTracker jobTracker = new IndexSamplingJobTracker( config, mock( JobScheduler.class ) );

            // When
            jobTracker.stopAndAwaitAllJobs();

            // Then
            assertFalse( jobTracker.canExecuteMoreSamplingJobs() );
        } );
    }

    @Test
    public void shouldStopAndWaitForAllJobsToFinish()
    {
        assertTimeout( ofMillis( 5_000 ), () -> {
            //  Given

            when( config.jobLimit() ).thenReturn( 2 );

            JobScheduler jobScheduler = new Neo4jJobScheduler();
            jobScheduler.init();

            final IndexSamplingJobTracker jobTracker = new IndexSamplingJobTracker( config, jobScheduler );
            final CountDownLatch latch1 = new CountDownLatch( 1 );
            final CountDownLatch latch2 = new CountDownLatch( 1 );

            WaitingIndexSamplingJob job1 = new WaitingIndexSamplingJob( indexId11, latch1 );
            WaitingIndexSamplingJob job2 = new WaitingIndexSamplingJob( indexId22, latch1 );

            jobTracker.scheduleSamplingJob( job1 );
            jobTracker.scheduleSamplingJob( job2 );

            Future<?> stopping = newSingleThreadExecutor().submit( () -> {
                latch2.countDown();
                jobTracker.stopAndAwaitAllJobs();
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
        } );
    }

    @Test
    public void shouldWaitForAllJobsToFinish()
    {
        assertTimeout( ofMillis( 5_000 ), () -> {
            //  Given

            when( config.jobLimit() ).thenReturn( 2 );

            JobScheduler jobScheduler = new Neo4jJobScheduler();
            jobScheduler.init();

            final IndexSamplingJobTracker jobTracker = new IndexSamplingJobTracker( config, jobScheduler );
            final CountDownLatch latch1 = new CountDownLatch( 1 );
            final CountDownLatch latch2 = new CountDownLatch( 1 );

            WaitingIndexSamplingJob job1 = new WaitingIndexSamplingJob( indexId11, latch1 );
            WaitingIndexSamplingJob job2 = new WaitingIndexSamplingJob( indexId22, latch1 );

            jobTracker.scheduleSamplingJob( job1 );
            jobTracker.scheduleSamplingJob( job2 );

            Future<?> stopping = newSingleThreadExecutor().submit( () -> {
                latch2.countDown();
                try
                {
                    jobTracker.awaitAllJobs( 10, SECONDS );
                }
                catch ( InterruptedException e )
                {
                    throw new RuntimeException( e );
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
        } );
    }

    private static class WaitingIndexSamplingJob implements IndexSamplingJob
    {
        final long indexId;
        final CountDownLatch latch;

        volatile boolean executed;

        WaitingIndexSamplingJob( long indexId, CountDownLatch latch )
        {
            this.indexId = indexId;
            this.latch = latch;
        }

        @Override
        public long indexId()
        {
            return indexId;
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
                currentThread().interrupt();
                throw new RuntimeException( e );
            }
        }
    }
}
