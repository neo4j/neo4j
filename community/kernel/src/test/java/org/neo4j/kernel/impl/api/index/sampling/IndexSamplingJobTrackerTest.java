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
package org.neo4j.kernel.impl.api.index.sampling;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.DoubleLatch;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

class IndexSamplingJobTrackerTest
{
    private static final long indexId11 = 0;
    private static final long indexId12 = 1;
    private static final long indexId22 = 2;
    private final IndexSamplingConfig config = mock( IndexSamplingConfig.class );
    private final JobScheduler jobScheduler = createInitialisedScheduler();
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    @AfterEach
    void tearDown() throws Exception
    {
        executorService.shutdownNow();
        jobScheduler.shutdown();
    }

    @Test
    void shouldNotRunASampleJobWhichIsAlreadyRunning()
    {
        // given
        IndexSamplingJobTracker jobTracker = new IndexSamplingJobTracker( config, jobScheduler );
        final DoubleLatch latch = new DoubleLatch();

        // when
        final AtomicInteger count = new AtomicInteger( 0 );

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
    void shouldDoNothingWhenUsedAfterBeingStopped()
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

    @Test
    void shouldStopAndWaitForAllJobsToFinish() throws Exception
    {
        // Given
        final IndexSamplingJobTracker jobTracker = new IndexSamplingJobTracker( config, jobScheduler );
        final CountDownLatch latch1 = new CountDownLatch( 1 );
        final CountDownLatch latch2 = new CountDownLatch( 1 );

        WaitingIndexSamplingJob job1 = new WaitingIndexSamplingJob( indexId11, latch1 );
        WaitingIndexSamplingJob job2 = new WaitingIndexSamplingJob( indexId22, latch1 );

        jobTracker.scheduleSamplingJob( job1 );
        jobTracker.scheduleSamplingJob( job2 );

        Future<?> stopping = executorService.submit( () ->
        {
            latch2.countDown();
            jobTracker.stopAndAwaitAllJobs();
        } );

        // When
        latch2.await();
        assertFalse( stopping.isDone() );
        latch1.countDown();
        stopping.get( 5, SECONDS );

        // Then
        assertTrue( stopping.isDone() );
        assertNull( stopping.get() );
        assertTrue( job1.executed );
        assertTrue( job2.executed );
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
                Thread.currentThread().interrupt();
                throw new RuntimeException( e );
            }
        }
    }
}
