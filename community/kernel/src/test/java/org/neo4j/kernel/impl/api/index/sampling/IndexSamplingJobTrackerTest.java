/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;
import org.neo4j.test.DoubleLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
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
}
