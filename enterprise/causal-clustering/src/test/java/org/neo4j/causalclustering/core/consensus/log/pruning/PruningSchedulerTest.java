/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.core.consensus.log.pruning;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.util.concurrent.TimeUnit;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.causalclustering.core.state.RaftLogPruner;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.OnDemandJobScheduler;

import static java.time.Clock.systemUTC;
import static java.time.Duration.ofMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.logging.NullLogProvider.getInstance;
import static org.neo4j.scheduler.JobScheduler.Groups.raftLogPruning;

public class PruningSchedulerTest
{
    private final RaftLogPruner logPruner = mock( RaftLogPruner.class );
    private final OnDemandJobScheduler jobScheduler = spy( new OnDemandJobScheduler() );

    @Test
    public void shouldScheduleTheCheckPointerJobOnStart()
    {
        // given
        PruningScheduler scheduler = new PruningScheduler( logPruner, jobScheduler, 20L, getInstance() );

        assertNull( jobScheduler.getJob() );

        // when
        scheduler.start();

        // then
        assertNotNull( jobScheduler.getJob() );
        verify( jobScheduler, times( 1 ) ).schedule( eq( raftLogPruning ), any( Runnable.class ), eq( 20L ), eq( MILLISECONDS ) );
    }

    @Test
    public void shouldRescheduleTheJobAfterARun() throws Throwable
    {
        // given
        PruningScheduler scheduler = new PruningScheduler( logPruner, jobScheduler, 20L, getInstance() );

        assertNull( jobScheduler.getJob() );

        scheduler.start();

        Runnable scheduledJob = jobScheduler.getJob();
        assertNotNull( scheduledJob );

        // when
        jobScheduler.runJob();

        // then
        verify( jobScheduler, times( 2 ) ).schedule( eq( raftLogPruning ), any( Runnable.class ), eq( 20L ), eq( MILLISECONDS ) );
        verify( logPruner, times( 1 ) ).prune();
        assertEquals( scheduledJob, jobScheduler.getJob() );
    }

    @Test
    public void shouldNotRescheduleAJobWhenStopped()
    {
        // given
        PruningScheduler scheduler = new PruningScheduler( logPruner, jobScheduler, 20L, getInstance() );

        assertNull( jobScheduler.getJob() );

        scheduler.start();

        assertNotNull( jobScheduler.getJob() );

        // when
        scheduler.stop();

        // then
        assertNull( jobScheduler.getJob() );
    }

    @Test
    public void stoppedJobCantBeInvoked() throws Throwable
    {
        PruningScheduler scheduler = new PruningScheduler( logPruner, jobScheduler, 10L, getInstance() );
        scheduler.start();
        jobScheduler.runJob();

        // verify checkpoint was triggered
        verify( logPruner ).prune();

        // simulate scheduled run that was triggered just before stop
        scheduler.stop();
        scheduler.start();
        jobScheduler.runJob();

        // logPruner should not be invoked now because job stopped
        verifyNoMoreInteractions( logPruner );
    }

    @Test
    public void shouldWaitOnStopUntilTheRunningCheckpointIsDone()
    {
        assertTimeout( ofMillis( 5000 ), () -> {
            //  given

            final AtomicReference<Throwable> ex = new AtomicReference<>();
            final DoubleLatch checkPointerLatch = new DoubleLatch( 1 );
            RaftLogPruner logPruner = new RaftLogPruner( null, null, systemUTC() )
            {
                @Override
                public void prune() throws IOException
                {
                    checkPointerLatch.startAndWaitForAllToStart();
                    checkPointerLatch.waitForAllToFinish();
                }
            };

            final PruningScheduler scheduler = new PruningScheduler( logPruner, jobScheduler, 20L, getInstance() );

            // when
            scheduler.start();

            Thread runCheckPointer = new Thread( jobScheduler::runJob );
            runCheckPointer.start();

            checkPointerLatch.waitForAllToStart();

            Thread stopper = new Thread( () -> {
                try
                {
                    scheduler.stop();
                }
                catch ( Throwable throwable )
                {
                    ex.set( throwable );
                }
            } );

            stopper.start();

            checkPointerLatch.finish();
            runCheckPointer.join();

            stopper.join();

            assertNull( ex.get() );
        } );
    }
}
