/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.consensus.log.pruning;

import org.junit.Test;

import java.time.Clock;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.causalclustering.core.state.RaftLogPruner;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.OnDemandJobScheduler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.scheduler.JobScheduler.Groups.raftLogPruning;

public class PruningSchedulerTest
{
    private final RaftLogPruner logPruner = mock( RaftLogPruner.class );
    private final OnDemandJobScheduler jobScheduler = spy( new OnDemandJobScheduler() );

    @Test
    public void shouldScheduleTheCheckPointerJobOnStart()
    {
        // given
        PruningScheduler scheduler = new PruningScheduler( logPruner, jobScheduler, 20L, NullLogProvider.getInstance() );

        assertNull( jobScheduler.getJob() );

        // when
        scheduler.start();

        // then
        assertNotNull( jobScheduler.getJob() );
        verify( jobScheduler, times( 1 ) ).schedule( eq( raftLogPruning ), any( Runnable.class ),
                eq( 20L ), eq( TimeUnit.MILLISECONDS ) );
    }

    @Test
    public void shouldRescheduleTheJobAfterARun() throws Throwable
    {
        // given
        PruningScheduler scheduler = new PruningScheduler( logPruner, jobScheduler, 20L, NullLogProvider.getInstance() );

        assertNull( jobScheduler.getJob() );

        scheduler.start();

        Runnable scheduledJob = jobScheduler.getJob();
        assertNotNull( scheduledJob );

        // when
        jobScheduler.runJob();

        // then
        verify( jobScheduler, times( 2 ) ).schedule( eq( raftLogPruning ), any( Runnable.class ),
                eq( 20L ), eq( TimeUnit.MILLISECONDS ) );
        verify( logPruner, times( 1 ) ).prune();
        assertEquals( scheduledJob, jobScheduler.getJob() );
    }

    @Test
    public void shouldNotRescheduleAJobWhenStopped()
    {
        // given
        PruningScheduler scheduler = new PruningScheduler( logPruner, jobScheduler, 20L, NullLogProvider.getInstance() );

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
        PruningScheduler scheduler = new PruningScheduler( logPruner, jobScheduler, 10L, NullLogProvider.getInstance() );
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

    @Test( timeout = 5000 )
    public void shouldWaitOnStopUntilTheRunningCheckpointIsDone() throws Throwable
    {
        // given
        final AtomicReference<Throwable> ex = new AtomicReference<>();
        final DoubleLatch checkPointerLatch = new DoubleLatch( 1 );
        RaftLogPruner logPruner = new RaftLogPruner( null, null, Clock.systemUTC() )
        {
            @Override
            public void prune()
            {
                checkPointerLatch.startAndWaitForAllToStart();
                checkPointerLatch.waitForAllToFinish();
            }
        };

        final PruningScheduler scheduler = new PruningScheduler( logPruner, jobScheduler, 20L, NullLogProvider.getInstance() );

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
    }
}
