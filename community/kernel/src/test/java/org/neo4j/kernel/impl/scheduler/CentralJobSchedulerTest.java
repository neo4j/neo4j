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
package org.neo4j.kernel.impl.scheduler;

import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.concurrent.BinaryLatch;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.scheduler.JobScheduler.JobHandle;

import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.scheduler.JobScheduler.Groups.indexPopulation;

public class CentralJobSchedulerTest
{
    private final AtomicInteger invocations = new AtomicInteger();
    private final LifeSupport life = new LifeSupport();
    private final CentralJobScheduler scheduler = life.add( new CentralJobScheduler() );

    private final Runnable countInvocationsJob = invocations::incrementAndGet;

    @After
    public void stopScheduler()
    {
        life.shutdown();
    }

    // Tests schedules a recurring job to run 5 times with 100ms in between.
    // The timeout of 10s should be enough.
    @Test( timeout = 10_000 )
    public void shouldRunRecurringJob() throws Throwable
    {
        // Given
        long period = 10;
        int count = 5;
        life.start();

        // When
        scheduler.scheduleRecurring( indexPopulation, countInvocationsJob, period, MILLISECONDS );
        awaitInvocationCount( count );
        scheduler.shutdown();

        // Then assert that the recurring job was stopped (when the scheduler was shut down)
        int actualInvocations = invocations.get();
        sleep( period * 5 );
        assertThat( invocations.get(), equalTo( actualInvocations ) );
    }

    @Test
    public void shouldCancelRecurringJob() throws Exception
    {
        // Given
        long period = 2;
        life.start();
        JobScheduler.JobHandle jobHandle =
                scheduler.scheduleRecurring( indexPopulation, countInvocationsJob, period, MILLISECONDS );
        awaitFirstInvocation();

        // When
        jobHandle.cancel( false );

        try
        {
            jobHandle.waitTermination();
            fail( "Task should be terminated" );
        }
        catch ( CancellationException ingored )
        {
            // task should be canceled
        }

        // Then
        int recorded = invocations.get();
        sleep( period * 100 );
        // we can have task that is already running during cancellation so lets count it as well
        assertThat( invocations.get(),
                both( greaterThanOrEqualTo( recorded ) ).and( lessThanOrEqualTo( recorded + 1 ) ) );
    }

    @Test
    public void shouldRunWithDelay() throws Throwable
    {
        // Given
        life.start();

        final AtomicLong runTime = new AtomicLong();
        final CountDownLatch latch = new CountDownLatch( 1 );

        long time = System.nanoTime();

        scheduler.schedule( new JobScheduler.Group( "group" ), () ->
        {
            runTime.set( System.nanoTime() );
            latch.countDown();
        }, 100, TimeUnit.MILLISECONDS );

        latch.await();

        assertTrue( time + TimeUnit.MILLISECONDS.toNanos( 100 ) <= runTime.get() );
    }

    @Test
    public void longRunningScheduledJobsMustNotDelayOtherLongRunningJobs()
    {
        life.start();

        List<JobHandle> handles = new ArrayList<>( 30 );
        JobScheduler.Group group = new JobScheduler.Group( "test" );
        AtomicLong startedCounter = new AtomicLong();
        BinaryLatch blockLatch = new BinaryLatch();
        Runnable task = () ->
        {
            startedCounter.incrementAndGet();
            blockLatch.await();
        };

        for ( int i = 0; i < 10; i++ )
        {
            handles.add( scheduler.schedule( group, task, 0, TimeUnit.MILLISECONDS ) );
        }
        for ( int i = 0; i < 10; i++ )
        {
            handles.add( scheduler.scheduleRecurring( group, task, Integer.MAX_VALUE, TimeUnit.MILLISECONDS ) );
        }
        for ( int i = 0; i < 10; i++ )
        {
            handles.add( scheduler.scheduleRecurring( group, task, 0, Integer.MAX_VALUE, TimeUnit.MILLISECONDS ) );
        }

        long deadline = TimeUnit.SECONDS.toNanos( 10 ) + System.nanoTime();
        do
        {
            if ( startedCounter.get() == handles.size() )
            {
                // All jobs got started. We're good!
                blockLatch.release();
                for ( JobHandle handle : handles )
                {
                    handle.cancel( false );
                }
                return;
            }
        }
        while ( System.nanoTime() < deadline );
        fail( "Only managed to start " + startedCounter.get() + " tasks in 10 seconds, when " +
              handles.size() + " was expected." );
    }

    @Test
    public void shouldNotifyCancelListeners()
    {
        // GIVEN
        CentralJobScheduler centralJobScheduler = new CentralJobScheduler();
        centralJobScheduler.init();

        // WHEN
        AtomicBoolean halted = new AtomicBoolean();
        Runnable job = () ->
        {
            while ( !halted.get() )
            {
                LockSupport.parkNanos( MILLISECONDS.toNanos( 10 ) );
            }
        };
        JobHandle handle = centralJobScheduler.schedule( indexPopulation, job );
        handle.registerCancelListener( mayBeInterrupted -> halted.set( true ) );
        handle.cancel( false );

        // THEN
        assertTrue( halted.get() );
        centralJobScheduler.shutdown();
    }

    @Test( timeout = 10_000 )
    public void waitTerminationOnDelayedJobMustWaitUntilJobCompletion() throws Exception
    {
        CentralJobScheduler scheduler = new CentralJobScheduler();
        scheduler.init();

        AtomicBoolean triggered = new AtomicBoolean();
        Runnable job = () ->
        {
            LockSupport.parkNanos( TimeUnit.MILLISECONDS.toNanos( 10 ) );
            triggered.set( true );
        };

        JobHandle handle = scheduler.schedule( indexPopulation, job, 10, TimeUnit.MILLISECONDS );

        handle.waitTermination();
        assertTrue( triggered.get() );
    }

    @Test( timeout = 10_000 )
    public void scheduledTasksThatThrowsMustPropagateException() throws Exception
    {
        CentralJobScheduler scheduler = new CentralJobScheduler();
        scheduler.init();

        RuntimeException boom = new RuntimeException( "boom" );
        AtomicInteger triggerCounter = new AtomicInteger();
        Runnable job = () ->
        {
            triggerCounter.incrementAndGet();
            throw boom;
        };

        JobHandle handle = scheduler.scheduleRecurring( indexPopulation, job, 1, TimeUnit.MILLISECONDS );
        try
        {
            handle.waitTermination();
            fail( "waitTermination should have failed." );
        }
        catch ( ExecutionException e )
        {
            assertThat( e.getCause(), is( boom ) );
        }
    }

    @Test( timeout = 10_000 )
    public void scheduledTasksThatThrowsShouldStop() throws Exception
    {
        CentralJobScheduler scheduler = new CentralJobScheduler();
        scheduler.init();

        BinaryLatch triggerLatch = new BinaryLatch();
        RuntimeException boom = new RuntimeException( "boom" );
        AtomicInteger triggerCounter = new AtomicInteger();
        Runnable job = () ->
        {
            triggerCounter.incrementAndGet();
            triggerLatch.release();
            throw boom;
        };

        scheduler.scheduleRecurring( indexPopulation, job, 1, TimeUnit.MILLISECONDS );

        triggerLatch.await();
        Thread.sleep( 50 );

        assertThat( triggerCounter.get(), is( 1 ) );
    }

    @Test( timeout = 10_000 )
    public void shutDownMustKillCancelledJobs()
    {
        CentralJobScheduler scheduler = new CentralJobScheduler();
        scheduler.init();

        BinaryLatch startLatch = new BinaryLatch();
        BinaryLatch stopLatch = new BinaryLatch();
        scheduler.schedule( indexPopulation, () ->
        {
            try
            {
                startLatch.release();
                Thread.sleep( 100_000 );
            }
            catch ( InterruptedException e )
            {
                stopLatch.release();
                throw new RuntimeException( e );
            }
        } );
        startLatch.await();
        scheduler.shutdown();
        stopLatch.await();
    }

    private void awaitFirstInvocation() throws InterruptedException
    {
        awaitInvocationCount( 1 );
    }

    private void awaitInvocationCount( int count ) throws InterruptedException
    {
        while ( invocations.get() < count )
        {
            Thread.sleep( 10 );
        }
    }
}
