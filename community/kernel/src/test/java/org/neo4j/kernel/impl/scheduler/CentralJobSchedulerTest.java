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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.resources.Profiler;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.SchedulerThreadFactory;
import org.neo4j.util.concurrent.BinaryLatch;

import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

class CentralJobSchedulerTest
{
    private final AtomicInteger invocations = new AtomicInteger();
    private final LifeSupport life = new LifeSupport();
    private final CentralJobScheduler scheduler = life.add( new CentralJobScheduler() );

    private final Runnable countInvocationsJob = invocations::incrementAndGet;

    @AfterEach
    void stopScheduler()
    {
        life.shutdown();
    }

    @Test
    void taskSchedulerGroupMustNotBeDirectlySchedulable()
    {
        life.start();
        assertThrows( RejectedExecutionException.class,
                () -> scheduler.schedule( Group.TASK_SCHEDULER, () -> fail( "This task should not have been executed." ) ) );
    }

    // Tests schedules a recurring job to run 5 times with 100ms in between.
    // The timeout of 10s should be enough.
    @Test
    void shouldRunRecurringJob()
    {
        assertTimeoutPreemptively( Duration.ofSeconds( 10 ), () ->
        {
            // Given
            long period = 10;
            int count = 5;
            life.start();

            // When
            scheduler.scheduleRecurring( Group.INDEX_POPULATION, countInvocationsJob, period, MILLISECONDS );
            awaitInvocationCount( count );
            scheduler.shutdown();

            // Then assert that the recurring job was stopped (when the scheduler was shut down)
            int actualInvocations = invocations.get();
            sleep( period * 5 );
            assertThat( invocations.get(), equalTo( actualInvocations ) );
        } );
    }

    @Test
    void shouldCancelRecurringJob() throws Exception
    {
        // Given
        long period = 2;
        life.start();
        JobHandle jobHandle = scheduler.scheduleRecurring( Group.INDEX_POPULATION, countInvocationsJob, period, MILLISECONDS );
        awaitFirstInvocation();

        // When
        jobHandle.cancel( false );

        assertThrows( CancellationException.class, jobHandle::waitTermination );

        // Then
        int recorded = invocations.get();
        sleep( period * 100 );
        // we can have task that is already running during cancellation so lets count it as well
        assertThat( invocations.get(),
                both( greaterThanOrEqualTo( recorded ) ).and( lessThanOrEqualTo( recorded + 1 ) ) );
    }

    @Test
    void shouldRunWithDelay() throws Throwable
    {
        // Given
        life.start();

        final AtomicLong runTime = new AtomicLong();
        final CountDownLatch latch = new CountDownLatch( 1 );

        long time = System.nanoTime();

        scheduler.schedule( Group.INDEX_POPULATION, () ->
        {
            runTime.set( System.nanoTime() );
            latch.countDown();
        }, 100, TimeUnit.MILLISECONDS );

        latch.await();

        assertTrue( time + TimeUnit.MILLISECONDS.toNanos( 100 ) <= runTime.get() );
    }

    @Test
    void longRunningScheduledJobsMustNotDelayOtherLongRunningJobs()
    {
        life.start();

        List<JobHandle> handles = new ArrayList<>( 30 );
        AtomicLong startedCounter = new AtomicLong();
        BinaryLatch blockLatch = new BinaryLatch();
        Runnable task = () ->
        {
            startedCounter.incrementAndGet();
            blockLatch.await();
        };

        for ( int i = 0; i < 10; i++ )
        {
            handles.add( scheduler.schedule( Group.INDEX_POPULATION, task, 0, TimeUnit.MILLISECONDS ) );
        }
        for ( int i = 0; i < 10; i++ )
        {
            handles.add( scheduler.scheduleRecurring( Group.INDEX_POPULATION, task, Integer.MAX_VALUE, TimeUnit.MILLISECONDS ) );
        }
        for ( int i = 0; i < 10; i++ )
        {
            handles.add( scheduler.scheduleRecurring( Group.INDEX_POPULATION, task, 0, Integer.MAX_VALUE, TimeUnit.MILLISECONDS ) );
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
    void shouldNotifyCancelListeners()
    {
        // GIVEN
        life.start();

        // WHEN
        AtomicBoolean halted = new AtomicBoolean();
        Runnable job = () ->
        {
            while ( !halted.get() )
            {
                LockSupport.parkNanos( MILLISECONDS.toNanos( 10 ) );
            }
        };
        JobHandle handle = scheduler.schedule( Group.INDEX_POPULATION, job );
        handle.registerCancelListener( mayBeInterrupted -> halted.set( true ) );
        handle.cancel( false );

        // THEN
        assertTrue( halted.get() );
    }

    @Test
    void waitTerminationOnDelayedJobMustWaitUntilJobCompletion()
    {
        assertTimeoutPreemptively( Duration.ofSeconds( 10 ), () ->
        {
            life.start();

            AtomicBoolean triggered = new AtomicBoolean();
            Runnable job = () ->
            {
                LockSupport.parkNanos( TimeUnit.MILLISECONDS.toNanos( 10 ) );
                triggered.set( true );
            };

            JobHandle handle = scheduler.schedule( Group.INDEX_POPULATION, job, 10, TimeUnit.MILLISECONDS );

            handle.waitTermination();
            assertTrue( triggered.get() );
        } );
    }

    @Test
    void scheduledTasksThatThrowsMustPropagateException()
    {
        assertTimeoutPreemptively( Duration.ofSeconds( 10 ), () ->
        {
            life.start();

            RuntimeException boom = new RuntimeException( "boom" );
            AtomicInteger triggerCounter = new AtomicInteger();
            Runnable job = () ->
            {
                triggerCounter.incrementAndGet();
                throw boom;
            };

            JobHandle handle = scheduler.scheduleRecurring( Group.INDEX_POPULATION, job, 1, TimeUnit.MILLISECONDS );
            ExecutionException e = assertThrows( ExecutionException.class, handle::waitTermination );
            assertThat( e.getCause(), is( boom ) );
        } );
    }

    @Test
    void scheduledTasksThatThrowsShouldStop()
    {
        assertTimeoutPreemptively( Duration.ofSeconds( 10 ), () ->
        {
            life.start();

            BinaryLatch triggerLatch = new BinaryLatch();
            RuntimeException boom = new RuntimeException( "boom" );
            AtomicInteger triggerCounter = new AtomicInteger();
            Runnable job = () ->
            {
                triggerCounter.incrementAndGet();
                triggerLatch.release();
                throw boom;
            };

            scheduler.scheduleRecurring( Group.INDEX_POPULATION, job, 1, TimeUnit.MILLISECONDS );

            triggerLatch.await();
            Thread.sleep( 50 );

            assertThat( triggerCounter.get(), is( 1 ) );
        } );
    }

    @Test
    void shutDownMustKillCancelledJobs()
    {
        assertTimeoutPreemptively( Duration.ofSeconds( 10 ), () ->
        {
            life.start();

            BinaryLatch startLatch = new BinaryLatch();
            BinaryLatch stopLatch = new BinaryLatch();
            scheduler.schedule( Group.INDEX_POPULATION, () ->
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
        } );
    }

    @Test
    void schedulerExecutorMustBeOfTypeDefinedByGroup()
    {
        life.start();
        Executor executor = scheduler.executor( Group.CYPHER_WORKER );
        // The CYPHER_WORKER group configures a ForkJoin pool, so that's what we should get.
        assertThat( executor, instanceOf( ForkJoinPool.class ) );
    }

    @Test
    void mustRespectDesiredParallelismSetPriorToPoolCreation() throws Exception
    {
        life.start();
        AtomicInteger counter = new AtomicInteger();
        AtomicInteger max = new AtomicInteger();

        scheduler.setParallelism( Group.CYPHER_WORKER, 3 );

        Runnable runnable = () ->
        {
            counter.getAndIncrement();
            LockSupport.parkNanos( MILLISECONDS.toNanos( 50 ) );
            int currentMax;
            int currentVal;
            do
            {
                currentVal = counter.get();
                currentMax = max.get();
            }
            while ( !max.compareAndSet( currentMax, Math.max( currentMax, currentVal ) ) );
            LockSupport.parkNanos( MILLISECONDS.toNanos( 50 ) );
            counter.getAndDecrement();
        };

        List<JobHandle> handles = new ArrayList<>();
        for ( int i = 0; i < 10; i++ )
        {
            handles.add( scheduler.schedule( Group.CYPHER_WORKER, runnable ) );
        }
        for ( JobHandle handle : handles )
        {
            handle.waitTermination();
        }

        assertThat( max.get(), is( 3 ) );
    }

    @Test
    void shouldUseProvidedThreadFactory()
    {
        life.start();

        SchedulerThreadFactory schedulerThreadFactory = mock( SchedulerThreadFactory.class );

        scheduler.setThreadFactory( Group.BOLT_WORKER, ( group, parentThreadGroup ) -> schedulerThreadFactory );
        assertThat( scheduler.threadFactory( Group.BOLT_WORKER ), sameInstance( schedulerThreadFactory ) );
    }

    @Test
    void shouldThrowIfModifyingParametersAfterStart()
    {
        life.start();

        scheduler.threadFactory( Group.BOLT_WORKER );
        assertThrows( IllegalStateException.class, () -> scheduler.setParallelism( Group.BOLT_WORKER, 2 ) );
        assertThrows( IllegalStateException.class, () -> scheduler.setThreadFactory( Group.BOLT_WORKER, ( a, b ) -> mock( SchedulerThreadFactory.class ) ) );
    }

    @Test
    void shouldListActiveGroups()
    {
        life.start();
        assertEquals( List.of(), scheduler.activeGroups().map( ag -> ag.group ).collect( toList() ) );

        BinaryLatch firstLatch = new BinaryLatch();
        scheduler.schedule( Group.CHECKPOINT, firstLatch::release );
        firstLatch.await();
        assertEquals( List.of( Group.CHECKPOINT ), scheduler.activeGroups().map( ag -> ag.group ).collect( toList() ) );
    }

    @Test
    void shouldProfileGroup() throws InterruptedException
    {
        life.start();
        BinaryLatch checkpointLatch = new BinaryLatch();
        scheduler.schedule( Group.CHECKPOINT, checkpointLatch::await );
        Profiler profiler = Profiler.profiler();
        scheduler.profileGroup( Group.CHECKPOINT, profiler );
        Thread.sleep( 1000 );
        checkpointLatch.release();
        profiler.finish();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream out = new PrintStream( baos );
        profiler.printProfile( out, "Test Title" );
        out.flush();

        assertThat( baos.toString(), containsString( "BinaryLatch.await" ) );
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
