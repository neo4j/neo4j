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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.time.FakeClock;
import org.neo4j.util.concurrent.BinaryLatch;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

class TimeBasedTaskSchedulerTest
{
    private FakeClock clock;
    private ThreadPoolManager pools;
    private TimeBasedTaskScheduler scheduler;
    private AtomicInteger counter;
    private Semaphore semaphore;

    @BeforeEach
    void setUp()
    {
        clock = new FakeClock();
        pools = new ThreadPoolManager( new ThreadGroup( "TestPool" ) );
        scheduler = new TimeBasedTaskScheduler( clock, pools );
        counter = new AtomicInteger();
        semaphore = new Semaphore( 0 );
    }

    @AfterEach
    void tearDown()
    {
        InterruptedException exception = pools.shutDownAll();
        if ( exception != null )
        {
            throw new RuntimeException( "Test was interrupted?", exception );
        }
    }

    private void assertSemaphoreAcquire() throws InterruptedException
    {
        // We do this in a loop, while calling tick after each iteration, because the task might have a previously
        // start run that hasn't yet finished. And in that case, tick() won't start another. So we have to loop
        // and call tick() until the task gets scheduled and releases our semaphore.
        long timeoutMillis = TimeUnit.SECONDS.toMillis( 10 );
        long sleepIntervalMillis = 10;
        long iterations = timeoutMillis / sleepIntervalMillis;
        for ( int i = 0; i < iterations; i++ )
        {
            if ( semaphore.tryAcquire( sleepIntervalMillis, TimeUnit.MILLISECONDS ) )
            {
                return; // All good.
            }
            scheduler.tick();
        }
        fail( "Semaphore acquire timeout" );
    }

    @Test
    void mustDelayExecution() throws Exception
    {
        JobHandle handle = scheduler.submit( Group.STORAGE_MAINTENANCE, counter::incrementAndGet, 100, 0 );
        scheduler.tick();
        assertThat( counter.get(), is( 0 ) );
        clock.forward( 99, TimeUnit.NANOSECONDS );
        scheduler.tick();
        assertThat( counter.get(), is( 0 ) );
        clock.forward( 1, TimeUnit.NANOSECONDS );
        scheduler.tick();
        handle.waitTermination();
        assertThat( counter.get(), is( 1 ) );
    }

    @Test
    void mustOnlyScheduleTasksThatAreDue() throws Exception
    {
        JobHandle handle1 = scheduler.submit( Group.STORAGE_MAINTENANCE, () -> counter.addAndGet( 10 ), 100, 0 );
        JobHandle handle2 = scheduler.submit( Group.STORAGE_MAINTENANCE, () -> counter.addAndGet( 100 ), 200, 0 );
        scheduler.tick();
        assertThat( counter.get(), is( 0 ) );
        clock.forward( 199, TimeUnit.NANOSECONDS );
        scheduler.tick();
        handle1.waitTermination();
        assertThat( counter.get(), is( 10 ) );
        clock.forward( 1, TimeUnit.NANOSECONDS );
        scheduler.tick();
        handle2.waitTermination();
        assertThat( counter.get(), is( 110 ) );
    }

    @Test
    void mustNotRescheduleDelayedTasks() throws Exception
    {
        JobHandle handle = scheduler.submit( Group.STORAGE_MAINTENANCE, counter::incrementAndGet, 100, 0 );
        clock.forward( 100, TimeUnit.NANOSECONDS );
        scheduler.tick();
        handle.waitTermination();
        assertThat( counter.get(), is( 1 ) );
        clock.forward( 100, TimeUnit.NANOSECONDS );
        scheduler.tick();
        handle.waitTermination();
        pools.getThreadPool( Group.STORAGE_MAINTENANCE ).shutDown();
        assertThat( counter.get(), is( 1 ) );
    }

    @Test
    void mustRescheduleRecurringTasks() throws Exception
    {
        scheduler.submit( Group.STORAGE_MAINTENANCE, semaphore::release, 100, 100 );
        clock.forward( 100, TimeUnit.NANOSECONDS );
        scheduler.tick();
        assertSemaphoreAcquire();
        clock.forward( 100, TimeUnit.NANOSECONDS );
        scheduler.tick();
        assertSemaphoreAcquire();
    }

    @Test
    void mustNotRescheduleRecurringTasksThatThrows() throws Exception
    {
        Runnable runnable = () ->
        {
            semaphore.release();
            throw new RuntimeException( "boom" );
        };
        JobHandle handle = scheduler.submit( Group.STORAGE_MAINTENANCE, runnable, 100, 100 );
        clock.forward( 100, TimeUnit.NANOSECONDS );
        scheduler.tick();
        assertSemaphoreAcquire();
        clock.forward( 100, TimeUnit.NANOSECONDS );
        scheduler.tick();
        ExecutionException exception = assertThrows( ExecutionException.class, handle::waitTermination );
        assertThat( exception.getCause().getMessage(), is( "boom" ) );
        assertThat( semaphore.drainPermits(), is( 0 ) );
    }

    @Test
    void mustNotStartRecurringTasksWherePriorExecutionHasNotYetFinished()
    {
        Runnable runnable = () ->
        {
            counter.incrementAndGet();
            semaphore.acquireUninterruptibly();
        };
        scheduler.submit( Group.STORAGE_MAINTENANCE, runnable, 100, 100 );
        for ( int i = 0; i < 4; i++ )
        {
            scheduler.tick();
            clock.forward( 100, TimeUnit.NANOSECONDS );
        }
        semaphore.release( Integer.MAX_VALUE );
        pools.getThreadPool( Group.STORAGE_MAINTENANCE ).shutDown();
        assertThat( counter.get(), is( 1 ) );
    }

    @Test
    void longRunningTasksMustNotDelayExecutionOfOtherTasks() throws Exception
    {
        BinaryLatch latch = new BinaryLatch();
        Runnable longRunning = latch::await;
        Runnable shortRunning = semaphore::release;
        scheduler.submit( Group.STORAGE_MAINTENANCE, longRunning, 100, 100 );
        scheduler.submit( Group.STORAGE_MAINTENANCE, shortRunning, 100, 100 );
        for ( int i = 0; i < 4; i++ )
        {
            clock.forward( 100, TimeUnit.NANOSECONDS );
            scheduler.tick();
            assertSemaphoreAcquire();
        }
        latch.release();
    }

    @Test
    void delayedTasksMustNotRunIfCancelledFirst()
    {
        List<Boolean> cancelListener = new ArrayList<>();
        JobHandle handle = scheduler.submit( Group.STORAGE_MAINTENANCE, counter::incrementAndGet, 100, 0 );
        handle.registerCancelListener( cancelListener::add );
        clock.forward( 90, TimeUnit.NANOSECONDS );
        scheduler.tick();
        handle.cancel( false );
        clock.forward( 10, TimeUnit.NANOSECONDS );
        scheduler.tick();
        pools.getThreadPool( Group.STORAGE_MAINTENANCE ).shutDown();
        assertThat( counter.get(), is( 0 ) );
        assertThat( cancelListener, contains( Boolean.FALSE ) );
        assertThrows( CancellationException.class, handle::waitTermination );
    }

    @Test
    void recurringTasksMustStopWhenCancelled() throws InterruptedException
    {
        List<Boolean> cancelListener = new ArrayList<>();
        Runnable recurring = () ->
        {
            counter.incrementAndGet();
            semaphore.release();
        };
        JobHandle handle = scheduler.submit( Group.STORAGE_MAINTENANCE, recurring, 100, 100 );
        handle.registerCancelListener( cancelListener::add );
        clock.forward( 100, TimeUnit.NANOSECONDS );
        scheduler.tick();
        assertSemaphoreAcquire();
        clock.forward( 100, TimeUnit.NANOSECONDS );
        scheduler.tick();
        assertSemaphoreAcquire();
        handle.cancel( true );
        clock.forward( 100, TimeUnit.NANOSECONDS );
        scheduler.tick();
        clock.forward( 100, TimeUnit.NANOSECONDS );
        scheduler.tick();
        pools.getThreadPool( Group.STORAGE_MAINTENANCE ).shutDown();
        assertThat( counter.get(), is( 2 ) );
        assertThat( cancelListener, contains( Boolean.TRUE ) );
    }

    @Test
    void cleanupCanceledHandles()
    {
        Runnable recurring = () -> counter.incrementAndGet();
        JobHandle handle = scheduler.submit( Group.STORAGE_MAINTENANCE, recurring, 0, 100 );
        // initial delay is 0 so this task will be scheduled right away
        scheduler.tick();
        // after the call to tick we know that we've scheduled the task and the thread pool will now race with this test to execute it.
        // wait until the task has been run (and re-enqueued since it's recurring).
        while ( scheduler.tasksLeft() == 0 )
        {
            Thread.yield();
        }
        assertThat( counter.get(), is( 1 ) );

        handle.cancel( false );
        // cancelling doesn't remove from queued tasks
        assertEquals( 1, scheduler.tasksLeft() );

        clock.forward( 100, TimeUnit.NANOSECONDS );
        // enough time has passed that this task, if not cancelled, would have been queued again
        scheduler.tick();
        // tick will remove cancelled tasks
        assertEquals( 0, scheduler.tasksLeft() );

        pools.getThreadPool( Group.STORAGE_MAINTENANCE ).shutDown();
        assertThat( counter.get(), is( 1 ) );
    }

    @Test
    void overdueRecurringTasksMustStartAsSoonAsPossible()
    {
        Runnable recurring = () ->
        {
            counter.incrementAndGet();
            semaphore.acquireUninterruptibly();
        };
        JobHandle handle = scheduler.submit( Group.STORAGE_MAINTENANCE, recurring, 100, 100 );
        clock.forward( 100, TimeUnit.NANOSECONDS );
        scheduler.tick();
        while ( counter.get() < 1 )
        {
            // Spin.
            Thread.yield();
        }
        clock.forward( 100, TimeUnit.NANOSECONDS );
        scheduler.tick();
        clock.forward( 100, TimeUnit.NANOSECONDS );
        semaphore.release();
        scheduler.tick();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos( 10 );
        while ( counter.get() < 2 && System.nanoTime() < deadline )
        {
            scheduler.tick();
            Thread.yield();
        }
        assertThat( counter.get(), is( 2 ) );
        semaphore.release( Integer.MAX_VALUE );
        handle.cancel( false );
    }
}
