/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.concurrent;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.neo4j.concurrent.Scheduler.OnRejection.DROP;
import static org.neo4j.concurrent.Scheduler.OnRejection.THROW;

public class SchedulerTest
{
    @Before
    public void resetSchedulerSettings()
    {
        Scheduler.resetSchedulerSettings();
    }

    @Test
    public void mustExecuteComputeBoundWork() throws Exception
    {
        Future<Integer> future = Scheduler.executeComputeBound(
                () -> Arrays.stream( new int[]{1, 2, 3, 4} ).parallel().sum(), THROW );
        assertThat( future.get(), equalTo( 10 ) );
    }

    @Test
    public void mustExecuteIoBoundWork() throws Exception
    {
        Future<Integer> future = Scheduler.executeIOBound(
                () -> Arrays.stream( new int[]{1, 2, 3, 4} ).parallel().sum(), THROW );
        assertThat( future.get(), equalTo( 10 ) );
    }

    @Test
    public void mustExecuteComputeBoundWorkInPooledThread() throws Exception
    {
        Future<Thread> future = Scheduler.executeComputeBound( Thread::currentThread, THROW );
        assertThat( future.get(), is( not( sameInstance( Thread.currentThread() ) ) ) );
    }

    @Test
    public void mustExecuteIoBoundWorkInPooledThread() throws Exception
    {
        Future<Thread> future = Scheduler.executeIOBound( Thread::currentThread, THROW );
        assertThat( future.get(), is( not( sameInstance( Thread.currentThread() ) ) ) );
    }

    @Test
    public void recurringTaskMustExecuteUntilCancelled() throws Exception
    {
        Semaphore latch = new Semaphore( 0 );
        Future<?> future = Scheduler.executeRecurring( latch::release, 0, 1, TimeUnit.MILLISECONDS );
        latch.acquire();
        long start = System.currentTimeMillis();
        latch.acquire( 9 );
        long elapsed = System.currentTimeMillis() - start;
        // We should get a permit released about once every millisecond, which ideally means our 9 requested permits
        // should be delivered in about 9 milliseconds. With virtual machines, thread wake-up time can be pretty long,
        // so lets add a good bit of slack, and assert that we get all of our permits in less than 100 milliseconds.
        assertThat( elapsed, lessThan( 100L ) );
        // Now cancel the future and sleep for 100 milliseconds. We should observe that no new permits are released.
        future.cancel( true );
        latch.drainPermits();
        Thread.sleep( 100 );
        assertThat( latch.drainPermits(), lessThan( 50 ) );
    }

    @Test
    public void changingCpuParallelismMustRebuildPool() throws Exception
    {
        int before = measureParallelism( Scheduler::executeComputeBound );
        Scheduler.modifySchedulerSettings( (cpu, io) -> cpu.parallelism += 1 );
        int after = measureParallelism( Scheduler::executeComputeBound );
        assertThat( after, is( before + 1 ) );
    }

    @Test
    public void changingIoParallelismMustRebuildPool() throws Exception
    {
        int before = measureParallelism( Scheduler::executeIOBound );
        Scheduler.modifySchedulerSettings( (cpu, io) -> io.parallelism += 1 );
        int after = measureParallelism( Scheduler::executeIOBound );
        assertThat( after, is( before + 1 ) );
    }

    private int measureParallelism( BiConsumer<Callable<?>,Scheduler.OnRejection> execute ) throws InterruptedException
    {
        BinaryLatch latch = new BinaryLatch();
        AtomicInteger counter = new AtomicInteger();
        Callable<?> callable = () ->
        {
            counter.incrementAndGet();
            latch.await();
            counter.decrementAndGet();
            return null;
        };

        try
        {
            for ( int i = 0; i < 500; i++ )
            {
                execute.accept( callable, DROP );
            }
            Thread.sleep( 100 );
            return counter.get();
        }
        finally
        {
            latch.release();
            boolean quiesced;
            do
            {
                quiesced = Scheduler.awaitQuiesce( 1, TimeUnit.SECONDS );
            }
            while ( !quiesced );
        }
    }

    @Test( expected = IllegalArgumentException.class )
    public void nullComputeRejectionMustThrow() throws Exception
    {
        Scheduler.executeComputeBound( () -> null, null );
    }

    @Test( expected = IllegalArgumentException.class )
    public void nullIoRejectionMustThrow() throws Exception
    {
        Scheduler.executeIOBound( () -> null, null );
    }
}
