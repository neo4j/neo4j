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

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.neo4j.concurrent.Scheduler.OnRejection.CALLER_RUNS;
import static org.neo4j.concurrent.Scheduler.OnRejection.DROP;
import static org.neo4j.concurrent.Scheduler.OnRejection.SPAWN;
import static org.neo4j.concurrent.Scheduler.OnRejection.THROW;

@Ignore // This test adds 3 minutes to the build time, and I'm not sure we need to run it every time.
public class SchedulerIT
{
    private BinaryLatch latch;
    private Callable<Object> callable;

    @Before
    public void resetSchedulerSettings()
    {
        Scheduler.resetSchedulerSettings();
        latch = new BinaryLatch();
        callable = () -> { latch.await(); return null; };
    }

    @After
    public void quiesceScheduler()
    {
        latch.release();
        boolean quiesced;
        do
        {
            quiesced = Scheduler.awaitQuiesce( 1, TimeUnit.SECONDS );
        }
        while ( !quiesced );
    }

    @Test
    public void callerMustRunComputeBoundIfExecutedWithCallerRunsRejection() throws Exception
    {
        assertCallerRunsExcessTasks( Scheduler::executeComputeBound );
    }

    @Test
    public void callerMustRunIoBoundIfExecuteWithCallerRunsRejection() throws Exception
    {
        assertCallerRunsExcessTasks( Scheduler::executeIOBound );
    }

    private void assertCallerRunsExcessTasks( BiConsumer<Callable<?>,Scheduler.OnRejection> execute )
    {
        Thread callerThread = Thread.currentThread();
        AtomicBoolean observedCallerThread = new AtomicBoolean();
        BinaryLatch latch = new BinaryLatch();
        Callable<?> callable = () ->
        {
            if ( Thread.currentThread() == callerThread )
            {
                observedCallerThread.set( true );
                latch.release();
            }
            else
            {
                latch.await();
            }
            return null;
        };

        while ( !observedCallerThread.get() )
        {
            execute.accept( callable, CALLER_RUNS );
        }
    }

    @Test
    public void mustDropExcessComputeBoundCallablesWithDropRejection() throws Exception
    {
        assertExcessTasksAreDroppedWithDropRejection( Scheduler::executeComputeBound );
    }

    @Test
    public void mustDropExcessIoBoundCallablesWithDropRejection() throws Exception
    {
        assertExcessTasksAreDroppedWithDropRejection( Scheduler::executeIOBound );
    }

    private void assertExcessTasksAreDroppedWithDropRejection(
            BiFunction<Callable<Object>,Scheduler.OnRejection,Future<Object>> execute )
    {
        try
        {
            Future<Object> future;
            do
            {
                future = execute.apply( callable, DROP );
            }
            while ( !future.isCancelled() );
        }
        finally
        {
            latch.release();
        }
    }

    @Test
    public void mustSpawnThreadForExcessComputeBoundTaskWithSpawnRejection() throws Exception
    {
        assertExcessTasksSpawnNewThreads( Scheduler::executeComputeBound, ( cpu, io ) -> cpu.parallelism = 1 );
    }

    @Test
    public void mustSpawnThreadForExcessIoBoundTaskWithSpawnRejection() throws Exception
    {
        assertExcessTasksSpawnNewThreads( Scheduler::executeIOBound, ( cpu, io ) -> io.parallelism = 1 );
    }

    private void assertExcessTasksSpawnNewThreads(
            BiFunction<Callable<Object>,Scheduler.OnRejection,Future<Object>> execute,
            BiConsumer<Scheduler.SchedulerSettings,Scheduler.SchedulerSettings> settings ) throws Exception
    {
        Scheduler.modifySchedulerSettings( settings );
        long initialExtraThreadCount = Scheduler.countSpawnedExtraThreads();
        Future<?> future;
        do
        {
            future = execute.apply( callable, SPAWN );
            assertThat( future, is( not( nullValue() ) ) );
        }
        while ( Scheduler.countSpawnedExtraThreads() < initialExtraThreadCount + 1 );
        latch.release();
        future.get();
    }

    @Test
    public void mustThrowOnExcessComputeBoundTasksWithThrowRejection() throws Exception
    {
        assertThrowsOnRejectingExcessTasks( Scheduler::executeComputeBound );
    }

    @Test
    public void mustThrowOnExcessIoBoundTasksWithThrowRejection() throws Exception
    {
        assertThrowsOnRejectingExcessTasks( Scheduler::executeIOBound );
    }

    private void assertThrowsOnRejectingExcessTasks( BiConsumer<Callable<?>,Scheduler.OnRejection> execute )
    {
        long counter = 0;
        try
        {
            //noinspection InfiniteLoopStatement
            for (;;)
            {
                execute.accept( callable, THROW );
                counter++;
            }
        }
        catch ( RejectedExecutionException e )
        {
            assertThat( counter, greaterThan( 100L ) );
        }
    }
}
