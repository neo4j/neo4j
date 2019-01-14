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

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.Clocks;

public class CentralJobScheduler extends LifecycleAdapter implements JobScheduler
{
    private static final AtomicInteger INSTANCE_COUNTER = new AtomicInteger();
    private static final Group SCHEDULER_GROUP = new Group( "Scheduler" );

    private final TimeBasedTaskScheduler scheduler;
    private final Thread schedulerThread;

    // Contains workStealingExecutors for each group that have asked for one.
    // If threads need to be created, they need to be inside one of these pools.
    // We also need to remember to shutdown all pools when we shutdown the database to shutdown queries in an orderly
    // fashion.
    private final ConcurrentHashMap<Group,ExecutorService> workStealingExecutors;

    private final TopLevelGroup topLevelGroup;
    private final ThreadPoolManager pools;

    private volatile boolean started;

    private static class TopLevelGroup extends ThreadGroup
    {
        TopLevelGroup()
        {
            super( "Neo4j-" + INSTANCE_COUNTER.incrementAndGet() );
        }

        public void setName( String name ) throws Exception
        {
            Field field = ThreadGroup.class.getDeclaredField( "name" );
            field.setAccessible( true );
            field.set( this, name );
        }
    }

    public CentralJobScheduler()
    {
        workStealingExecutors = new ConcurrentHashMap<>( 1 );
        topLevelGroup = new TopLevelGroup();
        pools = new ThreadPoolManager( topLevelGroup );
        ThreadFactory threadFactory = new GroupedDaemonThreadFactory( SCHEDULER_GROUP, topLevelGroup );
        scheduler = new TimeBasedTaskScheduler( Clocks.nanoClock(), pools );

        // The scheduler thread runs at slightly elevated priority for timeliness, and is started in init().
        schedulerThread = threadFactory.newThread( scheduler );
        int priority = Thread.NORM_PRIORITY + 1;
        schedulerThread.setPriority( priority );
    }

    @Override
    public void setTopLevelGroupName( String name )
    {
        try
        {
            topLevelGroup.setName( name );
        }
        catch ( Exception ignore )
        {
        }
    }

    @Override
    public void init()
    {
        schedulerThread.start();
        started = true;
    }

    @Override
    public Executor executor( Group group )
    {
        return job -> schedule( group, job );
    }

    @Override
    public ExecutorService workStealingExecutor( Group group, int parallelism )
    {
        return workStealingExecutors.computeIfAbsent( group, g -> createNewWorkStealingExecutor( g, parallelism ) );
    }

    @Override
    public ThreadFactory threadFactory( Group group )
    {
        return pools.getThreadPool( group ).getThreadFactory();
    }

    private ExecutorService createNewWorkStealingExecutor( Group group, int parallelism )
    {
        ForkJoinPool.ForkJoinWorkerThreadFactory factory =
                new GroupedDaemonThreadFactory( group, topLevelGroup );
        return new ForkJoinPool( parallelism, factory, null, false );
    }

    @Override
    public JobHandle schedule( Group group, Runnable job )
    {
        if ( !started )
        {
            throw new RejectedExecutionException( "Scheduler is not started" );
        }
        return pools.submit( group, job );
    }

    @Override
    public JobHandle scheduleRecurring( Group group, final Runnable runnable, long period, TimeUnit timeUnit )
    {
        return scheduleRecurring( group, runnable, 0, period, timeUnit );
    }

    @Override
    public JobHandle scheduleRecurring( Group group, Runnable runnable, long initialDelay, long period, TimeUnit unit )
    {
        return scheduler.submit(
                group, runnable, unit.toNanos( initialDelay ), unit.toNanos( period ) );
    }

    @Override
    public JobHandle schedule( Group group, final Runnable runnable, long initialDelay, TimeUnit unit )
    {
        return scheduler.submit( group, runnable, unit.toNanos( initialDelay ), 0 );
    }

    @Override
    public void shutdown()
    {
        started = false;

        // First shut down the scheduler, so no new tasks are queued up in the pools.
        InterruptedException exception = shutDownScheduler();

        // Then shut down the thread pools. This involves cancelling jobs which hasn't been cancelled already,
        // so we avoid having to wait the full maximum wait time on the executor service shut-downs.
        exception = Exceptions.chain( exception, pools.shutDownAll() );

        // Finally, we shut the work-stealing executors down.
        for ( ExecutorService workStealingExecutor : workStealingExecutors.values() )
        {
            exception = shutdownPool( workStealingExecutor, exception );
        }
        workStealingExecutors.clear();

        if ( exception != null )
        {
            throw new RuntimeException( "Unable to shut down job scheduler properly.", exception );
        }
    }

    private InterruptedException shutDownScheduler()
    {
        scheduler.stop();
        try
        {
            schedulerThread.join();
        }
        catch ( InterruptedException e )
        {
            return e;
        }
        return null;
    }

    private InterruptedException shutdownPool( ExecutorService pool, InterruptedException exception )
    {
        if ( pool != null )
        {
            pool.shutdown();
            try
            {
                pool.awaitTermination( 30, TimeUnit.SECONDS );
            }
            catch ( InterruptedException e )
            {
                return Exceptions.chain( exception, e );
            }
        }
        return exception;
    }
}
