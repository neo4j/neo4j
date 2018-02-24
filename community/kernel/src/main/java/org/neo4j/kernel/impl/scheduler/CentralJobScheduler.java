/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.scheduler;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.kernel.impl.util.DebugUtil.trackTest;

public class CentralJobScheduler extends LifecycleAdapter implements JobScheduler
{
    private static final AtomicInteger INSTANCE_COUNTER = new AtomicInteger();
    private static final Group SCHEDULER_GROUP = new Group( "Scheduler" );

    private ScheduledThreadPoolExecutor scheduledExecutor;

    // Contains workStealingExecutors for each group that have asked for one.
    // If threads need to be created, they need to be inside one of these pools.
    // We also need to remember to shutdown all pools when we shutdown the database to shutdown queries in an orderly
    // fashion.
    private final ConcurrentHashMap<Group,ExecutorService> workStealingExecutors;

    private final ThreadGroup topLevelGroup;
    private final ConcurrentHashMap<Group,ThreadPool> pools;
    private final Function<Group,ThreadPool> poolBuilder;

    private volatile boolean started;

    public CentralJobScheduler()
    {
        workStealingExecutors = new ConcurrentHashMap<>( 1 );
        topLevelGroup = new ThreadGroup( "Neo4j-" + INSTANCE_COUNTER.incrementAndGet() + trackTest() );
        pools = new ConcurrentHashMap<>();
        poolBuilder = group -> new ThreadPool( group, topLevelGroup );
    }

    @Override
    public void init()
    {
        ThreadFactory threadFactory = new GroupedDaemonThreadFactory( SCHEDULER_GROUP, topLevelGroup );
        this.scheduledExecutor = new ScheduledThreadPoolExecutor( 1, threadFactory );
        started = true;
    }

    private ThreadPool getThreadPool( Group group )
    {
        return pools.computeIfAbsent( group, poolBuilder );
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
        return getThreadPool( group ).getThreadFactory();
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
        ThreadPool threadPool = getThreadPool( group );
        return threadPool.submit( job );
    }

    @Override
    public JobHandle scheduleRecurring( Group group, final Runnable runnable, long period, TimeUnit timeUnit )
    {
        return scheduleRecurring( group, runnable, 0, period, timeUnit );
    }

    @Override
    public JobHandle scheduleRecurring( Group group, final Runnable runnable, long initialDelay, long period,
                                        TimeUnit timeUnit )
    {
        ScheduledTask scheduledTask = new ScheduledTask( this, group, runnable );
        ScheduledFuture<?> future = scheduledExecutor.scheduleAtFixedRate(
                scheduledTask, initialDelay, period, timeUnit );
        ScheduledJobHandle handle = new ScheduledJobHandle( future, scheduledTask );
        handle.registerCancelListener( scheduledTask );
        return handle;
    }

    @Override
    public JobHandle schedule( Group group, final Runnable runnable, long initialDelay, TimeUnit timeUnit )
    {
        ScheduledTask scheduledTask = new ScheduledTask( this, group, runnable );
        ScheduledFuture<?> future = scheduledExecutor.schedule( scheduledTask, initialDelay, timeUnit );
        ScheduledJobHandle handle = new ScheduledJobHandle( future, scheduledTask );
        handle.registerCancelListener( scheduledTask );
        return handle;
    }

    @Override
    public void shutdown()
    {
        started = false;

        // Cancel jobs which hasn't been cancelled already, this to avoid having to wait the full
        // max wait time and then just leave them.
        pools.forEach( ( group, pool ) -> pool.cancelAllJobs() );
        pools.forEach( ( group, pool ) -> pool.shutDown() );
        InterruptedException exception = pools.values().stream().reduce( null,
                ( e, p ) -> Exceptions.chain( e, p.getShutdownException() ), Exceptions::chain );

        ScheduledThreadPoolExecutor executor = scheduledExecutor;
        scheduledExecutor = null;
        exception = shutdownPool( executor, exception );

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
