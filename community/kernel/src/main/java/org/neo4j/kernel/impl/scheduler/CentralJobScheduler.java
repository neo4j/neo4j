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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.Clocks;

public class CentralJobScheduler extends LifecycleAdapter implements JobScheduler, AutoCloseable
{
    private static final AtomicInteger INSTANCE_COUNTER = new AtomicInteger();

    private final TimeBasedTaskScheduler scheduler;
    private final Thread schedulerThread;
    private final ConcurrentHashMap<Group,Integer> desiredParallelism;

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

    protected CentralJobScheduler()
    {
        desiredParallelism = new ConcurrentHashMap<>( 4 );
        topLevelGroup = new TopLevelGroup();
        pools = new ThreadPoolManager( topLevelGroup );
        ThreadFactory threadFactory = new GroupedDaemonThreadFactory( Group.TASK_SCHEDULER, topLevelGroup );
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
    public void setParallelism( Group group, int parallelism )
    {
        desiredParallelism.putIfAbsent( group, parallelism );
    }

    @Override
    public void init()
    {
        if ( !started )
        {
            schedulerThread.start();
            started = true;
        }
    }

    @Override
    public Executor executor( Group group )
    {
        return getThreadPool( group ).getExecutorService();
    }

    @Override
    public ThreadFactory threadFactory( Group group )
    {
        return getThreadPool( group ).getThreadFactory();
    }

    private ThreadPool getThreadPool( Group group )
    {
        return pools.getThreadPool( group, desiredParallelism.get( group ) );
    }

    @Override
    public ThreadFactory interruptableThreadFactory( Group group )
    {
        return getThreadPool( group ).getInterruptableThreadFactory();
    }

    @Override
    public JobHandle schedule( Group group, Runnable job )
    {
        if ( !started )
        {
            throw new RejectedExecutionException( "Scheduler is not started" );
        }
        return getThreadPool( group ).submit( job );
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

        if ( exception != null )
        {
            throw new RuntimeException( "Unable to shut down job scheduler properly.", exception );
        }
    }

    @Override
    public void close()
    {
        shutdown();
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
}
