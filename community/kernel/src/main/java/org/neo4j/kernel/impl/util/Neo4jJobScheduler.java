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
package org.neo4j.kernel.impl.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.neo4j.concurrent.Scheduler;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static java.util.concurrent.Executors.callable;
import static org.neo4j.concurrent.Scheduler.OnRejection.THROW;
import static org.neo4j.kernel.impl.util.JobScheduler.Group.NO_METADATA;

public class Neo4jJobScheduler extends LifecycleAdapter implements JobScheduler
{
    private final ConcurrentHashMap<Object, JobHandle> recurringJobs = new ConcurrentHashMap<>();

    @Override
    public void init()
    {
    }

    @Override
    public void stop()
    {
    }

    @Override
    public Executor executor( final Group group )
    {
        return job -> schedule( group, job );
    }

    @Override
    public ThreadFactory threadFactory( final Group group )
    {
        return job -> createNewThread( group, job, NO_METADATA );
    }

    @Override
    public JobHandle schedule( Group group, Runnable job )
    {
        return schedule( group, job, NO_METADATA );
    }

    @Override
    public JobHandle schedule( Group group, Runnable job, Map<String,String> metadata )
    {
        switch( group.strategy() )
        {
        case POOLED:
            return new PooledJobHandle( Scheduler.executeIOBound( callable( job ), THROW ) );
        case NEW_THREAD:
            Thread thread = createNewThread( group, job, metadata );
            thread.start();
            return new SingleThreadHandle( thread );
        default:
            throw new IllegalArgumentException( "Unsupported strategy for scheduling job: " + group.strategy() );
        }
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
        Object jobTag = new Object();
        switch ( group.strategy() )
        {
        case POOLED:
            Future<?> future = Scheduler.executeRecurring( runnable, initialDelay, period, timeUnit );
            PooledJobHandle handle = new PooledJobHandle( future );
            return register( jobTag, handle );
        default:
            throw new IllegalArgumentException( "Unsupported strategy to use for recurring jobs: " + group.strategy() );
        }
    }

    private JobHandle register( Object jobTag, PooledJobHandle handle )
    {
        recurringJobs.put( jobTag, handle );
        return new JobHandle()
        {
            @Override
            public void cancel( boolean mayInterruptIfRunning )
            {
                handle.cancel( mayInterruptIfRunning );
                recurringJobs.remove( jobTag );
            }

            @Override
            public void waitTermination() throws InterruptedException, ExecutionException
            {
                handle.waitTermination();
            }
        };
    }

    @Override
    public void shutdown()
    {
        recurringJobs.forEach( (tag, handle) -> handle.cancel( false ) );
    }

    /**
     * Used to spin up new threads for groups or access-patterns that don't use the pooled thread options.
     * The returned thread is not started, to allow users to modify it before setting it in motion.
     */
    private Thread createNewThread( Group group, Runnable job, Map<String,String> metadata )
    {
        Thread thread = new Thread( null, job, group.threadName( metadata ) );
        thread.setDaemon( true );
        return thread;
    }

    private static class PooledJobHandle implements JobHandle
    {
        private final Future<?> job;

        public PooledJobHandle( Future<?> job )
        {
            this.job = job;
        }

        @Override
        public void cancel( boolean mayInterruptIfRunning )
        {
            job.cancel( mayInterruptIfRunning );
        }

        @Override
        public void waitTermination() throws InterruptedException, ExecutionException
        {
            job.get();
        }
    }

    private static class SingleThreadHandle implements JobHandle
    {
        private final Thread thread;

        public SingleThreadHandle( Thread thread )
        {
            this.thread = thread;
        }

        @Override
        public void cancel( boolean mayInterruptIfRunning )
        {
            if ( mayInterruptIfRunning )
            {
                thread.interrupt();
            }
        }

        @Override
        public void waitTermination() throws InterruptedException
        {
            thread.join();
        }
    }
}
