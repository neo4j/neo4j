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
package org.neo4j.kernel.impl.util;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.neo4j.helpers.NamedThreadFactory.daemon;
import static org.neo4j.kernel.impl.util.DebugUtil.trackTest;
import static org.neo4j.kernel.impl.util.JobScheduler.Group.NO_METADATA;

public class Neo4jJobScheduler extends LifecycleAdapter implements JobScheduler
{
    private ExecutorService globalPool;
    private ScheduledThreadPoolExecutor scheduledExecutor;

    @Override
    public void init()
    {
        this.globalPool = newCachedThreadPool( daemon( "neo4j.Pooled" + trackTest() ) );
        this.scheduledExecutor = new ScheduledThreadPoolExecutor( 2, daemon( "neo4j.Scheduled" + trackTest() ) );
    }

    @Override
    public Executor executor( final Group group )
    {
        return new Executor()
        {
            @Override
            public void execute( Runnable command )
            {
                schedule( group, command );
            }
        };
    }

    @Override
    public ThreadFactory threadFactory( final Group group )
    {
        return new ThreadFactory()
        {
            @Override
            public Thread newThread( Runnable r )
            {
                return createNewThread( group, r, NO_METADATA );
            }
        };
    }

    @Override
    public JobHandle schedule( Group group, Runnable job )
    {
        return schedule( group, job, NO_METADATA );
    }

    @Override
    public JobHandle schedule( Group group, Runnable job, Map<String,String> metadata )
    {
        if (globalPool == null)
            throw new RejectedExecutionException( "Scheduler is not started" );

        switch( group.strategy() )
        {
        case POOLED:
            return new PooledJobHandle( this.globalPool.submit( job ) );
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
        switch ( group.strategy() )
        {
        case POOLED:
            return new PooledJobHandle( scheduledExecutor.scheduleAtFixedRate( runnable, initialDelay, period, timeUnit ) );
        default:
            throw new IllegalArgumentException( "Unsupported strategy to use for recurring jobs: " + group.strategy() );
        }
    }

    @Override
    public JobHandle schedule( Group group, final Runnable runnable, long initialDelay, TimeUnit timeUnit )
    {
        switch ( group.strategy() )
        {
        case POOLED:
            return new PooledJobHandle( scheduledExecutor.schedule( runnable, initialDelay, timeUnit ) );
        default:
            throw new IllegalArgumentException( "Unsupported strategy to use for delayed jobs: " + group.strategy() );
        }
    }

    @Override
    public void shutdown()
    {
        RuntimeException exception = null;
        try
        {
            if( globalPool != null)
            {
                globalPool.shutdownNow();
                globalPool.awaitTermination( 5, TimeUnit.SECONDS );
                globalPool = null;
            }
        } catch(RuntimeException e)
        {
            exception = e;
        }
        catch ( InterruptedException e )
        {
            exception = new RuntimeException(e);
        }

        try
        {
            if(scheduledExecutor != null)
            {
                scheduledExecutor.shutdown();
                scheduledExecutor.awaitTermination( 5, TimeUnit.SECONDS );
                scheduledExecutor = null;
            }
        } catch(RuntimeException e)
        {
            exception = e;
        }
        catch ( InterruptedException e )
        {
            exception = new RuntimeException(e);
        }

        if(exception != null)
        {
            throw new RuntimeException( "Unable to shut down job scheduler properly.", exception);
        }
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
    }
}
