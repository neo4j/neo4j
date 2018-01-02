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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.scheduler.JobScheduler;

import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.neo4j.helpers.NamedThreadFactory.daemon;
import static org.neo4j.kernel.impl.util.DebugUtil.trackTest;
import static org.neo4j.scheduler.JobScheduler.Group.NO_METADATA;

public class Neo4jJobScheduler extends LifecycleAdapter implements JobScheduler
{
    private ExecutorService globalPool;
    private ScheduledThreadPoolExecutor scheduledExecutor;

    // Contains JobHandles which hasn't been cancelled yet, this to be able to cancel those when shutting down
    // This Set is synchronized, which is fine because there are only a handful of jobs generally and only
    // added when starting a database.
    private final Set<JobHandle> jobs = Collections.synchronizedSet( new HashSet<>() );

    // Contains workStealingExecutors for each group that have asked for one.
    // If threads need to be created, they need to be inside one of these pools.
    // We also need to remember to shutdown all pools when we shutdown the database to shutdown queries in an orderly fashion.
    private final ConcurrentHashMap<Group,ExecutorService> workStealingExecutors = new ConcurrentHashMap<>( 1 );

    @Override
    public void init()
    {
        this.globalPool = newCachedThreadPool( daemon( "neo4j.Pooled" + trackTest() ) );
        this.scheduledExecutor = new ScheduledThreadPoolExecutor( 2, daemon( "neo4j.Scheduled" + trackTest() ) );
    }

    @Override
    public Executor executor( final Group group )
    {
        return job -> schedule( group, job );
    }

    @Override
    public ExecutorService workStealingExecutor( Group group, int parallelism )
    {
        return workStealingExecutors.computeIfAbsent( group, g -> createNewWorkStealingExecutor( g, parallelism ) );
    }

    @Override
    public ThreadFactory threadFactory( final Group group )
    {
        return job -> createNewThread( group, job, NO_METADATA );
    }

    private ExecutorService createNewWorkStealingExecutor( Group group, int parallelism )
    {
        final ForkJoinPool.ForkJoinWorkerThreadFactory factory = pool ->
        {
            final ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread( pool );
            worker.setName( group.threadName( new HashMap<>() ) );
            return worker;
        };

        return new ForkJoinPool( parallelism, factory, null, false );
    }

    @Override
    public JobHandle schedule( Group group, Runnable job )
    {
        return schedule( group, job, NO_METADATA );
    }

    @Override
    public JobHandle schedule( Group group, Runnable job, Map<String,String> metadata )
    {
        if ( globalPool == null )
        {
            throw new RejectedExecutionException( "Scheduler is not started" );
        }

        return register( new PooledJobHandle( this.globalPool.submit( job ) ) );
    }

    private JobHandle register( PooledJobHandle pooledJobHandle )
    {
        jobs.add( pooledJobHandle );

        // Return a JobHandle which removes itself from this register,
        // otherwise functions like the supplied handle
        return new JobHandle()
        {
            @Override
            public void waitTermination() throws InterruptedException, ExecutionException
            {
                pooledJobHandle.waitTermination();
            }

            @Override
            public void cancel( boolean mayInterruptIfRunning )
            {
                pooledJobHandle.cancel( mayInterruptIfRunning );
                jobs.remove( pooledJobHandle );
            }

            @Override
            public void registerCancelListener( CancelListener listener )
            {
                pooledJobHandle.registerCancelListener( listener );
            }
        };
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
        return new PooledJobHandle( scheduledExecutor.scheduleAtFixedRate( runnable, initialDelay, period, timeUnit ) );
    }

    @Override
    public JobHandle schedule( Group group, final Runnable runnable, long initialDelay, TimeUnit timeUnit )
    {
        return new PooledJobHandle( scheduledExecutor.schedule( runnable, initialDelay, timeUnit ) );
    }

    @Override
    public void stop()
    {
    }

    @Override
    public void shutdown()
    {
        RuntimeException exception = null;
        try
        {
            // Cancel jobs which hasn't been cancelled already, this to avoid having to wait the full
            // max wait time and then just leave them.
            for ( JobHandle handle : jobs )
            {
                handle.cancel( true );
            }
            jobs.clear();

            shutdownPool( globalPool );
        }
        catch ( RuntimeException e )
        {
            exception = e;
        }
        finally
        {
            globalPool = null;
        }

        try
        {
            shutdownPool( scheduledExecutor );
        }
        catch ( RuntimeException e )
        {
            exception = Exceptions.chain( exception, e );
        }
        finally
        {
            scheduledExecutor = null;
        }

        for ( ExecutorService executor : workStealingExecutors.values() )
        {
            try
            {
                shutdownPool( executor );
            }
            catch ( RuntimeException e )
            {
                exception = Exceptions.chain( exception, e );
            }
            finally
            {
                scheduledExecutor = null;
            }
        }

        if ( exception != null )
        {
            throw new RuntimeException( "Unable to shut down job scheduler properly.", exception );
        }
    }

    private void shutdownPool( ExecutorService pool )
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
                throw new RuntimeException( e );
            }
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
        private final List<CancelListener> cancelListeners = new CopyOnWriteArrayList<>();

        PooledJobHandle( Future<?> job )
        {
            this.job = job;
        }

        @Override
        public void cancel( boolean mayInterruptIfRunning )
        {
            job.cancel( mayInterruptIfRunning );
            for ( CancelListener cancelListener : cancelListeners )
            {
                cancelListener.cancelled( mayInterruptIfRunning );
            }
        }

        @Override
        public void waitTermination() throws InterruptedException, ExecutionException
        {
            job.get();
        }

        @Override
        public void registerCancelListener( CancelListener listener )
        {
            cancelListeners.add( listener );
        }
    }
}
