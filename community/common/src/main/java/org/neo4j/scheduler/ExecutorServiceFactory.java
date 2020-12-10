/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.scheduler;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.lang.Runtime.getRuntime;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

/**
 * Implementations of this interface are used by the {@link JobHandle} implementation to create the underlying {@link ExecutorService}s that actually run the
 * scheduled jobs. The choice of implementation is decided by the scheduling {@link Group}, which can thereby influence how jobs in the particular group are
 * executed.
 */
interface ExecutorServiceFactory
{
    /**
     * Create an {@link ExecutorService}, ideally with the desired thread count if possible.
     * Implementations are allowed to ignore the given thread count.
     *
     * @param group the group the executor service will handle.
     * @param factory the thread factory to use.
     * @param threadCount the desired thread count, 0 implies unlimited.
     */
    ExecutorService build( Group group, SchedulerThreadFactory factory, int threadCount );

    /**
     * This factory actually prevents the scheduling and execution of any jobs, which is useful for groups that are not meant to be scheduled directly.
     */
    static ExecutorServiceFactory unschedulable()
    {
        return ( group, factory, threadCount ) -> new ThrowingExecutorService( group );
    }

    /**
     * Executes all jobs in the same single thread.
     */
    static ExecutorServiceFactory singleThread()
    {
        return ( group, factory, threadCount ) ->
        {
            return newSingleThreadExecutor( factory ); // Just ignore the thread count.
        };
    }

    /**
     * Executes all jobs on the calling thread.
     */
    static ExecutorServiceFactory callingThread()
    {
        return ( group, factory, threadCount ) -> new CallingThreadExecutorService( group );
    }

    /**
     * Execute jobs in a dynamically growing pool of threads. The threads will be cached and kept around for a little while to cope with work load spikes
     * and troughs.
     */
    static ExecutorServiceFactory cached()
    {
        return ( group, factory, threadCount ) ->
        {
            if ( threadCount == 0 )
            {
                return newCachedThreadPool( factory );
            }
            return newFixedThreadPool( threadCount, factory );
        };
    }

    /**
     * Schedules jobs in a work-stealing (ForkJoin) thread pool. {@link java.util.stream.Stream#parallel Parallel streams} and {@link ForkJoinTask}s started
     * from within the scheduled jobs will also run inside the same {@link ForkJoinPool}.
     */
    static ExecutorServiceFactory workStealing()
    {
        return ( group, factory, threadCount ) ->
        {
            if ( threadCount == 0 )
            {
                threadCount = getRuntime().availableProcessors();
            }
            return new ForkJoinPool( threadCount, factory, null, false );
        };
    }

    /**
     * Schedules jobs in a work-stealing (ForkJoin) thread pool, configuring to be in an "asynchronous" mode, which is more suitable for event-processing.
     * <p>
     * You can read more about asynchronous mode in the {@link ForkJoinPool} documentation.
     * <p>
     * {@link java.util.stream.Stream#parallel Parallel streams} and {@link ForkJoinTask}s started from within the scheduled jobs will also run inside the
     * same {@link ForkJoinPool}.
     */
    static ExecutorServiceFactory workStealingAsync()
    {
        return ( group, factory, threadCount ) ->
        {
            if ( threadCount == 0 )
            {
                threadCount = getRuntime().availableProcessors();
            }
            return new ForkJoinPool( threadCount, factory, null, true );
        };
    }

    /**
     * Execute jobs in fixed size pool of threads and if job queue fills up, the caller executes the job and thereby applying back pressure.
     */
    static ExecutorServiceFactory fixedWithBackPressure()
    {
        return ( group, factory, threadCount ) ->
        {
            if ( threadCount == 0 )
            {
                threadCount = getRuntime().availableProcessors();
            }
            BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>( 2 * threadCount );
            RejectedExecutionHandler policy = new ThreadPoolExecutor.CallerRunsPolicy();
            return new ThreadPoolExecutor( threadCount, threadCount, 0, TimeUnit.SECONDS, workQueue, factory, policy );
        };
    }

    /**
     * Will execute at most thread-count jobs concurrently, and silently discard tasks when over-subscribed.
     * Tasks are not queued, but either stared immediately or discarded.
     * Threads are cached for one minute and reused when possible.
     */
    static ExecutorServiceFactory cachedWithDiscard()
    {
        return ( group, factory, threadCount ) ->
        {
            if ( threadCount == 0 )
            {
                return new DiscardingExecutorService( group );
            }
            ThreadPoolExecutor.DiscardPolicy policy = new ThreadPoolExecutor.DiscardPolicy();
            return new ThreadPoolExecutor( 0, threadCount, 60L, TimeUnit.SECONDS, new SynchronousQueue<>(), factory, policy );
        };
    }

    abstract class ExecutorServiceAdapter extends AbstractExecutorService
    {
        protected final Group group;
        private volatile boolean shutdown;

        private ExecutorServiceAdapter( Group group )
        {
            this.group = group;
        }

        @Override
        public void shutdown()
        {
            shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow()
        {
            return Collections.emptyList();
        }

        @Override
        public boolean isShutdown()
        {
            return shutdown;
        }

        @Override
        public boolean isTerminated()
        {
            return shutdown;
        }

        @Override
        public boolean awaitTermination( long timeout, TimeUnit unit )
        {
            return true;
        }
    }

    /**
     * An executor service which always throws a {@link RejectedExecutionException} on any task submission.
     */
    class ThrowingExecutorService extends ExecutorServiceAdapter
    {
        private ThrowingExecutorService( Group group )
        {
            super( group );
        }

        @Override
        public void execute( Runnable runnable )
        {
            throw new RejectedExecutionException( "Tasks cannot be scheduled directly to the " + group.groupName() + " group." );
        }
    }

    class DiscardingExecutorService extends ExecutorServiceAdapter
    {
        private DiscardingExecutorService( Group group )
        {
            super( group );
        }

        @Override
        public void execute( Runnable runnable )
        {
            if ( runnable instanceof FutureTask )
            {
                ((FutureTask<?>) runnable).cancel( false );
            }
        }
    }

    /**
     * An executor service which always executes the runnable on the calling thread.
     */
    class CallingThreadExecutorService extends ExecutorServiceAdapter
    {
        private CallingThreadExecutorService( Group group )
        {
            super( group );
        }

        @Override
        public void execute( Runnable runnable )
        {
            runnable.run();
        }
    }
}
