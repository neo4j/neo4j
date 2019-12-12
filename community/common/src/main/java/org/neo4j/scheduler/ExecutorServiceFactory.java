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
package org.neo4j.scheduler;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
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
    static ExecutorServiceFactory cachedWithBackPressure()
    {
        return ( group, factory, threadCount ) ->
        {
            if ( threadCount == 0 )
            {
                threadCount = getRuntime().availableProcessors();
            }
            BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>( 2 * threadCount );
            RejectedExecutionHandler callerRuns = new ThreadPoolExecutor.CallerRunsPolicy();
            return new ThreadPoolExecutor( 0, threadCount, 60L, TimeUnit.SECONDS, workQueue, factory, callerRuns );
        };
    }

    /**
     * An executor service that does not allow any submissions.
     */
    @SuppressWarnings( "NullableProblems" )
    class ThrowingExecutorService implements ExecutorService
    {
        private final Group group;
        private volatile boolean shutodwn;

        private ThrowingExecutorService( Group group )
        {
            this.group = group;
        }

        @Override
        public void shutdown()
        {
            shutodwn = true;
        }

        @Override
        public List<Runnable> shutdownNow()
        {
            return Collections.emptyList();
        }

        @Override
        public boolean isShutdown()
        {
            return shutodwn;
        }

        @Override
        public boolean isTerminated()
        {
            return shutodwn;
        }

        @Override
        public boolean awaitTermination( long timeout, TimeUnit unit )
        {
            return true;
        }

        @Override
        public <T> Future<T> submit( Callable<T> task )
        {
            throw newUnschedulableException( group );
        }

        @Override
        public <T> Future<T> submit( Runnable task, T result )
        {
            throw newUnschedulableException( group );
        }

        @Override
        public Future<?> submit( Runnable task )
        {
            throw newUnschedulableException( group );
        }

        @Override
        public <T> List<Future<T>> invokeAll( Collection<? extends Callable<T>> tasks )
        {
            throw newUnschedulableException( group );
        }

        @Override
        public <T> List<Future<T>> invokeAll( Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit )
        {
            throw newUnschedulableException( group );
        }

        @Override
        public <T> T invokeAny( Collection<? extends Callable<T>> tasks )
        {
            throw newUnschedulableException( group );
        }

        @Override
        public <T> T invokeAny( Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit )
        {
            throw newUnschedulableException( group );
        }

        @Override
        public void execute( Runnable command )
        {
            throw newUnschedulableException( group );
        }

        private static RejectedExecutionException newUnschedulableException( Group group )
        {
            return new RejectedExecutionException( "Tasks cannot be scheduled directly to the " + group.groupName() + " group." );
        }
    }
}
