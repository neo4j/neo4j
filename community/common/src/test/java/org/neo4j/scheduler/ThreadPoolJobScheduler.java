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

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static java.util.concurrent.Executors.newCachedThreadPool;

/**
 * Simple test scheduler implementation that is based on a cached thread pool.
 * All threads created by this scheduler can be identified by <i>ThreadPoolScheduler</i> prefix.
 */
public class ThreadPoolJobScheduler extends LifecycleAdapter implements JobScheduler
{
    private final ExecutorService executor = newCachedThreadPool( new DaemonThreadFactory( "ThreadPoolScheduler" ) );

    @Override
    public void setTopLevelGroupName( String name )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Executor executor( Group group )
    {
        return executor;
    }

    @Override
    public ExecutorService workStealingExecutor( Group group, int parallelism )
    {
        return executor;
    }

    @Override
    public ExecutorService workStealingExecutorAsyncMode( Group group, int parallelism )
    {
        return executor;
    }

    @Override
    public ThreadFactory threadFactory( Group group )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public JobHandle schedule( Group group, Runnable job )
    {
        return new FutureJobHandle<>( executor.submit( job ) );
    }

    @Override
    public JobHandle schedule( Group group, Runnable runnable, long initialDelay, TimeUnit timeUnit )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public JobHandle scheduleRecurring( Group group, Runnable runnable, long period, TimeUnit timeUnit )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public JobHandle scheduleRecurring( Group group, Runnable runnable, long initialDelay, long period, TimeUnit timeUnit )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close()
    {
        shutdown();
    }

    @Override
    public void shutdown()
    {
        executor.shutdown();
    }

    private static class FutureJobHandle<V> implements JobHandle
    {
        private final Future<V> future;

        FutureJobHandle( Future<V> future )
        {
            this.future = future;
        }

        @Override
        public void cancel( boolean mayInterruptIfRunning )
        {
            future.cancel( mayInterruptIfRunning );
        }

        @Override
        public void waitTermination() throws InterruptedException, ExecutionException, CancellationException
        {
            future.get();
        }
    }
}
