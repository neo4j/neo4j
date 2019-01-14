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
package org.neo4j.kernel.impl.util;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.kernel.impl.scheduler.CentralJobScheduler;
import org.neo4j.scheduler.JobScheduler;

public class CountingJobScheduler implements JobScheduler
{
    private final AtomicInteger counter;
    private final CentralJobScheduler delegate;

    public CountingJobScheduler( AtomicInteger counter, CentralJobScheduler delegate )
    {
        this.counter = counter;
        this.delegate = delegate;
    }

    @Override
    public void setTopLevelGroupName( String name )
    {
        delegate.setTopLevelGroupName( name );
    }

    @Override
    public Executor executor( Group group )
    {
        return delegate.executor( group );
    }

    @Override
    public ThreadFactory threadFactory( Group group )
    {
        return delegate.threadFactory( group );
    }

    @Override
    public ExecutorService workStealingExecutor( Group group, int parallelism )
    {
        return delegate.workStealingExecutor( group, parallelism );
    }

    @Override
    public JobHandle schedule( Group group, Runnable job )
    {
        counter.getAndIncrement();
        return delegate.schedule( group, job );
    }

    @Override
    public JobHandle schedule( Group group, Runnable runnable, long initialDelay, TimeUnit timeUnit )
    {
        counter.getAndIncrement();
        return delegate.schedule( group, runnable, initialDelay, timeUnit );
    }

    @Override
    public JobHandle scheduleRecurring( Group group, Runnable runnable, long period,
                                        TimeUnit timeUnit )
    {
        counter.getAndIncrement();
        return delegate.scheduleRecurring( group, runnable, period, timeUnit );
    }

    @Override
    public JobHandle scheduleRecurring( Group group, Runnable runnable, long initialDelay, long period,
                                        TimeUnit timeUnit )
    {
        counter.getAndIncrement();
        return delegate.scheduleRecurring( group, runnable, initialDelay, period, timeUnit );
    }

    @Override
    public void init()
    {
        delegate.init();
    }

    @Override
    public void start() throws Throwable
    {
        delegate.start();
    }

    @Override
    public void stop() throws Throwable
    {
        delegate.stop();
    }

    @Override
    public void shutdown()
    {
        delegate.shutdown();
    }
}
