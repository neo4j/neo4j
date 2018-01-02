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
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CountingJobScheduler implements JobScheduler
{
    private final AtomicInteger counter;
    private final Neo4jJobScheduler delegate;

    public CountingJobScheduler( AtomicInteger counter, Neo4jJobScheduler delegate )
    {
        this.counter = counter;
        this.delegate = delegate;
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
    public JobHandle schedule( Group group, Runnable job )
    {
        counter.getAndIncrement();
        return delegate.schedule( group, job );
    }

    @Override
    public JobHandle schedule( Group group, Runnable job, Map<String,String> metadata )
    {
        counter.getAndIncrement();
        return delegate.schedule( group, job, metadata );
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
