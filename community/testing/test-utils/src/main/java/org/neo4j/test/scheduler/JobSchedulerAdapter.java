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
package org.neo4j.test.scheduler;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.resources.Profiler;
import org.neo4j.scheduler.ActiveGroup;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.scheduler.SchedulerThreadFactoryFactory;

public class JobSchedulerAdapter extends LifecycleAdapter implements JobScheduler
{
    @Override
    public void setTopLevelGroupName( String name )
    {
    }

    @Override
    public void setParallelism( Group group, int parallelism )
    {
    }

    @Override
    public void setThreadFactory( Group group, SchedulerThreadFactoryFactory threadFactory )
    {
    }

    @Override
    public Executor executor( Group group )
    {
        return null;
    }

    @Override
    public ThreadFactory threadFactory( Group group )
    {
        return null;
    }

    @Override
    public <T> JobHandle<T> schedule( Group group, Callable<T> job )
    {
        return null;
    }

    @Override
    public JobHandle<?> schedule( Group group, Runnable job )
    {
        return null;
    }

    @Override
    public JobHandle<?> schedule( Group group, Runnable runnable, long initialDelay,
            TimeUnit timeUnit )
    {
        return null;
    }

    @Override
    public JobHandle<?> scheduleRecurring( Group group, Runnable runnable, long period,
            TimeUnit timeUnit )
    {
        return null;
    }

    @Override
    public JobHandle<?> scheduleRecurring( Group group, Runnable runnable, long initialDelay,
            long period, TimeUnit timeUnit )
    {
        return null;
    }

    @Override
    public Stream<ActiveGroup> activeGroups()
    {
        return Stream.empty();
    }

    @Override
    public void profileGroup( Group group, Profiler profiler )
    {
    }

    @Override
    public void close()
    {
        try
        {
            shutdown();
        }
        catch ( Throwable throwable )
        {
            throw new RuntimeException( throwable );
        }
    }
}
