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

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class JobSchedulerAdapter implements JobScheduler
{
    @Override
    public void init()
    {   // no-op
    }

    @Override
    public void start()
    {   // no-op
    }

    @Override
    public void stop()
    {   // no-op
    }

    @Override
    public void shutdown()
    {   // no-op
    }

    @Override
    public void setTopLevelGroupName( String name )
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
    public JobHandle schedule( Group group, Runnable job )
    {
        return null;
    }

    @Override
    public JobHandle schedule( Group group, Runnable runnable, long initialDelay,
            TimeUnit timeUnit )
    {
        return null;
    }

    @Override
    public JobHandle scheduleRecurring( Group group, Runnable runnable, long period,
            TimeUnit timeUnit )
    {
        return null;
    }

    @Override
    public JobHandle scheduleRecurring( Group group, Runnable runnable, long initialDelay,
            long period, TimeUnit timeUnit )
    {
        return null;
    }

    @Override
    public ExecutorService workStealingExecutor( Group group, int parallelism )
    {
        return null;
    }
}
