/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class FakeJobScheduler implements JobScheduler
{
    private final Set<Runnable> recurringJobs = new HashSet<>();

    @Override
    public JobHandle schedule( Group group, Runnable job )
    {
        throw new UnsupportedOperationException( "Not yet implemented" );
    }

    @Override
    public JobHandle scheduleRecurring( Group group, final Runnable runnable, long period, TimeUnit timeUnit )
    {
        recurringJobs.add(runnable);
        return new JobHandle()
        {
            @Override
            public void cancel( boolean mayInterruptIfRunning )
            {
                recurringJobs.remove( runnable );
            }
        };
    }

    @Override
    public JobHandle scheduleRecurring( Group group, Runnable runnable, long initialDelay, long period, TimeUnit timeUnit )
    {
        return scheduleRecurring( group, runnable, period, timeUnit );
    }

    public void runAllRecurringJobs()
    {
        for ( Runnable runnable : recurringJobs )
        {
            runnable.run();
        }
    }

    @Override
    public void init() throws Throwable
    {
    }

    @Override
    public void start() throws Throwable
    {
    }

    @Override
    public void stop() throws Throwable
    {
    }

    @Override
    public void shutdown() throws Throwable
    {
    }
}

