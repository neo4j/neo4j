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
package org.neo4j.test;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.kernel.impl.util.JobScheduler.Group.*;

public class OnDemandJobScheduler extends LifecycleAdapter implements JobScheduler
{
    private Runnable job;

    @Override
    public Executor executor( Group group )
    {
        return new Executor()
        {
            @Override
            public void execute( Runnable command )
            {
                job = command;
            }
        };
    }

    @Override
    public ThreadFactory threadFactory( Group group )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public JobHandle schedule( Group group, Runnable job )
    {
        return this.schedule( group, job, NO_METADATA );
    }

    @Override
    public JobHandle schedule( Group group, Runnable job, Map<String,String> metadata )
    {
        this.job = job;
        return new OnDemandJobHandle();
    }

    @Override
    public JobHandle schedule( Group group, Runnable job, long initialDelay, TimeUnit timeUnit )
    {
        this.job = job;
        return new OnDemandJobHandle();
    }

    @Override
    public JobHandle scheduleRecurring( Group group, Runnable runnable, long period, TimeUnit timeUnit )
    {
        this.job = runnable;
        return new OnDemandJobHandle();
    }

    @Override
    public JobHandle scheduleRecurring( Group group, Runnable runnable, long initialDelay,
            long period, TimeUnit timeUnit )
    {
        this.job = runnable;
        return new OnDemandJobHandle();
    }

    public Runnable getJob()
    {
        return job;
    }

    public void runJob()
    {
        job.run();
    }

    private class OnDemandJobHandle implements JobHandle
    {
        @Override
        public void cancel( boolean mayInterruptIfRunning )
        {
            job = null;
        }
    }
}
