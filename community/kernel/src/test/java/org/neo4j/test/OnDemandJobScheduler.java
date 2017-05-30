/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.neo4j.scheduler.JobScheduler;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.scheduler.JobScheduler.Group.NO_METADATA;

public class OnDemandJobScheduler extends LifecycleAdapter implements JobScheduler
{
    private List<Runnable> jobs = new CopyOnWriteArrayList<>();

    private final boolean removeJobsAfterExecution;

    public OnDemandJobScheduler()
    {
        this( true );
    }

    public OnDemandJobScheduler( boolean removeJobsAfterExecution )
    {
        this.removeJobsAfterExecution = removeJobsAfterExecution;
    }

    @Override
    public Executor executor( Group group )
    {
        return command -> jobs.add( command );
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
        jobs.add( job );
        return new OnDemandJobHandle();
    }

    @Override
    public JobHandle schedule( Group group, Runnable job, long initialDelay, TimeUnit timeUnit )
    {
        jobs.add( job );
        return new OnDemandJobHandle();
    }

    @Override
    public JobHandle scheduleRecurring( Group group, Runnable runnable, long period, TimeUnit timeUnit )
    {
        jobs.add( runnable );
        return new OnDemandJobHandle();
    }

    @Override
    public JobHandle scheduleRecurring( Group group, Runnable runnable, long initialDelay,
            long period, TimeUnit timeUnit )
    {
        jobs.add( runnable );
        return new OnDemandJobHandle();
    }

    public Runnable getJob()
    {
        return jobs.size() > 0 ? jobs.get( 0 ) : null;
    }

    public void runJob()
    {
        for ( Runnable job : jobs )
        {
            job.run();
            if ( removeJobsAfterExecution )
            {
                jobs.remove( job );
            }
        }
    }

    private class OnDemandJobHandle implements JobHandle
    {
        @Override
        public void cancel( boolean mayInterruptIfRunning )
        {
            jobs.clear();
        }

        @Override
        public void waitTermination() throws InterruptedException, ExecutionException
        {
            // on demand
        }
    }
}
