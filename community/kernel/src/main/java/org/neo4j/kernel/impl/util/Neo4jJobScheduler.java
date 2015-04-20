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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static java.util.concurrent.Executors.newCachedThreadPool;

import static org.neo4j.helpers.NamedThreadFactory.daemon;
import static org.neo4j.kernel.impl.util.DebugUtil.trackTest;

public class Neo4jJobScheduler extends LifecycleAdapter implements JobScheduler
{
    private final String id;

    private ExecutorService executor;
    private ScheduledThreadPoolExecutor scheduledExecutor;

    public Neo4jJobScheduler()
    {
        this.id = getClass().getSimpleName();
    }

    public Neo4jJobScheduler( String id )
    {
        this.id = id;
    }

    @Override
    public void init()
    {
        this.executor = newCachedThreadPool( daemon( "Neo4j " + id + trackTest() ) );
        this.scheduledExecutor = new ScheduledThreadPoolExecutor( 2, daemon( "Scheduled Neo4j " + id + trackTest() ) );
    }

    @Override
    public JobHandle schedule( Group group, Runnable job )
    {
        return new Handle( this.executor.submit( job ) );
    }

    @Override
    public JobHandle scheduleRecurring( Group group, final Runnable runnable, long period, TimeUnit timeUnit )
    {
        return scheduleRecurring( group, runnable, 0, period, timeUnit );
    }

    @Override
    public JobHandle scheduleRecurring( Group group, final Runnable runnable, long initialDelay, long period,
                                        TimeUnit timeUnit )
    {
        return new Handle( scheduledExecutor.scheduleAtFixedRate( runnable, initialDelay, period, timeUnit ) );
    }

    @Override
    public void shutdown()
    {
        RuntimeException exception = null;
        try
        {
            if(executor != null)
            {
                executor.shutdownNow();
                executor.awaitTermination( 5, TimeUnit.SECONDS );
                executor = null;
            }
        } catch(RuntimeException e)
        {
            exception = e;
        }
        catch ( InterruptedException e )
        {
            exception = new RuntimeException(e);
        }

        try
        {
            if(scheduledExecutor != null)
            {
                scheduledExecutor.shutdown();
                scheduledExecutor.awaitTermination( 5, TimeUnit.SECONDS );
                scheduledExecutor = null;
            }
        } catch(RuntimeException e)
        {
            exception = e;
        }
        catch ( InterruptedException e )
        {
            exception = new RuntimeException(e);
        }

        if(exception != null)
        {
            throw new RuntimeException( "Unable to shut down job scheduler properly.", exception);
        }
    }

    private class Handle implements JobHandle
    {
        private final Future<?> job;

        public Handle( Future<?> job )
        {
            this.job = job;
        }

        @Override
        public void cancel( boolean mayInterruptIfRunning )
        {
            job.cancel( mayInterruptIfRunning );
        }
    }
}
