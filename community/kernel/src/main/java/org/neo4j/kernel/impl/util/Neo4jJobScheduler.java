/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import static java.util.concurrent.Executors.newCachedThreadPool;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.DaemonThreadFactory;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class Neo4jJobScheduler extends LifecycleAdapter implements JobScheduler
{
    private final StringLogger log;
    private final String id;

    private ExecutorService executor;
    private ScheduledThreadPoolExecutor scheduledExecutor;
    private final ConcurrentMap<Runnable, ScheduledFuture<?>> recurringJobs = new ConcurrentHashMap<>();

    public Neo4jJobScheduler( StringLogger log )
    {
        this.log = log;
        this.id = getClass().getSimpleName();
    }

    public Neo4jJobScheduler( String id, StringLogger log )
    {
        this.log = log;
        this.id = id;
    }

    @Override
    public void init()
    {
        this.executor = newCachedThreadPool(new DaemonThreadFactory("Neo4j " + id));
        this.scheduledExecutor = new ScheduledThreadPoolExecutor( 2 );

        //scheduledExecutor.setContinueExistingPeriodicTasksAfterShutdownPolicy( false );
        //scheduledExecutor.setExecuteExistingDelayedTasksAfterShutdownPolicy( false );
    }

    @Override
    public void schedule( Group group, Runnable job )
    {
        this.executor.submit( job );
    }

    @Override
    public void scheduleRecurring( Group group, final Runnable runnable, long period, TimeUnit timeUnit )
    {
        scheduleRecurring( group, runnable, 0, period, timeUnit );
    }

    @Override
    public void scheduleRecurring( Group group, final Runnable runnable, long initialDelay, long period, TimeUnit timeUnit )
    {
        ScheduledFuture<?> scheduled = scheduledExecutor.scheduleAtFixedRate( runnable, initialDelay, period, timeUnit );
        if(recurringJobs.putIfAbsent( runnable, scheduled ) != null)
        {
            scheduled.cancel( true );
            throw new IllegalArgumentException( runnable + " is already scheduled. Please implement a unique " +
                    ".equals() method for each runnable you would like to execute." );
        }
    }

    @Override
    public void cancelRecurring( Group group, Runnable runnable )
    {
        ScheduledFuture<?> toCancel = recurringJobs.remove( runnable );
        if(toCancel != null)
        {
            toCancel.cancel( false );
        }
    }

    @Override
    public void shutdown() throws Throwable
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
}
