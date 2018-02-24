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
package org.neo4j.kernel.impl.scheduler;

import java.util.concurrent.ExecutionException;

import org.neo4j.concurrent.BinaryLatch;
import org.neo4j.scheduler.JobScheduler;

final class ScheduledTask implements Runnable, JobScheduler.CancelListener
{
    private final BinaryLatch handleRelease;
    private final JobScheduler scheduler;
    private final JobScheduler.Group group;
    private final Runnable task;
    private volatile JobScheduler.JobHandle latestHandle;
    private volatile Throwable lastException;

    ScheduledTask( JobScheduler scheduler, JobScheduler.Group group, Runnable task )
    {
        handleRelease = new BinaryLatch();
        this.scheduler = scheduler;
        this.group = group;
        this.task = () ->
        {
            try
            {
                task.run();
            }
            catch ( Throwable e )
            {
                lastException = e;
            }
        };
    }

    @Override
    public void run()
    {
        checkPreviousRunFailure();
        latestHandle = scheduler.schedule( group, task );
        handleRelease.release();
    }

    private void checkPreviousRunFailure()
    {
        Throwable e = lastException;
        if ( e != null )
        {
            if ( e instanceof Error )
            {
                throw (Error) e;
            }
            if ( e instanceof RuntimeException )
            {
                throw (RuntimeException) e;
            }
            throw new RuntimeException( e );
        }
    }

    @Override
    public void cancelled( boolean mayInterruptIfRunning )
    {
        JobScheduler.JobHandle handle = this.latestHandle;
        if ( handle != null )
        {
            handle.cancel( mayInterruptIfRunning );
        }
    }

    public void waitTermination() throws ExecutionException, InterruptedException
    {

        handleRelease.await();

        latestHandle.waitTermination();
    }
}
