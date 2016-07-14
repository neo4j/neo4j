/*
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

import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public abstract class PeriodicBackgroundTask extends LifecycleAdapter
{
    private final JobScheduler scheduler;
    private final long recurringPeriodMillis;
    private final Runnable job = new Runnable()
    {
        @Override
        public void run()
        {
            boolean gotExecutionPermit = false;
            try
            {
                int attempts = 100;
                do
                {
                    if ( stopped | --attempts < 0)
                    {
                        return;
                    }
                }
                while ( !executionInProcess.tryAcquire( 100, TimeUnit.MILLISECONDS ) );
                gotExecutionPermit = true;
                performTask();
            }
            catch ( IOException e )
            {
                // no need to reschedule since the check pointer has raised a kernel panic and a shutdown is expected
                throw new UnderlyingStorageException( e );
            }
            catch ( InterruptedException e )
            {
                // We should not get these in this thread. Ignore it.
            }
            finally
            {
                if ( gotExecutionPermit )
                {
                    executionInProcess.release();
                }
            }
        }
    };

    private final JobScheduler.Group group;
    private volatile JobScheduler.JobHandle handle;
    private volatile boolean stopped;
    private final Semaphore executionInProcess;
    private final Log log;

    public PeriodicBackgroundTask( JobScheduler scheduler,
                                   long recurringPeriodMillis,
                                   JobScheduler.Group group,
                                   LogProvider logProvider )
    {
        if ( recurringPeriodMillis <= 0 )
        {
            throw new IllegalArgumentException(
                    "Recurring period in milliseconds must be a positive number: " + recurringPeriodMillis );
        }
        this.scheduler = scheduler;
        this.recurringPeriodMillis = recurringPeriodMillis;
        this.group = group;
        executionInProcess = new Semaphore( 1 );
        log = logProvider.getLog( getClass() );
    }

    @Override
    public void start() throws Throwable
    {
        handle = scheduler.scheduleRecurring(
                group, job, recurringPeriodMillis, recurringPeriodMillis, MILLISECONDS );
    }

    @Override
    public void stop() throws Throwable
    {
        log.info( getClass().getSimpleName() + " stopping" );
        stopped = true;
        JobScheduler.JobHandle h = this.handle;
        if ( h != null )
        {
            h.cancel( false );
        }
        executionInProcess.acquire();
    }

    protected abstract void performTask() throws IOException;
}
