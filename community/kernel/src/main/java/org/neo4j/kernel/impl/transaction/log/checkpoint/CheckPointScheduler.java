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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import java.io.IOException;

import org.neo4j.function.Predicates;
import org.neo4j.function.Supplier;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.kernel.impl.util.JobScheduler.Groups.checkPoint;

public class CheckPointScheduler extends LifecycleAdapter
{
    private final CheckPointer checkPointer;
    private final JobScheduler scheduler;
    private final long recurringPeriodMillis;
    private final Runnable job = new Runnable()
    {
        @Override
        public void run()
        {
            try
            {
                checkPointing = true;
                if ( stopped )
                {
                    return;
                }
                checkPointer.checkPointIfNeeded( new SimpleTriggerInfo( "scheduler" ) );
            }
            catch ( IOException e )
            {
                // no need to reschedule since the check pointer has raised a kernel panic and a shutdown is expected
                throw new UnderlyingStorageException( e );
            }
            finally
            {
                checkPointing = false;
            }

            // reschedule only if it is not stopped
            if ( !stopped )
            {
                handle = scheduler.schedule( checkPoint, job, recurringPeriodMillis, MILLISECONDS );
            }
        }
    };

    private volatile JobScheduler.JobHandle handle;
    private volatile boolean stopped;
    private volatile boolean checkPointing;
    private final Supplier<Boolean> checkPointingCondition = new Supplier<Boolean>()
    {
        @Override
        public Boolean get()
        {
            return !checkPointing;
        }
    };

    public CheckPointScheduler( CheckPointer checkPointer, JobScheduler scheduler, long recurringPeriodMillis )
    {
        this.checkPointer = checkPointer;
        this.scheduler = scheduler;
        this.recurringPeriodMillis = recurringPeriodMillis;
    }

    @Override
    public void start() throws Throwable
    {
        handle = scheduler.schedule( checkPoint, job, recurringPeriodMillis, MILLISECONDS );
    }

    @Override
    public void stop() throws Throwable
    {
        stopped = true;
        if ( handle != null )
        {
            handle.cancel( false );
        }
        Predicates.awaitForever( checkPointingCondition, 100, MILLISECONDS );
    }
}
