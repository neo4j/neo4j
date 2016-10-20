/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.core.consensus.log.pruning;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.kernel.impl.util.JobScheduler.Groups.raftLogPruning;

import java.io.IOException;
import java.util.function.BooleanSupplier;

import org.neo4j.function.Predicates;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class PruningScheduler extends LifecycleAdapter
{
    private final LogPruner logPruner;
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
                logPruner.prune();
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
                handle = scheduler.schedule( raftLogPruning, job, recurringPeriodMillis, MILLISECONDS );
            }
        }
    };
    private final Log log;

    private volatile JobScheduler.JobHandle handle;
    private volatile boolean stopped;
    private volatile boolean checkPointing;
    private final BooleanSupplier checkPointingCondition = new BooleanSupplier()
    {
        @Override
        public boolean getAsBoolean()
        {
            return !checkPointing;
        }
    };

    public PruningScheduler( LogPruner logPruner, JobScheduler scheduler, long recurringPeriodMillis, LogProvider
            logProvider )
    {
        this.logPruner = logPruner;
        this.scheduler = scheduler;
        this.recurringPeriodMillis = recurringPeriodMillis;
        log = logProvider.getLog( getClass() );
    }

    @Override
    public void start() throws Throwable
    {
        handle = scheduler.schedule( raftLogPruning, job, recurringPeriodMillis, MILLISECONDS );
    }

    @Override
    public void stop() throws Throwable
    {
        log.info( "PruningScheduler stopping" );
        stopped = true;
        if ( handle != null )
        {
            handle.cancel( false );
        }
        Predicates.awaitForever( checkPointingCondition, 100, MILLISECONDS );
    }
}
