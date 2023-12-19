/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.consensus.log.pruning;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.scheduler.JobScheduler.Groups.raftLogPruning;

import java.io.IOException;
import java.util.function.BooleanSupplier;

import org.neo4j.causalclustering.core.state.RaftLogPruner;
import org.neo4j.function.Predicates;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class PruningScheduler extends LifecycleAdapter
{
    private final RaftLogPruner logPruner;
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

    public PruningScheduler( RaftLogPruner logPruner, JobScheduler scheduler, long recurringPeriodMillis, LogProvider
            logProvider )
    {
        this.logPruner = logPruner;
        this.scheduler = scheduler;
        this.recurringPeriodMillis = recurringPeriodMillis;
        log = logProvider.getLog( getClass() );
    }

    @Override
    public void start()
    {
        handle = scheduler.schedule( raftLogPruning, job, recurringPeriodMillis, MILLISECONDS );
    }

    @Override
    public void stop()
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
