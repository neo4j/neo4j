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
package org.neo4j.kernel.ha;

import java.util.concurrent.TimeUnit;

import org.neo4j.scheduler.JobScheduler;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.scheduler.JobScheduler.Groups.pullUpdates;

/**
 * This scheduler is part of slave lifecycle that will schedule periodic pulling on slave switch
 * and turn them off during slave shutdown.
 *
 * @see UpdatePuller
 */
public class UpdatePullerScheduler extends LifecycleAdapter
{
    private final JobScheduler scheduler;
    private final Log log;
    private final UpdatePuller updatePuller;
    private final long pullIntervalMillis;
    private JobScheduler.JobHandle intervalJobHandle;

    public UpdatePullerScheduler( JobScheduler scheduler, LogProvider logProvider, UpdatePuller updatePullingThread,
            long pullIntervalMillis )
    {
        this.scheduler = scheduler;
        this.log = logProvider.getLog( getClass() );
        this.updatePuller = updatePullingThread;
        this.pullIntervalMillis = pullIntervalMillis;
    }

    @Override
    public void init()
    {
        if ( pullIntervalMillis > 0 )
        {
            intervalJobHandle = scheduler.scheduleRecurring( pullUpdates, () ->
            {
                try
                {
                    updatePuller.pullUpdates();
                }
                catch ( InterruptedException e )
                {
                    log.error( "Pull updates failed", e );
                }
            }, pullIntervalMillis, pullIntervalMillis, TimeUnit.MILLISECONDS );
        }
    }

    @Override
    public void shutdown()
    {
        if ( intervalJobHandle != null )
        {
            intervalJobHandle.cancel( false );
        }
    }
}
