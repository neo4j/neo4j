/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.ha;

import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.JobScheduler.JobHandle;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.logging.Logging;

public class UpdatePullerClient extends LifecycleAdapter
{
    private final JobScheduler scheduler;
    private final StringLogger logger;
    private final UpdatePuller updatePuller;
    private final AvailabilityGuard availabilityGuard;
    private final long pullIntervalMillis;
    private JobHandle intervalJobHandle;

    public UpdatePullerClient( long pullIntervalMillis, JobScheduler scheduler, final Logging logging,
            UpdatePuller updatePullingThread, AvailabilityGuard availabilityGuard )
    {
        this.pullIntervalMillis = pullIntervalMillis;
        this.scheduler = scheduler;
        this.availabilityGuard = availabilityGuard;
        this.logger = logging.getMessagesLog( getClass() );
        updatePuller = updatePullingThread;
    }

    public void pullUpdates() throws InterruptedException
    {
        if ( !updatePuller.isActive() || !availabilityGuard.isAvailable( 5000 ) )
        {
            return;
        }

        updatePuller.await( UpdatePuller.NEXT_TICKET,
                false /*we're OK with the update puller becoming inactive while we await the condition*/ );
    }

    @Override
    public void init() throws Throwable
    {
        if ( pullIntervalMillis > 0 )
        {
            intervalJobHandle = scheduler.scheduleRecurring( JobScheduler.Group.pullUpdates, new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        pullUpdates();
                    }
                    catch ( InterruptedException e )
                    {
                        logger.error( "Pull updates failed", e );
                    }
                }
            }, pullIntervalMillis, pullIntervalMillis, TimeUnit.MILLISECONDS );
        }
    }

    @Override
    public void shutdown() throws Throwable
    {
        if ( intervalJobHandle != null )
        {
            intervalJobHandle.cancel( false );
        }
    }
}
