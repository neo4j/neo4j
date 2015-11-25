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
package org.neo4j.coreedge.server;

import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.Neo4jJobScheduler;

import static java.util.concurrent.TimeUnit.MINUTES;

public class ExpiryScheduler
{
    private final JobScheduler scheduler;
    private final long reclamationIntervalTime;
    private final TimeUnit reclamationIntervalTimeUnit;

    /**
     * Checking for expired items every 2 minutes by default.
     */
    public ExpiryScheduler( JobScheduler scheduler )
    {
        this( scheduler, 2, MINUTES );
    }

    public ExpiryScheduler( JobScheduler scheduler,
                            long reclamationIntervalTime,
                            TimeUnit reclamationIntervalTimeUnit )
    {
        this.scheduler = scheduler;
        this.reclamationIntervalTime = reclamationIntervalTime;
        this.reclamationIntervalTimeUnit = reclamationIntervalTimeUnit;
    }

    public JobScheduler.JobHandle schedule( Runnable runnable )
    {
        return scheduler.scheduleRecurring( new JobScheduler.Group( "LazyChannelsGarbageCollection",
                        Neo4jJobScheduler.SchedulingStrategy.POOLED ),
                runnable, reclamationIntervalTime, reclamationIntervalTimeUnit );
    }
}
