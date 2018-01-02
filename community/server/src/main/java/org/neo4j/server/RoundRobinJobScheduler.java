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
package org.neo4j.server;

import java.util.LinkedList;
import java.util.List;

import org.neo4j.logging.LogProvider;
import org.neo4j.server.rrd.JobScheduler;
import org.neo4j.server.rrd.ScheduledJob;

/**
 * @deprecated This class is for internal use only and will be moved to an internal package in a future release.
 */
@Deprecated
public class RoundRobinJobScheduler implements JobScheduler
{
    private final List<ScheduledJob> scheduledJobs = new LinkedList<ScheduledJob>();
    private final LogProvider logProvider;

    public RoundRobinJobScheduler( LogProvider logProvider )
    {
        this.logProvider = logProvider;
    }

    @Override
    public void scheduleAtFixedRate( Runnable job, String jobName, long delay, long period )
    {
        ScheduledJob scheduledJob = new ScheduledJob( job, jobName, delay, period, logProvider );
        scheduledJobs.add( scheduledJob );
    }

    public void stopJobs()
    {
        for ( ScheduledJob job : scheduledJobs )
        {
            job.cancel();
        }
    }
}
