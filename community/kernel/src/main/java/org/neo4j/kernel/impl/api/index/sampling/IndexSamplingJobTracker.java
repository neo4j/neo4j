/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index.sampling;

import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.kernel.impl.util.JobScheduler;

public class IndexSamplingJobTracker
{
    private final JobScheduler jobScheduler;
    private final int jobLimit;
    private final AtomicInteger executingJobs;

    public IndexSamplingJobTracker( JobScheduler jobScheduler, int jobLimit )
    {
        this.jobScheduler = jobScheduler;
        this.jobLimit = jobLimit;
        this.executingJobs = new AtomicInteger( jobLimit );
    }

    public boolean canExecuteMoreSamplingJobs()
    {
        return executingJobs.get() <= jobLimit;
    }

    public void scheduleSamplingJob( final Runnable runnable )
    {
        executingJobs.incrementAndGet();
        jobScheduler.schedule( JobScheduler.Group.indexSampling, new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    runnable.run();
                }
                finally
                {
                    executingJobs.decrementAndGet();
                }
            }
        } );
    }
}
