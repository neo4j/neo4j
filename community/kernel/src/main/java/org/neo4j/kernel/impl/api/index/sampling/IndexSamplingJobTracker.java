/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.neo4j.common.Subject;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.scheduler.JobScheduler;

class IndexSamplingJobTracker
{
    private final JobScheduler jobScheduler;
    private final Set<Long> executingJobs;
    private final Lock lock = new ReentrantLock( true );
    private final Condition allJobsFinished = lock.newCondition();
    private final String databaseName;

    private final AtomicBoolean stopped;

    IndexSamplingJobTracker( JobScheduler jobScheduler, String databaseName )
    {
        this.jobScheduler = jobScheduler;
        this.executingJobs = new HashSet<>();
        this.databaseName = databaseName;
        this.stopped = new AtomicBoolean( false );
    }

    JobHandle scheduleSamplingJob( final IndexSamplingJob samplingJob )
    {
        lock.lock();
        try
        {
            if ( stopped.get() )
            {
                return JobHandle.EMPTY;
            }

            long indexId = samplingJob.indexId();
            if ( executingJobs.contains( indexId ) )
            {
                return JobHandle.EMPTY;
            }

            executingJobs.add( indexId );
            var monitoringParams = new JobMonitoringParams( Subject.SYSTEM, databaseName, "Sampling of index '" + samplingJob.indexName() + "'" );
            return jobScheduler.schedule( Group.INDEX_SAMPLING, monitoringParams, () ->
            {
                try
                {
                    samplingJob.run( stopped );
                }
                finally
                {
                    samplingJobCompleted( samplingJob );
                }
            } );
        }
        finally
        {
            lock.unlock();
        }
    }

    private void samplingJobCompleted( IndexSamplingJob samplingJob )
    {
        lock.lock();
        try
        {
            executingJobs.remove( samplingJob.indexId() );
            allJobsFinished.signalAll();
        }
        finally
        {
            lock.unlock();
        }
    }

    void stopAndAwaitAllJobs()
    {
        lock.lock();
        try
        {
            stopped.set( true );

            while ( !executingJobs.isEmpty() )
            {
                allJobsFinished.awaitUninterruptibly();
            }
        }
        finally
        {
            lock.unlock();
        }
    }
}
