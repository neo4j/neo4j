/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.neo4j.scheduler.JobScheduler;

public class IndexSamplingJobTracker
{
    private final JobScheduler jobScheduler;
    private final int jobLimit;
    private final Set<Long> executingJobs;
    private final Lock lock = new ReentrantLock( true );
    private final Condition canSchedule = lock.newCondition();
    private final Condition allJobsFinished = lock.newCondition();

    private boolean stopped;

    public IndexSamplingJobTracker( IndexSamplingConfig config, JobScheduler jobScheduler )
    {
        this.jobScheduler = jobScheduler;
        this.jobLimit = config.jobLimit();
        this.executingJobs = new HashSet<>();
    }

    public boolean canExecuteMoreSamplingJobs()
    {
        lock.lock();
        try
        {
            return !stopped && executingJobs.size() < jobLimit;
        }
        finally
        {
            lock.unlock();
        }
    }

    public void scheduleSamplingJob( final IndexSamplingJob samplingJob )
    {
        lock.lock();
        try
        {
            if ( stopped )
            {
                return;
            }

            long indexId = samplingJob.indexId();
            if ( executingJobs.contains( indexId ) )
            {
                return;
            }

            executingJobs.add( indexId );
            jobScheduler.schedule( JobScheduler.Groups.indexSampling, () ->
            {
                try
                {
                    samplingJob.run();
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
            canSchedule.signalAll();
            allJobsFinished.signalAll();
        }
        finally
        {
            lock.unlock();
        }
    }

    public void waitUntilCanExecuteMoreSamplingJobs()
    {
        lock.lock();
        try
        {
            while ( !canExecuteMoreSamplingJobs() )
            {
                if ( stopped )
                {
                    return;
                }

                canSchedule.awaitUninterruptibly();
            }
        }
        finally
        {
            lock.unlock();
        }
    }

    public void awaitAllJobs( long time, TimeUnit unit ) throws InterruptedException
    {
        lock.lock();
        try
        {
            if ( stopped )
            {
                return;
            }

            while ( !executingJobs.isEmpty() )
            {
                allJobsFinished.await( time, unit );
            }
        }
        finally
        {
            lock.unlock();
        }
    }

    public void stopAndAwaitAllJobs()
    {
        lock.lock();
        try
        {
            stopped = true;

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
