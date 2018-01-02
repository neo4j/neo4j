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
package org.neo4j.kernel.impl.api.index.sampling;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.util.JobScheduler;

public class IndexSamplingJobTracker
{
    private final JobScheduler jobScheduler;
    private final int jobLimit;
    private final Set<IndexDescriptor> executingJobDescriptors;
    private final Lock lock = new ReentrantLock( true );
    private final Condition canSchedule = lock.newCondition();
    private final Condition allJobsFinished = lock.newCondition();

    private boolean stopped;

    public IndexSamplingJobTracker( IndexSamplingConfig config, JobScheduler jobScheduler )
    {
        this.jobScheduler = jobScheduler;
        this.jobLimit = config.jobLimit();
        this.executingJobDescriptors = new HashSet<>();
    }

    public boolean canExecuteMoreSamplingJobs()
    {
        lock.lock();
        try
        {
            return !stopped && executingJobDescriptors.size() < jobLimit;
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

            IndexDescriptor descriptor = samplingJob.descriptor();
            if ( executingJobDescriptors.contains( descriptor ) )
            {
                return;
            }

            executingJobDescriptors.add( descriptor );
            jobScheduler.schedule( JobScheduler.Groups.indexSampling, new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        samplingJob.run();
                    }
                    finally
                    {
                        samplingJobCompleted( samplingJob );
                    }
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
            executingJobDescriptors.remove( samplingJob.descriptor() );
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

    public void awaitAllJobs(long time, TimeUnit unit) throws InterruptedException
    {
        lock.lock();
        try
        {
            if ( stopped )
            {
                return;
            }

            while ( !executingJobDescriptors.isEmpty() )
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

            while ( !executingJobDescriptors.isEmpty() )
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
