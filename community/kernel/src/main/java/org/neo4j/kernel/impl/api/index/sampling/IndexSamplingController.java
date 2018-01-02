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

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.neo4j.function.Predicate;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.impl.api.index.IndexMap;
import org.neo4j.kernel.impl.api.index.IndexMapSnapshotProvider;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.JobScheduler.JobHandle;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode.BACKGROUND_REBUILD_UPDATED;
import static org.neo4j.kernel.impl.util.JobScheduler.Groups.indexSamplingController;

public class IndexSamplingController
{
    private final IndexSamplingJobFactory jobFactory;
    private final IndexSamplingJobQueue<IndexDescriptor> jobQueue;
    private final IndexSamplingJobTracker jobTracker;
    private final IndexMapSnapshotProvider indexMapSnapshotProvider;
    private final JobScheduler scheduler;
    private final Predicate<IndexDescriptor> indexRecoveryCondition;
    private final boolean backgroundSampling;
    private final Lock samplingLock = new ReentrantLock( true );

    private JobHandle backgroundSamplingHandle;

    // use IndexSamplingControllerFactory.create do not instantiate directly
    IndexSamplingController( IndexSamplingConfig config,
                             IndexSamplingJobFactory jobFactory,
                             IndexSamplingJobQueue<IndexDescriptor> jobQueue,
                             IndexSamplingJobTracker jobTracker,
                             IndexMapSnapshotProvider indexMapSnapshotProvider,
                             JobScheduler scheduler,
                             Predicate<IndexDescriptor> indexRecoveryCondition )
    {
        this.backgroundSampling = config.backgroundSampling();
        this.jobFactory = jobFactory;
        this.indexMapSnapshotProvider = indexMapSnapshotProvider;
        this.jobQueue = jobQueue;
        this.jobTracker = jobTracker;
        this.scheduler = scheduler;
        this.indexRecoveryCondition = indexRecoveryCondition;
    }

    public void sampleIndexes( IndexSamplingMode mode )
    {
        IndexMap indexMap = indexMapSnapshotProvider.indexMapSnapshot();
        jobQueue.addAll( !mode.sampleOnlyIfUpdated, indexMap.descriptors() );
        scheduleSampling( mode, indexMap );
    }

    public void sampleIndex( IndexDescriptor descriptor, IndexSamplingMode mode )
    {
        IndexMap indexMap = indexMapSnapshotProvider.indexMapSnapshot();
        jobQueue.add( !mode.sampleOnlyIfUpdated, descriptor );
        scheduleSampling( mode, indexMap );
    }

    public void recoverIndexSamples()
    {
        samplingLock.lock();
        try
        {
            IndexMap indexMap = indexMapSnapshotProvider.indexMapSnapshot();
            Iterator<IndexDescriptor> descriptors = indexMap.descriptors();
            while ( descriptors.hasNext() )
            {
                IndexDescriptor descriptor = descriptors.next();
                if ( indexRecoveryCondition.test( descriptor ) )
                {
                    sampleIndexOnCurrentThread( indexMap, descriptor );
                }
            }
        }
        finally
        {
            samplingLock.unlock();
        }
    }

    private void scheduleSampling( IndexSamplingMode mode, IndexMap indexMap )
    {
        if ( mode.blockUntilAllScheduled )
        {
            // Wait until last sampling job has been started
            scheduleAllSampling( indexMap );
        }
        else
        {
            // Try to schedule as many sampling jobs as possible
            tryScheduleSampling( indexMap );
        }
    }


    private void tryScheduleSampling( IndexMap indexMap )
    {
        if ( tryEmptyLock() )
        {
            try
            {
                while ( jobTracker.canExecuteMoreSamplingJobs() )
                {
                    IndexDescriptor descriptor = jobQueue.poll();
                    if ( descriptor == null )
                    {
                        return;
                    }

                    sampleIndexOnTracker( indexMap, descriptor );
                }
            }
            finally
            {
                samplingLock.unlock();
            }
        }
    }

    private boolean tryEmptyLock()
    {
        try
        {
            return samplingLock.tryLock( 0, SECONDS );
        }
        catch ( InterruptedException ex )
        {
            // ignored
            return false;
        }
    }

    private void scheduleAllSampling( IndexMap indexMap )
    {
        samplingLock.lock();
        try
        {
            Iterable<IndexDescriptor> descriptors = jobQueue.pollAll();

            for ( IndexDescriptor descriptor : descriptors )
            {
                jobTracker.waitUntilCanExecuteMoreSamplingJobs();
                sampleIndexOnTracker( indexMap, descriptor );
            }
        }
        finally
        {
            samplingLock.unlock();
        }
    }

    private void sampleIndexOnTracker( IndexMap indexMap, IndexDescriptor descriptor )
    {
        IndexSamplingJob job = createSamplingJob( indexMap, descriptor );
        if ( job != null )
        {
            jobTracker.scheduleSamplingJob( job );
        }
    }

    private void sampleIndexOnCurrentThread( IndexMap indexMap, IndexDescriptor descriptor )
    {
        IndexSamplingJob job = createSamplingJob( indexMap, descriptor );
        if ( job != null )
        {
            job.run();
        }
    }

    private IndexSamplingJob createSamplingJob( IndexMap indexMap, IndexDescriptor descriptor )
    {
        IndexProxy proxy = indexMap.getIndexProxy( descriptor );
        if ( proxy == null || proxy.getState() != InternalIndexState.ONLINE )
        {
            return null;
        }
        return jobFactory.create( proxy );
    }

    public void start()
    {
        if ( backgroundSampling )
        {
            Runnable samplingRunner = new Runnable()
            {
                @Override
                public void run()
                {
                    sampleIndexes( BACKGROUND_REBUILD_UPDATED );
                }
            };
            backgroundSamplingHandle = scheduler.scheduleRecurring( indexSamplingController, samplingRunner, 10, SECONDS );
        }
    }

    public void awaitSamplingCompleted( long time, TimeUnit unit ) throws InterruptedException
    {
        jobTracker.awaitAllJobs( time, unit );
    }

    public void stop()
    {
        if ( backgroundSamplingHandle != null )
        {
            backgroundSamplingHandle.cancel( true );
        }
        jobTracker.stopAndAwaitAllJobs();
    }
}
