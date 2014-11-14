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

import java.util.Iterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.impl.api.index.IndexMap;
import org.neo4j.kernel.impl.api.index.IndexMapSnapshotProvider;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.util.JobScheduler;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode.BACKGROUND_REBUILD_UPDATED;
import static org.neo4j.kernel.impl.util.JobScheduler.Group.indexSamplingController;

public class IndexSamplingController
{

    private final IndexSamplingConfig config;
    private final IndexSamplingJobFactory jobFactory;
    private final IndexSamplingJobQueue jobQueue;
    private final IndexSamplingJobTracker jobTracker;
    private final IndexMapSnapshotProvider indexMapSnapshotProvider;
    private final JobScheduler scheduler;
    private final Predicate<IndexDescriptor> indexRecoveryCondition;
    private final Lock emptyLock = new ReentrantLock( true );

    // use IndexSamplingControllerFactory.create do not instantiate directly
    IndexSamplingController( IndexSamplingConfig config,
                             IndexSamplingJobFactory jobFactory,
                             IndexSamplingJobQueue jobQueue,
                             IndexSamplingJobTracker jobTracker,
                             IndexMapSnapshotProvider indexMapSnapshotProvider,
                             JobScheduler scheduler,
                             Predicate<IndexDescriptor> indexRecoveryCondition )
    {
        this.config = config;
        this.jobFactory = jobFactory;
        this.indexMapSnapshotProvider = indexMapSnapshotProvider;
        this.jobQueue = jobQueue;
        this.jobTracker = jobTracker;
        this.scheduler = scheduler;
        this.indexRecoveryCondition = indexRecoveryCondition;
    }

    public IndexSamplingConfig config()
    {
        return config;
    }

    public void sampleIndexes( IndexSamplingMode mode )
    {
        IndexMap indexMap = indexMapSnapshotProvider.indexMapSnapshot();
        fillQueue( mode, indexMap );

        if ( mode.blockUntilAllScheduled )
        {
            // Wait until last sampling job has been started
            emptyQueue( indexMap );
        }
        else
        {
            // Try to schedule as many sampling jobs as possible
            tryEmptyQueue( indexMap );
        }
    }

    public void sampleIndex( IndexDescriptor descriptor, IndexSamplingMode mode )
    {
        IndexMap indexMap = indexMapSnapshotProvider.indexMapSnapshot();
        jobQueue.sampleIndex( mode, descriptor );

        if ( mode.blockUntilAllScheduled )
        {
            // Wait until last sampling job has been started
            emptyQueue( indexMap );
        }
        else
        {
            // Try to schedule as many sampling jobs as possible
            tryEmptyQueue( indexMap );
        }
    }

    public void recoverIndexSamples()
    {
        emptyLock.lock();
        try
        {
            IndexMap indexMap = indexMapSnapshotProvider.indexMapSnapshot();
            Iterator<IndexDescriptor> descriptors = indexMap.descriptors();
            while ( descriptors.hasNext() )
            {
                IndexDescriptor descriptor = descriptors.next();
                if ( indexRecoveryCondition.accept( descriptor ) )
                {
                    sampleIndexOnCurrentThread( indexMap, descriptor );
                }
            }
        }
        finally
        {
            emptyLock.unlock();
        }
    }

    private void fillQueue( IndexSamplingMode mode, IndexMap indexMap )
    {
        Iterator<IndexDescriptor> descriptors = indexMap.descriptors();
        while ( descriptors.hasNext() )
        {
            jobQueue.sampleIndex( mode, descriptors.next() );
        }
    }

    private void tryEmptyQueue( IndexMap indexMap )
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
                emptyLock.unlock();
            }
        }
    }

    private boolean tryEmptyLock()
    {
        try
        {
            return emptyLock.tryLock( 0, SECONDS );
        }
        catch ( InterruptedException ex )
        {
            // ignored
            return false;
        }
    }

    private void emptyQueue( IndexMap indexMap )
    {
        emptyLock.lock();
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
            emptyLock.unlock();
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
        IndexSamplingJob job;
        if ( proxy == null || proxy.getState() != InternalIndexState.ONLINE )
        {
            job = null;
        }
        else
        {
            job = jobFactory.create( config, proxy );
        }
        return job;
    }

    public void start()
    {
        if ( config.backgroundSampling() )
        {
            Runnable samplingRunner = new Runnable()
            {
                @Override
                public void run()
                {
                    sampleIndexes( BACKGROUND_REBUILD_UPDATED );
                }
            };
            scheduler.scheduleRecurring( indexSamplingController, samplingRunner, 10, SECONDS );
        }
    }
}
