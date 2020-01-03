/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.eclipse.collections.api.iterator.LongIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.kernel.impl.api.index.IndexMap;
import org.neo4j.kernel.impl.api.index.IndexMapSnapshotProvider;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.schema.CapableIndexDescriptor;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.util.FeatureToggles;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.kernel.impl.api.index.sampling.IndexSamplingMode.BACKGROUND_REBUILD_UPDATED;

public class IndexSamplingController
{
    private final IndexSamplingJobFactory jobFactory;
    private final IndexSamplingJobQueue<Long> jobQueue;
    private final IndexSamplingJobTracker jobTracker;
    private final IndexMapSnapshotProvider indexMapSnapshotProvider;
    private final JobScheduler scheduler;
    private final RecoveryCondition indexRecoveryCondition;
    private final boolean backgroundSampling;
    private final Lock samplingLock = new ReentrantLock();
    private final Log log;
    static final String LOG_RECOVER_INDEX_SAMPLES_NAME = "log_recover_index_samples";
    static final String ASYNC_RECOVER_INDEX_SAMPLES_NAME = "async_recover_index_samples";
    static final String ASYNC_RECOVER_INDEX_SAMPLES_WAIT_NAME = "async_recover_index_samples_wait";
    private final boolean logRecoverIndexSamples;
    private final boolean asyncRecoverIndexSamples;
    private final boolean asyncRecoverIndexSamplesWait;

    private JobHandle backgroundSamplingHandle;

    // use IndexSamplingControllerFactory.create do not instantiate directly
    IndexSamplingController( IndexSamplingConfig config,
                             IndexSamplingJobFactory jobFactory,
                             IndexSamplingJobQueue<Long> jobQueue,
                             IndexSamplingJobTracker jobTracker,
                             IndexMapSnapshotProvider indexMapSnapshotProvider,
                             JobScheduler scheduler,
                             RecoveryCondition indexRecoveryCondition,
                             LogProvider logProvider )
    {
        this.backgroundSampling = config.backgroundSampling();
        this.jobFactory = jobFactory;
        this.indexMapSnapshotProvider = indexMapSnapshotProvider;
        this.jobQueue = jobQueue;
        this.jobTracker = jobTracker;
        this.scheduler = scheduler;
        this.indexRecoveryCondition = indexRecoveryCondition;
        this.log = logProvider.getLog( getClass() );
        this.logRecoverIndexSamples = FeatureToggles.flag( IndexSamplingController.class, LOG_RECOVER_INDEX_SAMPLES_NAME, false );
        this.asyncRecoverIndexSamples = FeatureToggles.flag( IndexSamplingController.class, ASYNC_RECOVER_INDEX_SAMPLES_NAME, false );
        this.asyncRecoverIndexSamplesWait =
                FeatureToggles.flag( IndexSamplingController.class, ASYNC_RECOVER_INDEX_SAMPLES_WAIT_NAME, asyncRecoverIndexSamples );
    }

    public void sampleIndexes( IndexSamplingMode mode )
    {
        IndexMap indexMap = indexMapSnapshotProvider.indexMapSnapshot();
        jobQueue.addAll( !mode.sampleOnlyIfUpdated, PrimitiveLongCollections.toIterator( indexMap.indexIds() ) );
        scheduleSampling( mode, indexMap );
    }

    public void sampleIndex( long indexId, IndexSamplingMode mode )
    {
        IndexMap indexMap = indexMapSnapshotProvider.indexMapSnapshot();
        jobQueue.add( !mode.sampleOnlyIfUpdated, indexId );
        scheduleSampling( mode, indexMap );
    }

    public void recoverIndexSamples()
    {
        samplingLock.lock();
        try
        {
            IndexMap indexMap = indexMapSnapshotProvider.indexMapSnapshot();
            final LongIterator indexIds = indexMap.indexIds();
            List<IndexSamplingJobHandle> asyncSamplingJobs = new ArrayList<>();
            while ( indexIds.hasNext() )
            {
                long indexId = indexIds.next();
                CapableIndexDescriptor descriptor = indexMap.getIndexProxy( indexId ).getDescriptor();
                if ( indexRecoveryCondition.test( descriptor ) )
                {
                    if ( logRecoverIndexSamples )
                    {
                        log.info( "Index requires sampling, id=%d, name=%s.", indexId, descriptor.getName() );
                    }

                    if ( asyncRecoverIndexSamples )
                    {
                        asyncSamplingJobs.add( sampleIndexOnTracker( indexMap, indexId ) );
                    }
                    else
                    {
                        sampleIndexOnCurrentThread( indexMap, indexId );
                    }
                }
                else
                {
                    if ( logRecoverIndexSamples )
                    {
                        log.info( "Index does not require sampling, id=%d, name=%s.", indexId, descriptor.getName() );
                    }
                }
            }
            if ( asyncRecoverIndexSamplesWait )
            {
                waitForAsyncIndexSamples( asyncSamplingJobs );
            }
        }
        finally
        {
            samplingLock.unlock();
        }
    }

    private void waitForAsyncIndexSamples( List<IndexSamplingJobHandle> asyncSamplingJobs )
    {
        for ( IndexSamplingJobHandle asyncSamplingJob : asyncSamplingJobs )
        {
            try
            {
                asyncSamplingJob.jobHandle.waitTermination();
            }
            catch ( InterruptedException | ExecutionException e )
            {
                throw new RuntimeException(
                        "Failed to asynchronously sample index during recovery, index: " + asyncSamplingJob.indexSamplingJob.indexId(), e );
            }
        }
    }

    public interface RecoveryCondition
    {
        boolean test( StoreIndexDescriptor descriptor );
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
                    Long indexId = jobQueue.poll();
                    if ( indexId == null )
                    {
                        return;
                    }

                    sampleIndexOnTracker( indexMap, indexId );
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
            Iterable<Long> indexIds = jobQueue.pollAll();

            for ( Long indexId : indexIds )
            {
                jobTracker.waitUntilCanExecuteMoreSamplingJobs();
                sampleIndexOnTracker( indexMap, indexId );
            }
        }
        finally
        {
            samplingLock.unlock();
        }
    }

    private IndexSamplingJobHandle sampleIndexOnTracker( IndexMap indexMap, long indexId )
    {
        IndexSamplingJob job = createSamplingJob( indexMap, indexId );
        if ( job != null )
        {
            return new IndexSamplingJobHandle( job, jobTracker.scheduleSamplingJob( job ) );
        }
        return new IndexSamplingJobHandle( job, JobHandle.nullInstance );
    }

    private void sampleIndexOnCurrentThread( IndexMap indexMap, long indexId )
    {
        IndexSamplingJob job = createSamplingJob( indexMap, indexId );
        if ( job != null )
        {
            job.run();
        }
    }

    private IndexSamplingJob createSamplingJob( IndexMap indexMap, long indexId )
    {
        IndexProxy proxy = indexMap.getIndexProxy( indexId );
        if ( proxy == null || proxy.getState() != InternalIndexState.ONLINE )
        {
            return null;
        }
        return jobFactory.create( indexId, proxy );
    }

    public void start()
    {
        if ( backgroundSampling )
        {
            Runnable samplingRunner = () -> sampleIndexes( BACKGROUND_REBUILD_UPDATED );
            backgroundSamplingHandle = scheduler.scheduleRecurring( Group.INDEX_SAMPLING, samplingRunner, 10, SECONDS );
        }
    }

    public void stop()
    {
        if ( backgroundSamplingHandle != null )
        {
            backgroundSamplingHandle.cancel( true );
        }
        jobTracker.stopAndAwaitAllJobs();
    }

    private static class IndexSamplingJobHandle
    {
        final IndexSamplingJob indexSamplingJob;
        final JobHandle jobHandle;

        IndexSamplingJobHandle( IndexSamplingJob indexSamplingJob, JobHandle jobHandle )
        {
            this.indexSamplingJob = indexSamplingJob;
            this.jobHandle = jobHandle;
        }
    }
}
