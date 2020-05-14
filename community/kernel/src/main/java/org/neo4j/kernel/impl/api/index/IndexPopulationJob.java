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
package org.neo4j.kernel.impl.api.index;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.impl.index.schema.UnsafeDirectByteBufferAllocator;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.memory.ThreadSafePeakMemoryTracker;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.util.concurrent.Runnables;

import static org.neo4j.kernel.impl.index.schema.BlockBasedIndexPopulator.parseBlockSize;

/**
 * A background job for initially populating one or more index over existing data in the database.
 * Use provided store view to scan store. Participating {@link IndexPopulator} are added with
 * {@link #addPopulator(IndexPopulator, IndexDescriptor, String, FlippableIndexProxy, FailedIndexProxyFactory)}
 * before {@link #run() running} this job.
 */
public class IndexPopulationJob implements Runnable
{
    private static final String INDEX_POPULATION_TAG = "indexPopulationJob";
    private final IndexingService.Monitor monitor;
    private final boolean verifyBeforeFlipping;
    private final PageCacheTracer pageCacheTracer;
    private final MemoryTracker memoryTracker;
    private final ByteBufferFactory bufferFactory;
    private final ThreadSafePeakMemoryTracker memoryAllocationTracker;
    private final MultipleIndexPopulator multiPopulator;
    private final CountDownLatch doneSignal = new CountDownLatch( 1 );

    private volatile StoreScan<IndexPopulationFailedKernelException> storeScan;
    private volatile boolean stopped;
    /**
     * The {@link JobHandle} that represents the scheduling of this index population job.
     * This is used in the cancellation of the job.
     */
    private volatile JobHandle<?> jobHandle;

    public IndexPopulationJob( MultipleIndexPopulator multiPopulator, IndexingService.Monitor monitor, boolean verifyBeforeFlipping,
            PageCacheTracer pageCacheTracer, MemoryTracker memoryTracker )
    {
        this.multiPopulator = multiPopulator;
        this.monitor = monitor;
        this.verifyBeforeFlipping = verifyBeforeFlipping;
        this.pageCacheTracer = pageCacheTracer;
        this.memoryTracker = memoryTracker;
        this.memoryAllocationTracker = new ThreadSafePeakMemoryTracker();
        this.bufferFactory = new ByteBufferFactory( UnsafeDirectByteBufferAllocator::new, parseBlockSize() );
    }

    /**
     * Adds an {@link IndexPopulator} to be populated in this store scan. All participating populators must
     * be added before calling {@link #run()}.
     *  @param populator {@link IndexPopulator} to participate.
     * @param indexDescriptor {@link IndexDescriptor} meta information about index.
     * @param indexUserDescription user description of this index.
     * @param flipper {@link FlippableIndexProxy} to call after a successful population.
     * @param failedIndexProxyFactory {@link FailedIndexProxyFactory} to use after an unsuccessful population.
     */
    MultipleIndexPopulator.IndexPopulation addPopulator( IndexPopulator populator, IndexDescriptor indexDescriptor, String indexUserDescription,
            FlippableIndexProxy flipper, FailedIndexProxyFactory failedIndexProxyFactory )
    {
        assert storeScan == null : "Population have already started, too late to add populators at this point";
        return this.multiPopulator.addPopulator( populator, indexDescriptor, flipper, failedIndexProxyFactory,
                indexUserDescription );
    }

    /**
     * Scans the store using store view and populates all participating {@link IndexPopulator} with data relevant to
     * each index.
     * The scan continues as long as there's at least one non-failed populator.
     */
    @Override
    public void run()
    {
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( INDEX_POPULATION_TAG ) )
        {
            if ( !multiPopulator.hasPopulators() )
            {
                return;
            }
            if ( storeScan != null )
            {
                throw new IllegalStateException( "Population already started." );
            }

            try
            {
                multiPopulator.create( cursorTracer );
                multiPopulator.resetIndexCounts( cursorTracer );

                monitor.indexPopulationScanStarting();
                indexAllEntities( cursorTracer );
                monitor.indexPopulationScanComplete();
                if ( stopped )
                {
                    multiPopulator.stop( cursorTracer );
                    // We remain in POPULATING state
                    return;
                }
                multiPopulator.flipAfterStoreScan( verifyBeforeFlipping, cursorTracer );
            }
            catch ( Throwable t )
            {
                multiPopulator.cancel( t, cursorTracer );
            }
        }
        finally
        {
            // will only close "additional" resources, not the actual populators, since that's managed by flip
            Runnables.runAll( "Failed to close resources in IndexPopulationJob",
                    multiPopulator::close,
                    bufferFactory::close,
                    () -> monitor.populationJobCompleted( memoryAllocationTracker.peakMemoryUsage() ),
                    doneSignal::countDown );
        }
    }

    private void indexAllEntities( PageCursorTracer cursorTracer ) throws IndexPopulationFailedKernelException
    {
        storeScan = multiPopulator.createStoreScan( cursorTracer );
        storeScan.run();
    }

    PopulationProgress getPopulationProgress( MultipleIndexPopulator.IndexPopulation indexPopulation )
    {
        if ( storeScan == null )
        {
            // indexing hasn't begun yet
            return PopulationProgress.NONE;
        }
        PopulationProgress storeScanProgress = storeScan.getProgress();
        return indexPopulation.progress( storeScanProgress );
    }

    /**
     * Signal to stop index population.
     * All populating indexes will remain in {@link InternalIndexState#POPULATING populating state} to be rebuilt on next db start up.
     * Asynchronous call, need to {@link #awaitCompletion(long, TimeUnit) await completion}.
     */
    public void stop()
    {
        // Stop the population
        if ( storeScan != null )
        {
            stopped = true;
            storeScan.stop();
            jobHandle.cancel();
            monitor.populationCancelled();
        }
    }

    /**
     * Stop population of specific index. Index will remain in {@link InternalIndexState#POPULATING populating state} to be rebuilt on next db start up.
     * @param population {@link MultipleIndexPopulator.IndexPopulation} to be stopped.
     */
    void stop( MultipleIndexPopulator.IndexPopulation population, PageCursorTracer cursorTracer )
    {
        multiPopulator.stop( population, cursorTracer );
    }

    /**
     * Stop population of specific index and drop it.
     * @param population {@link MultipleIndexPopulator.IndexPopulation} to be dropped.
     */
    void dropPopulation( MultipleIndexPopulator.IndexPopulation population )
    {
        multiPopulator.dropIndexPopulation( population );
    }

    /**
     * A transaction happened that produced the given updates. Let this job incorporate its data,
     * feeding it to the {@link IndexPopulator}.
     *
     * @param update {@link IndexEntryUpdate} to queue.
     */
    public void update( IndexEntryUpdate<?> update )
    {
        multiPopulator.queueConcurrentUpdate( update );
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[populator:" + multiPopulator + "]";
    }

    /**
     * Awaits completion of this population job, but waits maximum the given time.
     *
     * @param time time to wait at the most for completion. A value of 0 means indefinite wait.
     * @param unit {@link TimeUnit unit} of the {@code time}.
     * @return {@code true} if the job is still running when leaving this method, otherwise {@code false} meaning that the job is completed.
     * @throws InterruptedException if the wait got interrupted.
     */
    public boolean awaitCompletion( long time, TimeUnit unit ) throws InterruptedException
    {
        if ( time == 0 )
        {
            doneSignal.await();
            return false;
        }
        boolean completed = doneSignal.await( time, unit );
        return !completed;
    }

    /**
     * Assign the job-handle that was created when this index population job was scheduled.
     * This makes it possible to {@link JobHandle#cancel() cancel} the scheduled index population,
     * making it never start, through {@link IndexPopulationJob#stop()}.
     */
    public void setHandle( JobHandle handle )
    {
        this.jobHandle = handle;
    }

    public ByteBufferFactory bufferFactory()
    {
        return bufferFactory;
    }

    public MemoryTracker getMemoryTracker()
    {
        return memoryTracker;
    }
}
