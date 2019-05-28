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
package org.neo4j.kernel.impl.api.index;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.function.Suppliers;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.impl.index.schema.ByteBufferFactory;
import org.neo4j.kernel.impl.index.schema.UnsafeDirectByteBufferAllocator;
import org.neo4j.memory.GlobalMemoryTracker;
import org.neo4j.memory.ThreadSafePeakMemoryAllocationTracker;
import org.neo4j.storageengine.api.schema.CapableIndexDescriptor;
import org.neo4j.storageengine.api.schema.PopulationProgress;
import org.neo4j.util.concurrent.Runnables;

import static java.lang.Thread.currentThread;
import static org.neo4j.helpers.FutureAdapter.latchGuardedValue;
import static org.neo4j.kernel.impl.index.schema.BlockBasedIndexPopulator.parseBlockSize;

/**
 * A background job for initially populating one or more index over existing data in the database.
 * Use provided store view to scan store. Participating {@link IndexPopulator} are added with
 * {@link #addPopulator(IndexPopulator, CapableIndexDescriptor, String, FlippableIndexProxy, FailedIndexProxyFactory)}
 * before {@link #run() running} this job.
 */
public class IndexPopulationJob implements Runnable
{
    private final IndexingService.Monitor monitor;
    private final boolean verifyBeforeFlipping;
    private final ByteBufferFactory bufferFactory;
    private final ThreadSafePeakMemoryAllocationTracker memoryAllocationTracker;
    private final MultipleIndexPopulator multiPopulator;
    private final CountDownLatch doneSignal = new CountDownLatch( 1 );

    private volatile StoreScan<IndexPopulationFailedKernelException> storeScan;
    private volatile boolean cancelled;

    public IndexPopulationJob( MultipleIndexPopulator multiPopulator, IndexingService.Monitor monitor, boolean verifyBeforeFlipping )
    {
        this.multiPopulator = multiPopulator;
        this.monitor = monitor;
        this.verifyBeforeFlipping = verifyBeforeFlipping;
        this.memoryAllocationTracker = new ThreadSafePeakMemoryAllocationTracker( GlobalMemoryTracker.INSTANCE );
        this.bufferFactory = new ByteBufferFactory( () -> new UnsafeDirectByteBufferAllocator( memoryAllocationTracker ), parseBlockSize() );
    }

    /**
     * Adds an {@link IndexPopulator} to be populated in this store scan. All participating populators must
     * be added before calling {@link #run()}.
     *  @param populator {@link IndexPopulator} to participate.
     * @param capableIndexDescriptor {@link CapableIndexDescriptor} meta information about index.
     * @param indexUserDescription user description of this index.
     * @param flipper {@link FlippableIndexProxy} to call after a successful population.
     * @param failedIndexProxyFactory {@link FailedIndexProxyFactory} to use after an unsuccessful population.
     */
    MultipleIndexPopulator.IndexPopulation addPopulator( IndexPopulator populator, CapableIndexDescriptor capableIndexDescriptor, String indexUserDescription,
            FlippableIndexProxy flipper, FailedIndexProxyFactory failedIndexProxyFactory )
    {
        assert storeScan == null : "Population have already started, too late to add populators at this point";
        return this.multiPopulator.addPopulator( populator, capableIndexDescriptor, flipper, failedIndexProxyFactory,
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
        String oldThreadName = currentThread().getName();
        try
        {
            if ( !multiPopulator.hasPopulators() )
            {
                return;
            }
            if ( storeScan != null )
            {
                throw new IllegalStateException( "Population already started." );
            }

            currentThread().setName( "Index populator" );
            try
            {
                multiPopulator.create();
                multiPopulator.resetIndexCounts();

                monitor.indexPopulationScanStarting();
                indexAllEntities();
                monitor.indexPopulationScanComplete();
                if ( cancelled )
                {
                    multiPopulator.cancel();
                    // We remain in POPULATING state
                    return;
                }
                multiPopulator.flipAfterPopulation( verifyBeforeFlipping );
            }
            catch ( Throwable t )
            {
                multiPopulator.fail( t );
            }
        }
        finally
        {
            // will only close "additional" resources, not the actual populators, since that's managed by flip
            Runnables.runAll( "Failed to close resources in IndexPopulationJob",
                    () -> multiPopulator.close( true ),
                    () -> monitor.populationJobCompleted( memoryAllocationTracker.peakMemoryUsage() ),
                    bufferFactory::close,
                    doneSignal::countDown,
                    () -> currentThread().setName( oldThreadName ) );
        }
    }

    private void indexAllEntities() throws IndexPopulationFailedKernelException
    {
        storeScan = multiPopulator.indexAllEntities();
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

    public Future<Void> cancel()
    {
        // Stop the population
        if ( storeScan != null )
        {
            cancelled = true;
            storeScan.stop();
            monitor.populationCancelled();
        }

        return latchGuardedValue( Suppliers.singleton( null ), doneSignal, "Index population job cancel" );
    }

    void cancelPopulation( MultipleIndexPopulator.IndexPopulation population )
    {
        multiPopulator.cancelIndexPopulation( population );
    }

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
        multiPopulator.queueUpdate( update );
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

    public ByteBufferFactory bufferFactory()
    {
        return bufferFactory;
    }
}
