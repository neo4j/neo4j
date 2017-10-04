/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import org.neo4j.function.Suppliers;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.storageengine.api.schema.PopulationProgress;

import static java.lang.Thread.currentThread;
import static org.neo4j.helpers.FutureAdapter.latchGuardedValue;

/**
 * A background job for initially populating one or more index over existing data in the database.
 * Use provided store view to scan store. Participating {@link IndexPopulator} are added with
 * {@link #addPopulator(IndexPopulator, long, IndexDescriptor, org.neo4j.kernel.api.index.SchemaIndexProvider.Descriptor,
 * String, FlippableIndexProxy, FailedIndexProxyFactory)}
 * before {@link #run() running} this job.
 */
public class IndexPopulationJob implements Runnable
{
    private final IndexingService.Monitor monitor;
    private final MultipleIndexPopulator multiPopulator;
    private final CountDownLatch doneSignal = new CountDownLatch( 1 );
    private final SchemaState schemaState;

    private volatile StoreScan<IndexPopulationFailedKernelException> storeScan;
    private volatile boolean cancelled;

    public IndexPopulationJob( MultipleIndexPopulator multiPopulator, IndexingService.Monitor monitor, SchemaState schemaState )
    {
        this.multiPopulator = multiPopulator;
        this.schemaState = schemaState;
        this.monitor = monitor;
    }

    /**
     * Adds an {@link IndexPopulator} to be populated in this store scan. All participating populators must
     * be added before calling {@link #run()}.
     *
     * @param populator {@link IndexPopulator} to participate.
     * @param descriptor {@link IndexDescriptor} describing the index.
     * @param providerDescriptor provider of this index.
     * @param indexUserDescription user description of this index.
     * @param flipper {@link FlippableIndexProxy} to call after a successful population.
     * @param failedIndexProxyFactory {@link FailedIndexProxyFactory} to use after an unsuccessful population.
     */
    public void addPopulator( IndexPopulator populator,
            long indexId,
            IndexDescriptor descriptor,
            SchemaIndexProvider.Descriptor providerDescriptor,
            String indexUserDescription ,
            FlippableIndexProxy flipper,
            FailedIndexProxyFactory failedIndexProxyFactory )
    {
        assert storeScan == null : "Population have already started, too late to add populators at this point";
        this.multiPopulator.addPopulator( populator, indexId, descriptor, providerDescriptor, flipper,
                failedIndexProxyFactory, indexUserDescription );
    }

    /**
     * Scans the store using store view and populates all participating {@link IndexPopulator} with data relevant to
     * each index.
     * The scan continues as long as there's at least one non-failed populator.
     */
    @Override
    public void run()
    {
        assert multiPopulator.hasPopulators() : "No index populators was added so there'd be no point in running this job";
        assert storeScan == null : "Population have already started";

        String oldThreadName = currentThread().getName();
        currentThread().setName( "Index populator" );

        try
        {
            try
            {
                multiPopulator.create();
                multiPopulator.replaceIndexCounts( 0, 0, 0 );

                indexAllNodes();
                monitor.indexPopulationScanComplete();
                if ( cancelled )
                {
                    multiPopulator.cancel();
                    // We remain in POPULATING state
                    return;
                }

                multiPopulator.flipAfterPopulation();

                schemaState.clear();
            }
            catch ( Throwable t )
            {
                multiPopulator.fail( t );
            }
        }
        finally
        {
            doneSignal.countDown();
            currentThread().setName( oldThreadName );
        }
    }

    private void indexAllNodes() throws IndexPopulationFailedKernelException
    {
        storeScan = multiPopulator.indexAllNodes();
        storeScan.run();
    }

    public PopulationProgress getPopulationProgress()
    {
        if ( storeScan == null )
        {
            // indexing hasn't begun yet
            return PopulationProgress.NONE;
        }
        return storeScan.getProgress();
    }

    public Future<Void> cancel()
    {
        // Stop the population
        if ( storeScan != null )
        {
            cancelled = true;
            storeScan.stop();
        }

        return latchGuardedValue( Suppliers.<Void>singleton( null ), doneSignal, "Index population job cancel" );
    }

    /**
     * A transaction happened that produced the given updates. Let this job incorporate its data,
     * feeding it to the {@link IndexPopulator}.
     *
     * @param update {@link IndexEntryUpdate} to queue.
     */
    public void update( IndexEntryUpdate<?> update )
    {
        multiPopulator.queue( update );
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[populator:" + multiPopulator + "]";
    }

    public void awaitCompletion() throws InterruptedException
    {
        doneSignal.await();
    }
}
