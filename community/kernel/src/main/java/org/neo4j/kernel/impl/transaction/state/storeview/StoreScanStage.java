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
package org.neo4j.kernel.impl.transaction.state.storeview;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.function.LongFunction;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.executor.ProcessorScheduler;
import org.neo4j.internal.batchimport.staging.Stage;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.api.index.PhaseTracker;
import org.neo4j.kernel.impl.api.index.PropertyScanConsumer;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.api.index.TokenScanConsumer;
import org.neo4j.lock.Lock;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEntityScanCursor;
import org.neo4j.storageengine.api.StorageReader;

import static org.neo4j.internal.batchimport.staging.Step.ORDER_SEND_DOWNSTREAM;
import static org.neo4j.internal.batchimport.stats.Keys.total_processing_wall_clock_time;

public class StoreScanStage<CURSOR extends StorageEntityScanCursor<?>> extends Stage
{
    private final ReadEntityIdsStep feedStep;
    private final GenerateIndexUpdatesStep<CURSOR> generatorStep;
    private WriteUpdatesStep writeStep;

    public StoreScanStage( Config dbConfig, Configuration config, Function<PageCursorTracer,EntityIdIterator> entityIdIteratorSupplier,
            StoreScan.ExternalUpdatesCheck externalUpdatesCheck,
            AtomicBoolean continueScanning, StorageReader storageReader, int[] entityTokenIdFilter, IntPredicate propertyKeyIdFilter,
            PropertyScanConsumer propertyScanConsumer, TokenScanConsumer tokenScanConsumer,
            EntityScanCursorBehaviour<CURSOR> entityCursorBehaviour, LongFunction<Lock> lockFunction, boolean parallelWrite,
            JobScheduler scheduler, PageCacheTracer cacheTracer, MemoryTracker memoryTracker )
    {
        super( "IndexPopulation store scan", null, config, parallelWrite ? 0 : ORDER_SEND_DOWNSTREAM, runInJobScheduler( scheduler ) );
        int parallelism = dbConfig.get( GraphDatabaseInternalSettings.index_population_workers );
        long maxBatchByteSize = dbConfig.get( GraphDatabaseInternalSettings.index_population_batch_max_byte_size );
        // Read from entity iterator --> long[]
        add( feedStep = new ReadEntityIdsStep( control(), config, entityIdIteratorSupplier, cacheTracer, externalUpdatesCheck, continueScanning ) );
        // Read entities --> List<EntityUpdates>
        add( generatorStep = new GenerateIndexUpdatesStep<>( control(), config, storageReader, propertyKeyIdFilter, entityCursorBehaviour, entityTokenIdFilter,
                propertyScanConsumer, tokenScanConsumer, lockFunction, parallelism, maxBatchByteSize, parallelWrite, cacheTracer, memoryTracker ) );
        if ( !parallelWrite )
        {
            // Write the updates with a single thread if we're not allowed to do this in parallel. The updates are still generated in parallel
            add( writeStep = new WriteUpdatesStep( control(), config, cacheTracer ) );
        }
    }

    private static ProcessorScheduler runInJobScheduler( JobScheduler scheduler )
    {
        return ( job, name ) -> scheduler.schedule( Group.INDEX_POPULATION_WORK, job );
    }

    void reportTo( PhaseTracker phaseTracker )
    {
        phaseTracker.registerTime( PhaseTracker.Phase.SCAN,
                feedStep.stats().stat( total_processing_wall_clock_time ).asLong() +
                generatorStep.stats().stat( total_processing_wall_clock_time ).asLong() );
        if ( writeStep != null )
        {
            phaseTracker.registerTime( PhaseTracker.Phase.WRITE,
                    writeStep.stats().stat( total_processing_wall_clock_time ).asLong() );
        }
    }

    long numberOfIteratedEntities()
    {
        return feedStep.position();
    }
}
