/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.state.storeview;

import static org.neo4j.internal.batchimport.staging.StageExecution.DEFAULT_PANIC_MONITOR;
import static org.neo4j.internal.batchimport.staging.Step.ORDER_SEND_DOWNSTREAM;
import static org.neo4j.internal.batchimport.stats.Keys.total_processing_wall_clock_time;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongFunction;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.executor.ProcessorScheduler;
import org.neo4j.internal.batchimport.staging.Stage;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.api.index.PhaseTracker;
import org.neo4j.kernel.impl.api.index.PropertyScanConsumer;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.api.index.TokenScanConsumer;
import org.neo4j.lock.Lock;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.StorageEntityScanCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.cursor.StoreCursors;

public class StoreScanStage<CURSOR extends StorageEntityScanCursor<?>> extends Stage {
    private final ReadEntityIdsStep feedStep;
    private final GenerateIndexUpdatesStep<CURSOR> generatorStep;
    private WriteUpdatesStep writeStep;

    public StoreScanStage(
            Config dbConfig,
            Configuration config,
            BiFunction<CursorContext, StoreCursors, EntityIdIterator> entityIdIteratorSupplier,
            StoreScan.ExternalUpdatesCheck externalUpdatesCheck,
            AtomicBoolean continueScanning,
            StorageReader storageReader,
            Function<CursorContext, StoreCursors> storeCursorsFactory,
            int[] entityTokenIdFilter,
            PropertySelection propertySelection,
            PropertyScanConsumer propertyScanConsumer,
            TokenScanConsumer tokenScanConsumer,
            EntityScanCursorBehaviour<CURSOR> entityCursorBehaviour,
            LongFunction<Lock> lockFunction,
            boolean parallelWrite,
            JobScheduler scheduler,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker,
            boolean canDetermineExternalUpdatesCutOffPoint) {
        super(
                "IndexPopulation store scan",
                null,
                config,
                parallelWrite ? 0 : ORDER_SEND_DOWNSTREAM,
                runInJobScheduler(scheduler),
                DEFAULT_PANIC_MONITOR);
        int parallelism = dbConfig.get(GraphDatabaseInternalSettings.index_population_workers);
        long maxBatchByteSize = dbConfig.get(GraphDatabaseInternalSettings.index_population_batch_max_byte_size);
        // Read from entity iterator --> long[]
        add(
                feedStep = new ReadEntityIdsStep(
                        control(),
                        config,
                        entityIdIteratorSupplier,
                        storeCursorsFactory,
                        contextFactory,
                        externalUpdatesCheck,
                        continueScanning,
                        canDetermineExternalUpdatesCutOffPoint));
        // Read entities --> List<EntityUpdates>
        add(
                generatorStep = new GenerateIndexUpdatesStep<>(
                        control(),
                        config,
                        storageReader,
                        storeCursorsFactory,
                        propertySelection,
                        entityCursorBehaviour,
                        entityTokenIdFilter,
                        propertyScanConsumer,
                        tokenScanConsumer,
                        lockFunction,
                        parallelism,
                        maxBatchByteSize,
                        parallelWrite,
                        contextFactory,
                        memoryTracker));
        if (!parallelWrite) {
            // Write the updates with a single thread if we're not allowed to do this in parallel. The updates are still
            // generated in parallel
            add(writeStep = new WriteUpdatesStep(control(), config, contextFactory));
        }
    }

    private static ProcessorScheduler runInJobScheduler(JobScheduler scheduler) {
        return (job, name) -> scheduler.schedule(Group.INDEX_POPULATION_WORK, job);
    }

    void reportTo(PhaseTracker phaseTracker) {
        var scanNanos = feedStep.stats().stat(total_processing_wall_clock_time).asLong()
                + generatorStep.stats().stat(total_processing_wall_clock_time).asLong();
        phaseTracker.registerTime(PhaseTracker.Phase.SCAN, TimeUnit.NANOSECONDS.toMillis(scanNanos));

        if (writeStep != null) {
            var writeNanos =
                    writeStep.stats().stat(total_processing_wall_clock_time).asLong();
            phaseTracker.registerTime(PhaseTracker.Phase.WRITE, TimeUnit.NANOSECONDS.toMillis(writeNanos));
        }
    }

    long numberOfCompletedEntities() {
        return generatorStep.numberOfCompletedEntities();
    }
}
