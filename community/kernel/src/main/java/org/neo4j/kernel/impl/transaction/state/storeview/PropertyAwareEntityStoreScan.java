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

import static org.neo4j.internal.batchimport.staging.ExecutionMonitor.INVISIBLE;
import static org.neo4j.internal.batchimport.staging.ExecutionSupervisors.superviseDynamicExecution;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.LongFunction;
import org.neo4j.collection.PrimitiveLongResourceCollections.AbstractPrimitiveLongBaseResourceIterator;
import org.neo4j.configuration.Config;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.api.index.PhaseTracker;
import org.neo4j.kernel.impl.api.index.PropertyScanConsumer;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.api.index.TokenScanConsumer;
import org.neo4j.lock.Lock;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.StorageEntityScanCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.cursor.StoreCursors;

/**
 * Scan store with the view given by iterator created by {@link #getEntityIdIterator(CursorContext, StoreCursors)}. This might be a full scan of the store
 * or a partial scan backed by the node label index.
 *
 * @param <CURSOR> the type of cursor used to read the records.
 */
public abstract class PropertyAwareEntityStoreScan<CURSOR extends StorageEntityScanCursor<?>> implements StoreScan {
    protected final StorageReader storageReader;
    private final Function<CursorContext, StoreCursors> storeCursorsFactory;
    protected final EntityScanCursorBehaviour<CURSOR> cursorBehaviour;
    private final boolean parallelWrite;
    private final JobScheduler scheduler;
    protected final CursorContextFactory contextFactory;
    private final AtomicBoolean continueScanning = new AtomicBoolean();
    private final long totalCount;
    private final MemoryTracker memoryTracker;
    private final boolean canDetermineExternalUpdatesCutOffPoint;
    protected final int[] entityTokenIdFilter;
    private final PropertySelection propertySelection;
    private final LongFunction<Lock> lockFunction;
    private final Config dbConfig;
    private PhaseTracker phaseTracker;
    protected final TokenScanConsumer tokenScanConsumer;
    protected final PropertyScanConsumer propertyScanConsumer;
    private volatile StoreScanStage<CURSOR> stage;
    private volatile boolean closed;

    protected PropertyAwareEntityStoreScan(
            Config config,
            StorageReader storageReader,
            Function<CursorContext, StoreCursors> storeCursorsFactory,
            long totalEntityCount,
            int[] entityTokenIdFilter,
            PropertySelection propertySelection,
            PropertyScanConsumer propertyScanConsumer,
            TokenScanConsumer tokenScanConsumer,
            LongFunction<Lock> lockFunction,
            EntityScanCursorBehaviour<CURSOR> cursorBehaviour,
            boolean parallelWrite,
            JobScheduler scheduler,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker,
            boolean canDetermineExternalUpdatesCutOffPoint) {
        this.storageReader = storageReader;
        this.storeCursorsFactory = storeCursorsFactory;
        this.cursorBehaviour = cursorBehaviour;
        this.parallelWrite = parallelWrite;
        this.scheduler = scheduler;
        this.contextFactory = contextFactory;
        this.entityTokenIdFilter = entityTokenIdFilter;
        this.propertySelection = propertySelection;
        this.lockFunction = lockFunction;
        this.totalCount = totalEntityCount;
        this.memoryTracker = memoryTracker;
        this.canDetermineExternalUpdatesCutOffPoint = canDetermineExternalUpdatesCutOffPoint;
        this.phaseTracker = PhaseTracker.nullInstance;
        this.tokenScanConsumer = tokenScanConsumer;
        this.propertyScanConsumer = propertyScanConsumer;
        this.dbConfig = config;
    }

    @Override
    public void run(ExternalUpdatesCheck externalUpdatesCheck) {
        continueScanning.set(true);
        stage = new StoreScanStage<>(
                dbConfig,
                Configuration.DEFAULT,
                this::getEntityIdIterator,
                externalUpdatesCheck,
                continueScanning,
                storageReader,
                storeCursorsFactory,
                entityTokenIdFilter,
                propertySelection,
                propertyScanConsumer,
                tokenScanConsumer,
                cursorBehaviour,
                lockFunction,
                parallelWrite,
                scheduler,
                contextFactory,
                memoryTracker,
                canDetermineExternalUpdatesCutOffPoint);
        superviseDynamicExecution(INVISIBLE, stage);
        stage.reportTo(phaseTracker);
    }

    @Override
    public void stop() {
        continueScanning.set(false);
    }

    @Override
    public void close() {
        storageReader.close();
        closed = true;
    }

    @Override
    public PopulationProgress getProgress() {
        if (closed) {
            return PopulationProgress.DONE;
        }
        StoreScanStage<CURSOR> observedStage = stage;
        if (totalCount > 0 || observedStage == null) {
            return PopulationProgress.single(
                    observedStage != null ? observedStage.numberOfCompletedEntities() : 0, totalCount);
        }

        // nothing to do 100% completed
        return PopulationProgress.DONE;
    }

    @Override
    public void setPhaseTracker(PhaseTracker phaseTracker) {
        this.phaseTracker = phaseTracker;
    }

    public EntityIdIterator getEntityIdIterator(CursorContext cursorContext, StoreCursors storeCursors) {
        return new CursorEntityIdIterator<>(cursorBehaviour.allocateEntityScanCursor(cursorContext, storeCursors));
    }

    static class CursorEntityIdIterator<CURSOR extends StorageEntityScanCursor<?>>
            extends AbstractPrimitiveLongBaseResourceIterator implements EntityIdIterator {
        private final CURSOR entityCursor;

        CursorEntityIdIterator(CURSOR entityCursor) {
            super(entityCursor::close);
            this.entityCursor = entityCursor;
            entityCursor.scan();
        }

        @Override
        public void invalidateCache() {
            // Nothing to invalidate, we're reading directly from the store
        }

        @Override
        protected boolean fetchNext() {
            return entityCursor.next() && next(entityCursor.entityReference());
        }
    }
}
