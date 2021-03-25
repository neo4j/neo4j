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
import java.util.function.IntPredicate;
import java.util.function.LongFunction;

import org.neo4j.collection.PrimitiveLongResourceCollections.AbstractPrimitiveLongBaseResourceIterator;
import org.neo4j.configuration.Config;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.api.index.PhaseTracker;
import org.neo4j.kernel.impl.api.index.PropertyScanConsumer;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.api.index.TokenScanConsumer;
import org.neo4j.kernel.impl.index.schema.LabelScanStore;
import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStore;
import org.neo4j.lock.Lock;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEntityScanCursor;
import org.neo4j.storageengine.api.StorageReader;

import static org.neo4j.internal.batchimport.staging.ExecutionMonitor.INVISIBLE;
import static org.neo4j.internal.batchimport.staging.ExecutionSupervisors.superviseDynamicExecution;
import static org.neo4j.io.IOUtils.closeAllUnchecked;

/**
 * Scan store with the view given by iterator created by {@link #getEntityIdIterator(PageCursorTracer)}. This might be a full scan of the store
 * or a partial scan backed by {@link LabelScanStore} or {@link RelationshipTypeScanStore}.
 *
 * @param <CURSOR> the type of cursor used to read the records.
 */
public abstract class PropertyAwareEntityStoreScan<CURSOR extends StorageEntityScanCursor<?>> implements StoreScan
{
    protected final StorageReader storageReader;
    protected final EntityScanCursorBehaviour<CURSOR> cursorBehaviour;
    private final boolean parallelWrite;
    private final JobScheduler scheduler;
    protected final PageCacheTracer cacheTracer;
    private final AtomicBoolean continueScanning = new AtomicBoolean();
    private final long totalCount;
    private final MemoryTracker memoryTracker;
    protected final int[] entityTokenIdFilter;
    private final IntPredicate propertyKeyIdFilter;
    private final LongFunction<Lock> lockFunction;
    private final Config dbConfig;
    private PhaseTracker phaseTracker;
    protected final TokenScanConsumer tokenScanConsumer;
    protected final PropertyScanConsumer propertyScanConsumer;
    private volatile StoreScanStage<CURSOR> stage;

    protected PropertyAwareEntityStoreScan( Config config, StorageReader storageReader, long totalEntityCount, int[] entityTokenIdFilter,
            IntPredicate propertyKeyIdFilter, PropertyScanConsumer propertyScanConsumer, TokenScanConsumer tokenScanConsumer, LongFunction<Lock> lockFunction,
            EntityScanCursorBehaviour<CURSOR> cursorBehaviour, boolean parallelWrite, JobScheduler scheduler,
            PageCacheTracer cacheTracer, MemoryTracker memoryTracker )
    {
        this.storageReader = storageReader;
        this.cursorBehaviour = cursorBehaviour;
        this.parallelWrite = parallelWrite;
        this.scheduler = scheduler;
        this.cacheTracer = cacheTracer;
        this.entityTokenIdFilter = entityTokenIdFilter;
        this.propertyKeyIdFilter = propertyKeyIdFilter;
        this.lockFunction = lockFunction;
        this.totalCount = totalEntityCount;
        this.memoryTracker = memoryTracker;
        this.phaseTracker = PhaseTracker.nullInstance;
        this.tokenScanConsumer = tokenScanConsumer;
        this.propertyScanConsumer = propertyScanConsumer;
        this.dbConfig = config;
    }

    @Override
    public void run( ExternalUpdatesCheck externalUpdatesCheck )
    {
        try
        {
            continueScanning.set( true );
            Configuration config = Configuration.DEFAULT;
            stage = new StoreScanStage<>( dbConfig, config, this::getEntityIdIterator, externalUpdatesCheck, continueScanning, storageReader,
                    entityTokenIdFilter, propertyKeyIdFilter, propertyScanConsumer, tokenScanConsumer, cursorBehaviour, lockFunction, parallelWrite,
                    scheduler, cacheTracer, memoryTracker );
            superviseDynamicExecution( INVISIBLE, stage );
            stage.reportTo( phaseTracker );
        }
        finally
        {
            closeAllUnchecked( storageReader );
        }
    }

    @Override
    public void stop()
    {
        continueScanning.set( false );
    }

    @Override
    public PopulationProgress getProgress()
    {
        StoreScanStage<CURSOR> observedStage = stage;
        if ( totalCount > 0 || observedStage == null )
        {
            return PopulationProgress.single( observedStage != null ? observedStage.numberOfIteratedEntities() : 0, totalCount );
        }

        // nothing to do 100% completed
        return PopulationProgress.DONE;
    }

    @Override
    public void setPhaseTracker( PhaseTracker phaseTracker )
    {
        this.phaseTracker = phaseTracker;
    }

    protected EntityIdIterator getEntityIdIterator( PageCursorTracer cursorTracer )
    {
        return new CursorEntityIdIterator<>( cursorBehaviour.allocateEntityScanCursor( cursorTracer ) );
    }

    static class CursorEntityIdIterator<CURSOR extends StorageEntityScanCursor<?>> extends AbstractPrimitiveLongBaseResourceIterator
            implements EntityIdIterator
    {
        private final CURSOR entityCursor;

        CursorEntityIdIterator( CURSOR entityCursor )
        {
            super( entityCursor::close );
            this.entityCursor = entityCursor;
            entityCursor.scan();
        }

        @Override
        public void invalidateCache()
        {
            // Nothing to invalidate, we're reading directly from the store
        }

        @Override
        protected boolean fetchNext()
        {
            return entityCursor.next() && next( entityCursor.entityReference() );
        }
    }
}
