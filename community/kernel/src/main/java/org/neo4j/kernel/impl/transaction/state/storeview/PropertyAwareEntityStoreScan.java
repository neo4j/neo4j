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

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntPredicate;
import java.util.function.LongFunction;

import org.neo4j.collection.PrimitiveLongResourceCollections.AbstractPrimitiveLongBaseResourceIterator;
import org.neo4j.configuration.Config;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.internal.kernel.api.PopulationProgress;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.api.index.PhaseTracker;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.index.schema.LabelScanStore;
import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStore;
import org.neo4j.lock.Lock;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.EntityTokenUpdate;
import org.neo4j.storageengine.api.EntityUpdates;
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
 * @param <FAILURE> on failure during processing.
 */
public abstract class PropertyAwareEntityStoreScan<CURSOR extends StorageEntityScanCursor<?>, FAILURE extends Exception>
        implements StoreScan<FAILURE>
{
    protected final StorageReader storageReader;
    protected final EntityScanCursorBehaviour<CURSOR> cursorBehaviour;
    private final boolean parallelWrite;
    protected final PageCacheTracer cacheTracer;
    private final AtomicBoolean continueScanning = new AtomicBoolean();
    private final long totalCount;
    private final MemoryTracker memoryTracker;
    protected final int[] entityTokenIdFilter;
    private final IntPredicate propertyKeyIdFilter;
    private final LongFunction<Lock> lockFunction;
    private final Config dbConfig;
    private PhaseTracker phaseTracker;
    protected final Visitor<List<EntityTokenUpdate>,FAILURE> tokenUpdateVisitor;
    protected final Visitor<List<EntityUpdates>,FAILURE> propertyUpdateVisitor;
    private volatile StoreScanStage<FAILURE,CURSOR> stage;

    protected PropertyAwareEntityStoreScan( Config config, StorageReader storageReader, long totalEntityCount, int[] entityTokenIdFilter,
            IntPredicate propertyKeyIdFilter, Visitor<List<EntityTokenUpdate>,FAILURE> tokenUpdateVisitor,
            Visitor<List<EntityUpdates>,FAILURE> propertyUpdateVisitor, LongFunction<Lock> lockFunction,
            EntityScanCursorBehaviour<CURSOR> cursorBehaviour, boolean parallelWrite, PageCacheTracer cacheTracer, MemoryTracker memoryTracker )
    {
        this.storageReader = storageReader;
        this.cursorBehaviour = cursorBehaviour;
        this.parallelWrite = parallelWrite;
        this.cacheTracer = cacheTracer;
        this.entityTokenIdFilter = entityTokenIdFilter;
        this.propertyKeyIdFilter = propertyKeyIdFilter;
        this.lockFunction = lockFunction;
        this.totalCount = totalEntityCount;
        this.memoryTracker = memoryTracker;
        this.phaseTracker = PhaseTracker.nullInstance;
        this.tokenUpdateVisitor = tokenUpdateVisitor;
        this.propertyUpdateVisitor = propertyUpdateVisitor;
        this.dbConfig = config;
    }

    @Override
    public void run( ExternalUpdatesCheck externalUpdatesCheck ) throws FAILURE
    {
        try
        {
            continueScanning.set( true );
            Configuration config = Configuration.DEFAULT;
            stage = new StoreScanStage<>( dbConfig, config, this::getEntityIdIterator, externalUpdatesCheck, continueScanning, storageReader,
                    entityTokenIdFilter, propertyKeyIdFilter, propertyUpdateVisitor, tokenUpdateVisitor, cursorBehaviour, lockFunction, parallelWrite,
                    cacheTracer, memoryTracker );
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
        if ( totalCount > 0 )
        {
            return PopulationProgress.single( stage.numberOfIteratedEntities(), totalCount );
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
