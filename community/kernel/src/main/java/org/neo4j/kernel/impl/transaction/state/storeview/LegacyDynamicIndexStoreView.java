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

import org.apache.commons.lang3.ArrayUtils;

import java.util.function.IntPredicate;
import java.util.function.Supplier;

import org.neo4j.configuration.Config;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.PropertyScanConsumer;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.api.index.TokenScanConsumer;
import org.neo4j.kernel.impl.index.schema.LabelScanStore;
import org.neo4j.lock.LockService;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.storageengine.api.StorageReader;

/**
 * Store view that will try to use label scan store {@link LabelScanStore} to produce the view unless label scan
 * store is empty or explicitly told to use store in which cases it will fallback to whole store scan.
 */
public class LegacyDynamicIndexStoreView implements IndexStoreView
{
    private static final String ALL_NODE_STORE_SCAN_TAG = "DynamicIndexStoreView_useAllNodeStoreScan";

    private final FullScanStoreView fullScanStoreView;
    private final LabelScanStore labelScanStore;
    protected final LockService locks;
    private final Log log;
    private final Config config;
    protected final Supplier<StorageReader> storageEngine;

    public LegacyDynamicIndexStoreView( FullScanStoreView fullScanStoreView, LabelScanStore labelScanStore,
            LockService locks, Supplier<StorageReader> storageEngine, LogProvider logProvider, Config config )
    {
        this.fullScanStoreView = fullScanStoreView;
        this.labelScanStore = labelScanStore;
        this.locks = locks;
        this.storageEngine = storageEngine;
        this.log = logProvider.getLog( getClass() );
        this.config = config;
    }

    @Override
    public StoreScan visitNodes( int[] labelIds, IntPredicate propertyKeyIdFilter, PropertyScanConsumer propertyScanConsumer,
            TokenScanConsumer labelScanConsumer, boolean forceStoreScan, boolean parallelWrite, PageCacheTracer cacheTracer,
            MemoryTracker memoryTracker )
    {
        if ( forceStoreScan || useAllNodeStoreScan( labelIds, cacheTracer ) )
        {
            return fullScanStoreView.visitNodes( labelIds, propertyKeyIdFilter, propertyScanConsumer, labelScanConsumer, forceStoreScan, parallelWrite,
                                                 cacheTracer, memoryTracker );
        }
        return new LegacyLabelViewNodeStoreScan( config, storageEngine.get(), locks, labelScanStore, labelScanConsumer, propertyScanConsumer, labelIds,
                                                 propertyKeyIdFilter, parallelWrite, fullScanStoreView.scheduler, cacheTracer, memoryTracker );
    }

    @Override
    public StoreScan visitRelationships( int[] relationshipTypeIds, IntPredicate propertyKeyIdFilter, PropertyScanConsumer propertyScanConsumer,
            TokenScanConsumer relationshipTypeScanConsumer, boolean forceStoreScan, boolean parallelWrite, PageCacheTracer cacheTracer,
            MemoryTracker memoryTracker )
    {
        return fullScanStoreView.visitRelationships( relationshipTypeIds, propertyKeyIdFilter, propertyScanConsumer, relationshipTypeScanConsumer,
                forceStoreScan, parallelWrite, cacheTracer, memoryTracker );
    }

    @Override
    public boolean isEmpty()
    {
        return fullScanStoreView.isEmpty();
    }

    private boolean useAllNodeStoreScan( int[] labelIds, PageCacheTracer cacheTracer )
    {
        try ( PageCursorTracer cursorTracer = cacheTracer.createPageCursorTracer( ALL_NODE_STORE_SCAN_TAG ) )
        {
            return ArrayUtils.isEmpty( labelIds ) || isEmptyLabelScanStore( cursorTracer );
        }
        catch ( Exception e )
        {
            log.error( "Cannot determine number of labeled nodes, falling back to all nodes scan.", e );
            return true;
        }
    }

    private boolean isEmptyLabelScanStore( PageCursorTracer cursorTracer ) throws Exception
    {
        return labelScanStore.isEmpty( cursorTracer );
    }

    @Override
    public NodePropertyAccessor newPropertyAccessor( PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        return fullScanStoreView.newPropertyAccessor( cursorTracer, memoryTracker );
    }
}
