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
package org.neo4j.kernel.impl.transaction.state.storeview;

import org.apache.commons.lang3.ArrayUtils;

import java.io.IOException;
import java.util.function.IntPredicate;
import java.util.function.Supplier;

import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.index.schema.LabelScanStore;
import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStore;
import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings;
import org.neo4j.lock.LockService;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.EntityTokenUpdate;
import org.neo4j.storageengine.api.EntityUpdates;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.storageengine.api.StorageReader;

/**
 * Store view that will try to use label scan store {@link LabelScanStore} to produce the view unless label scan
 * store is empty or explicitly told to use store in which cases it will fallback to whole store scan.
 */
public class DynamicIndexStoreView implements IndexStoreView
{
    private final NeoStoreIndexStoreView neoStoreIndexStoreView;
    private final LabelScanStore labelScanStore;
    private final RelationshipTypeScanStore relationshipTypeScanStore;
    protected final LockService locks;
    private final Log log;
    private final Config config;
    protected final Supplier<StorageReader> storageEngine;

    public DynamicIndexStoreView( NeoStoreIndexStoreView neoStoreIndexStoreView, LabelScanStore labelScanStore,
            RelationshipTypeScanStore relationshipTypeScanStore, LockService locks,
            Supplier<StorageReader> storageEngine, LogProvider logProvider, Config config )
    {
        this.neoStoreIndexStoreView = neoStoreIndexStoreView;
        this.labelScanStore = labelScanStore;
        this.relationshipTypeScanStore = relationshipTypeScanStore;
        this.locks = locks;
        this.storageEngine = storageEngine;
        this.log = logProvider.getLog( getClass() );
        this.config = config;
    }

    @Override
    public <FAILURE extends Exception> StoreScan<FAILURE> visitNodes( int[] labelIds,
            IntPredicate propertyKeyIdFilter, Visitor<EntityUpdates,FAILURE> propertyUpdatesVisitor,
            Visitor<EntityTokenUpdate,FAILURE> labelUpdateVisitor,
            boolean forceStoreScan, PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        if ( forceStoreScan || useAllNodeStoreScan( labelIds, cursorTracer ) )
        {
            return neoStoreIndexStoreView.visitNodes( labelIds, propertyKeyIdFilter, propertyUpdatesVisitor, labelUpdateVisitor,
                    forceStoreScan, cursorTracer, memoryTracker );
        }
        return new LabelViewNodeStoreScan<>( storageEngine.get(), locks, labelScanStore, labelUpdateVisitor,
                propertyUpdatesVisitor, labelIds, propertyKeyIdFilter, cursorTracer, memoryTracker );
    }

    @Override
    public <FAILURE extends Exception> StoreScan<FAILURE> visitRelationships( int[] relationshipTypeIds, IntPredicate propertyKeyIdFilter,
            Visitor<EntityUpdates,FAILURE> propertyUpdateVisitor,
            Visitor<EntityTokenUpdate,FAILURE> relationshipTypeUpdateVisitor,
            boolean forceStoreScan, PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        if ( forceStoreScan || useAllRelationshipStoreScan( relationshipTypeIds, cursorTracer ) )
        {
            return neoStoreIndexStoreView.visitRelationships( relationshipTypeIds, propertyKeyIdFilter, propertyUpdateVisitor, relationshipTypeUpdateVisitor,
                    forceStoreScan, cursorTracer, memoryTracker );
        }
        return new RelationshipTypeViewRelationshipStoreScan<>( storageEngine.get(), locks, relationshipTypeScanStore, relationshipTypeUpdateVisitor,
                propertyUpdateVisitor, relationshipTypeIds, propertyKeyIdFilter, cursorTracer, memoryTracker );
    }

    @Override
    public boolean isEmpty()
    {
        return neoStoreIndexStoreView.isEmpty();
    }

    private boolean useAllNodeStoreScan( int[] labelIds, PageCursorTracer cursorTracer )
    {
        try
        {
            return ArrayUtils.isEmpty( labelIds ) || isEmptyLabelScanStore( cursorTracer );
        }
        catch ( Exception e )
        {
            log.error( "Cannot determine number of labeled nodes, falling back to all nodes scan.", e );
            return true;
        }
    }

    private boolean useAllRelationshipStoreScan( int[] relationshipTypeIds, PageCursorTracer cursorTracer )
    {
        try
        {
            return !config.get( RelationshipTypeScanStoreSettings.enable_relationship_type_scan_store ) || ArrayUtils.isEmpty( relationshipTypeIds ) ||
                    isEmptyRelationshipTypeStoreScan( cursorTracer );
        }
        catch ( Exception e )
        {
            log.error( "Cannot determine number of relationships in scan store, falling back to all relationships scan.", e );
            return true;
        }
    }

    private boolean isEmptyLabelScanStore( PageCursorTracer cursorTracer ) throws Exception
    {
        return labelScanStore.isEmpty( cursorTracer );
    }

    private boolean isEmptyRelationshipTypeStoreScan( PageCursorTracer cursorTracer ) throws IOException
    {
        return relationshipTypeScanStore.isEmpty( cursorTracer );
    }

    @Override
    public NodePropertyAccessor newPropertyAccessor( PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        return neoStoreIndexStoreView.newPropertyAccessor( cursorTracer, memoryTracker );
    }
}
