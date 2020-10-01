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

import java.util.function.IntPredicate;
import java.util.function.Supplier;

import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.lock.LockService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.EntityTokenUpdate;
import org.neo4j.storageengine.api.EntityUpdates;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.storageengine.api.StorageReader;

/**
 * Node store view that will always visit all nodes during store scan.
 */
public class NeoStoreIndexStoreView implements IndexStoreView
{
    protected final LockService locks;
    protected final Supplier<StorageReader> storageEngine;

    public NeoStoreIndexStoreView( LockService locks, Supplier<StorageReader> storageEngine )
    {
        this.locks = locks;
        this.storageEngine = storageEngine;
    }

    @Override
    public <FAILURE extends Exception> StoreScan<FAILURE> visitNodes(
            final int[] labelIds, IntPredicate propertyKeyIdFilter,
            final Visitor<EntityUpdates, FAILURE> propertyUpdatesVisitor,
            final Visitor<EntityTokenUpdate, FAILURE> labelUpdateVisitor,
            boolean forceStoreScan, PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        return new NodeStoreScan<>( storageEngine.get(), locks, labelUpdateVisitor,
                propertyUpdatesVisitor, labelIds, propertyKeyIdFilter, cursorTracer, memoryTracker );
    }

    @Override
    public <FAILURE extends Exception> StoreScan<FAILURE> visitRelationships( final int[] relationshipTypeIds, IntPredicate propertyKeyIdFilter,
            final Visitor<EntityUpdates,FAILURE> propertyUpdatesVisitor,
            Visitor<EntityTokenUpdate,FAILURE> relationshipTypeUpdateVisitor,
            boolean forceStoreScan, PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        return new RelationshipStoreScan<>( storageEngine.get(), locks, relationshipTypeUpdateVisitor, propertyUpdatesVisitor, relationshipTypeIds,
                propertyKeyIdFilter, cursorTracer, memoryTracker );
    }

    @Override
    public NodePropertyAccessor newPropertyAccessor( PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        return new DefaultNodePropertyAccessor( storageEngine.get(), cursorTracer, memoryTracker );
    }

    @Override
    public boolean isEmpty()
    {
        try ( StorageReader reader = storageEngine.get() )
        {
            return reader.nodesGetCount( PageCursorTracer.NULL ) == 0 && reader.relationshipsGetCount() == 0;
        }
    }
}
