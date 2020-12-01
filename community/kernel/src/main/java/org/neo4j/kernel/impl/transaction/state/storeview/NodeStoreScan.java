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
import java.util.function.IntPredicate;
import javax.annotation.Nullable;

import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.lock.LockService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.EntityTokenUpdate;
import org.neo4j.storageengine.api.EntityUpdates;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StorageReader;

import static org.neo4j.lock.LockType.SHARED;

/**
 * Scan the node store and produce {@link EntityUpdates updates for indexes} and/or {@link EntityTokenUpdate updates for label index}
 * depending on which {@link Visitor visitors} that are used.
 */
public class NodeStoreScan<FAILURE extends Exception> extends PropertyAwareEntityStoreScan<StorageNodeCursor,FAILURE>
{
    private static final String TRACER_TAG = "NodeStoreScan_getNodeCount";

    public NodeStoreScan( Config config, StorageReader storageReader, LockService locks,
            @Nullable Visitor<List<EntityTokenUpdate>,FAILURE> labelUpdateVisitor,
            @Nullable Visitor<List<EntityUpdates>,FAILURE> propertyUpdatesVisitor,
            int[] labelIds, IntPredicate propertyKeyIdFilter, boolean parallelWrite,
            PageCacheTracer cacheTracer, MemoryTracker memoryTracker )
    {
        super( config, storageReader, getNodeCount( storageReader, cacheTracer ), labelIds, propertyKeyIdFilter, labelUpdateVisitor, propertyUpdatesVisitor,
                id -> locks.acquireNodeLock( id, SHARED ), new NodeCursorBehaviour( storageReader ), parallelWrite, cacheTracer, memoryTracker );
    }

    private static long getNodeCount( StorageReader storageReader, PageCacheTracer cacheTracer )
    {
        try ( PageCursorTracer cursorTracer = cacheTracer.createPageCursorTracer( TRACER_TAG ) )
        {
            return storageReader.nodesGetCount( cursorTracer );
        }
    }
}
