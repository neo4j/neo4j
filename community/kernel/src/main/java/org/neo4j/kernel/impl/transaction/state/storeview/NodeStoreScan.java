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

import java.util.function.IntPredicate;

import org.neo4j.configuration.Config;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.api.index.PropertyScanConsumer;
import org.neo4j.kernel.impl.api.index.TokenScanConsumer;
import org.neo4j.lock.LockService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.EntityUpdates;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.TokenIndexEntryUpdate;

import static org.neo4j.lock.LockType.SHARED;

/**
 * Scan the node store and produce {@link EntityUpdates updates for indexes} and/or {@link TokenIndexEntryUpdate updates for label index}
 * depending on which scan consumer ({@link TokenScanConsumer}, {@link PropertyScanConsumer} or both) is used.
 * <p>
 * {@code labelIds} and {@code propertyKeyIdFilter} are relevant only for {@link PropertyScanConsumer} and don't influence
 * {@link TokenScanConsumer}.
 */
public class NodeStoreScan extends PropertyAwareEntityStoreScan<StorageNodeCursor>
{
    private static final String TRACER_TAG = "NodeStoreScan_getNodeCount";

    public NodeStoreScan( Config config, StorageReader storageReader, LockService locks,
            TokenScanConsumer labelScanConsumer, PropertyScanConsumer propertyScanConsumer,
            int[] labelIds, IntPredicate propertyKeyIdFilter, boolean parallelWrite,
            JobScheduler scheduler, PageCacheTracer cacheTracer, MemoryTracker memoryTracker )
    {
        super( config, storageReader, getNodeCount( storageReader, cacheTracer ), labelIds, propertyKeyIdFilter, propertyScanConsumer, labelScanConsumer,
                id -> locks.acquireNodeLock( id, SHARED ), new NodeCursorBehaviour( storageReader ), parallelWrite, scheduler, cacheTracer, memoryTracker );
    }

    private static long getNodeCount( StorageReader storageReader, PageCacheTracer cacheTracer )
    {
        try ( CursorContext cursorContext = new CursorContext( cacheTracer.createPageCursorTracer( TRACER_TAG ) ) )
        {
            return storageReader.nodesGetCount( cursorContext );
        }
    }
}
