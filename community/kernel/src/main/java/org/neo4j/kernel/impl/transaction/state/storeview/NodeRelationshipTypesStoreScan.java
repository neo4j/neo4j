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

import static org.neo4j.kernel.impl.transaction.state.storeview.NodeStoreScan.getNodeCount;
import static org.neo4j.lock.LockType.SHARED;

import java.util.function.Function;
import org.neo4j.configuration.Config;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.api.index.TokenScanConsumer;
import org.neo4j.lock.LockService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.cursor.StoreCursors;

/**
 * Used as store scan for building a relationship type lookup index if
 * {@link StorageEngineIndexingBehaviour#useNodeIdsInRelationshipTokenIndex() storage engine uses node-based relationship type lookup index}.
 * It visits nodes consecutively and yields its relationship types.
 */
public class NodeRelationshipTypesStoreScan extends PropertyAwareEntityStoreScan<StorageNodeCursor> {
    public NodeRelationshipTypesStoreScan(
            Config config,
            StorageReader reader,
            Function<CursorContext, StoreCursors> storeCursorsFactory,
            LockService locks,
            TokenScanConsumer relationshipTypeScanConsumer,
            int[] relationshipTypeIds,
            boolean parallelWrite,
            JobScheduler scheduler,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker) {
        super(
                config,
                reader,
                storeCursorsFactory,
                getNodeCount(reader, contextFactory),
                relationshipTypeIds,
                null,
                null,
                relationshipTypeScanConsumer,
                id -> locks.acquireNodeLock(id, SHARED),
                new NodeRelationshipTypesCursorBehaviour(reader),
                parallelWrite,
                scheduler,
                contextFactory,
                memoryTracker,
                false);
    }
}
