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
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.api.index.TokenIndexReader;
import org.neo4j.kernel.impl.api.index.PropertyScanConsumer;
import org.neo4j.kernel.impl.api.index.TokenScanConsumer;
import org.neo4j.lock.LockService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.util.StorageRelationshipByNodeScanCursor;

/**
 * Used for driving an index population scan for relationship index backed by a node-based relationship type lookup index,
 * i.e. for storage engines that uses {@link StorageEngineIndexingBehaviour#useNodeIdsInRelationshipTokenIndex() node-based relationship type lookup index}.
 *
 * Since visited relationship IDs aren't strictly sequential then the optimization that index population has, where external updates "ahead of"
 * the current position are skipped cannot be used.
 */
public class NodeRelationshipsIndexedStoreScan
        extends PropertyAwareEntityStoreScan<StorageRelationshipByNodeScanCursor> {
    private final TokenIndexReader tokenIndexReader;

    public NodeRelationshipsIndexedStoreScan(
            Config config,
            StorageReader reader,
            Function<CursorContext, StoreCursors> storeCursorsFactory,
            LockService locks,
            TokenIndexReader tokenIndexReader,
            TokenScanConsumer relationshipTypeScanConsumer,
            PropertyScanConsumer propertyScanConsumer,
            int[] relationshipTypeIds,
            PropertySelection propertySelection,
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
                propertySelection,
                propertyScanConsumer,
                relationshipTypeScanConsumer,
                id -> locks.acquireNodeLock(id, SHARED),
                new NodeRelationshipsCursorBehaviour(
                        reader, RelationshipSelection.selection(relationshipTypeIds, Direction.OUTGOING)),
                parallelWrite,
                scheduler,
                contextFactory,
                memoryTracker,
                false);
        this.tokenIndexReader = tokenIndexReader;
    }

    @Override
    public EntityIdIterator getEntityIdIterator(CursorContext cursorContext, StoreCursors storeCursors) {
        return new TokenIndexScanIdIterator(tokenIndexReader, entityTokenIdFilter, cursorContext);
    }
}
