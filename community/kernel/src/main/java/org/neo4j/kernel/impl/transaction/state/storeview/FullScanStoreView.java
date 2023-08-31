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

import org.neo4j.configuration.Config;
import org.neo4j.io.pagecache.PageCacheOpenOptions;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.PropertyScanConsumer;
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.api.index.TokenScanConsumer;
import org.neo4j.lock.LockService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.ReadableStorageEngine;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.util.Preconditions;

/**
 * Node store view that will always visit all nodes during store scan.
 */
public class FullScanStoreView implements IndexStoreView {
    protected final LockService locks;
    protected final ReadableStorageEngine storageEngine;
    protected final Config config;
    protected final JobScheduler scheduler;

    public FullScanStoreView(
            LockService locks, ReadableStorageEngine storageEngine, Config config, JobScheduler scheduler) {
        this.locks = locks;
        this.storageEngine = storageEngine;
        this.config = config;
        this.scheduler = scheduler;
    }

    @Override
    public StoreScan visitNodes(
            int[] labelIds,
            PropertySelection propertySelection,
            PropertyScanConsumer propertyScanConsumer,
            TokenScanConsumer labelScanConsumer,
            boolean forceStoreScan,
            boolean parallelWrite,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker) {
        return new NodeStoreScan(
                config,
                storageEngine.newReader(),
                storageEngine::createStorageCursors,
                locks,
                labelScanConsumer,
                propertyScanConsumer,
                labelIds,
                propertySelection,
                parallelWrite,
                scheduler,
                contextFactory,
                memoryTracker,
                storageEngine.getOpenOptions().contains(PageCacheOpenOptions.MULTI_VERSIONED));
    }

    @Override
    public StoreScan visitRelationships(
            int[] relationshipTypeIds,
            PropertySelection propertySelection,
            PropertyScanConsumer propertyScanConsumer,
            TokenScanConsumer relationshipTypeScanConsumer,
            boolean forceStoreScan,
            boolean parallelWrite,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker) {
        if (relationshipTypeScanConsumer != null
                && storageEngine.indexingBehaviour().useNodeIdsInRelationshipTokenIndex()) {
            // This scan will visit data to populate relationship type lookup index. The storage engine can have an
            // indexing behaviour where each ID in it represents a node and the storage cursors will yield all types
            // of relationships that the node has. This strategy doesn't work in combination with populating
            // relationship property indexes because they are always relationship-based.
            // This scenario is avoided in IndexingService where it will make sure that a single population won't
            // be configured to populate both of these types of indexes in the same scan.
            Preconditions.checkArgument(
                    propertyScanConsumer == null,
                    "Cannot run a node-based relationship type lookup index population together with a relationship property index population");
            return new NodeRelationshipTypesStoreScan(
                    config,
                    storageEngine.newReader(),
                    storageEngine::createStorageCursors,
                    locks,
                    relationshipTypeScanConsumer,
                    relationshipTypeIds,
                    parallelWrite,
                    scheduler,
                    contextFactory,
                    memoryTracker);
        }

        return new RelationshipStoreScan(
                config,
                storageEngine.newReader(),
                storageEngine::createStorageCursors,
                locks,
                relationshipTypeScanConsumer,
                propertyScanConsumer,
                relationshipTypeIds,
                propertySelection,
                parallelWrite,
                scheduler,
                contextFactory,
                memoryTracker,
                storageEngine.getOpenOptions().contains(PageCacheOpenOptions.MULTI_VERSIONED));
    }

    @Override
    public boolean isEmpty(CursorContext cursorContext) {
        try (StorageReader reader = storageEngine.newReader()) {
            return reader.nodesGetCount(cursorContext) == 0 && reader.relationshipsGetCount(cursorContext) == 0;
        }
    }
}
