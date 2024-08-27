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
package org.neo4j.internal.recordstorage;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.neo4j.internal.schema.SchemaCache;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.lock.LockGroup;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.util.IdUpdateListener;
import org.neo4j.storageengine.util.IndexUpdatesWorkSync;
import org.neo4j.util.VisibleForTesting;

/**
 * A batch context implementation that does not do anything with scan stores.
 * It assumes use of token indexes.
 * This will be the only implementation when migration to token indexes is done!
 */
public class BatchContextImpl implements BatchContext {
    private final IndexUpdatesWorkSync indexUpdatesSync;
    private final CursorContext cursorContext;
    private final IdUpdateListener idUpdateListener;

    private final IndexActivator indexActivator;
    private final LockGroup lockGroup;
    private final IndexUpdates indexUpdates;
    private final MemoryTracker memoryTracker;

    public BatchContextImpl(
            IndexUpdateListener indexUpdateListener,
            IndexUpdatesWorkSync indexUpdatesSync,
            NodeStore nodeStore,
            PropertyStore propertyStore,
            StorageEngine recordStorageEngine,
            SchemaCache schemaCache,
            CursorContext cursorContext,
            MemoryTracker memoryTracker,
            IdUpdateListener idUpdateListener,
            StoreCursors storeCursors) {
        this.indexActivator = new IndexActivator(indexUpdateListener);
        this.indexUpdatesSync = indexUpdatesSync;
        this.cursorContext = cursorContext;
        this.idUpdateListener = idUpdateListener;
        this.lockGroup = new LockGroup();
        this.indexUpdates = new OnlineIndexUpdates(
                nodeStore,
                schemaCache,
                new PropertyPhysicalToLogicalConverter(propertyStore, storeCursors, memoryTracker),
                recordStorageEngine.newReader(),
                cursorContext,
                memoryTracker,
                storeCursors);
        this.memoryTracker = memoryTracker;
    }

    @Override
    public LockGroup getLockGroup() {
        return lockGroup;
    }

    @Override
    public void close() throws Exception {
        applyPendingIndexUpdates();

        IOUtils.closeAll(indexUpdates, idUpdateListener, lockGroup, indexActivator);
    }

    @Override
    public IndexActivator getIndexActivator() {
        return indexActivator;
    }

    @Override
    public void applyPendingIndexUpdates() throws IOException {
        if (hasUpdates()) {
            IndexUpdatesWorkSync.Batch indexUpdatesBatch = indexUpdatesSync.newBatch();
            indexUpdatesBatch.add(indexUpdates);
            try {
                indexUpdatesBatch.apply(cursorContext);
            } catch (ExecutionException e) {
                throw new IOException("Failed to flush index updates", e);
            } finally {
                indexUpdates.reset();
            }
        }
    }

    @VisibleForTesting
    boolean hasUpdates() {
        return indexUpdates.hasUpdates();
    }

    @Override
    public IndexUpdates indexUpdates() {
        return indexUpdates;
    }

    @Override
    public IdUpdateListener getIdUpdateListener() {
        return idUpdateListener;
    }

    @Override
    public MemoryTracker memoryTracker() {
        return memoryTracker;
    }
}
