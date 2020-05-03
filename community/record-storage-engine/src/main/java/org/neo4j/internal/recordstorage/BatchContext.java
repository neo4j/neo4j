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
package org.neo4j.internal.recordstorage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.IdUpdateListener;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.lock.LockGroup;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.EntityTokenUpdate;
import org.neo4j.storageengine.api.EntityTokenUpdateListener;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.util.concurrent.AsyncApply;
import org.neo4j.util.concurrent.WorkSync;

public class BatchContext implements AutoCloseable
{
    private final WorkSync<EntityTokenUpdateListener,TokenUpdateWork> labelScanStoreSync;
    private final WorkSync<EntityTokenUpdateListener,TokenUpdateWork> relationshipTypeScanStoreSync;
    private final WorkSync<IndexUpdateListener,IndexUpdatesWork> indexUpdatesSync;
    private final NodeStore nodeStore;
    private final PropertyStore propertyStore;
    private final StorageEngine storageEngine;
    private final SchemaCache schemaCache;
    private final PageCursorTracer cursorTracer;
    private final MemoryTracker memoryTracker;
    private final IdUpdateListener idUpdateListener;

    private final IndexActivator indexActivator;
    private final LockGroup lockGroup;
    private List<EntityTokenUpdate> labelUpdates;
    private List<EntityTokenUpdate> relationshipTypeUpdates;
    private IndexUpdates indexUpdates;

    public BatchContext( IndexUpdateListener indexUpdateListener,
            WorkSync<EntityTokenUpdateListener,TokenUpdateWork> labelScanStoreSync,
            WorkSync<EntityTokenUpdateListener,TokenUpdateWork> relationshipTypeScanStoreSync,
            WorkSync<IndexUpdateListener,IndexUpdatesWork> indexUpdatesSync, NodeStore nodeStore, PropertyStore propertyStore,
            RecordStorageEngine recordStorageEngine, SchemaCache schemaCache, PageCursorTracer cursorTracer, MemoryTracker memoryTracker,
            IdUpdateListener idUpdateListener )
    {
        this.indexActivator = new IndexActivator( indexUpdateListener );
        this.labelScanStoreSync = labelScanStoreSync;
        this.relationshipTypeScanStoreSync = relationshipTypeScanStoreSync;
        this.indexUpdatesSync = indexUpdatesSync;
        this.nodeStore = nodeStore;
        this.propertyStore = propertyStore;
        this.storageEngine = recordStorageEngine;
        this.schemaCache = schemaCache;
        this.cursorTracer = cursorTracer;
        this.memoryTracker = memoryTracker;
        this.idUpdateListener = idUpdateListener;
        this.lockGroup = new LockGroup();
    }

    public LockGroup getLockGroup()
    {
        return lockGroup;
    }

    @Override
    public void close() throws Exception
    {
        applyPendingLabelAndIndexUpdates();

        IOUtils.closeAll( indexUpdates, idUpdateListener, lockGroup, indexActivator );
    }

    public IndexActivator getIndexActivator()
    {
        return indexActivator;
    }

    public void applyPendingLabelAndIndexUpdates() throws IOException
    {
        AsyncApply labelUpdatesApply = null;
        AsyncApply relationshipTypeUpdatesApply = null;
        if ( labelUpdates != null )
        {
            // Updates are sorted according to node id here, an artifact of node commands being sorted
            // by node id when extracting from TransactionRecordState.
            labelUpdatesApply = labelScanStoreSync.applyAsync( new TokenUpdateWork( labelUpdates, cursorTracer ) );
            labelUpdates = null;
        }
        if ( relationshipTypeUpdates != null )
        {
            relationshipTypeUpdatesApply = relationshipTypeScanStoreSync.applyAsync( new TokenUpdateWork( relationshipTypeUpdates, cursorTracer ) );
            relationshipTypeUpdates = null;
        }
        if ( indexUpdates != null && indexUpdates.hasUpdates() )
        {
            try
            {
                indexUpdatesSync.apply( new IndexUpdatesWork( indexUpdates, cursorTracer ) );
            }
            catch ( ExecutionException e )
            {
                throw new IOException( "Failed to flush index updates", e );
            }
            indexUpdates = null;
        }

        if ( labelUpdatesApply != null )
        {
            try
            {
                labelUpdatesApply.await();
            }
            catch ( ExecutionException e )
            {
                throw new IOException( "Failed to flush label updates", e );
            }
        }
        if ( relationshipTypeUpdatesApply != null )
        {
            try
            {
                relationshipTypeUpdatesApply.await();
            }
            catch ( ExecutionException e )
            {
                throw new IOException( "Failed to flush relationship type updates", e );
            }
        }
    }

    public IndexUpdates indexUpdates()
    {
        if ( indexUpdates == null )
        {
            indexUpdates = new OnlineIndexUpdates( nodeStore, schemaCache, new PropertyPhysicalToLogicalConverter( propertyStore, cursorTracer ),
                    storageEngine.newReader(), cursorTracer, memoryTracker );
        }
        return indexUpdates;
    }

    public IdUpdateListener getIdUpdateListener()
    {
        return idUpdateListener;
    }

    public List<EntityTokenUpdate> labelUpdates()
    {
        if ( labelUpdates == null )
        {
            labelUpdates = new ArrayList<>();
        }
        return labelUpdates;
    }

    public List<EntityTokenUpdate> relationshipTypeUpdates()
    {
        if ( relationshipTypeUpdates == null )
        {
            relationshipTypeUpdates = new ArrayList<>();
        }
        return relationshipTypeUpdates;
    }
}
