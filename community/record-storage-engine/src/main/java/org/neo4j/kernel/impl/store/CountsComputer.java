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
package org.neo4j.kernel.impl.store;

import org.neo4j.common.ProgressReporter;
import org.neo4j.counts.CountsAccessor;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.NodeCountsStage;
import org.neo4j.internal.batchimport.RelationshipCountsStage;
import org.neo4j.internal.batchimport.cache.NodeLabelsCache;
import org.neo4j.internal.batchimport.cache.NumberArrayFactories;
import org.neo4j.internal.batchimport.cache.NumberArrayFactory;
import org.neo4j.internal.counts.CountsBuilder;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.logging.Log;
import org.neo4j.memory.MemoryTracker;

import static org.neo4j.internal.batchimport.cache.NumberArrayFactories.NO_MONITOR;
import static org.neo4j.internal.batchimport.staging.ExecutionSupervisors.superviseDynamicExecution;

public class CountsComputer implements CountsBuilder
{
    private final NodeStore nodes;
    private final RelationshipStore relationships;
    private final int highLabelId;
    private final int highRelationshipTypeId;
    private final long lastCommittedTransactionId;
    private final DatabaseLayout databaseLayout;
    private final ProgressReporter progressMonitor;
    private final NumberArrayFactory numberArrayFactory;
    private final PageCacheTracer pageCacheTracer;
    private final MemoryTracker memoryTracker;

    public CountsComputer( NeoStores stores, PageCache pageCache, PageCacheTracer pageCacheTracer, DatabaseLayout databaseLayout,
            MemoryTracker memoryTracker, Log log )
    {
        this( stores.getMetaDataStore().getLastCommittedTransactionId(),
              stores.getNodeStore(), stores.getRelationshipStore(),
              (int) stores.getLabelTokenStore().getHighId(),
              (int) stores.getRelationshipTypeTokenStore().getHighId(),
                NumberArrayFactories.auto( pageCache, pageCacheTracer, databaseLayout.databaseDirectory(), true, NO_MONITOR, log,
                        databaseLayout.getDatabaseName() ),
              databaseLayout, pageCacheTracer, memoryTracker );
    }

    private CountsComputer( long lastCommittedTransactionId, NodeStore nodes, RelationshipStore relationships, int highLabelId, int highRelationshipTypeId,
            NumberArrayFactory numberArrayFactory, DatabaseLayout databaseLayout, PageCacheTracer pageCacheTracer, MemoryTracker memoryTracker )
    {
        this( lastCommittedTransactionId, nodes, relationships, highLabelId, highRelationshipTypeId,
                numberArrayFactory, databaseLayout, ProgressReporter.SILENT, pageCacheTracer, memoryTracker );
    }

    public CountsComputer( long lastCommittedTransactionId, NodeStore nodes, RelationshipStore relationships, int highLabelId, int highRelationshipTypeId,
            NumberArrayFactory numberArrayFactory, DatabaseLayout databaseLayout, ProgressReporter progressMonitor,
            PageCacheTracer pageCacheTracer, MemoryTracker memoryTracker )
    {
        this.lastCommittedTransactionId = lastCommittedTransactionId;
        this.nodes = nodes;
        this.relationships = relationships;
        this.highLabelId = highLabelId;
        this.highRelationshipTypeId = highRelationshipTypeId;
        this.numberArrayFactory = numberArrayFactory;
        this.databaseLayout = databaseLayout;
        this.progressMonitor = progressMonitor;
        this.pageCacheTracer = pageCacheTracer;
        this.memoryTracker = memoryTracker;
    }

    @Override
    public void initialize( CountsAccessor.Updater countsUpdater, PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        if ( hasNotEmptyNodesOrRelationshipsStores( cursorTracer ) )
        {
            progressMonitor.start( nodes.getHighestPossibleIdInUse( cursorTracer ) + relationships.getHighestPossibleIdInUse( cursorTracer ) );
            populateCountStore( countsUpdater );
        }
        progressMonitor.completed();
    }

    private boolean hasNotEmptyNodesOrRelationshipsStores( PageCursorTracer cursorTracer )
    {
        return (nodes.getHighestPossibleIdInUse( cursorTracer ) != -1) || (relationships.getHighestPossibleIdInUse( cursorTracer ) != -1);
    }

    private void populateCountStore( CountsAccessor.Updater countsUpdater )
    {
        try ( NodeLabelsCache cache = new NodeLabelsCache( numberArrayFactory, nodes.getHighId(), highLabelId, memoryTracker ) )
        {
            Configuration configuration = Configuration.defaultConfiguration( databaseLayout.databaseDirectory() );

            // Count nodes
            superviseDynamicExecution(
                    new NodeCountsStage( configuration, cache, nodes, highLabelId, countsUpdater, progressMonitor, pageCacheTracer ) );
            // Count relationships
            superviseDynamicExecution(
                    new RelationshipCountsStage( configuration, cache, relationships, highLabelId, highRelationshipTypeId, countsUpdater,
                            numberArrayFactory, progressMonitor, pageCacheTracer, memoryTracker ) );
        }
    }

    @Override
    public long lastCommittedTxId()
    {
        return lastCommittedTransactionId;
    }
}
