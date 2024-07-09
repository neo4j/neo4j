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
package org.neo4j.internal.batchimport;

import java.util.function.Function;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.internal.batchimport.cache.ByteArray;
import org.neo4j.internal.batchimport.cache.NumberArrayFactory;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.internal.batchimport.staging.ExecutionSupervisors;
import org.neo4j.internal.batchimport.staging.Stage;
import org.neo4j.internal.batchimport.stats.StatsProvider;
import org.neo4j.internal.batchimport.store.BatchingNeoStores;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;

/**
 * Defragments {@link RelationshipGroupRecord} so that they end up sequential per node in the group store.
 * There's one constraint which is assumed to be true here: any relationship group that we see in the store
 * for any given {@link RelationshipGroupRecord#getOwningNode() owner} must have a lower
 * {@link RelationshipGroupRecord#getType() type} than any previous group encountered for that node,
 * i.e. all {@link RelationshipGroupRecord#getNext() next pointers} must be either
 * {@link Record#NO_NEXT_RELATIONSHIP NULL} or lower than the group id at hand. When this is true,
 * and the defragmenter verifies this constraint, the groups will be reversed so that types are instead
 * ascending and groups are always co-located.
 */
public class RelationshipGroupDefragmenter {

    private final Configuration config;
    private final ExecutionMonitor executionMonitor;
    private final Monitor monitor;
    private final NumberArrayFactory numberArrayFactory;
    private final CursorContextFactory contextFactory;
    private final MemoryTracker memoryTracker;

    public interface Monitor {
        /**
         * When defragmenting the relationship group store it may happen in chunks, selected by node range.
         * Every time a chunk is selected this method is called.
         *
         * @param fromNodeId low node id in the range to process (inclusive).
         * @param toNodeId high node id in the range to process (exclusive).
         */
        default void defragmentingNodeRange(long fromNodeId, long toNodeId) { // empty
        }

        Monitor EMPTY = new Monitor() { // empty
                };
    }

    public RelationshipGroupDefragmenter(
            Configuration config,
            ExecutionMonitor executionMonitor,
            Monitor monitor,
            NumberArrayFactory numberArrayFactory,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker) {
        this.config = config;
        this.executionMonitor = executionMonitor;
        this.monitor = monitor;
        this.numberArrayFactory = numberArrayFactory;
        this.contextFactory = contextFactory;
        this.memoryTracker = memoryTracker;
    }

    public void run(long memoryWeCanHoldForCertain, BatchingNeoStores neoStore, long highNodeId) {
        try (RelationshipGroupCache groupCache =
                new RelationshipGroupCache(numberArrayFactory, memoryWeCanHoldForCertain, highNodeId, memoryTracker)) {
            // Read from the temporary relationship group store...
            RecordStore<RelationshipGroupRecord> fromStore = neoStore.getTemporaryRelationshipGroupStore();
            // and write into the main relationship group store
            RecordStore<RelationshipGroupRecord> toStore = neoStore.getRelationshipGroupStore();
            Function<CursorContext, StoreCursors> storeCursorCreator =
                    cursorContext -> new CachedStoreCursors(neoStore.getNeoStores(), cursorContext);

            // Count all nodes, how many groups each node has each
            Configuration groupConfig = Configuration.withBatchSize(
                    config, neoStore.getRelationshipGroupStore().getRecordsPerPage());
            StatsProvider memoryUsage = new MemoryUsageStatsProvider(neoStore, groupCache);
            executeStage(new CountGroupsStage(groupConfig, fromStore, groupCache, contextFactory, memoryUsage));
            long fromNodeId = 0;
            while (fromNodeId < highNodeId) {
                // See how many nodes' groups we can fit into the cache this iteration of the loop.
                // Groups that doesn't fit in this round will be included in consecutive rounds.
                long toNodeId = groupCache.prepare(fromNodeId);
                monitor.defragmentingNodeRange(fromNodeId, toNodeId);
                // Cache those groups
                executeStage(
                        new ScanAndCacheGroupsStage(groupConfig, fromStore, groupCache, contextFactory, memoryUsage));
                // And write them in sequential order in the store
                executeStage(
                        new WriteGroupsStage(groupConfig, groupCache, toStore, contextFactory, storeCursorCreator));

                // Make adjustments for the next iteration
                fromNodeId = toNodeId;
            }

            // Now update nodes to point to the new groups
            ByteArray groupCountCache = groupCache.getGroupCountCache();
            groupCountCache.clear();
            Configuration nodeConfig =
                    Configuration.withBatchSize(config, neoStore.getNodeStore().getRecordsPerPage());
            executeStage(new NodeFirstGroupStage(
                    nodeConfig, toStore, neoStore.getNodeStore(), groupCountCache, contextFactory, storeCursorCreator));
        }
    }

    private void executeStage(Stage stage) {
        ExecutionSupervisors.superviseExecution(executionMonitor, stage);
    }
}
