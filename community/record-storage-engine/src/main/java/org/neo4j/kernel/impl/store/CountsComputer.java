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
package org.neo4j.kernel.impl.store;

import static org.neo4j.internal.batchimport.cache.NumberArrayFactories.NO_MONITOR;
import static org.neo4j.internal.batchimport.staging.ExecutionSupervisors.superviseDynamicExecution;

import java.util.function.Function;
import org.neo4j.counts.CountsUpdater;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.NodeCountsStage;
import org.neo4j.internal.batchimport.RelationshipCountsStage;
import org.neo4j.internal.batchimport.cache.NodeLabelsCache;
import org.neo4j.internal.batchimport.cache.NumberArrayFactories;
import org.neo4j.internal.batchimport.cache.NumberArrayFactory;
import org.neo4j.internal.counts.CountsBuilder;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.logging.InternalLog;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;

public class CountsComputer implements CountsBuilder {
    private final NeoStores neoStores;
    private final NodeStore nodes;
    private final RelationshipStore relationships;
    private final int highLabelId;
    private final int highRelationshipTypeId;
    private final long lastCommittedTransactionId;
    private final ProgressMonitorFactory progressMonitorFactory;
    private final NumberArrayFactory numberArrayFactory;
    private final CursorContextFactory contextFactory;
    private final MemoryTracker memoryTracker;

    public CountsComputer(
            NeoStores stores,
            PageCache pageCache,
            CursorContextFactory contextFactory,
            DatabaseLayout databaseLayout,
            MemoryTracker memoryTracker,
            InternalLog log) {
        this(
                stores,
                stores.getMetaDataStore().getLastCommittedTransactionId(),
                pageCache,
                contextFactory,
                databaseLayout,
                memoryTracker,
                log);
    }

    public CountsComputer(
            NeoStores stores,
            long lastCommittedTransactionId,
            PageCache pageCache,
            CursorContextFactory contextFactory,
            DatabaseLayout databaseLayout,
            MemoryTracker memoryTracker,
            InternalLog log) {
        this(
                stores,
                lastCommittedTransactionId,
                stores.getNodeStore(),
                stores.getRelationshipStore(),
                (int) stores.getLabelTokenStore().getIdGenerator().getHighId(),
                (int) stores.getRelationshipTypeTokenStore().getIdGenerator().getHighId(),
                NumberArrayFactories.auto(
                        pageCache,
                        contextFactory,
                        databaseLayout.databaseDirectory(),
                        true,
                        NO_MONITOR,
                        log,
                        databaseLayout.getDatabaseName()),
                ProgressMonitorFactory.NONE,
                contextFactory,
                memoryTracker);
    }

    public CountsComputer(
            NeoStores stores,
            long lastCommittedTransactionId,
            NodeStore nodes,
            RelationshipStore relationships,
            int highLabelId,
            int highRelationshipTypeId,
            NumberArrayFactory numberArrayFactory,
            ProgressMonitorFactory progressMonitorFactory,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker) {
        this.neoStores = stores;
        this.lastCommittedTransactionId = lastCommittedTransactionId;
        this.nodes = nodes;
        this.relationships = relationships;
        this.highLabelId = highLabelId;
        this.highRelationshipTypeId = highRelationshipTypeId;
        this.numberArrayFactory = numberArrayFactory;
        this.progressMonitorFactory = progressMonitorFactory;
        this.contextFactory = contextFactory;
        this.memoryTracker = memoryTracker;
    }

    @Override
    public void initialize(CountsUpdater countsUpdater, CursorContext cursorContext, MemoryTracker memoryTracker) {
        if (hasNotEmptyNodesOrRelationshipsStores(cursorContext)) {
            var total = nodes.getHighestPossibleIdInUse(cursorContext)
                    + relationships.getHighestPossibleIdInUse(cursorContext);

            try (var progress = progressMonitorFactory.singlePart("CountsUpdater", total)) {
                populateCountStore(countsUpdater, progress);
            }
        }
    }

    private boolean hasNotEmptyNodesOrRelationshipsStores(CursorContext cursorContext) {
        return (nodes.getHighestPossibleIdInUse(cursorContext) != -1)
                || (relationships.getHighestPossibleIdInUse(cursorContext) != -1);
    }

    private void populateCountStore(CountsUpdater countsUpdater, ProgressListener progress) {
        try (NodeLabelsCache cache = new NodeLabelsCache(
                numberArrayFactory, nodes.getIdGenerator().getHighId(), highLabelId, memoryTracker)) {
            Configuration configuration = Configuration.defaultConfiguration();

            // Count nodes
            Function<CursorContext, StoreCursors> storeCursorsFunction =
                    cursorContext -> new CachedStoreCursors(neoStores, cursorContext);
            superviseDynamicExecution(new NodeCountsStage(
                    configuration,
                    cache,
                    nodes,
                    highLabelId,
                    countsUpdater,
                    progress,
                    contextFactory,
                    storeCursorsFunction));
            // Count relationships
            superviseDynamicExecution(new RelationshipCountsStage(
                    configuration,
                    cache,
                    relationships,
                    highLabelId,
                    highRelationshipTypeId,
                    countsUpdater,
                    numberArrayFactory,
                    progress,
                    contextFactory,
                    storeCursorsFunction,
                    memoryTracker));
        }
    }

    @Override
    public long lastCommittedTxId() {
        return lastCommittedTransactionId;
    }
}
