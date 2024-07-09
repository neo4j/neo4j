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

import static org.neo4j.internal.batchimport.RecordIdIterator.forwards;

import org.neo4j.batchimport.api.Configuration;
import org.neo4j.internal.batchimport.cache.NodeRelationshipCache;
import org.neo4j.internal.batchimport.staging.BatchFeedStep;
import org.neo4j.internal.batchimport.staging.ReadRecordsStep;
import org.neo4j.internal.batchimport.staging.Stage;
import org.neo4j.internal.batchimport.staging.Step;
import org.neo4j.internal.batchimport.stats.StatsProvider;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.store.RelationshipStore;

/**
 * Goes through {@link RelationshipStore} and increments counts per start/end node,
 * calling {@link NodeRelationshipCache#incrementCount(long)}. This is in preparation of linking relationships.
 */
public class NodeDegreeCountStage extends Stage {
    public static final String NAME = "Node Degrees";

    public NodeDegreeCountStage(
            Configuration config,
            RelationshipStore store,
            NodeRelationshipCache cache,
            StatsProvider memoryUsageStatsProvider,
            CursorContextFactory contextFactory) {
        super(NAME, null, config, Step.RECYCLE_BATCHES);
        add(new BatchFeedStep(
                control(), config, forwards(0, store.getIdGenerator().getHighId(), config), store.getRecordSize()));
        add(new ReadRecordsStep<>(control(), config, false, store, contextFactory));
        add(new CalculateDenseNodesStep(control(), config, cache, contextFactory, memoryUsageStatsProvider));
    }
}
