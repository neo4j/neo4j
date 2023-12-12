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

import static org.neo4j.internal.batchimport.RecordIdIterators.allIn;

import java.util.function.Function;
import org.neo4j.counts.CountsUpdater;
import org.neo4j.internal.batchimport.cache.NodeLabelsCache;
import org.neo4j.internal.batchimport.staging.BatchFeedStep;
import org.neo4j.internal.batchimport.staging.ReadRecordsStep;
import org.neo4j.internal.batchimport.staging.Stage;
import org.neo4j.internal.batchimport.staging.Step;
import org.neo4j.internal.batchimport.stats.StatsProvider;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.storageengine.api.cursor.StoreCursors;

/**
 * Reads all records from {@link NodeStore} and process the counts in them, populating {@link NodeLabelsCache}
 * for later use of {@link RelationshipCountsStage}.
 */
public class NodeCountsStage extends Stage {
    public static final String NAME = "Node counts";

    public NodeCountsStage(
            Configuration config,
            NodeLabelsCache cache,
            NodeStore nodeStore,
            int highLabelId,
            CountsUpdater countsUpdater,
            ProgressListener progressListener,
            CursorContextFactory contextFactory,
            Function<CursorContext, StoreCursors> storeCursorsCreator,
            StatsProvider... additionalStatsProviders) {
        super(NAME, null, config, Step.RECYCLE_BATCHES);
        add(new BatchFeedStep(control(), config, allIn(nodeStore, config), nodeStore.getRecordSize()));
        add(new ReadRecordsStep<>(control(), config, false, nodeStore, contextFactory));
        add(new RecordProcessorStep<>(
                control(),
                "COUNT",
                config,
                () -> new NodeCountsProcessor(nodeStore, cache, highLabelId, 0, countsUpdater, progressListener),
                true,
                0,
                contextFactory,
                storeCursorsCreator,
                additionalStatsProviders));
    }
}
