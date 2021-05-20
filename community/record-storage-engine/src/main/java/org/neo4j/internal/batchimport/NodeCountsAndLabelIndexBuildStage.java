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
package org.neo4j.internal.batchimport;

import org.neo4j.common.ProgressReporter;
import org.neo4j.configuration.Config;
import org.neo4j.counts.CountsAccessor;
import org.neo4j.internal.batchimport.cache.NodeLabelsCache;
import org.neo4j.internal.batchimport.staging.BatchFeedStep;
import org.neo4j.internal.batchimport.staging.ReadRecordsStep;
import org.neo4j.internal.batchimport.staging.Stage;
import org.neo4j.internal.batchimport.staging.Step;
import org.neo4j.internal.batchimport.store.BatchingNeoStores;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.index.schema.LabelScanStore;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.memory.MemoryTracker;

import static org.neo4j.internal.batchimport.RecordIdIterators.allIn;

/**
 * Counts nodes and their labels and also builds {@link LabelScanStore label index} while doing so.
 */
public class NodeCountsAndLabelIndexBuildStage extends Stage
{
    public static final String NAME = "Node counts and label index build";

    public NodeCountsAndLabelIndexBuildStage( Configuration config, Config dbConfig, BatchingNeoStores neoStores, NodeLabelsCache cache,
            NodeStore nodeStore, int highLabelId, CountsAccessor.Updater countsUpdater, ProgressReporter progressReporter,
            IndexImporterFactory indexImporterFactory, PageCacheTracer pageCacheTracer,
            MemoryTracker memoryTracker, MemoryUsageStatsProvider additionalStatsProviders )
    {
        super( NAME, null, config, Step.ORDER_SEND_DOWNSTREAM | Step.RECYCLE_BATCHES );
        add( new BatchFeedStep( control(), config, allIn( nodeStore, config ), nodeStore.getRecordSize() ) );
        add( new ReadRecordsStep<>( control(), config, false, nodeStore, pageCacheTracer ) );
        if ( config.indexConfig().createLabelIndex() )
        {
            add( new LabelIndexWriterStep( control(), config, dbConfig, neoStores, indexImporterFactory, memoryTracker, pageCacheTracer ) );
        }
        add( new RecordProcessorStep<>( control(), "COUNT", config, () -> new NodeCountsProcessor(
                nodeStore, cache, highLabelId, countsUpdater, progressReporter ), true, 0, pageCacheTracer, additionalStatsProviders ) );
    }
}
