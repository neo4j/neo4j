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

import static org.neo4j.internal.batchimport.RecordIdIterators.allInReversed;

import org.neo4j.batchimport.api.Configuration;
import org.neo4j.internal.batchimport.staging.BatchFeedStep;
import org.neo4j.internal.batchimport.staging.ReadRecordsStep;
import org.neo4j.internal.batchimport.staging.Stage;
import org.neo4j.internal.batchimport.staging.Step;
import org.neo4j.internal.batchimport.stats.StatsProvider;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;

/**
 * Scans {@link RelationshipGroupRecord} from store in reverse, this because during import the relationships
 * are imported per type in descending type id order, i.e. with highest type first. This stage runs as part
 * of defragmenting the relationship group store so that relationship groups for a particular node will be
 * co-located on disk. The {@link RelationshipGroupCache} given to this stage has already been primed with
 * information about which groups to cache, i.e. for which nodes (id range). This step in combination
 * with {@link WriteGroupsStage} alternating each other can run multiple times to limit max memory consumption
 * caching relationship groups.
 */
public class ScanAndCacheGroupsStage extends Stage {
    public static final String NAME = "Gather";

    public ScanAndCacheGroupsStage(
            Configuration config,
            RecordStore<RelationshipGroupRecord> store,
            RelationshipGroupCache cache,
            CursorContextFactory contextFactory,
            StatsProvider... additionalStatsProviders) {
        super(NAME, null, config, Step.RECYCLE_BATCHES);
        add(new BatchFeedStep(control(), config, allInReversed(store, config), store.getRecordSize()));
        add(new ReadRecordsStep<>(control(), config, false, store, contextFactory));
        add(new CacheGroupsStep(control(), config, cache, contextFactory, additionalStatsProviders));
    }
}
