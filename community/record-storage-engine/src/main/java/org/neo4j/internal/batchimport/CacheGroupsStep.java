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

import org.neo4j.batchimport.api.Configuration;
import org.neo4j.internal.batchimport.staging.BatchSender;
import org.neo4j.internal.batchimport.staging.ProcessorStep;
import org.neo4j.internal.batchimport.staging.StageControl;
import org.neo4j.internal.batchimport.stats.StatsProvider;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.memory.MemoryTracker;

/**
 * Caches {@link RelationshipGroupRecord} into {@link RelationshipGroupCache}.
 */
public class CacheGroupsStep extends ProcessorStep<RelationshipGroupRecord[]> {
    private final RelationshipGroupCache cache;

    public CacheGroupsStep(
            StageControl control,
            Configuration config,
            RelationshipGroupCache cache,
            CursorContextFactory contextFactory,
            StatsProvider... additionalStatsProviders) {
        super(control, "CACHE", config, 1, contextFactory, additionalStatsProviders);
        this.cache = cache;
    }

    @Override
    protected void process(
            RelationshipGroupRecord[] batch,
            BatchSender sender,
            CursorContext cursorContext,
            MemoryTracker memoryTracker) {
        // These records are read page-wise forwards, but should be cached in reverse
        // since the records exists in the store in reverse order.
        for (int i = batch.length - 1; i >= 0; i--) {
            RelationshipGroupRecord record = batch[i];
            if (record.inUse()) {
                cache.put(record);
            }
        }
    }
}
