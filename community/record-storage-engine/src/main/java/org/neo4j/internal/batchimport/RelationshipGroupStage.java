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

import static org.neo4j.internal.recordstorage.RecordCursorTypes.GROUP_CURSOR;

import java.util.function.Function;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.internal.batchimport.cache.NodeRelationshipCache;
import org.neo4j.internal.batchimport.staging.BatchFeedStep;
import org.neo4j.internal.batchimport.staging.Stage;
import org.neo4j.internal.batchimport.store.StorePrepareIdSequence;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.storageengine.api.cursor.StoreCursors;

/**
 * Takes information about relationship groups in the {@link NodeRelationshipCache}, which is produced
 * as a side-effect of linking relationships together, and writes them out into {@link RelationshipGroupStore}.
 */
public class RelationshipGroupStage extends Stage {
    public static final String NAME = "RelationshipGroup";

    RelationshipGroupStage(
            String topic,
            Configuration config,
            RecordStore<RelationshipGroupRecord> store,
            NodeRelationshipCache cache,
            CursorContextFactory contextFactory,
            Function<CursorContext, StoreCursors> storeCursorsCreator) {
        super(NAME, topic, config, 0);
        add(new BatchFeedStep(control(), config, RecordIdIterator.forwards(0, cache.highNodeId(), config), 10));
        add(new ReadGroupRecordsByCacheStep(control(), config, store, cache, contextFactory));
        add(new UpdateRecordsStep<>(
                control(),
                config,
                store,
                new StorePrepareIdSequence(),
                contextFactory,
                storeCursorsCreator,
                GROUP_CURSOR));
    }
}
