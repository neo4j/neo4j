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

import static org.neo4j.internal.batchimport.staging.ReadRecordsStep.NO_MONITOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.RELATIONSHIP_CURSOR;

import java.util.function.Function;
import java.util.function.Predicate;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.internal.batchimport.cache.NodeRelationshipCache;
import org.neo4j.internal.batchimport.staging.BatchFeedStep;
import org.neo4j.internal.batchimport.staging.ReadRecordsStep;
import org.neo4j.internal.batchimport.staging.RecordDataAssembler;
import org.neo4j.internal.batchimport.staging.Stage;
import org.neo4j.internal.batchimport.staging.Step;
import org.neo4j.internal.batchimport.stats.StatsProvider;
import org.neo4j.internal.batchimport.store.BatchingNeoStores;
import org.neo4j.internal.batchimport.store.PrepareIdSequence;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.cursor.StoreCursors;

public class RelationshipLinkforwardStage extends Stage {
    public static final String NAME = "Relationship --> Relationship";

    public RelationshipLinkforwardStage(
            String topic,
            Configuration config,
            BatchingNeoStores stores,
            NodeRelationshipCache cache,
            Predicate<RelationshipRecord> readFilter,
            Function<CursorContext, StoreCursors> storeCursorsCreator,
            Predicate<RelationshipRecord> denseChangeFilter,
            int nodeTypes,
            CursorContextFactory contextFactory,
            StatsProvider... additionalStatsProvider) {
        super(NAME, topic, config, Step.ORDER_SEND_DOWNSTREAM | Step.RECYCLE_BATCHES);
        RelationshipStore store = stores.getRelationshipStore();
        add(new BatchFeedStep(
                control(),
                config,
                RecordIdIterator.forwards(0, store.getIdGenerator().getHighId(), config),
                store.getRecordSize()));
        add(new ReadRecordsStep<>(
                control(),
                config,
                true,
                store,
                contextFactory,
                new RecordDataAssembler<>(store::newRecord, readFilter, true),
                NO_MONITOR));
        add(new RelationshipLinkforwardStep(
                control(), config, cache, denseChangeFilter, nodeTypes, additionalStatsProvider));
        add(new UpdateRecordsStep<>(
                control(),
                config,
                store,
                PrepareIdSequence.of(stores.usesDoubleRelationshipRecordUnits()),
                contextFactory,
                storeCursorsCreator,
                RELATIONSHIP_CURSOR));
    }
}
