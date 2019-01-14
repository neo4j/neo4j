/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.unsafe.impl.batchimport;

import java.util.function.Predicate;

import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.staging.BatchFeedStep;
import org.neo4j.unsafe.impl.batchimport.staging.ReadRecordsStep;
import org.neo4j.unsafe.impl.batchimport.staging.RecordDataAssembler;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;
import org.neo4j.unsafe.impl.batchimport.stats.StatsProvider;
import org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStores;
import org.neo4j.unsafe.impl.batchimport.store.PrepareIdSequence;

import static org.neo4j.unsafe.impl.batchimport.RecordIdIterator.backwards;
import static org.neo4j.unsafe.impl.batchimport.staging.Step.ORDER_SEND_DOWNSTREAM;
import static org.neo4j.unsafe.impl.batchimport.staging.Step.RECYCLE_BATCHES;

/**
 * Sets {@link RelationshipRecord#setFirstPrevRel(long)} and {@link RelationshipRecord#setSecondPrevRel(long)}
 * by going through the {@link RelationshipStore} in reversed order. It uses the {@link NodeRelationshipCache}
 * to link chains together. Steps:
 *
 * <ol>
 * <li>{@link ReadRecordsStep} reads records from store and passes on downwards to be processed.
 * Ids are read page-wise by {@link RecordIdIterator}, where each page is read forwards internally,
 * i.e. the records in the batches are ordered by ascending id and so consecutive steps needs to
 * process the records within each batch from end to start.</li>
 * <li>{@link RelationshipLinkbackStep} processes each batch and assigns the "prev" pointers in
 * {@link RelationshipRecord} by using {@link NodeRelationshipCache}.</li>
 * <li>{@link UpdateRecordsStep} writes the updated records back into store.</li>
 * </ol>
 */
public class RelationshipLinkbackStage extends Stage
{
    public static final String NAME = "Relationship <-- Relationship";

    public RelationshipLinkbackStage( String topic, Configuration config, BatchingNeoStores stores,
            NodeRelationshipCache cache, Predicate<RelationshipRecord> readFilter,
            Predicate<RelationshipRecord> changeFilter, int nodeTypes, StatsProvider... additionalStatsProvider )
    {
        super( NAME, topic, config, ORDER_SEND_DOWNSTREAM | RECYCLE_BATCHES );
        RelationshipStore store = stores.getRelationshipStore();
        add( new BatchFeedStep( control(), config, backwards( 0, store.getHighId(), config ), store.getRecordSize() ) );
        add( new ReadRecordsStep<>( control(), config, true, store, new RecordDataAssembler<>( store::newRecord, readFilter ) ) );
        add( new RelationshipLinkbackStep( control(), config, cache, changeFilter, nodeTypes, additionalStatsProvider ) );
        add( new UpdateRecordsStep<>( control(), config, store, PrepareIdSequence.of( stores.usesDoubleRelationshipRecordUnits() ) ) );
    }
}
