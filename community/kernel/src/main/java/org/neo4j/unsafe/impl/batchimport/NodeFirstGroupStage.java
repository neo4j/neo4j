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

import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.unsafe.impl.batchimport.cache.ByteArray;
import org.neo4j.unsafe.impl.batchimport.staging.BatchFeedStep;
import org.neo4j.unsafe.impl.batchimport.staging.ReadRecordsStep;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;
import org.neo4j.unsafe.impl.batchimport.store.StorePrepareIdSequence;

import static org.neo4j.unsafe.impl.batchimport.RecordIdIterator.allIn;

/**
 * Updates dense nodes with which will be the {@link NodeRecord#setNextRel(long) first group} to point to,
 * after a {@link RelationshipGroupDefragmenter} has been run.
 */
public class NodeFirstGroupStage extends Stage
{
    public static final String NAME = "Node --> Group";

    public NodeFirstGroupStage( Configuration config, RecordStore<RelationshipGroupRecord> groupStore,
            RecordStore<NodeRecord> nodeStore, ByteArray cache )
    {
        super( NAME, null, config, 0 );
        add( new BatchFeedStep( control(), config, allIn( groupStore, config ), groupStore.getRecordSize() ) );
        add( new ReadRecordsStep<>( control(), config, true, groupStore ) );
        add( new NodeSetFirstGroupStep( control(), config, nodeStore, cache ) );
        add( new UpdateRecordsStep<>( control(), config, nodeStore, new StorePrepareIdSequence() ) );
    }
}
