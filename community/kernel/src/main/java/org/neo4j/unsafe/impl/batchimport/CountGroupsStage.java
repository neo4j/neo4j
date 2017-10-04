/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.unsafe.impl.batchimport.staging.BatchFeedStep;
import org.neo4j.unsafe.impl.batchimport.staging.ReadRecordsStep;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;

import static org.neo4j.unsafe.impl.batchimport.RecordIdIterator.allIn;

/**
 * Stage for counting groups per node, populates {@link RelationshipGroupCache}. Steps:
 *
 * <ol>
 * <li>{@link ReadRecordsStep} reads {@link RelationshipGroupRecord relationship group records} for later counting.</li>
 * <li>{@link CountGroupsStep} populates {@link RelationshipGroupCache} with how many relationship groups each
 * node has. This is useful for calculating how to divide the work of defragmenting the relationship groups
 * in a {@link ScanAndCacheGroupsStage later stage}.</li>
 * </ol>
 */
public class CountGroupsStage extends Stage
{
    public CountGroupsStage( Configuration config, RecordStore<RelationshipGroupRecord> store,
            RelationshipGroupCache groupCache )
    {
        super( "Count groups", config );
        add( new BatchFeedStep( control(), config, allIn( store, config ), store.getRecordSize() ) );
        add( new ReadRecordsStep<>( control(), config, false, store, null ) );
        add( new CountGroupsStep( control(), config, groupCache ) );
    }
}
