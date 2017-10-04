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

import static org.neo4j.unsafe.impl.batchimport.RecordIdIterator.allInReversed;

/**
 * Scans {@link RelationshipGroupRecord} from store in reverse, this because during import the relationships
 * are imported per type in descending type id order, i.e. with highest type first. This stage runs as part
 * of defragmenting the relationship group store so that relationship groups for a particular node will be
 * co-located on disk. The {@link RelationshipGroupCache} given to this stage has already been primed with
 * information about which groups to cache, i.e. for which nodes (id range). This step in combination
 * with {@link WriteGroupsStage} alternating each other can run multiple times to limit max memory consumption
 * caching relationship groups.
 */
public class ScanAndCacheGroupsStage extends Stage
{
    public ScanAndCacheGroupsStage( Configuration config, RecordStore<RelationshipGroupRecord> store,
            RelationshipGroupCache cache )
    {
        super( "Gather", config );
        add( new BatchFeedStep( control(), config, allInReversed( store, config ), store.getRecordSize() ) );
        add( new ReadRecordsStep<>( control(), config, false, store, null ) );
        add( new CacheGroupsStep( control(), config, cache ) );
    }
}
