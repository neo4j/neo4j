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
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;
import org.neo4j.unsafe.impl.batchimport.store.StorePrepareIdSequence;

import static org.neo4j.unsafe.impl.batchimport.RelationshipGroupCache.GROUP_ENTRY_SIZE;

/**
 * Writes cached {@link RelationshipGroupRecord} from {@link ScanAndCacheGroupsStage} to store. This is done
 * as a separate step because here the cache is supposed to contain complete chains of relationship group records
 * for a section of the node store. Steps:
 *
 * <ol>
 * <li>{@link ReadGroupsFromCacheStep} reads complete relationship group chains from {@link RelationshipGroupCache}.
 * </li>
 * <li>{@link EncodeGroupsStep} sets correct {@link RelationshipGroupRecord#setNext(long)} pointers for records.</li>
 * <li>{@link UpdateRecordsStep} writes the relationship group records to store.</li>
 * </ol>
 */
public class WriteGroupsStage extends Stage
{
    public static final String NAME = "Write";

    public WriteGroupsStage( Configuration config, RelationshipGroupCache cache,
            RecordStore<RelationshipGroupRecord> store )
    {
        super( NAME, null, config, 0 );
        add( new ReadGroupsFromCacheStep( control(), config, cache.iterator(), GROUP_ENTRY_SIZE ) );
        add( new EncodeGroupsStep( control(), config, store ) );
        add( new UpdateRecordsStep<>( control(), config, store, new StorePrepareIdSequence() ) );
    }
}
