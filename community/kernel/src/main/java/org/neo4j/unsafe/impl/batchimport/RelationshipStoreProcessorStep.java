/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

/**
 * Convenient step for processing all in use {@link RelationshipRecord records} in the
 * {@link RelationshipStore relationship store}.
 */
public class RelationshipStoreProcessorStep extends StoreProcessorStep<RelationshipRecord>
{
    private final RelationshipStore relationshipStore;

    protected RelationshipStoreProcessorStep( StageControl control, String name, Configuration config,
            RelationshipStore relationshipStore, StoreProcessor<RelationshipRecord> processor )
    {
        super( control, name, config.batchSize(), config.movingAverageSize(), relationshipStore, processor );
        this.relationshipStore = relationshipStore;
    }

    @Override
    protected RelationshipRecord loadRecord( long id, RelationshipRecord into )
    {
        return relationshipStore.fillRecord( id, into, RecordLoad.CHECK ) ? into : null;
    }

    @Override
    protected RelationshipRecord createReusableRecord()
    {
        return new RelationshipRecord( -1 );
    }
}
