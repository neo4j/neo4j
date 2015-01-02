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
import org.neo4j.unsafe.impl.batchimport.staging.LonelyProcessingStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

/**
 * Convenient step for processing all in use {@link RelationshipRecord records} in the
 * {@link RelationshipStore relationship store}.
 */
public abstract class RelationshipStoreProcessorStep extends LonelyProcessingStep
{
    private final RelationshipStore relationshipStore;

    protected RelationshipStoreProcessorStep( StageControl control, String name, Configuration config,
            RelationshipStore relationshipStore )
    {
        super( control, name, config.batchSize(), config.movingAverageSize() );
        this.relationshipStore = relationshipStore;
    }

    @Override
    protected final void process()
    {
        long highId = relationshipStore.getHighestPossibleIdInUse();
        RelationshipRecord heavilyReusedRecord = new RelationshipRecord( -1 );
        for ( long i = highId; i >= 0; i-- )
        {
            if ( relationshipStore.fillRecord( i, heavilyReusedRecord, RecordLoad.CHECK )
                    && process( heavilyReusedRecord ) )
            {
                relationshipStore.updateRecord( heavilyReusedRecord );
            }
            itemProcessed();
        }
    }

    /**
     * Processes a {@link RelationshipRecord relationship}.
     *
     * @return {@code true} if the relationship changed and should be updated in the store.
     */
    protected abstract boolean process( RelationshipRecord record );
}
