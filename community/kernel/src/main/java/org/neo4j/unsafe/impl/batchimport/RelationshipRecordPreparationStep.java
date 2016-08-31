/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.Configuration;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingRelationshipTypeTokenRepository;

import static org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper.ID_NOT_FOUND;

/**
 * Creates and initializes {@link RelationshipRecord} batches to later be filled with actual data
 * and pointers. This is a separate step to remove work from main step.
 */
public class RelationshipRecordPreparationStep extends ProcessorStep<Batch<InputRelationship,RelationshipRecord>>
{
    private final BatchingRelationshipTypeTokenRepository relationshipTypeRepository;

    public RelationshipRecordPreparationStep( StageControl control, Configuration config,
            BatchingRelationshipTypeTokenRepository relationshipTypeRepository )
    {
        super( control, "RECORDS", config, 0 );
        this.relationshipTypeRepository = relationshipTypeRepository;
    }

    @Override
    protected void process( Batch<InputRelationship,RelationshipRecord> batch, BatchSender sender ) throws Throwable
    {
        batch.records = new RelationshipRecord[batch.input.length];
        long id = batch.firstRecordId;
        for ( int i = 0, idIndex = 0; i < batch.records.length; i++, id++ )
        {
            RelationshipRecord relationship = batch.records[i] = new RelationshipRecord( id );
            InputRelationship batchRelationship = batch.input[i];
            long startNodeId = batch.ids[idIndex++];
            long endNodeId = batch.ids[idIndex++];
            if ( startNodeId == ID_NOT_FOUND || endNodeId == ID_NOT_FOUND )
            {
                relationship.setInUse( false );
            }
            else
            {
                relationship.setInUse( true );

                // Most rels will not be first in chain
                relationship.setFirstInFirstChain( false );
                relationship.setFirstInSecondChain( false );
                relationship.setFirstPrevRel( Record.NO_NEXT_RELATIONSHIP.intValue() );
                relationship.setSecondPrevRel( Record.NO_NEXT_RELATIONSHIP.intValue() );
                relationship.setFirstNode( startNodeId );
                relationship.setSecondNode( endNodeId );

                int typeId = batchRelationship.hasTypeId() ? batchRelationship.typeId() :
                    relationshipTypeRepository.getOrCreateId( batchRelationship.type() );
                relationship.setType( typeId );
            }
        }
        sender.send( batch );
    }
}
