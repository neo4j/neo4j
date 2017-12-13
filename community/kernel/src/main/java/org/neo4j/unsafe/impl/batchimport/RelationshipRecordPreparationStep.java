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

import org.neo4j.kernel.impl.store.id.IdRange;
import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.stats.StatsProvider;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingRelationshipTypeTokenRepository;

import static org.neo4j.kernel.impl.store.id.validation.IdValidator.hasReservedIdInRange;
import static org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper.ID_NOT_FOUND;

/**
 * Creates and initializes {@link RelationshipRecord} batches to later be filled with actual data
 * and pointers. This is a separate step to remove work from main step.
 */
public class RelationshipRecordPreparationStep extends ProcessorStep<Batch<InputRelationship,RelationshipRecord>>
{
    private final BatchingRelationshipTypeTokenRepository relationshipTypeRepository;
    private final Collector badCollector;
    private final IdSequence idSequence;
    private final boolean doubleRecordUnits;
    private final int idsPerRecord;

    public RelationshipRecordPreparationStep( StageControl control, Configuration config,
            BatchingRelationshipTypeTokenRepository relationshipTypeRepository, Collector badCollector,
            IdSequence idSequence, boolean doubleRecordUnits,
            StatsProvider... statsProviders )
    {
        super( control, "RECORDS", config, 0, statsProviders );
        this.relationshipTypeRepository = relationshipTypeRepository;
        this.badCollector = badCollector;
        this.idSequence = idSequence;
        this.doubleRecordUnits = doubleRecordUnits;
        this.idsPerRecord = doubleRecordUnits ? 2 : 1;
    }

    @Override
    protected void process( Batch<InputRelationship,RelationshipRecord> batch, BatchSender sender ) throws Throwable
    {
        batch.records = new RelationshipRecord[batch.input.length];
        IdRange idRange = idSequence.nextIdBatch( batch.records.length * idsPerRecord );
        if ( hasReservedIdInRange( idRange.getRangeStart(), idRange.getRangeStart() + idRange.getRangeLength() ) )
        {
            idRange = idSequence.nextIdBatch( batch.records.length * idsPerRecord );
        }
        IdSequence ids = idRange.iterator();
        for ( int i = 0, idIndex = 0; i < batch.records.length; i++ )
        {
            RelationshipRecord relationship = batch.records[i] = new RelationshipRecord( ids.nextId() );
            InputRelationship batchRelationship = batch.input[i];
            long startNodeId = batch.ids[idIndex++];
            long endNodeId = batch.ids[idIndex++];
            boolean hasType = batchRelationship.hasType();
            if ( startNodeId == ID_NOT_FOUND || endNodeId == ID_NOT_FOUND || !hasType )
            {
                collectBadRelationship( batchRelationship, startNodeId, endNodeId, hasType );
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

            if ( doubleRecordUnits )
            {
                ids.nextId(); // reserve it
            }
        }
        sender.send( batch );
    }

    private void collectBadRelationship( InputRelationship batchRelationship, long startNodeId, long endNodeId, boolean hasType )
    {
        if ( !hasType )
        {
            badCollector.collectBadRelationship( batchRelationship, null );
        }
        else
        {
            if ( startNodeId == ID_NOT_FOUND )
            {
                badCollector.collectBadRelationship( batchRelationship, batchRelationship.startNode() );
            }
            if ( endNodeId == ID_NOT_FOUND )
            {
                badCollector.collectBadRelationship( batchRelationship, batchRelationship.endNode() );
            }
        }
    }
}
