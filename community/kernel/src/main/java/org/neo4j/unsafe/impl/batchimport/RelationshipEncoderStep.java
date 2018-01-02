/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.graphdb.Resource;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository;

import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

/**
 * Creates batches of relationship records, with the "next" relationship
 * pointers set to the next relationships (previously created) in their respective chains. The previous
 * relationship ids are kept in {@link NodeRelationshipCache node cache}, which is a point of scalability issues,
 * although mitigated using multi-pass techniques.
 */
public class RelationshipEncoderStep extends ProcessorStep<Batch<InputRelationship,RelationshipRecord>>
{
    private final BatchingTokenRepository<?, ?> relationshipTypeRepository;
    private final NodeRelationshipCache cache;
    private final ParallelizationCoordinator parallelization = new ParallelizationCoordinator();

    // There are two "modes" in generating relationship ids
    // - ids are decided by InputRelationship#id() (f.ex. store migration, where ids should be kept intact).
    //   nextRelationshipId will not be used, and all InputRelationships will have to specify ids
    // - ids are incremented for each one, starting at a specific id (0 on empty db)
    //   nextRelationshipId is used and _no_ id from InputRelationship is used, rather no id is allowed to be specified.
    private final boolean specificIds;

    public RelationshipEncoderStep( StageControl control,
            Configuration config,
            BatchingTokenRepository<?, ?> relationshipTypeRepository,
            NodeRelationshipCache cache,
            boolean specificIds )
    {
        super( control, "RELATIONSHIP", config, 0 );
        this.relationshipTypeRepository = relationshipTypeRepository;
        this.cache = cache;
        this.specificIds = specificIds;
    }

    @Override
    protected Resource permit( Batch<InputRelationship,RelationshipRecord> batch )
    {
        return parallelization.coordinate( batch.parallelizableWithPrevious );
    }

    @Override
    protected void process( Batch<InputRelationship,RelationshipRecord> batch, BatchSender sender ) throws Throwable
    {
        InputRelationship[] input = batch.input;
        batch.records = new RelationshipRecord[input.length];
        long[] ids = batch.ids;
        long nextRelationshipId = batch.firstRecordId;
        for ( int i = 0; i < input.length; i++ )
        {
            InputRelationship batchRelationship = input[i];
            if ( specificIds != batchRelationship.hasSpecificId() )
            {
                throw new IllegalStateException( "Input was declared to have specificRelationshipIds=" +
                        specificIds + ", but " + batchRelationship + " didn't honor that" );
            }
            long relationshipId = specificIds ? batchRelationship.specificId() : nextRelationshipId++;
            // Ids have been verified to exist in CalculateDenseNodeStep
            long startNodeId = ids[i*2];
            long endNodeId = ids[i*2+1];
            if ( startNodeId == -1 || endNodeId == -1 )
            {   // This means that we here have a relationship that refers to missing nodes.
                // It also means that we tolerate some amount of bad relationships and CalculateDenseNodesStep
                // already have reported this to the bad collector.
                batch.records[i] = new RelationshipRecord( relationshipId );
                batch.records[i].setInUse( false );
                continue;
            }

            int typeId = batchRelationship.hasTypeId() ? batchRelationship.typeId() :
                    relationshipTypeRepository.getOrCreateId( batchRelationship.type() );
            RelationshipRecord relationshipRecord = batch.records[i] = new RelationshipRecord( relationshipId,
                    startNodeId, endNodeId, typeId );
            relationshipRecord.setInUse( true );

            // Set first/second next rel
            boolean loop = startNodeId == endNodeId;
            long firstNextRel = cache.getAndPutRelationship(
                    startNodeId, typeId, loop ? BOTH : OUTGOING, relationshipId, true );
            relationshipRecord.setFirstNextRel( firstNextRel );
            if ( loop )
            {
                relationshipRecord.setSecondNextRel( firstNextRel );
            }
            else
            {
                relationshipRecord.setSecondNextRel( cache.getAndPutRelationship(
                        endNodeId, typeId, INCOMING, relationshipId, true ) );
            }

            // Most rels will not be first in chain
            relationshipRecord.setFirstInFirstChain( false );
            relationshipRecord.setFirstInSecondChain( false );
            relationshipRecord.setFirstPrevRel( Record.NO_NEXT_RELATIONSHIP.intValue() );
            relationshipRecord.setSecondPrevRel( Record.NO_NEXT_RELATIONSHIP.intValue() );
        }
        sender.send( batch );
    }
}
