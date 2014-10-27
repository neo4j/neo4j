/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.util.ArrayList;
import java.util.List;

import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipLink;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutorServiceStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository;

import static org.neo4j.graphdb.Direction.INCOMING;

/**
 * Creates batches of relationship records, with the "next" relationship
 * pointers set to the next relationships (previously created) in their respective chains. The previous
 * relationship ids are kept in {@link NodeRelationshipLink node cache}, which is a point of scalability issues,
 * although mitigated using multi-pass techniques.
 */
public class RelationshipEncoderStep extends ExecutorServiceStep<List<InputRelationship>>
{
    private final IdMapper idMapper;
    private final BatchingTokenRepository<?> relationshipTypeRepository;
    private final RelationshipStore relationshipStore;
    private final NodeRelationshipLink nodeRelationshipLink;

    public RelationshipEncoderStep( StageControl control,
            int workAheadSize, int numberOfExecutors,
            IdMapper idMapper,
            BatchingTokenRepository<?> relationshipTypeRepository,
            RelationshipStore relationshipStore,
            NodeRelationshipLink nodeRelationshipLink )
    {
        super( control, "RELATIONSHIP", workAheadSize, numberOfExecutors );
        this.idMapper = idMapper;
        this.relationshipTypeRepository = relationshipTypeRepository;
        this.relationshipStore = relationshipStore;
        this.nodeRelationshipLink = nodeRelationshipLink;
    }

    @Override
    protected Object process( long ticket, List<InputRelationship> batch )
    {
        List<BatchEntity<RelationshipRecord,InputRelationship>> entities = new ArrayList<>( batch.size() );
        for ( InputRelationship batchRelationship : batch )
        {
            long relationshipId = batchRelationship.id();
            relationshipStore.setHighestPossibleIdInUse( relationshipId );
            long startNodeId = idMapper.get( batchRelationship.startNode() );
            long endNodeId = idMapper.get( batchRelationship.endNode() );
            int typeId = batchRelationship.hasTypeId() ? batchRelationship.typeId() :
                    relationshipTypeRepository.getOrCreateId( batchRelationship.type() );
            RelationshipRecord relationshipRecord = new RelationshipRecord( relationshipId,
                    startNodeId, endNodeId, typeId );
            relationshipRecord.setInUse( true );

            // Set first/second next rel
            long firstNextRel = nodeRelationshipLink.getAndPutRelationship(
                    startNodeId, typeId, batchRelationship.startDirection(), relationshipId, true );
            relationshipRecord.setFirstNextRel( firstNextRel );
            if ( batchRelationship.isLoop() )
            {
                relationshipRecord.setSecondNextRel( firstNextRel );
            }
            else
            {
                relationshipRecord.setSecondNextRel( nodeRelationshipLink.getAndPutRelationship(
                        endNodeId, typeId, INCOMING, relationshipId, true ) );
            }

            // Most rels will not be first in chain
            relationshipRecord.setFirstInFirstChain( false );
            relationshipRecord.setFirstInSecondChain( false );
            relationshipRecord.setFirstPrevRel( Record.NO_NEXT_RELATIONSHIP.intValue() );
            relationshipRecord.setSecondPrevRel( Record.NO_NEXT_RELATIONSHIP.intValue() );
            entities.add( new BatchEntity<>( relationshipRecord, batchRelationship ) );
        }
        return entities;
    }
}
