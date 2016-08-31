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

import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.Configuration;
import org.neo4j.unsafe.impl.batchimport.staging.ForkedProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

/**
 * Creates batches of relationship records, with the "next" relationship
 * pointers set to the next relationships (previously created) in their respective chains. The previous
 * relationship ids are kept in {@link NodeRelationshipCache node cache}, which is a point of scalability issues,
 * although mitigated using multi-pass techniques.
 */
public class RelationshipEncoderStep extends ForkedProcessorStep<Batch<InputRelationship,RelationshipRecord>>
{
    private final NodeRelationshipCache cache;

    public RelationshipEncoderStep( StageControl control, Configuration config, NodeRelationshipCache cache )
    {
        super( control, "RELATIONSHIP", config, 0 );
        this.cache = cache;
    }

    @Override
    protected void forkedProcess( int id, int processors, Batch<InputRelationship,RelationshipRecord> batch )
    {
        for ( int i = 0; i < batch.records.length; i++ )
        {
            RelationshipRecord relationship = batch.records[i];
            long startNode = relationship.getFirstNode();
            long endNode = relationship.getSecondNode();
            if ( !relationship.inUse() )
            {   // This means that we here have a relationship that refers to missing nodes.
                // It also means that we tolerate some amount of bad relationships and CalculateDenseNodesStep
                // already have reported this to the bad collector.
                continue;
            }

            // Set first/second next rel
            boolean loop = startNode == endNode;
            if ( startNode % processors == id )
            {
                long firstNextRel = cache.getAndPutRelationship(
                        startNode, loop ? BOTH : OUTGOING, relationship.getId(), true );
                relationship.setFirstNextRel( firstNextRel );
                if ( loop )
                {
                    relationship.setSecondNextRel( firstNextRel );
                }
            }

            if ( !loop && endNode % processors == id )
            {
                relationship.setSecondNextRel( cache.getAndPutRelationship(
                        endNode, INCOMING, relationship.getId(), true ) );
            }
        }
    }
}
