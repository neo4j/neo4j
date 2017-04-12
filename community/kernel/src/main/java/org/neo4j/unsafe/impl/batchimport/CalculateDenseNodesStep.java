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

import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.ForkedProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

import static org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper.ID_NOT_FOUND;

/**
 * Increments counts for each visited relationship, once for start node and once for end node
 * (unless for loops). This to be able to determine which nodes are dense before starting to import relationships.
 */
public class CalculateDenseNodesStep extends ForkedProcessorStep<Batch<InputRelationship,RelationshipRecord>>
{
    private final NodeRelationshipCache cache;
    private final Collector badCollector;

    public CalculateDenseNodesStep( StageControl control, Configuration config, NodeRelationshipCache cache,
            Collector badCollector )
    {
        super( control, "CALCULATE", config, 0 );
        this.cache = cache;
        this.badCollector = badCollector;
    }

    @Override
    protected void forkedProcess( int id, int processors, Batch<InputRelationship,RelationshipRecord> batch )
    {
        for ( int i = 0, idIndex = 0; i < batch.input.length; i++ )
        {
            InputRelationship relationship = batch.input[i];
            long startNodeId = batch.ids[idIndex++];
            long endNodeId = batch.ids[idIndex++];
            processNodeId( id, processors, startNodeId, relationship, relationship.startNode() );
            if ( startNodeId != endNodeId ||                 // avoid counting loops twice
                 startNodeId == ID_NOT_FOUND ) // although always collect bad relationships
            {
                // Loops only counts as one
                processNodeId( id, processors, endNodeId, relationship, relationship.endNode() );
            }
        }
    }

    private void processNodeId( int id, int processors, long nodeId,
            InputRelationship relationship, Object inputId )
    {
        if ( nodeId == ID_NOT_FOUND )
        {
            if ( id == MAIN )
            {
                // Only let the processor with id=0 (which always exists) report the bad relationships
                badCollector.collectBadRelationship( relationship, inputId );
            }
        }
        else if ( nodeId % processors == id )
        {
            cache.incrementCount( nodeId );
        }
    }
}
