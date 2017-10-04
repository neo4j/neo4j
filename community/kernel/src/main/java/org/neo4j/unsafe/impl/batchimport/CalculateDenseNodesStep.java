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
import org.neo4j.unsafe.impl.batchimport.staging.ForkedProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

/**
 * Increments counts for each visited relationship, once for start node and once for end node
 * (unless for loops). This to be able to determine which nodes are dense before starting to import relationships.
 */
public class CalculateDenseNodesStep extends ForkedProcessorStep<RelationshipRecord[]>
{
    private final NodeRelationshipCache cache;

    public CalculateDenseNodesStep( StageControl control, Configuration config, NodeRelationshipCache cache )
    {
        super( control, "CALCULATE", config );
        this.cache = cache;
    }

    @Override
    protected void forkedProcess( int id, int processors, RelationshipRecord[] batch )
    {
        for ( int i = 0; i < batch.length; i++ )
        {
            RelationshipRecord relationship = batch[i];
            long startNodeId = relationship.getFirstNode();
            long endNodeId = relationship.getSecondNode();
            processNodeId( id, processors, startNodeId );
            if ( startNodeId != endNodeId ) // avoid counting loops twice
            {
                // Loops only counts as one
                processNodeId( id, processors, endNodeId );
            }
        }
    }

    private void processNodeId( int id, int processors, long nodeId )
    {
        if ( nodeId % processors == id )
        {
            cache.incrementCount( nodeId );
        }
    }
}
