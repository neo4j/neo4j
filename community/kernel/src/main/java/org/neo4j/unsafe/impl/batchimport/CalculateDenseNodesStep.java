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

import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipLink;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutorServiceStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

/**
 * Runs through relationship input and counts relationships per node so that dense nodes can be designated.
 */
public class CalculateDenseNodesStep extends ExecutorServiceStep<Batch<InputRelationship,RelationshipRecord>>
{
    private final NodeRelationshipLink nodeRelationshipLink;
    private final Collector<InputRelationship> badRelationshipsCollector;

    public CalculateDenseNodesStep( StageControl control, Configuration config,
            NodeRelationshipLink nodeRelationshipLink, Collector<InputRelationship> badRelationshipsCollector )
    {
        super( control, "CALCULATOR", config.workAheadSize(), config.movingAverageSize(), 1 );
        this.nodeRelationshipLink = nodeRelationshipLink;
        this.badRelationshipsCollector = badRelationshipsCollector;
    }

    @Override
    protected Object process( long ticket, Batch<InputRelationship,RelationshipRecord> batch )
    {
        InputRelationship[] input = batch.input;
        long[] ids = batch.ids;
        for ( int i = 0; i < input.length; i++ )
        {
            InputRelationship rel = input[i];
            long startNode = ids[i*2];
            long endNode = ids[i*2+1];

            incrementCount( rel, startNode, rel.startNode() );
            if ( startNode != endNode )
            {
                incrementCount( rel, endNode, rel.endNode() );
            }
        }
        return null; // end of the line
    }

    private void incrementCount( InputRelationship relationship, long nodeId, Object inputNodeId )
    {
        if ( nodeId != -1 )
        {
            try
            {
                nodeRelationshipLink.incrementCount( nodeId );
                return;
            }
            catch ( ArrayIndexOutOfBoundsException e )
            {   // This is odd, but may happen. We'll tell the bad relationship collector below
            }
        }

        badRelationshipsCollector.collect( relationship, inputNodeId );
    }
}
