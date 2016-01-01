/**
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

import java.util.List;

import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipLink;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutorServiceStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

import static java.lang.Math.max;
import static java.lang.Math.round;

/**
 * Runs through relationship input and counts relationships per node so that dense nodes can be designated.
 */
public class CalculateDenseNodesStep extends ExecutorServiceStep<List<InputRelationship>>
{
    private final NodeRelationshipLink nodeRelationshipLink;
    private long highestSeenNodeId;
    private final StringLogger logger;
    private final IdMapper idMapper;

    public CalculateDenseNodesStep( StageControl control, int workAheadSize,
            NodeRelationshipLink nodeRelationshipLink, IdMapper idMapper, StringLogger logger )
    {
        super( control, "CALCULATOR", workAheadSize, 1 );
        this.nodeRelationshipLink = nodeRelationshipLink;
        this.idMapper = idMapper;
        this.logger = logger;
    }

    @Override
    protected Object process( long ticket, List<InputRelationship> batch )
    {
        for ( InputRelationship rel : batch )
        {
            long startNode = idMapper.get( rel.startNode() );
            long endNode = idMapper.get( rel.endNode() );
            try
            {
                nodeRelationshipLink.incrementCount( startNode );
            }
            catch ( ArrayIndexOutOfBoundsException e )
            {
                throw new RuntimeException( "Input relationship " + rel + " refers to missing start node " +
                        rel.startNode(), e );
            }
            if ( !rel.isLoop() )
            {
                try
                {
                    nodeRelationshipLink.incrementCount( endNode );
                }
                catch ( ArrayIndexOutOfBoundsException e )
                {
                    throw new RuntimeException( "Input relationship " + rel + " refers to missing end node " +
                            rel.endNode(), e );
                }
            }

            highestSeenNodeId = max( highestSeenNodeId, max( startNode, endNode ) );
        }
        return null; // end of the line
    }

    @Override
    protected void done()
    {
        // Prints a percent of dense nodes, given the supplied dense node threshold
        long numberOfDenseNodes = 0;
        for ( long i = 0; i < highestSeenNodeId; i++ )
        {
            if ( nodeRelationshipLink.isDense( i ) )
            {
                numberOfDenseNodes++;
            }
        }
        logger.info( "# dense nodes: " + numberOfDenseNodes + ", which is " +
                round( 100D*numberOfDenseNodes/highestSeenNodeId ) + " %" );
        super.done();
    }
}
