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

import java.util.Arrays;

import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

/**
 * Makes some preparations to incoming batches so that {@link CalculateDenseNodesStep} can run parallel batches.
 * Sends long[] batches downstream, where each batch is for node ids with a certain radix, such that
 * the {@link NodeRelationshipCache} cache can be updated in parallel without synchronization.
 * Each id in the long[] has the 0x40000000_00000000 bit set if end node. End node on loop relationships
 * are not counted.
 */
public class CalculateDenseNodePrepareStep extends ProcessorStep<Batch<InputRelationship,RelationshipRecord>>
{
    public static final int RADIXES = 10;

    private final int batchSize;
    private final long[][] inProgressBatches;
    private final int[] cursors;
    private final Collector badCollector;

    public CalculateDenseNodePrepareStep( StageControl control, Configuration config, Collector badCollector )
    {
        super( control, "DIVIDE", config, 1 );
        this.badCollector = badCollector;
        this.batchSize = config.batchSize() * 2; // x2 since we receive (and send) 2 ids per relationship
        this.inProgressBatches = new long[RADIXES][batchSize];
        this.cursors = new int[inProgressBatches.length];
    }

    @Override
    protected void process( Batch<InputRelationship,RelationshipRecord> batch, BatchSender sender )
    {
        long[] batchIds = batch.ids;
        InputRelationship[] input = batch.input;
        for ( int i = 0, r = 0; i < batchIds.length; i++, r++ )
        {
            // start node
            long startNodeId = batchIds[i++];
            InputRelationship relationship = input[r];
            processNodeId( startNodeId, sender, relationship, relationship.startNode() );

            // end node
            long endNodeId = batchIds[i];
            boolean loop = startNodeId == endNodeId;
            if ( !loop )
            {
                processNodeId( endNodeId, sender, relationship, relationship.endNode() );
            }
        }
    }

    private void processNodeId( long nodeId, BatchSender sender, InputRelationship relationship, Object inputId )
    {
        if ( nodeId != -1 )
        {
            int startNodeRadix = radixOf( nodeId );
            inProgressBatches[startNodeRadix][cursors[startNodeRadix]++] = nodeId;
            if ( cursors[startNodeRadix] == batchSize )
            {
                sender.send( inProgressBatches[startNodeRadix] );
                inProgressBatches[startNodeRadix] = new long[batchSize];
                cursors[startNodeRadix] = 0;
            }
        }
        else
        {
            badCollector.collectBadRelationship( relationship, inputId );
        }
    }

    @Override
    protected void lastCallForEmittingOutstandingBatches( BatchSender sender )
    {
        for ( int i = 0; i < cursors.length; i++ )
        {
            if ( cursors[i] > 0 )
            {
                sender.send( cursors[i] == batchSize
                        ? inProgressBatches[i]
                        : Arrays.copyOf( inProgressBatches[i], cursors[i] ) );
            }
        }
    }

    public static int radixOf( long nodeId )
    {
        return (int) (nodeId%RADIXES);
    }
}
