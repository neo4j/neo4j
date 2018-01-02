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

import java.util.Collections;

import org.neo4j.kernel.impl.store.InlineNodeLabels;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.stats.StatsProvider;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingLabelTokenRepository;

import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;

/**
 * Creates {@link NodeRecord nodes} with labels from input.
 */
public final class NodeEncoderStep extends ProcessorStep<Batch<InputNode,NodeRecord>>
{
    private final IdMapper idMapper;
    private final IdGenerator idGenerator;
    private final NodeStore nodeStore;
    private final BatchingLabelTokenRepository labelHolder;

    public NodeEncoderStep( StageControl control, Configuration config,
            IdMapper idMapper, IdGenerator idGenerator,
            BatchingLabelTokenRepository labelHolder,
            NodeStore nodeStore,
            StatsProvider memoryUsageStats )
    {
        super( control, "NODE", config, 1, memoryUsageStats );
        this.idMapper = idMapper;
        this.idGenerator = idGenerator;
        this.nodeStore = nodeStore;
        this.labelHolder = labelHolder;
    }

    @Override
    protected void process( Batch<InputNode,NodeRecord> batch, BatchSender sender )
    {
        InputNode[] input = batch.input;
        batch.records = new NodeRecord[input.length];
        batch.labels = new long[input.length][];
        for ( int i = 0; i < input.length; i++ )
        {
            InputNode batchNode = input[i];
            long nodeId = idGenerator.generate( batchNode.id() );
            if ( batchNode.id() != null )
            {
                // Nodes are allowed to be anonymous, they just can't be found when creating relationships
                // later on, that's all. Anonymous nodes have null id.
                idMapper.put( batchNode.id(), nodeId, batchNode.group() );
            }
            NodeRecord nodeRecord = batch.records[i] = new NodeRecord( nodeId, false,
                    NO_NEXT_RELATIONSHIP.intValue(), NO_NEXT_PROPERTY.intValue() );
            nodeRecord.setInUse( true );

            // Labels
            if ( batchNode.hasLabelField() )
            {
                nodeRecord.setLabelField( batchNode.labelField(), Collections.<DynamicRecord>emptyList() );
            }
            else
            {
                long[] labels = batch.labels[i] = labelHolder.getOrCreateIds( batchNode.labels() );
                InlineNodeLabels.putSorted( nodeRecord, labels, null, nodeStore.getDynamicLabelStore() );
            }
        }
        sender.send( batch );
    }
}
