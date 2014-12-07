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
import java.util.Collections;
import java.util.List;

import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.kernel.impl.store.InlineNodeLabels;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutorServiceStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingLabelTokenRepository;

import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.store.record.Record.NO_NEXT_RELATIONSHIP;

/**
 * Creates {@link NodeRecord nodes} with labels from input.
 */
public final class NodeEncoderStep extends ExecutorServiceStep<List<InputNode>>
{
    private final IdMapper idMapper;
    private final IdGenerator idGenerator;
    private final NodeStore nodeStore;
    private final BatchingLabelTokenRepository labelHolder;
    private final ResourceIterable<Object> allIds;

    public NodeEncoderStep( StageControl control, Configuration config,
            IdMapper idMapper, IdGenerator idGenerator,
            BatchingLabelTokenRepository labelHolder,
            NodeStore nodeStore,
            ResourceIterable<Object> allIds )
    {
        super( control, "NODE", config.workAheadSize(), config.movingAverageSize(), 1 );
        this.idMapper = idMapper;
        this.idGenerator = idGenerator;
        this.nodeStore = nodeStore;
        this.labelHolder = labelHolder;
        this.allIds = allIds;
    }

    @Override
    protected Object process( long ticket, List<InputNode> batch )
    {
        List<BatchEntity<NodeRecord,InputNode>> entities = new ArrayList<>( batch.size() );
        for ( InputNode batchNode : batch )
        {
            long nodeId = idGenerator.generate( batchNode.id() );
            if ( batchNode.id() != null )
            {
                // Nodes are allowed to be anonymous, they just can't be found when creating relationships
                // later on, that's all. Anonymous nodes have null id.
                idMapper.put( batchNode.id(), nodeId );
            }
            NodeRecord nodeRecord = new NodeRecord( nodeId, false,
                    NO_NEXT_RELATIONSHIP.intValue(), NO_NEXT_PROPERTY.intValue() );
            nodeRecord.setInUse( true );
            entities.add( new BatchEntity<>( nodeRecord, batchNode ) );

            // Labels
            if ( batchNode.hasLabelField() )
            {
                nodeRecord.setLabelField( batchNode.labelField(), Collections.<DynamicRecord>emptyList() );
            }
            else
            {
                long[] labels = labelHolder.getOrCreateIds( batchNode.labels() );
                InlineNodeLabels.put( nodeRecord, labels, null, nodeStore.getDynamicLabelStore() );
            }
        }
        return entities;
    }

    @Override
    protected void done()
    {
        // We're done adding ids to the IdMapper, prepare for other stages querying it.
        // We pass in allIds because they may be needed to sort out colliding values in case of String->long
        // encoding.
        idMapper.prepare( allIds );
        super.done();
    }
}
