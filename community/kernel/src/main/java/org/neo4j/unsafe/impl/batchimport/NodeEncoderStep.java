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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.labels.InlineNodeLabels;
import org.neo4j.kernel.impl.nioneo.xa.PropertyCreator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutorServiceStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.store.BatchingPropertyRecordAccess;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository;

import static org.neo4j.kernel.impl.nioneo.store.Record.NO_NEXT_PROPERTY;
import static org.neo4j.kernel.impl.nioneo.store.Record.NO_NEXT_RELATIONSHIP;
import static org.neo4j.unsafe.impl.batchimport.Utils.propertyKeysAndValues;

/**
 * Creates {@link NodeRecord nodes} with properties and labels from input. Emits {@link RecordBatch batches}
 * downstream.
 */
public final class NodeEncoderStep extends ExecutorServiceStep<List<InputNode>>
{
    private final IdMapper idMapper;
    private final IdGenerator idGenerator;
    private final NodeStore nodeStore;
    private final BatchingTokenRepository<?> propertyKeyHolder;
    private final BatchingTokenRepository<?> labelHolder;
    private final PropertyCreator propertyCreator;

    public NodeEncoderStep( StageControl control, String name, int workAheadSize, int numberOfExecutors,
            IdMapper idMapper, IdGenerator idGenerator, BatchingTokenRepository<?> propertyKeyHolder,
            BatchingTokenRepository<?> labelHolder,
            NodeStore nodeStore, PropertyStore propertyStore )
    {
        super( control, name, workAheadSize, numberOfExecutors );
        this.idMapper = idMapper;
        this.idGenerator = idGenerator;
        this.nodeStore = nodeStore;
        this.propertyKeyHolder = propertyKeyHolder;
        this.labelHolder = labelHolder;
        this.propertyCreator = new PropertyCreator( propertyStore, null );
    }

    @Override
    protected Object process( long ticket, List<InputNode> batch )
    {
        BatchingPropertyRecordAccess propertyRecords = new BatchingPropertyRecordAccess();
        List<NodeRecord> nodeRecords = new ArrayList<>( batch.size() );
        for ( InputNode batchNode : batch )
        {
            // TODO Should we have this piece of logic (below) that creates a node with its properties and labels
            // in a service as well, that the old BatchInserter as well as perhaps NeoStoreTransaction could use?
            // Node itself
            long nodeId = idGenerator.generate( batchNode.id() );
            idMapper.put( batchNode.id(), nodeId );
            NodeRecord nodeRecord = new NodeRecord( nodeId, false,
                    NO_NEXT_RELATIONSHIP.intValue(), NO_NEXT_PROPERTY.intValue() );
            nodeRecord.setInUse( true );
            nodeRecords.add( nodeRecord );

            // Properties
            long nextProp;
            if ( batchNode.hasFirstPropertyId() )
            {
                nextProp = batchNode.firstPropertyId();
            }
            else
            {
                nextProp = propertyCreator.createPropertyChain( nodeRecord, propertyKeysAndValues(
                        batchNode.properties(), propertyKeyHolder, propertyCreator ), propertyRecords );
            }
            nodeRecord.setNextProp( nextProp );

            // Labels
            if ( batchNode.hasLabelField() )
            {
                nodeRecord.setLabelField( batchNode.labelField(), Collections.<DynamicRecord>emptyList() );
            }
            else
            {
                long[] labels = Utils.labelNamesToIds( labelHolder, batchNode.labels() );
                new InlineNodeLabels( nodeRecord.getLabelField(), nodeRecord ).put(
                        labels, null, nodeStore.getDynamicLabelStore() );
            }
        }
        return new RecordBatch<>( nodeRecords, propertyRecords.records() );
    }

    @Override
    protected void done()
    {
        // We're done adding ids to the IdMapper, sort so that the following stages can query it.
        idMapper.prepare();
    }
}
