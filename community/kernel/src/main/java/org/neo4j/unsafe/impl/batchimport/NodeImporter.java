/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.unsafe.impl.batchimport.DataImporter.Monitor;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Group;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;
import org.neo4j.unsafe.impl.batchimport.store.BatchingNeoStores;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingLabelTokenRepository;
import org.neo4j.values.storable.Values;

import static java.lang.Long.max;
import static java.util.Arrays.copyOf;

import static org.neo4j.kernel.impl.store.record.Record.NULL_REFERENCE;

/**
 * Imports nodes using data from {@link InputChunk}.
 */
public class NodeImporter extends EntityImporter
{
    private final BatchingLabelTokenRepository labelTokenRepository;
    private final NodeStore nodeStore;
    private final NodeRecord nodeRecord;
    private final IdMapper idMapper;
    private final BatchingIdGetter nodeIds;
    private final PropertyStore idPropertyStore;
    private final PropertyRecord idPropertyRecord;
    private final PropertyBlock idPropertyBlock = new PropertyBlock();
    private String[] labels = new String[10];
    private int labelsCursor;

    private long nodeCount;
    private long highestId = -1;
    private boolean hasLabelField;

    public NodeImporter( BatchingNeoStores stores, IdMapper idMapper, Monitor monitor )
    {
        super( stores, monitor );
        this.labelTokenRepository = stores.getLabelRepository();
        this.idMapper = idMapper;
        this.nodeStore = stores.getNodeStore();
        this.nodeRecord = nodeStore.newRecord();
        this.nodeIds = new BatchingIdGetter( nodeStore );
        this.idPropertyStore = stores.getTemporaryPropertyStore();
        this.idPropertyRecord = idPropertyStore.newRecord();
        nodeRecord.setInUse( true );
    }

    @Override
    public boolean id( long id )
    {
        nodeRecord.setId( id );
        highestId = max( highestId, id );
        return true;
    }

    @Override
    public boolean id( Object id, Group group )
    {
        long nodeId = nodeIds.next();
        nodeRecord.setId( nodeId );
        idMapper.put( id, nodeId, group );

        // also store this id as property in temp property store
        if ( id != null )
        {
            idPropertyStore.encodeValue( idPropertyBlock, 0, Values.of( id ) );
            idPropertyRecord.addPropertyBlock( idPropertyBlock );
            idPropertyRecord.setId( nodeId ); // yes nodeId
            idPropertyRecord.setInUse( true );
            idPropertyStore.updateRecord( idPropertyRecord );
            idPropertyRecord.clear();
        }
        return true;
    }

    @Override
    public boolean labels( String[] labels )
    {
        assert !hasLabelField;
        if ( labelsCursor + labels.length > this.labels.length )
        {
            this.labels = copyOf( this.labels, this.labels.length * 2 );
        }
        System.arraycopy( labels, 0, this.labels, labelsCursor, labels.length );
        labelsCursor += labels.length;
        return true;
    }

    @Override
    public boolean labelField( long labelField )
    {
        hasLabelField = true;
        nodeRecord.setLabelField( labelField, Collections.emptyList() );
        return true;
    }

    @Override
    public void endOfEntity()
    {
        // Make sure we have an ID
        if ( nodeRecord.getId() == NULL_REFERENCE.longValue() )
        {
            nodeRecord.setId( nodeIds.next() );
        }

        // Compose the labels
        if ( !hasLabelField )
        {
            long[] labelIds = labelTokenRepository.getOrCreateIds( labels, labelsCursor );
            InlineNodeLabels.putSorted( nodeRecord, labelIds, null, nodeStore.getDynamicLabelStore() );
        }
        labelsCursor = 0;

        // Write data to stores
        nodeRecord.setNextProp( createAndWritePropertyChain() );
        nodeRecord.setInUse( true );
        nodeStore.updateRecord( nodeRecord );
        nodeCount++;
        nodeRecord.clear();
        nodeRecord.setId( NULL_REFERENCE.longValue() );
        hasLabelField = false;
        super.endOfEntity();
    }

    @Override
    protected PrimitiveRecord primitiveRecord()
    {
        return nodeRecord;
    }

    @Override
    public void close()
    {
        super.close();
        monitor.nodesImported( nodeCount );
        nodeStore.setHighestPossibleIdInUse( highestId ); // for the case of #id(long)
    }
}
