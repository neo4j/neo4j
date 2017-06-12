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

import java.util.Collections;

import org.neo4j.kernel.impl.store.InlineNodeLabels;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.unsafe.impl.batchimport.DataImporter.Monitor;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Group;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingLabelTokenRepository;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingPropertyKeyTokenRepository;

import static java.lang.Long.max;

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

    private long nodeCount;
    private long highestId;

    public NodeImporter( NeoStores stores, BatchingPropertyKeyTokenRepository propertyKeyTokenRepository,
            BatchingLabelTokenRepository labelTokenRepository, IdMapper idMapper, Monitor monitor )
    {
        super( stores.getPropertyStore(), propertyKeyTokenRepository, monitor );
        this.labelTokenRepository = labelTokenRepository;
        this.idMapper = idMapper;
        this.nodeStore = stores.getNodeStore();
        this.nodeRecord = nodeStore.newRecord();
        this.nodeIds = new BatchingIdGetter( nodeStore );
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
        return true;
    }

    @Override
    public boolean labels( String[] labels )
    {
        long[] labelIds = labelTokenRepository.getOrCreateIds( labels );
        InlineNodeLabels.putSorted( nodeRecord, labelIds, null, nodeStore.getDynamicLabelStore() );
        return true;
    }

    @Override
    public boolean labelField( long labelField )
    {
        nodeRecord.setLabelField( labelField, Collections.emptyList() );
        return true;
    }

    @Override
    public void endOfEntity()
    {
        // Write data to stores
        nodeRecord.setNextProp( createAndWritePropertyChain() );
        nodeRecord.setInUse( true );
        nodeStore.updateRecord( nodeRecord );
        nodeCount++;
        nodeRecord.clear();
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
        nodeStore.setHighestPossibleIdInUse( highestId );
    }
}
