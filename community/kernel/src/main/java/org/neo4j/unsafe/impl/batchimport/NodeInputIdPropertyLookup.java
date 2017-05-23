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

import java.util.function.LongFunction;

import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.EncodingIdMapper;

import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

/**
 * Looks up "input id" from a node. This is used when importing nodes and where the input data specifies ids
 * using its own id name space, such as arbitrary strings. Those ids are called input ids and are converted
 * into actual record ids during import. However there may be duplicate such input ids in the input data
 * and the {@link EncodingIdMapper} may need to double check some input ids since it's only caching a hash
 * of the input id in memory. The input ids are stored as properties on the nodes to be able to retrieve
 * them for such an event. This class can look up those input id properties for arbitrary nodes.
 */
class NodeInputIdPropertyLookup implements LongFunction<Object>
{
    private final NodeStore nodeStore;
    private final PropertyStore propertyStore;
    private final NodeRecord nodeRecord;
    private final PropertyRecord propertyRecord;
    private final int inputIdTokenId;

    NodeInputIdPropertyLookup( NodeStore nodeStore, PropertyStore propertyStore, int inputIdPropertyKeyTokenId )
    {
        this.nodeStore = nodeStore;
        this.propertyStore = propertyStore;
        this.nodeRecord = nodeStore.newRecord();
        this.propertyRecord = propertyStore.newRecord();
        this.inputIdTokenId = inputIdPropertyKeyTokenId;
    }

    @Override
    public Object apply( long nodeId )
    {
        nodeStore.getRecord( nodeId, nodeRecord, NORMAL );
        long propertyId = nodeRecord.getNextProp();
        while ( propertyId != Record.NULL_REFERENCE.longValue() )
        {
            propertyStore.getRecord( propertyId, propertyRecord, NORMAL );
            // TODO: using the iterator approach is a bit heavy on the garbage,
            // but these collisions should be rare, right?
            for ( PropertyBlock block : propertyRecord )
            {
                if ( block.getKeyIndexId() == inputIdTokenId )
                {
                    return block.getType().getValue( block, propertyStore );
                }
            }
            propertyId = propertyRecord.getNextProp();
        }
        throw new IllegalStateException( "Input id not found for node " + nodeId );
    }
}
