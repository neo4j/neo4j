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
package org.neo4j.consistency.checking.full;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;

public class PropertyReader implements PropertyAccessor
{
    private final PropertyStore propertyStore;
    private final NodeStore nodeStore;

    public PropertyReader( StoreAccess storeAccess )
    {
        this.propertyStore = storeAccess.getRawNeoStores().getPropertyStore();
        this.nodeStore = storeAccess.getRawNeoStores().getNodeStore();
    }

    public Collection<PropertyRecord> getPropertyRecordChain( NodeRecord nodeRecord )
    {
        return getPropertyRecordChain( nodeRecord.getNextProp() );
    }

    public Collection<PropertyRecord> getPropertyRecordChain( long firstId )
    {
        long nextProp = firstId;
        List<PropertyRecord> toReturn = new LinkedList<>();
        while ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
        {
            PropertyRecord propRecord = propertyStore.getRecord( nextProp, propertyStore.newRecord(), FORCE );
            toReturn.add( propRecord );
            nextProp = propRecord.getNextProp();
        }
        return toReturn;
    }

    public List<PropertyBlock> propertyBlocks( Collection<PropertyRecord> records )
    {
        List<PropertyBlock> propertyBlocks = new ArrayList<>();
        for ( PropertyRecord record : records )
        {
            for ( PropertyBlock block : record )
            {
                propertyBlocks.add( block );
            }
        }
        return propertyBlocks;
    }

    public List<PropertyBlock> propertyBlocks( NodeRecord nodeRecord )
    {
        Collection<PropertyRecord> records = propertyStore.getPropertyRecordChain( nodeRecord.getNextProp() );
        List<PropertyBlock> propertyBlocks = new ArrayList<>();
        for ( PropertyRecord record : records )
        {
            for ( PropertyBlock block : record )
            {
                propertyBlocks.add( block );
            }
        }
        return propertyBlocks;
    }

    public Value propertyValue( PropertyBlock block )
    {
        return block.getType().value( block, propertyStore );
    }

    @Override
    public Value getPropertyValue( long nodeId, int propertyKeyId )
    {
        NodeRecord nodeRecord = nodeStore.newRecord();
        if ( nodeStore.getRecord( nodeId, nodeRecord, FORCE ).inUse() )
        {
            for ( PropertyBlock block : propertyBlocks( nodeRecord ) )
            {
                if ( block.getKeyIndexId() == propertyKeyId )
                {
                    return propertyValue( block );
                }
            }
        }
        return Values.NO_VALUE;
    }
}
