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
package org.neo4j.consistency.checking.full;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.function.Suppliers;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.PropertyLookup;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;

public class PropertyReader implements PropertyLookup
{
    private final PropertyStore propertyStore;
    private final NodeStore nodeStore;
    private final StoreAccess storeAccess;

    public PropertyReader( StoreAccess storeAccess )
    {
        this.storeAccess = storeAccess;
        propertyStore = storeAccess.getRawNeoStores().getPropertyStore();
        nodeStore = storeAccess.getRawNeoStores().getNodeStore();
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
            PropertyRecord propRecord = storeAccess.getPropertyStore().forceGetRecord( nextProp );
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

    public DefinedProperty propertyValue( PropertyBlock block )
    {
        return block.getType().readProperty( block.getKeyIndexId(), block, Suppliers.singleton( propertyStore ) );
    }

    @Override
    public Property nodeProperty( long nodeId, int propertyKeyId )
    {
        NodeRecord nodeRecord = storeAccess.getNodeStore().forceGetRecord( nodeId );
        if ( nodeRecord != null )
        {
            for ( PropertyBlock block : propertyBlocks( nodeRecord ) )
            {
                if ( block.getKeyIndexId() == propertyKeyId )
                {
                    return propertyValue( block );
                }
            }
        }
        return Property.noNodeProperty( nodeId, propertyKeyId );
    }
}
