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
package org.neo4j.legacy.consistency.checking.full;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.neo4j.function.Suppliers;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.PropertyLookup;
import org.neo4j.kernel.impl.store.InvalidRecordException;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;

public class PropertyReader implements PropertyLookup
{
    private final PropertyStore propertyStore;
    private final NodeStore nodeStore;

    public PropertyReader( PropertyStore propertyStore, NodeStore nodeStore )
    {
        this.propertyStore = propertyStore;
        this.nodeStore = nodeStore;
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
        try
        {
            NodeRecord nodeRecord = nodeStore.getRecord( nodeId );
            for ( PropertyBlock block : propertyBlocks( nodeRecord ) )
            {
                if ( block.getKeyIndexId() == propertyKeyId )
                {
                    return propertyValue( block );
                }
            }
        }
        catch ( InvalidRecordException e )
        {
            // Fine, we'll just return an empty property below
        }

        return Property.noNodeProperty( nodeId, propertyKeyId );
    }
}
