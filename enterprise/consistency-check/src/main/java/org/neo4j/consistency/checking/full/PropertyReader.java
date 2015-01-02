/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency.checking.full;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;

import static org.neo4j.kernel.impl.util.Providers.singletonProvider;

public class PropertyReader
{
    private final PropertyStore propertyStore;

    public PropertyReader( PropertyStore propertyStore )
    {
        this.propertyStore = propertyStore;
    }

    public List<PropertyBlock> propertyBlocks( NodeRecord nodeRecord )
    {
        Collection<PropertyRecord> records = propertyStore.getPropertyRecordChain( nodeRecord.getNextProp() );
        List<PropertyBlock> propertyBlocks = new ArrayList<>();
        for ( PropertyRecord record : records )
        {
            propertyBlocks.addAll( record.getPropertyBlocks() );
        }
        return propertyBlocks;
    }

    public DefinedProperty propertyValue( PropertyBlock block )
    {
        return block.getType().readProperty( block.getKeyIndexId(), block, singletonProvider(propertyStore) );
    }
}
