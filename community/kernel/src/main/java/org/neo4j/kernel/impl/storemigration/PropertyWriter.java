/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration;

import java.util.List;

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;

public class PropertyWriter
{
    private PropertyStore propertyStore;

    public PropertyWriter( PropertyStore propertyStore )
    {
        this.propertyStore = propertyStore;
    }

    public long writeProperties( List<Pair<Integer, Object>> properties )
    {
        PropertyRecord propertyRecord = new PropertyRecord( propertyStore.nextId() );

        for ( Pair<Integer, Object> property : properties )
        {
            PropertyBlock block = new PropertyBlock();
            propertyStore.encodeValue( block, property.first(), property.other() );
            block.setInUse( true );
            propertyRecord.addPropertyBlock( block );
        }

        propertyRecord.setInUse( true );
        propertyStore.updateRecord( propertyRecord );

        return propertyRecord.getId();
    }
}
