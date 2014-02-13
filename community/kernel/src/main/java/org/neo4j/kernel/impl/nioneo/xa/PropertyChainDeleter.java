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
package org.neo4j.kernel.impl.nioneo.xa;

import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.PrimitiveRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.util.ArrayMap;

public class PropertyChainDeleter
{
    private final PropertyStore propertyStore;

    public PropertyChainDeleter( PropertyStore propertyStore )
    {
        this.propertyStore = propertyStore;
    }

    public ArrayMap<Integer, DefinedProperty> getAndDeletePropertyChain( PrimitiveRecord primitive,
                                                                         RecordChanges<Long, PropertyRecord, PrimitiveRecord> propertyRecords )
    {
        ArrayMap<Integer, DefinedProperty> result = new ArrayMap<>( (byte) 9, false, true );
        long nextProp = primitive.getNextProp();
        while ( nextProp != Record.NO_NEXT_PROPERTY.intValue() )
        {
            RecordChanges.RecordChange<Long, PropertyRecord, PrimitiveRecord> propertyChange =
                    propertyRecords.getOrLoad( nextProp, primitive );

            // TODO forChanging/forReading piggy-backing
            PropertyRecord propRecord = propertyChange.forChangingData();
            PropertyRecord before = propertyChange.getBefore();
            for ( PropertyBlock block : before.getPropertyBlocks() )
            {
                result.put( block.getKeyIndexId(), block.newPropertyData( propertyStore ) );
            }
            for ( PropertyBlock block : propRecord.getPropertyBlocks() )
            {
                for ( DynamicRecord valueRecord : block.getValueRecords() )
                {
                    assert valueRecord.inUse();
                    valueRecord.setInUse( false );
                    propRecord.addDeletedRecord( valueRecord );
                }
            }
            nextProp = propRecord.getNextProp();
            propRecord.setInUse( false );
            propRecord.setChanged( primitive );
            // We do not remove them individually, but all together here
            propRecord.getPropertyBlocks().clear();
        }
        return result;
    }
}
