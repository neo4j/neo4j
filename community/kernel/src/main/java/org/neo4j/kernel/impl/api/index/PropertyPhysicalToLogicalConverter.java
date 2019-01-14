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
package org.neo4j.kernel.impl.api.index;

import org.eclipse.collections.api.iterator.IntIterator;
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import org.neo4j.kernel.impl.storageengine.impl.recordstorage.PropertyRecordChange;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.values.storable.Value;

public class PropertyPhysicalToLogicalConverter
{
    private final PropertyStore propertyStore;

    public PropertyPhysicalToLogicalConverter( PropertyStore propertyStore )
    {
        this.propertyStore = propertyStore;
    }

    /**
     * Converts physical changes to PropertyRecords for a entity into logical updates
     */
    public void convertPropertyRecord( long entityId, Iterable<PropertyRecordChange> changes,
            EntityUpdates.Builder properties )
    {
        MutableIntObjectMap<PropertyBlock> beforeMap = new IntObjectHashMap<>();
        MutableIntObjectMap<PropertyBlock> afterMap = new IntObjectHashMap<>();
        mapBlocks( entityId, changes, beforeMap, afterMap );

        final IntIterator uniqueIntIterator = uniqueIntIterator( beforeMap, afterMap );
        while ( uniqueIntIterator.hasNext() )
        {
            int key = uniqueIntIterator.next();
            PropertyBlock beforeBlock = beforeMap.get( key );
            PropertyBlock afterBlock = afterMap.get( key );

            if ( beforeBlock != null && afterBlock != null )
            {
                // CHANGE
                if ( !beforeBlock.hasSameContentsAs( afterBlock ) )
                {
                    Value beforeVal = valueOf( beforeBlock );
                    Value afterVal = valueOf( afterBlock );
                    properties.changed( key, beforeVal, afterVal );
                }
            }
            else
            {
                // ADD/REMOVE
                if ( afterBlock != null )
                {
                    properties.added( key, valueOf( afterBlock ) );
                }
                else if ( beforeBlock != null )
                {
                    properties.removed( key, valueOf( beforeBlock ) );
                }
                else
                {
                    throw new IllegalStateException( "Weird, an update with no property value for before or after" );
                }
            }
        }
    }

    private IntIterator uniqueIntIterator( IntObjectMap<PropertyBlock> beforeMap, IntObjectMap<PropertyBlock> afterMap )
    {
        final MutableIntSet keys = new IntHashSet();
        keys.addAll( beforeMap.keySet() );
        keys.addAll( afterMap.keySet() );
        return keys.intIterator();
    }

    private void mapBlocks( long entityId, Iterable<PropertyRecordChange> changes,
            MutableIntObjectMap<PropertyBlock> beforeMap, MutableIntObjectMap<PropertyBlock> afterMap )
    {
        for ( PropertyRecordChange change : changes )
        {
            equalCheck( change.getBefore().getEntityId(), entityId );
            equalCheck( change.getAfter().getEntityId(), entityId );
            mapBlocks( change.getBefore(), beforeMap );
            mapBlocks( change.getAfter(), afterMap );
        }
    }

    private void equalCheck( long entityId, long expectedEntityId )
    {
        assert entityId == expectedEntityId : "Entity id differs expected " + expectedEntityId + ", but was " + entityId;
    }

    private void mapBlocks( PropertyRecord record, MutableIntObjectMap<PropertyBlock> blocks )
    {
        for ( PropertyBlock block : record )
        {
            blocks.put( block.getKeyIndexId(), block );
        }
    }

    private Value valueOf( PropertyBlock block )
    {
        if ( block == null )
        {
            return null;
        }
        return block.getType().value( block, propertyStore );
    }
}
