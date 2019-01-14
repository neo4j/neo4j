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

import java.util.Iterator;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.transaction.state.PropertyRecordChange;
import org.neo4j.values.storable.Value;

import static org.neo4j.collection.primitive.PrimitiveIntCollections.concat;
import static org.neo4j.collection.primitive.PrimitiveIntCollections.deduplicate;
import static org.neo4j.helpers.collection.Iterators.asIterator;

public class PropertyPhysicalToLogicalConverter
{
    private final PropertyStore propertyStore;

    public PropertyPhysicalToLogicalConverter( PropertyStore propertyStore )
    {
        this.propertyStore = propertyStore;
    }

    /**
     * Converts physical changes to PropertyRecords for a node into logical updates
     */
    public void convertPropertyRecord( long nodeId, Iterable<PropertyRecordChange> changes,
            NodeUpdates.Builder properties )
    {
        PrimitiveIntObjectMap<PropertyBlock> beforeMap = Primitive.intObjectMap();
        PrimitiveIntObjectMap<PropertyBlock> afterMap = Primitive.intObjectMap();
        mapBlocks( nodeId, changes, beforeMap, afterMap );

        PrimitiveIntIterator uniqueIntIterator = uniqueIntIterator( beforeMap, afterMap );
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

    private PrimitiveIntIterator uniqueIntIterator( PrimitiveIntObjectMap<PropertyBlock> beforeMap,
            PrimitiveIntObjectMap<PropertyBlock> afterMap )
    {
        Iterator<PrimitiveIntIterator> intIterator =
                asIterator( 2, beforeMap.iterator(), afterMap.iterator() );
        return deduplicate( concat( intIterator ) );
    }

    private void mapBlocks( long nodeId, Iterable<PropertyRecordChange> changes,
            PrimitiveIntObjectMap<PropertyBlock> beforeMap, PrimitiveIntObjectMap<PropertyBlock> afterMap )
    {
        for ( PropertyRecordChange change : changes )
        {
            equalCheck( change.getBefore().getNodeId(), nodeId );
            equalCheck( change.getAfter().getNodeId(), nodeId );
            mapBlocks( change.getBefore(), beforeMap );
            mapBlocks( change.getAfter(), afterMap );
        }
    }

    private void equalCheck( long nodeId, long expectedNodeId )
    {
        assert nodeId == expectedNodeId : "Node id differs expected " + expectedNodeId + ", but was " + nodeId;
    }

    private void mapBlocks( PropertyRecord record, PrimitiveIntObjectMap<PropertyBlock> blocks )
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
