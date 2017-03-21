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
package org.neo4j.kernel.impl.api.index;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.transaction.state.PropertyRecordChange;

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
        Map<Integer, PropertyBlock> beforeMap = new HashMap<>(), afterMap = new HashMap<>();
        mapBlocks( nodeId, changes, beforeMap, afterMap );

        for ( int key : union( beforeMap.keySet(), afterMap.keySet() ) )
        {
            PropertyBlock beforeBlock = beforeMap.get( key );
            PropertyBlock afterBlock = afterMap.get( key );

            if ( beforeBlock != null && afterBlock != null )
            {
                // CHANGE
                if ( !beforeBlock.hasSameContentsAs( afterBlock ) )
                {
                    Object beforeVal = valueOf( beforeBlock );
                    Object afterVal = valueOf( afterBlock );
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

    private <T> Set<T> union( Set<T> first, Set<T> other )
    {
        Set<T> union = new HashSet<>( first );
        union.addAll( other );
        return union;
    }

    private long mapBlocks( long nodeId, Iterable<PropertyRecordChange> changes,
            Map<Integer,PropertyBlock> beforeMap, Map<Integer,PropertyBlock> afterMap )
    {
        for ( PropertyRecordChange change : changes )
        {
            equalCheck( change.getBefore().getNodeId(), nodeId );
            equalCheck( change.getAfter().getNodeId(), nodeId );
            mapBlocks( change.getBefore(), beforeMap );
            mapBlocks( change.getAfter(), afterMap );
        }
        return nodeId;
    }

    private void equalCheck( long nodeId, long expectedNodeId )
    {
        assert nodeId == expectedNodeId : "Node id differs expected " + expectedNodeId + ", but was " + nodeId;
    }

    private void mapBlocks( PropertyRecord record, Map<Integer, PropertyBlock> blocks )
    {
        for ( PropertyBlock block : record )
        {
            blocks.put( block.getKeyIndexId(), block );
        }
    }

    private Object valueOf( PropertyBlock block )
    {
        if ( block == null )
        {
            return null;
        }

        return block.getType().getValue( block, propertyStore );
    }
}
