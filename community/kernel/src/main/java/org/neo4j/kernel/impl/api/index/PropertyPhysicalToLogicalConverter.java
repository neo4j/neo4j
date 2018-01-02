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
package org.neo4j.kernel.impl.api.index;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.transaction.state.LabelChangeSummary;
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
     *
     * @param labelsBefore labels that node had before the change.
     * @param labelsAfter labels that node has after the change.
     * @return logical updates of the physical property record changes.
     */
    public void apply( Collection<NodePropertyUpdate> target, Iterable<PropertyRecordChange> changes,
            long[] labelsBefore, long[] labelsAfter )
    {
        Map<Integer, PropertyBlock> beforeMap = new HashMap<>(), afterMap = new HashMap<>();
        long nodeId = mapBlocks( changes, beforeMap, afterMap );

        for ( int key : union( beforeMap.keySet(), afterMap.keySet() ) )
        {
            PropertyBlock beforeBlock = beforeMap.get( key );
            PropertyBlock afterBlock = afterMap.get( key );
            NodePropertyUpdate update = null;

            if ( beforeBlock != null && afterBlock != null )
            {
                // CHANGE
                if ( !beforeBlock.hasSameContentsAs( afterBlock ) )
                {
                    Object beforeVal = valueOf( beforeBlock );
                    Object afterVal = valueOf( afterBlock );
                    update = NodePropertyUpdate.change( nodeId, key, beforeVal, labelsBefore, afterVal, labelsAfter );
                }
            }
            else
            {
                // ADD/REMOVE
                if ( afterBlock != null )
                {
                    final LabelChangeSummary summary = new LabelChangeSummary( labelsBefore, labelsAfter );
                    if ( summary.hasUnchangedLabels() )
                    {
                        update = NodePropertyUpdate.add( nodeId, key, valueOf( afterBlock ),
                                summary.getUnchangedLabels() );
                    }
                }
                else if ( beforeBlock != null )
                {
                    update = NodePropertyUpdate.remove( nodeId, key, valueOf( beforeBlock ), labelsBefore );
                }
                else
                {
                    throw new IllegalStateException( "Weird, an update with no property value for before or after" );
                }
            }

            if ( update != null)
            {
                target.add( update );
            }
        }
    }

    private <T> Set<T> union( Set<T> first, Set<T> other )
    {
        Set<T> union = new HashSet<>( first );
        union.addAll( other );
        return union;
    }

    private long mapBlocks( Iterable<PropertyRecordChange> changes,
            Map<Integer,PropertyBlock> beforeMap, Map<Integer,PropertyBlock> afterMap )
    {
        long nodeId = -1;
        for ( PropertyRecordChange change : changes )
        {
            nodeId = equalCheck( change.getBefore().getNodeId(), nodeId );
            nodeId = equalCheck( change.getAfter().getNodeId(), nodeId );
            mapBlocks( change.getBefore(), beforeMap );
            mapBlocks( change.getAfter(), afterMap );
        }
        return nodeId;
    }

    private long equalCheck( long nodeId, long expectedNodeId )
    {
        assert expectedNodeId == -1 || nodeId == expectedNodeId : "Node id differs expected " + expectedNodeId + ", but was " + nodeId;
        return nodeId;
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
