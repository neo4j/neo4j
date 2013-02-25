/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static org.neo4j.kernel.impl.api.index.NodePropertyUpdate.EMPTY_LONG_ARRAY;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;

public class PropertyPhysicalToLogicalConverter
{
    private final PropertyStore propertyStore;

    public PropertyPhysicalToLogicalConverter( PropertyStore propertyStore )
    {
        this.propertyStore = propertyStore;
    }
    
    public Iterable<NodePropertyUpdate> apply(
            PropertyRecord before, long[] labelsBefore,
            PropertyRecord after, long[] labelsAfter )
    {
        assert before.getNodeId() == after.getNodeId() :
            "Node ids differ between before(" + before.getNodeId() + ") and after(" + after.getNodeId() + ")";
        long nodeId = before.getNodeId();
        Map<Integer, PropertyBlock> beforeMap = mapBlocks( before );
        Map<Integer, PropertyBlock> afterMap = mapBlocks( after );
        
        @SuppressWarnings( "unchecked" )
        Set<Integer> allKeys = union( beforeMap.keySet(), afterMap.keySet() );
        
        Collection<NodePropertyUpdate> result = new ArrayList<NodePropertyUpdate>();
        for ( int key : allKeys )
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
                    update = new NodePropertyUpdate( nodeId, key, beforeVal, labelsBefore, afterVal, labelsAfter );
                }
            }
            else
            {
                // ADD/REMOVE
                try {
                    long[] beforeLabelIds = beforeBlock == null ? EMPTY_LONG_ARRAY : labelsBefore;
                    long[] afterLabelIds = afterBlock == null ? EMPTY_LONG_ARRAY : labelsAfter;
                    Object beforeVal = valueOf( beforeBlock );
                    Object afterVal = valueOf( afterBlock );
                    update = new NodePropertyUpdate( nodeId, key, beforeVal, beforeLabelIds, afterVal, afterLabelIds );
                }
                catch (Exception e)
                {
                    // MP: break hear to see the issue: There is a broken next pointer chain in the before block
                    throw Exceptions.launderedException( e );
                }
            }
            if (update != null)
                result.add( update );
        }
        return result;
    }

    private <T> Set<T> union( Set<T>... sets )
    {
        Set<T> union = new HashSet<T>();
        for ( Set<T> set : sets )
            union.addAll( set );
        return union;
    }

    private Map<Integer, PropertyBlock> mapBlocks( PropertyRecord before )
    {
        HashMap<Integer, PropertyBlock> map = new HashMap<Integer, PropertyBlock>();
        for ( PropertyBlock block : before.getPropertyBlocks() )
            map.put( block.getKeyIndexId(), block );
        return map;
    }

    private Object valueOf( PropertyBlock block )
    {
        if ( block == null )
            return null;
        
        return block.getType().getValue( block, propertyStore );
    }
}
