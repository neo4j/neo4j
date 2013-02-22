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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.helpers.Function;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;

public class PropertyPhysicalToLogicalConverter implements Function<Pair<PropertyRecord,PropertyRecord>, Iterable<NodePropertyUpdate>>
{
    private final PropertyStore propertyStore;

    public PropertyPhysicalToLogicalConverter( PropertyStore propertyStore )
    {
        this.propertyStore = propertyStore;
    }
    
    @Override
    public Iterable<NodePropertyUpdate> apply( Pair<PropertyRecord, PropertyRecord> from )
    {
        assert from.first().getNodeId() == from.other().getNodeId();
        long nodeId = from.first().getNodeId();
        Map<Integer, PropertyBlock> beforeMap = mapBlocks( from.first() );
        Map<Integer, PropertyBlock> afterMap = mapBlocks( from.other() );
        
        @SuppressWarnings( "unchecked" )
        Set<Integer> allKeys = union( beforeMap.keySet(), afterMap.keySet() );
        
        Collection<NodePropertyUpdate> result = new ArrayList<NodePropertyUpdate>();
        for ( int key : allKeys )
        {
            PropertyBlock beforeBlock = beforeMap.get( key );
            PropertyBlock afterBlock = afterMap.get( key );
            
            if ( beforeBlock != null && afterBlock != null )
            {
                if ( !beforeBlock.hasSameContentsAs( afterBlock ) )
                    result.add( new NodePropertyUpdate( nodeId, key, valueOf( beforeBlock ), valueOf( beforeBlock ) ) );
            }
            else
            {
                result.add( new NodePropertyUpdate( nodeId, key, valueOf( beforeBlock ), valueOf( afterBlock ) ) );
            }
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
