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
package org.neo4j.kernel.api.index;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class NodePropertyAccessor implements PropertyAccessor
{
    private final Map<Long, Map<Integer,Value>> nodePropertyMap;

    NodePropertyAccessor( long nodeId, SchemaDescriptor schema, Value... values )
    {
        nodePropertyMap = new HashMap<>();
        addNode( nodeId, schema, values );
    }

    public void addNode( long nodeId, SchemaDescriptor schema, Value... values )
    {
        Map<Integer,Value> propertyMap = new HashMap<>();
        int[] propertyIds = schema.getPropertyIds();
        for ( int i = 0; i < propertyIds.length; i++ )
        {
            propertyMap.put( propertyIds[i], values[i] );
        }
        nodePropertyMap.put( nodeId, propertyMap );
    }

    @Override
    public Value getPropertyValue( long nodeId, int propertyKeyId )
    {
        if ( nodePropertyMap.containsKey( nodeId ) )
        {
            Value value = nodePropertyMap.get( nodeId ).get( propertyKeyId );
            if ( value == null )
            {
                return Values.NO_VALUE;
            }
            else
            {
                return value;
            }
        }
        return Values.NO_VALUE;
    }
}
