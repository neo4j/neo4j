/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.core;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.cursor.Cursor;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.cursor.PropertyItem;

/**
 * Base class for NodeProxy/RelationshipProxy with common methods for PropertyContainer
 */
public abstract class PropertyContainerProxy
{
    protected Map<String, Object> getProperties( Statement statement,
            Cursor<PropertyItem> propertyCursor,
            String[] keys )
    {
        // Create a map which is slightly larger than the list of keys
        Map<String, Object> properties = new HashMap<>( ((int) (keys.length * 1.3f)) );

        // Specific properties given
        int[] propertyKeys = new int[keys.length];
        for ( int i = 0; i < keys.length; i++ )
        {
            String key = keys[i];

            if ( key == null )
            {
                throw new NullPointerException( String.format( "Key %d was null", i + 1 ) );
            }

            propertyKeys[i] = statement.readOperations().propertyKeyGetForName( key );
        }

        while ( propertyCursor.next() )
        {
            for ( int i = 0; i < propertyKeys.length; i++ )
            {
                int propertyKey = propertyKeys[i];
                if ( propertyKey == propertyCursor.get().propertyKeyId() )
                {
                    properties.put( keys[i], propertyCursor.get().value() );
                    break;
                }
            }
        }

        return properties;
    }
}
