/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.storageengine.api.PropertyItem;

class PropertyContainerProxyHelper
{
    private PropertyContainerProxyHelper()
    {
    }

    static Map<String, Object> getProperties( Statement statement, Cursor<PropertyItem> propertyCursor, String[] keys )
    {
        // Create a map which is slightly larger than the list of keys

        int numberOfKeys = keys.length;
        Map<String,Object> properties = new HashMap<>( ((int) (numberOfKeys * 1.3f)) );

        // Specific properties given
        int[] propertyKeys = getPropertyKeys( statement, keys, numberOfKeys );

        int propertiesToFind = numberOfKeys;
        while ( propertiesToFind > 0 && propertyCursor.next() )
        {
            PropertyItem propertyItem = propertyCursor.get();
            int propertyKeyId = propertyItem.propertyKeyId();
            for ( int i = 0; i < propertyKeys.length; i++ )
            {
                if ( propertyKeys[i] == propertyKeyId )
                {
                    properties.put( keys[i], propertyItem.value() );
                    propertiesToFind--;
                    break;
                }
            }
        }

        return properties;
    }

    private static int[] getPropertyKeys( Statement statement, String[] keys, int numberOfKeys )
    {
        int[] propertyKeys = new int[numberOfKeys];
        ReadOperations readOperations = statement.readOperations();
        for ( int i = 0; i < numberOfKeys; i++ )
        {
            String key = keys[i];
            if ( key == null )
            {
                throw new NullPointerException( String.format( "Key %d was null", i + 1 ) );
            }
            propertyKeys[i] = readOperations.propertyKeyGetForName( key );
        }
        return propertyKeys;
    }
}
