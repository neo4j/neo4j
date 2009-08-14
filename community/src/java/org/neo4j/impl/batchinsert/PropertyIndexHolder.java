/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.batchinsert;

import org.neo4j.impl.nioneo.store.PropertyIndexData;
import org.neo4j.impl.util.ArrayMap;

public class PropertyIndexHolder
{
    private final ArrayMap<String,Integer> propertyIndexes = 
        new ArrayMap<String,Integer>( 5, false, false);
    private final ArrayMap<Integer,String> idToIndex = 
        new ArrayMap<Integer,String>( 5, false, false);
    
    PropertyIndexHolder( PropertyIndexData[] indexes )
    {
        for ( PropertyIndexData index : indexes )
        {
            propertyIndexes.put( index.getValue(), index.getKeyId() );
            idToIndex.put( index.getKeyId(), index.getValue() );
        }
    }
    
    void addPropertyIndex( String stringKey, int keyId )
    {
        propertyIndexes.put( stringKey, keyId );
        idToIndex.put( keyId, stringKey );
    }
    
    int getKeyId( String stringKey )
    {
        Integer keyId = propertyIndexes.get( stringKey );
        if ( keyId != null )
        {
            return keyId;
        }
        return -1;
    }
    
    String getStringKey( int keyId )
    {
        String stringKey = idToIndex.get( keyId );
        if ( stringKey == null )
        {
            throw new RuntimeException( "No such property index[" + keyId + 
                "]" );
        }
        return stringKey;
    }
}
