/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.impl.batchinsert;

import org.neo4j.graphdb.NotFoundException;
import org.neo4j.kernel.impl.nioneo.store.NameData;
import org.neo4j.kernel.impl.util.ArrayMap;

public class PropertyIndexHolder
{
    private final ArrayMap<String,Integer> propertyIndexes = 
        new ArrayMap<String,Integer>( (byte)5, false, false);
    private final ArrayMap<Integer,String> idToIndex = 
        new ArrayMap<Integer,String>( (byte)5, false, false);
    
    PropertyIndexHolder( NameData[] indexes )
    {
        for ( NameData index : indexes )
        {
            propertyIndexes.put( index.getName(), index.getId() );
            idToIndex.put( index.getId(), index.getName() );
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
            throw new NotFoundException( "No such property index[" + keyId + "]" );
        }
        return stringKey;
    }
}
