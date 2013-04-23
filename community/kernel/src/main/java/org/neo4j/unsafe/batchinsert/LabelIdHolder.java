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
package org.neo4j.unsafe.batchinsert;

import org.neo4j.graphdb.NotFoundException;
import org.neo4j.kernel.impl.nioneo.store.NameData;
import org.neo4j.kernel.impl.util.ArrayMap;

class LabelIdHolder
{
    private final ArrayMap<String,Integer> nameToId =
            new ArrayMap<String,Integer>( (byte)5, false, false);
    private final ArrayMap<Integer,String> idToName =
            new ArrayMap<Integer,String>( (byte)5, false, false);

    LabelIdHolder( NameData[] labelIds )
    {
        for ( NameData labelId : labelIds )
        {
            nameToId.put( labelId.getName(), labelId.getId() );
            idToName.put( labelId.getId(), labelId.getName() );
        }
    }

    void addLabelId( String stringKey, int keyId )
    {
        nameToId.put( stringKey, keyId );
        idToName.put( keyId, stringKey );
    }

    long getKeyId( String name )
    {
        Integer keyId = nameToId.get( name );
        if ( keyId != null )
        {
            return keyId;
        }
        return -1;
    }
    
    String getStringKey( long keyId )
    {
        String stringKey = idToName.get( (int) keyId );
        if ( stringKey == null )
        {
            throw new NotFoundException( "No such label[" + keyId + "]" );
        }
        return stringKey;
    }
}
