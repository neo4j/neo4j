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
package org.neo4j.unsafe.batchinsert;

import org.neo4j.graphdb.NotFoundException;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.util.ArrayMap;

class BatchTokenHolder
{
    private final ArrayMap<String,Integer> nameToId =
        new ArrayMap<String,Integer>( (byte)5, false, false);
    private final ArrayMap<Integer,String> idToName =
        new ArrayMap<Integer,String>( (byte)5, false, false);
    
    BatchTokenHolder( Token[] tokens )
    {
        for ( Token token : tokens )
        {
            nameToId.put( token.name(), token.id() );
            idToName.put( token.id(), token.name() );
        }
    }
    
    void addToken( String stringKey, int keyId )
    {
        nameToId.put( stringKey, keyId );
        idToName.put( keyId, stringKey );
    }
    
    int idOf( String stringKey )
    {
        Integer id = nameToId.get( stringKey );
        if ( id != null )
        {
            return id;
        }
        return -1;
    }
    
    String nameOf( int id )
    {
        String name = idToName.get( id );
        if ( name == null )
        {
            throw new NotFoundException( "No token with id:" + id );
        }
        return name;
    }
}
