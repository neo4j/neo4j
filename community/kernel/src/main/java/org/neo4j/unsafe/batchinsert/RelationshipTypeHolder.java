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
package org.neo4j.unsafe.batchinsert;

import org.neo4j.graphdb.NotFoundException;
import org.neo4j.kernel.impl.nioneo.store.NameData;
import org.neo4j.kernel.impl.util.ArrayMap;

class RelationshipTypeHolder
{
    private final ArrayMap<String,Integer> relTypes = 
        new ArrayMap<String,Integer>( (byte)5, false, false);
    private final ArrayMap<Integer,String> idToName = 
        new ArrayMap<Integer,String>( (byte)5, false, false);
    
    RelationshipTypeHolder( NameData[] types )
    {
        for ( NameData type : types )
        {
           relTypes.put( type.getName(), type.getId() );
           idToName.put( type.getId(), type.getName() );
        }
    }
    
    void addRelationshipType( String name, int id )
    {
        relTypes.put( name, id );
        idToName.put( id, name );
    }
    
    int getTypeId( String name )
    {
        Integer id = relTypes.get( name );
        if ( id != null )
        {
            return id;
        }
        return -1;
    }
    
    String getName( int id )
    {
        String name = idToName.get( id );
        if ( name == null )
        {
            throw new NotFoundException( "No such relationship type[" + id + 
                "]" );
        }
        return name;
    }
}
