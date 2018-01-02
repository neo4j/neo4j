/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.util.List;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.util.ArrayMap;

class BatchTokenHolder
{
    private final ArrayMap<String,Token> nameToToken = new ArrayMap<>( (byte) 5, false, false );
    private final PrimitiveIntObjectMap<Token> idToToken = Primitive.intObjectMap( 20 );

    BatchTokenHolder( List<? extends Token> tokens )
    {
        for ( Token token : tokens )
        {
            addToken( token );
        }
    }

    void addToken( Token token )
    {
        nameToToken.put( token.name(), token );
        idToToken.put( token.id(), token );
    }

    Token byId( int id )
    {
        return idToToken.get( id );
    }

    Token byName( String name )
    {
        return nameToToken.get( name );
    }

//    int idOf( String stringKey )
//    {
//        Integer id = nameToToken.get( stringKey );
//        if ( id != null )
//        {
//            return id;
//        }
//        return -1;
//    }
//
//    String nameOf( int id )
//    {
//        String name = idToName.get( id );
//        if ( name == null )
//        {
//            throw new NotFoundException( "No token with id:" + id );
//        }
//        return name;
//    }
}
