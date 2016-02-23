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
import java.util.List;
import java.util.Map;

import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;
import org.neo4j.storageengine.api.Token;

public class InMemoryTokenCache<TOKEN extends Token>
{
    private final Map<String, Integer> nameToId = new CopyOnWriteHashMap<>();
    private final Map<Integer, TOKEN> idToToken = new CopyOnWriteHashMap<>();
    private final String tokenType;

    public InMemoryTokenCache( String tokenType )
    {
        this.tokenType = tokenType;
    }

    public void clear()
    {
        nameToId.clear();
        idToToken.clear();
    }

    private static void putAndEnsureUnique( Map<String,Integer> nameToId, Token token, String tokenType )
    {
        Integer previous;
        if ( (previous = nameToId.put( token.name(), token.id() )) != null && previous != token.id() )
        {
            throw new NonUniqueTokenException( tokenType, token.name(), token.id(), previous );
        }
    }

    public void putAll( List<TOKEN> tokens ) throws NonUniqueTokenException
    {
        Map<String, Integer> newNameToId = new HashMap<>();
        Map<Integer, TOKEN> newIdToToken = new HashMap<>();

        for ( TOKEN token : tokens )
        {
            putAndEnsureUnique( newNameToId, token, tokenType );
            newIdToToken.put( token.id(), token );
        }

        nameToId.putAll( newNameToId );
        idToToken.putAll( newIdToToken );
    }

    public void put( TOKEN token ) throws NonUniqueTokenException
    {
        putAndEnsureUnique( nameToId, token, tokenType );
        idToToken.put( token.id(), token );
    }

    public Integer getId( String name )
    {
        return nameToId.get( name );
    }

    public TOKEN getToken( int id )
    {
        return idToToken.get( id );
    }

    public Iterable<TOKEN> allTokens()
    {
        return idToToken.values();
    }

    public int size()
    {
        return nameToId.size();
    }
}
