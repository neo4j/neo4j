/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.core;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.internal.kernel.api.NamedToken;

import static java.util.Collections.unmodifiableCollection;

/**
 * Token registry provide id -> TOKEN and name -> id mappings.
 * Name -> id mapping will be updated last since it's used to check if the token already exists.
 *
 * Implementation does not provide any atomicity guarantees. Mapping updates will be visible independently from each
 * other.
 * Implementation is not thread safe.
 */
public class TokenRegistry
{
    private final Map<String, Integer> nameToId = new ConcurrentHashMap<>();
    private final Map<Integer,NamedToken> idToToken = new ConcurrentHashMap<>();
    private final String tokenType;

    public TokenRegistry( String tokenType )
    {
        this.tokenType = tokenType;
    }

    public String getTokenType()
    {
        return tokenType;
    }

    public void setInitialTokens( List<NamedToken> tokens )
    {
        nameToId.clear();
        idToToken.clear();
        putAll( tokens );
    }

    private void putAndEnsureUnique( Map<String,Integer> nameToId, NamedToken token )
    {
        Integer previous = nameToId.putIfAbsent( token.name(), token.id() );
        if ( previous != null && previous != token.id() )
        {
            // since we optimistically put token into a map before, now we need to remove it.
            idToToken.remove( token.id(), token );
            throw new NonUniqueTokenException( tokenType, token.name(), token.id(), previous );
        }
    }

    public void putAll( List<NamedToken> tokens ) throws NonUniqueTokenException
    {
        Map<String, Integer> newNameToId = new HashMap<>();
        Map<Integer, NamedToken> newIdToToken = new HashMap<>();

        for ( NamedToken token : tokens )
        {
            newIdToToken.put( token.id(), token );
            putAndEnsureUnique( newNameToId, token );
        }

        idToToken.putAll( newIdToToken );
        nameToId.putAll( newNameToId );
    }

    public void put( NamedToken token ) throws NonUniqueTokenException
    {
        idToToken.put( token.id(), token );
        putAndEnsureUnique( nameToId, token );
    }

    public Integer getId( String name )
    {
        return nameToId.get( name );
    }

    public NamedToken getToken( int id )
    {
        return idToToken.get( id );
    }

    public Iterable<NamedToken> allTokens()
    {
        return unmodifiableCollection( idToToken.values() );
    }

    public int size()
    {
        return nameToId.size();
    }
}
