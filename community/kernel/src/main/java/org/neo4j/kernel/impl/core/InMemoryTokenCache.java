/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.internal.kernel.api.NamedToken;
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;

/**
 * Token cache that provide id -> TOKEN and name -> id mappings.
 * Name -> id mapping will be updated last since it's used as part of the check for token existence in a cache.
 * As soon as token visible through it - it's considered added into a cache.
 *
 * Implementation does not provide any atomicity guarantees. Mapping updates will be visible independently from each
 * other.
 * Implementation is not thread safe.
 */
public class InMemoryTokenCache
{
    private final Map<String, Integer> nameToId = new CopyOnWriteHashMap<>();
    private final Map<Integer,NamedToken> idToToken = new CopyOnWriteHashMap<>();
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

    private void putAndEnsureUnique( Map<String,Integer> nameToId, NamedToken token, String tokenType )
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
            putAndEnsureUnique( newNameToId, token, tokenType );
        }

        idToToken.putAll( newIdToToken );
        nameToId.putAll( newNameToId );
    }

    public void put( NamedToken token ) throws NonUniqueTokenException
    {
        idToToken.put( token.id(), token );
        putAndEnsureUnique( nameToId, token, tokenType );
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
        return idToToken.values();
    }

    public int size()
    {
        return nameToId.size();
    }
}
