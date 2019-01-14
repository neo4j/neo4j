/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;
import org.neo4j.storageengine.api.Token;

import static java.util.Collections.unmodifiableCollection;

/**
 * Token cache that provide id -> TOKEN and name -> id mappings.
 * Name -> id mapping will be updated last since it's used as part of the check for token existence in a cache.
 * As soon as token visible through it - it's considered added into a cache.
 *
 * Implementation does not provide any atomicity guarantees. Mapping updates will be visible independently from each
 * other.
 * Implementation is not thread safe.
 *
 * @param <TOKEN> token type
 */
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

    private void putAndEnsureUnique( Map<String,Integer> nameToId, TOKEN token, String tokenType )
    {
        Integer previous = nameToId.putIfAbsent( token.name(), token.id() );
        if ( previous != null && previous != token.id() )
        {
            // since we optimistically put token into a map before, now we need to remove it.
            idToToken.remove( token.id(), token );
            throw new NonUniqueTokenException( tokenType, token.name(), token.id(), previous );
        }
    }

    public void putAll( List<TOKEN> tokens ) throws NonUniqueTokenException
    {
        Map<String, Integer> newNameToId = new HashMap<>();
        Map<Integer, TOKEN> newIdToToken = new HashMap<>();

        for ( TOKEN token : tokens )
        {
            newIdToToken.put( token.id(), token );
            putAndEnsureUnique( newNameToId, token, tokenType );
        }

        idToToken.putAll( newIdToToken );
        nameToId.putAll( newNameToId );
    }

    public void put( TOKEN token ) throws NonUniqueTokenException
    {
        idToToken.put( token.id(), token );
        putAndEnsureUnique( nameToId, token, tokenType );
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
        return unmodifiableCollection( idToToken.values() );
    }

    public int size()
    {
        return nameToId.size();
    }
}
