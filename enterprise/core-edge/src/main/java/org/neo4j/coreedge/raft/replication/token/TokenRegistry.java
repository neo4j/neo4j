/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.coreedge.raft.replication.token;

import java.util.List;

import org.neo4j.kernel.impl.core.InMemoryTokenCache;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.storageengine.api.Token;

public class TokenRegistry<TOKEN extends Token>
{
    private final InMemoryTokenCache<TOKEN> tokenCache;
    private final TokenFutures tokenFutures = new TokenFutures();

    public TokenRegistry( String tokenType )
    {
        this.tokenCache = new InMemoryTokenCache<>( tokenType );
    }

    public TokenFutures.CompletableFutureTokenId createFuture( String tokenName )
    {
        return tokenFutures.createFuture( tokenName );
    }

    public void setInitialTokens( List<TOKEN> tokens )
    {
        tokenCache.clear();
        tokenCache.putAll( tokens );
    }

    public int size()
    {
        return tokenCache.size();
    }

    public Iterable<TOKEN> allTokens()
    {
        return tokenCache.allTokens();
    }

    public Integer getId( String name )
    {
        return tokenCache.getId( name );
    }

    public TOKEN getToken( int id )
    {
        return tokenCache.getToken( id );
    }

    public void addToken( TOKEN token )
    {
        tokenCache.put( token );
    }

    public void complete( String key, Integer result )
    {
        tokenFutures.complete( key, result );
    }
}
