/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.state.machines.token;

import java.util.List;

import org.neo4j.kernel.impl.core.InMemoryTokenCache;
import org.neo4j.storageengine.api.Token;

public class TokenRegistry<TOKEN extends Token>
{
    private final InMemoryTokenCache<TOKEN> tokenCache;
    private final String tokenType;

    public TokenRegistry( String tokenType )
    {
        this.tokenType = tokenType;
        this.tokenCache = new InMemoryTokenCache<>( tokenType );
    }

    void setInitialTokens( List<TOKEN> tokens )
    {
        tokenCache.clear();
        tokenCache.putAll( tokens );
    }

    public String getTokenType()
    {
        return tokenType;
    }

    public int size()
    {
        return tokenCache.size();
    }

    Iterable<TOKEN> allTokens()
    {
        return tokenCache.allTokens();
    }

    Integer getId( String name )
    {
        return tokenCache.getId( name );
    }

    TOKEN getToken( int id )
    {
        return tokenCache.getToken( id );
    }

    void addToken( TOKEN token )
    {
        tokenCache.put( token );
    }
}
