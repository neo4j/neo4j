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
package org.neo4j.kernel.impl.core;

import java.util.List;

import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * Keeps a cache of tokens using {@link InMemoryTokenCache}.
 * When asked for a token that isn't in the cache, delegates to a TokenCreator to create the token,
 * then stores it in the cache.
 */
public class DelegatingTokenHolder<TOKEN extends Token> extends LifecycleAdapter implements TokenHolder<TOKEN>
{
    protected InMemoryTokenCache<TOKEN> tokenCache = new InMemoryTokenCache<>(this.getClass());
    private final TokenCreator tokenCreator;
    private final TokenFactory<TOKEN> tokenFactory;

    public DelegatingTokenHolder( TokenCreator tokenCreator, TokenFactory<TOKEN> tokenFactory )
    {
        this.tokenCreator = tokenCreator;
        this.tokenFactory = tokenFactory;
    }

    @Override
    public void setInitialTokens( List<TOKEN> tokens ) throws NonUniqueTokenException
    {
        tokenCache.clear();
        tokenCache.putAll( tokens );
    }

    @Override
    public void addToken( TOKEN token ) throws NonUniqueTokenException
    {
        tokenCache.put( token );
    }

    @Override
    public int getOrCreateId( String name )
    {
        Integer id = tokenCache.getId( name );
        if ( id != null )
        {
            return id;
        }

        // Let's create it
        try
        {
            id = createToken( name );
            return id;
        }
        catch ( Throwable e )
        {
            throw new TransactionFailureException( "Could not create token", e );
        }
    }

    private synchronized int createToken( String name )
            throws KernelException
    {
        Integer id = tokenCache.getId( name );
        if ( id != null )
        {
            return id;
        }

        id = tokenCreator.getOrCreate( name );
        try
        {
            tokenCache.put( tokenFactory.newToken( name, id ) );
        }
        catch ( NonUniqueTokenException e )
        {
            throw new IllegalStateException( "Newly created token should be unique.", e );
        }
        return id;
    }

    @Override
    public TOKEN getTokenById( int id ) throws TokenNotFoundException
    {
        TOKEN result = getTokenByIdOrNull( id );
        if ( result == null )
        {
            throw new TokenNotFoundException( "Token for id " + id );
        }
        return result;
    }

    @Override
    public TOKEN getTokenByIdOrNull( int id )
    {
        return tokenCache.getToken( id );
    }

    @Override
    public int getIdByName( String name )
    {
        Integer id = tokenCache.getId( name );
        if ( id == null )
        {
            return NO_ID;
        }
        return id;
    }

    @Override
    public Iterable<TOKEN> getAllTokens()
    {
        return tokenCache.allTokens();
    }
}
