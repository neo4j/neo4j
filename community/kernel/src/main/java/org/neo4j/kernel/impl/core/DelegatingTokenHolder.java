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

import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.util.List;
import java.util.function.IntPredicate;

import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.ReadOnlyDbException;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.storageengine.api.Token;
import org.neo4j.storageengine.api.TokenFactory;

import static org.neo4j.function.Predicates.ALWAYS_FALSE_INT;
import static org.neo4j.function.Predicates.ALWAYS_TRUE_INT;

/**
 * Keeps a cache of tokens using {@link InMemoryTokenCache}.
 * When asked for a token that isn't in the cache, delegates to a TokenCreator to create the token,
 * then stores it in the cache.
 */
public abstract class DelegatingTokenHolder<TOKEN extends Token> extends LifecycleAdapter implements TokenHolder<TOKEN>
{
    protected InMemoryTokenCache<TOKEN> tokenCache = new InMemoryTokenCache<>( tokenType() );

    protected abstract String tokenType();

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
        catch ( ReadOnlyDbException e )
        {
            throw new TransactionFailureException( e.getMessage(), e );
        }
        catch ( Throwable e )
        {
            throw new TransactionFailureException( "Could not create token.", e );
        }
    }

    /**
     * Create and put new token in cache.
     *
     * @param name token name
     * @return newly created token id
     * @throws KernelException
     */
    private synchronized int createToken( String name ) throws KernelException
    {
        Integer id = tokenCache.getId( name );
        if ( id != null )
        {
            return id;
        }

        id = tokenCreator.createToken( name );
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
    public void getOrCreateIds( String[] names, int[] ids )
    {
        if ( names.length != ids.length )
        {
            throw new IllegalArgumentException( "Name and id arrays must have the same length." );
        }
        // Assume all tokens exist and try to resolve them. Break out on the first missing token.
        boolean hasUnresolvedTokens = resolveIds( names, ids, ALWAYS_TRUE_INT );

        if ( hasUnresolvedTokens )
        {
            createMissingTokens( names, ids );
        }
    }

    private boolean resolveIds( String[] names, int[] ids, IntPredicate unresolvedIndexCheck )
    {
        boolean foundUnresolvable = false;
        for ( int i = 0; i < ids.length; i++ )
        {
            Integer id = tokenCache.getId( names[i] );
            if ( id != null )
            {
                ids[i] = id;
            }
            else
            {
                foundUnresolvable = true;
                if ( unresolvedIndexCheck.test( i ) )
                {
                    // If the check returns `true`, it's a signal that we should stop early.
                    break;
                }
            }
        }
        return foundUnresolvable;
    }

    private synchronized void createMissingTokens( String[] names, int[] ids )
    {
        // We redo the resolving under the lock, to make sure that these ids are really missing, and won't be
        // created concurrently with us.
        MutableIntSet unresolvedIndexes = new IntHashSet();
        resolveIds( names, ids, i -> !unresolvedIndexes.add( i ) );
        if ( !unresolvedIndexes.isEmpty() )
        {
            // We still have unresolved ids to create.
            createUnresolvedTokens( unresolvedIndexes, names, ids );
            unresolvedIndexes.forEach( i ->
                    tokenCache.put( tokenFactory.newToken( names[i], ids[i] ) ) );
        }
    }

    private void createUnresolvedTokens( IntSet unresolvedIndexes, String[] names, int[] ids )
    {
        try
        {
            tokenCreator.createTokens( names, ids, unresolvedIndexes::contains );
        }
        catch ( ReadOnlyDbException e )
        {
            throw new TransactionFailureException( e.getMessage(), e );
        }
        catch ( Throwable e )
        {
            throw new TransactionFailureException( "Could not create tokens.", e );
        }
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
    public boolean getIdsByNames( String[] names, int[] ids )
    {
        return resolveIds( names, ids, ALWAYS_FALSE_INT );
    }

    @Override
    public Iterable<TOKEN> getAllTokens()
    {
        return tokenCache.allTokens();
    }

    @Override
    public int size()
    {
        return tokenCache.size();
    }
}
