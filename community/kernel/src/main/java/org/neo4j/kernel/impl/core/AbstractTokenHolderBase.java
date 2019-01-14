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

import java.util.List;
import java.util.function.IntPredicate;

import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.internal.kernel.api.NamedToken;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.ReadOnlyDbException;

import static org.neo4j.function.Predicates.ALWAYS_FALSE_INT;
import static org.neo4j.internal.kernel.api.TokenRead.NO_TOKEN;

public abstract class AbstractTokenHolderBase implements TokenHolder
{
    protected final TokenRegistry tokenRegistry;

    public AbstractTokenHolderBase( TokenRegistry tokenRegistry )
    {
        this.tokenRegistry = tokenRegistry;
    }

    @Override
    public void setInitialTokens( List<NamedToken> tokens ) throws NonUniqueTokenException
    {
        tokenRegistry.setInitialTokens( tokens );
    }

    @Override
    public void addToken( NamedToken token ) throws NonUniqueTokenException
    {
        tokenRegistry.put( token );
    }

    @Override
    public int getOrCreateId( String name )
    {
        Integer id = tokenRegistry.getId( name );
        if ( id != null )
        {
            return id;
        }

        // Let's create it
        try
        {
            return createToken( name );
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

    @Override
    public NamedToken getTokenById( int id ) throws TokenNotFoundException
    {
        NamedToken result = tokenRegistry.getToken( id );
        if ( result == null )
        {
            throw new TokenNotFoundException( "Token for id " + id );
        }
        return result;
    }

    @Override
    public int getIdByName( String name )
    {
        Integer id = tokenRegistry.getId( name );
        if ( id == null )
        {
            return NO_TOKEN;
        }
        return id;
    }

    @Override
    public boolean getIdsByNames( String[] names, int[] ids )
    {
        return resolveIds( names, ids, ALWAYS_FALSE_INT );
    }

    @Override
    public Iterable<NamedToken> getAllTokens()
    {
        return tokenRegistry.allTokens();
    }

    @Override
    public int size()
    {
        return tokenRegistry.size();
    }

    protected abstract int createToken( String tokenName ) throws KernelException;

    boolean resolveIds( String[] names, int[] ids, IntPredicate unresolvedIndexCheck )
    {
        boolean foundUnresolvable = false;
        for ( int i = 0; i < ids.length; i++ )
        {
            Integer id = tokenRegistry.getId( names[i] );
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
}
