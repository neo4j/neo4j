/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.util.Map;

import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * Exists:
 * get from map
 * <p>
 * Previously when it doesn't exist:
 * tokenCreator.create
 * record changes
 * command execution
 * <p>
 * Doesn't exist:
 * tokenCreator.create( name, id )
 * new kernel transaction
 * change in statement
 * commit
 * record changes
 * command execution
 * add to holder
 */
public abstract class TokenHolder<TOKEN extends Token> extends LifecycleAdapter
{
    public static final int NO_ID = -1;
    private final Map<String,Integer> nameToId = new CopyOnWriteHashMap<>();
    private final Map<Integer,TOKEN> idToToken = new CopyOnWriteHashMap<>();
    private final TokenCreator tokenCreator;

    public TokenHolder( TokenCreator tokenCreator )
    {
        this.tokenCreator = tokenCreator;
    }

    public void setInitialTokens( Token... tokens ) throws NonUniqueTokenException
    {
        nameToId.clear();
        idToToken.clear();

        Map<String,Integer> newNameToId = new HashMap<>();
        Map<Integer,TOKEN> newIdToToken = new HashMap<>();

        for ( Token token : tokens )
        {
            addToken( token.name(), token.id(), newNameToId, newIdToToken );
        }

        nameToId.putAll( newNameToId );
        idToToken.putAll( newIdToToken );
    }

    public void addToken( String name, int id ) throws NonUniqueTokenException
    {
        addToken( name, id, nameToId, idToToken );
    }

    public void addToken( Token token ) throws NonUniqueTokenException
    {
        addToken( token.name(), token.id() );
    }

    void addToken( String name, int id, Map<String,Integer> nameToIdMap, Map<Integer,TOKEN> idToTokenMap )
            throws NonUniqueTokenException
    {
        TOKEN token = newToken( name, id );
        Integer previous;
        if ( (previous = nameToIdMap.put( name, id )) != null && previous != id )
        {
            throw new NonUniqueTokenException( getClass(), name, id, previous );
        }
        idToTokenMap.put( id, token );
    }

    public void removeToken( int id )
    {
        TOKEN token = idToToken.remove( id );
        nameToId.remove( token.name() );
    }

    public int getOrCreateId( String name )
    {
        Integer id = nameToId.get( name );
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
        Integer id = nameToId.get( name );
        if ( id != null )
        {
            return id;
        }

        id = tokenCreator.getOrCreate( name );
        try
        {
            addToken( name, id );
        }
        catch ( NonUniqueTokenException e )
        {
            throw new IllegalStateException( "Newly created token should be unique.", e );
        }
        return id;
    }

    public TOKEN getTokenById( int id ) throws TokenNotFoundException
    {
        TOKEN result = getTokenByIdOrNull( id );
        if ( result == null )
        {
            throw new TokenNotFoundException( "Token for id " + id );
        }
        return result;
    }

    public TOKEN getTokenByIdOrNull( int id )
    {
        return idToToken.get( id );
    }

    public boolean hasTokenWithId( int id )
    {
        return idToToken.containsKey( id );
    }

    /** Returns the id, or {@link #NO_ID} if no token with this name exists. */
    public final int idOf( TOKEN token )
    {
        return getIdByName( token.name() );
    }

    /** Returns the id, or {@link #NO_ID} if no token with this name exists. */
    public int getIdByName( String name )
    {
        Integer id = nameToId.get( name );
        if ( id == null )
        {
            return NO_ID;
        }
        return id;
    }

    public Iterable<TOKEN> getAllTokens()
    {
        return idToToken.values();
    }

    protected abstract TOKEN newToken( String name, int id );
}
