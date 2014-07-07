/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 *
 * Exists:
 *   get from map
 *
 * Previously when it doesn't exist:
 *   tokenCreator.create
 *     record changes
 *       command execution
 *
 * Doesn't exist:
 *   tokenCreator.create( name, id )
 *     new kernel transaction
 *       change in statement
 *         commit
 *           record changes
 *             command execution
 *               add to holder
 */
public abstract class TokenHolder<TOKEN extends Token> extends LifecycleAdapter
{
    public static final int NO_ID = -1;
    private final Map<String,Integer> nameToId = new CopyOnWriteHashMap<>();
    private final Map<Integer, TOKEN> idToToken = new CopyOnWriteHashMap<>();
    private final TokenCreator tokenCreator;

    public TokenHolder( TokenCreator tokenCreator )
    {
        this.tokenCreator = tokenCreator;
    }

    public void addTokens( Token... tokens )
    {
        Map<String, Integer> newNameToId = new HashMap<>();
        Map<Integer, TOKEN> newIdToToken = new HashMap<>();

        for ( Token token : tokens )
        {
            addToken( token.name(), token.id(), newNameToId, newIdToToken );
        }

        nameToId.putAll( newNameToId );
        idToToken.putAll( newIdToToken );
    }

    public void addToken( String name, int id )
    {
        addToken( name, id, nameToId, idToToken );
    }

    public void addToken( Token token )
    {
        addToken( token.name(), token.id() );
    }

    void addToken( String name, int id, Map<String, Integer> nameToIdMap, Map<Integer, TOKEN> idToTokenMap )
    {
        TOKEN token = newToken( name, id );
        nameToIdMap.put( name, id );
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
        id = createToken( name );
        return id;
    }

    private synchronized int createToken( String name )
    {
        Integer id = nameToId.get( name );
        if ( id != null )
        {
            return id;
        }

        id = tokenCreator.getOrCreate( name );
        addToken( name, id );
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

    public TOKEN getTokenByName( String name ) throws TokenNotFoundException
    {
        Integer id = nameToId.get( name );
        if ( id == null )
        {
            throw new TokenNotFoundException( name );
        }
        return idToToken.get( id );
    }

    public TOKEN getTokenByNameOrNull( String name )
    {
        Integer id = nameToId.get( name );
        return id != null ? idToToken.get( id ) : null;
    }

    public Iterable<TOKEN> getAllTokens()
    {
        return idToToken.values();
    }

    @Override
    public void stop()
    {
        nameToId.clear();
        idToToken.clear();
    }

    protected abstract TOKEN newToken( String name, int id );
}
