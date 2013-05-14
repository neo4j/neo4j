/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.neo4j.kernel.impl.nioneo.store.Token;
import org.neo4j.kernel.impl.persistence.EntityIdGenerator;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public abstract class TokenHolder<TOKEN> extends LifecycleAdapter
{
    private Map<String, Integer> nameToId = new CopyOnWriteHashMap<String, Integer>();
    private Map<Integer, TOKEN> idToToken = new CopyOnWriteHashMap<Integer, TOKEN>();

    private final AbstractTransactionManager transactionManager;
    protected final PersistenceManager persistenceManager;
    private final EntityIdGenerator idGenerator;
    private final TokenCreator tokenCreator;

    public TokenHolder( AbstractTransactionManager transactionManager,
                        PersistenceManager persistenceManager, EntityIdGenerator idGenerator,
                        TokenCreator tokenCreator )
    {
        this.transactionManager = transactionManager;
        this.persistenceManager = persistenceManager;
        this.idGenerator = idGenerator;
        this.tokenCreator = tokenCreator;
    }

    void addTokens( Token... tokens )
    {
        Map<String, Integer> newNameToId = new HashMap<String, Integer>();
        Map<Integer, TOKEN> newIdToToken = new HashMap<Integer, TOKEN>();

        for ( Token token : tokens )
        {
            addToken( token.getName(), token.getId(), newNameToId, newIdToToken );
            notifyMeOfTokensAdded( token.getName(), token.getId() );
        }

        nameToId.putAll( newNameToId );
        idToToken.putAll( newIdToToken );
    }

    /**
     * Overload this if you want to know of tokens being added
     */
    protected void notifyMeOfTokensAdded( String name, int id )
    {
    }

    void addToken( String name, int id )
    {
        addToken( name, id, nameToId, idToToken );
        notifyMeOfTokensAdded( name, id );
    }

    void addToken( String name, int id, Map<String, Integer> nameToIdMap, Map<Integer, TOKEN> idToTokenMap )
    {
        TOKEN token = newToken( name, id );
        nameToIdMap.put( name, id );
        idToTokenMap.put( id, token );
    }

    void removeToken( int id )
    {
        TOKEN token = idToToken.remove( id );
        nameToId.remove( nameOf( token ) );
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

        id = tokenCreator.getOrCreate( transactionManager, idGenerator,
                persistenceManager, name );
        addToken( name, id );
        return id;
    }

    protected abstract String nameOf( TOKEN token );

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

    public final int idOf( TOKEN token ) throws TokenNotFoundException
    {
        return getIdByName( nameOf( token ) );
    }

    public int getIdByName( String name ) throws TokenNotFoundException
    {
        Integer id = nameToId.get( name );
        if ( id == null )
        {
            throw new TokenNotFoundException( name );
        }
        return id;
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
