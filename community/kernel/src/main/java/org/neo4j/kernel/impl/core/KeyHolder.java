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

import java.util.Map;

import org.neo4j.kernel.impl.nioneo.store.NameData;
import org.neo4j.kernel.impl.persistence.EntityIdGenerator;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public abstract class KeyHolder<KEY> extends LifecycleAdapter
{
    private Map<String,Integer> keyToId = new CopyOnWriteHashMap<String, Integer>();
    private Map<Integer,KEY> idToKey = new CopyOnWriteHashMap<Integer, KEY>();

    private final AbstractTransactionManager transactionManager;
    protected final PersistenceManager persistenceManager;
    private final EntityIdGenerator idGenerator;
    private final KeyCreator keyCreator;

    public KeyHolder( AbstractTransactionManager transactionManager,
            PersistenceManager persistenceManager, EntityIdGenerator idGenerator,
            KeyCreator keyCreator )
    {
        this.transactionManager = transactionManager;
        this.persistenceManager = persistenceManager;
        this.idGenerator = idGenerator;
        this.keyCreator = keyCreator;
    }
    
    void addKeyEntries( NameData... keys )
    {
        for ( NameData key : keys )
            addKeyEntry( key.getName(), key.getId() );
    }
    
    protected void addKeyEntry( String key, int id )
    {
        KEY keyImpl = newKey( key, id );
        keyToId.put( key, id );
        idToKey.put( id, keyImpl );
    }
    
    void removeKeyEntry( int id )
    {
        KEY key = idToKey.remove( id );
        keyToId.remove( nameOf( key ) );
    }

    public int getOrCreateId( String key )
    {
        Integer id = keyToId.get( key );
        if ( id != null )
            return id;
        
        // Let's create it
        id = createKey( key );
        return id;
    }
    
    private synchronized int createKey( String keyName )
    {
        Integer id = keyToId.get( keyName );
        if ( id != null )
            return id;
        
        id = keyCreator.getOrCreate( transactionManager, idGenerator,
                persistenceManager, keyName );
        addKeyEntry( keyName, id );
        return id;
    }

    protected abstract String nameOf( KEY key );

    public KEY getKeyById( int id ) throws KeyNotFoundException
    {
        KEY result = getKeyByIdOrNull( id );
        if ( result == null )
            throw new KeyNotFoundException( "Key for id " + id );
        return result;
    }

    public KEY getKeyByIdOrNull( int id )
    {
        KEY result = idToKey.get( id );
        return result;
    }
    
    public boolean hasKeyById( int id )
    {
        return idToKey.containsKey( id );
    }
    
    public final int getIdByKey( KEY key ) throws KeyNotFoundException
    {
        return getIdByKeyName( nameOf( key ) );
    }
    
    public int getIdByKeyName( String keyName ) throws KeyNotFoundException
    {
        Integer id = keyToId.get( keyName );
        if ( id == null )
            throw new KeyNotFoundException( keyName );
        return id;
    }
    
    public Iterable<KEY> getAllKeys()
    {
        return idToKey.values();
    }
    
    @Override
    public void stop()
    {
        keyToId.clear();
        idToKey.clear();
    }
    
    protected abstract KEY newKey( String key, int id );
}
