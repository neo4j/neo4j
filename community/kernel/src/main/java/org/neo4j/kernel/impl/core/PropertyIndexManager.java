/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static java.lang.System.arraycopy;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.kernel.impl.nioneo.store.NameData;
import org.neo4j.kernel.impl.persistence.EntityIdGenerator;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class PropertyIndexManager
    extends LifecycleAdapter
{
    private static final PropertyIndex[] EMPTY_PROPERTY_INDEXES = new PropertyIndex[0];
    
    private Map<String, PropertyIndex[]> indexMap = new CopyOnWriteHashMap<String, PropertyIndex[]>();
    private Map<Integer, PropertyIndex> idToIndexMap = new CopyOnWriteHashMap<Integer, PropertyIndex>();

    private final PersistenceManager persistenceManager;
    private final EntityIdGenerator idGenerator;

    private boolean hasAll = false;

    public PropertyIndexManager( PersistenceManager persistenceManager, EntityIdGenerator idGenerator )
    {
        this.persistenceManager = persistenceManager;
        this.idGenerator = idGenerator;
    }

    @Override
    public void stop()
    {
        indexMap = new ConcurrentHashMap<String, PropertyIndex[]>();
        idToIndexMap = new ConcurrentHashMap<Integer, PropertyIndex>();
    }

    @Override
    public void shutdown()
    {
    }

    public PropertyIndex[] index( String key, TransactionState state )
    {
        PropertyIndex[] existing = null;
        if ( key != null )
        {
            existing = indexMap.get( key );
            if ( state != null )
            {
                PropertyIndex[] fullList;
                PropertyIndex added = state.getIndex( key );
                if ( added != null )
                {
                    if ( existing != null )
                    {
                        fullList = new PropertyIndex[existing.length+1];
                        arraycopy( existing, 0, fullList, 0, existing.length );
                    }
                    else
                        fullList = new PropertyIndex[1];
                    fullList[fullList.length-1] = added;
                    return fullList;
                }
            }
        }
        if ( existing == null )
        {
            existing = EMPTY_PROPERTY_INDEXES;
        }
        return existing;
    }

    void setHasAll( boolean status )
    {
        hasAll = status;
    }

    boolean hasAll()
    {
        return hasAll;
    }

    public boolean hasIndexFor( int keyId )
    {
        return idToIndexMap.get( keyId ) != null;
    }

    void addPropertyIndexes( NameData[] indexes )
    {
        for ( NameData rawIndex : indexes )
        {
            addPropertyIndex( new PropertyIndex( rawIndex.getName(),
                rawIndex.getId() ) );
        }
    }

    void addPropertyIndex( NameData rawIndex )
    {
        addPropertyIndex( new PropertyIndex( rawIndex.getName(),
            rawIndex.getId() ) );
    }

    public PropertyIndex getIndexFor( int keyId, TransactionState state )
    {
        PropertyIndex index = idToIndexMap.get( keyId );
        if ( index == null )
        {
            if ( state != null )
            {
                PropertyIndex added = state.getIndex( keyId );
                if ( added != null )
                    return added;
            }
            String indexString;
            indexString = persistenceManager.loadIndex( keyId );
            if ( indexString == null )
            {
                throw new NotFoundException( "Index not found [" + keyId + "]" );
            }
            index = new PropertyIndex( indexString, keyId );
            addPropertyIndex( index );
        }
        return index;
    }

    // need synch here so we don't create multiple lists
    synchronized void addPropertyIndex( PropertyIndex index )
    {
        PropertyIndex[] list = indexMap.get( index.getKey() );
        if ( list == null )
        {
            list = new PropertyIndex[] { index };
        }
        else
        {
            PropertyIndex[] extendedList = new PropertyIndex[list.length+1];
            arraycopy( list, 0, extendedList, 0, list.length );
            extendedList[list.length] = index;
            list = extendedList;
        }
        indexMap.put( index.getKey(), list );
        idToIndexMap.put( index.getKeyId(), index );
    }
    
    // concurrent transactions may create duplicate keys, oh well
    PropertyIndex createPropertyIndex( String key, TransactionState state )
    {
        if ( state == null )
        {
            throw new NotInTransactionException(
                "Unable to create property index for " + key );
        }
        PropertyIndex index = state.getIndex( key );
        if ( index != null )
        {
            return index;
        }
        int id = (int) idGenerator.nextId( PropertyIndex.class );
        index = new PropertyIndex( key, id );
        state.addIndex( index );
        persistenceManager.createPropertyIndex( key, id );
        return index;
    }
}
