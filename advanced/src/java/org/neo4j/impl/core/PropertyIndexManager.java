/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.impl.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.neo4j.api.core.NotFoundException;
import org.neo4j.api.core.NotInTransactionException;
import org.neo4j.impl.nioneo.store.PropertyIndexData;
import org.neo4j.impl.persistence.IdGenerator;
import org.neo4j.impl.persistence.PersistenceManager;
import org.neo4j.impl.util.ArrayMap;

public class PropertyIndexManager
{
    private ArrayMap<String,List<PropertyIndex>> indexMap = 
        new ArrayMap<String,List<PropertyIndex>>( 5, true, false );
    private ArrayMap<Integer,PropertyIndex> idToIndexMap = 
        new ArrayMap<Integer,PropertyIndex>( 9, true, false );

    private ArrayMap<Transaction,TxCommitHook> txCommitHooks = 
        new ArrayMap<Transaction,TxCommitHook>( 5, true, false );

    private final TransactionManager transactionManager;
    private final PersistenceManager persistenceManager;
    private final IdGenerator idGenerator;

    private boolean hasAll = false;

    PropertyIndexManager( TransactionManager transactionManager,
        PersistenceManager persistenceManager, IdGenerator idGenerator )
    {
        this.transactionManager = transactionManager;
        this.persistenceManager = persistenceManager;
        this.idGenerator = idGenerator;
    }

    void clear()
    {
        indexMap = new ArrayMap<String,List<PropertyIndex>>( 5, true, false );
        idToIndexMap = new ArrayMap<Integer,PropertyIndex>( 9, true, false );
        txCommitHooks.clear();
    }

    public Iterable<PropertyIndex> index( String key )
    {
        List<PropertyIndex> list = indexMap.get( key );
        TxCommitHook hook = txCommitHooks.get( getTransaction() );
        if ( hook != null )
        {
            PropertyIndex index = hook.getIndex( key );
            if ( index != null )
            {
                List<PropertyIndex> added = new ArrayList<PropertyIndex>();
                if ( list != null )
                {
                    added.addAll( list );
                }
                added.add( index );
                return added;
            }
        }
        if ( list == null )
        {
            list = Collections.emptyList();
        }
        return list;
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

    void addPropertyIndexes( PropertyIndexData[] indexes )
    {
        for ( PropertyIndexData rawIndex : indexes )
        {
            addPropertyIndex( new PropertyIndex( rawIndex.getValue(), 
                rawIndex.getKeyId() ) );
        }
    }

    public PropertyIndex getIndexFor( int keyId )
    {
        PropertyIndex index = idToIndexMap.get( keyId );
        if ( index == null )
        {
            TxCommitHook commitHook = txCommitHooks.get( getTransaction() );
            if ( commitHook != null )
            {
                index = commitHook.getIndex( keyId );
                if ( index != null )
                {
                    return index;
                }
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
    private synchronized void addPropertyIndex( PropertyIndex index )
    {
        List<PropertyIndex> list = indexMap.get( index.getKey() );
        if ( list == null )
        {
            list = new CopyOnWriteArrayList<PropertyIndex>();
            indexMap.put( index.getKey(), list );
        }
        list.add( index );
        idToIndexMap.put( index.getKeyId(), index );
    }

    private Transaction getTransaction()
    {
        try
        {
            return transactionManager.getTransaction();
        }
        catch ( javax.transaction.SystemException e )
        {
            throw new NotInTransactionException( e );
        }
        catch ( Exception e )
        {
            throw new NotInTransactionException( e );
        }
    }

    // concurent transactions may create duplicate keys, oh well
    PropertyIndex createPropertyIndex( String key )
    {
        Transaction tx = getTransaction();
        if ( tx == null )
        {
            throw new NotInTransactionException(
                "Unable to create property index for " + key );
        }
        TxCommitHook hook = txCommitHooks.get( tx );
        if ( hook == null )
        {
            hook = new TxCommitHook( tx );
            txCommitHooks.put( tx, hook );
        }
        PropertyIndex index = hook.getIndex( key );
        if ( index != null )
        {
            return index;
        }
        int id = idGenerator.nextId( PropertyIndex.class );
        index = new PropertyIndex( key, id );
        hook.addIndex( index );
        persistenceManager.createPropertyIndex( key, id );
        return index;
    }

    void setRollbackOnly()
    {
        try
        {
            transactionManager.setRollbackOnly();
        }
        catch ( javax.transaction.SystemException se )
        {
            se.printStackTrace();
        }
    }
    
    void commit( Transaction tx )
    {
        if ( tx != null )
        {
            TxCommitHook hook = txCommitHooks.remove( tx );
            if ( hook != null )
            {
                for ( PropertyIndex index : hook.getAddedPropertyIndexes() )
                {
                    addPropertyIndex( index );
                }
            }
        }
    }
    
    void rollback( Transaction tx )
    {
        txCommitHooks.remove( tx );
    }

    private class TxCommitHook
    {
        private Map<String,PropertyIndex> createdIndexes = 
            new HashMap<String,PropertyIndex>();
        private Map<Integer,PropertyIndex> idToIndex = 
            new HashMap<Integer,PropertyIndex>();

        private final Transaction tx;

        TxCommitHook( Transaction tx )
        {
            this.tx = tx;
        }

        void addIndex( PropertyIndex index )
        {
            assert !createdIndexes.containsKey( index.getKey() );
            createdIndexes.put( index.getKey(), index );
            idToIndex.put( index.getKeyId(), index );
        }

        PropertyIndex getIndex( String key )
        {
            return createdIndexes.get( key );
        }

        PropertyIndex getIndex( int keyId )
        {
            return idToIndex.get( keyId );
        }

        Iterable<PropertyIndex> getAddedPropertyIndexes()
        {
            return createdIndexes.values();
        }
    }
}