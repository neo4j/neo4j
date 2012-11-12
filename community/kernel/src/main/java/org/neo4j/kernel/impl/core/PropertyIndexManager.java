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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.impl.nioneo.store.NameData;
import org.neo4j.kernel.impl.persistence.EntityIdGenerator;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.lifecycle.Lifecycle;

public class PropertyIndexManager
    implements Lifecycle
{
    private ConcurrentHashMap<String, List<PropertyIndex>> indexMap = new ConcurrentHashMap<String, List<PropertyIndex>>();
    private ConcurrentHashMap<Integer, PropertyIndex> idToIndexMap = new ConcurrentHashMap<Integer, PropertyIndex>();

    private ConcurrentHashMap<Transaction, TxCommitHook> txCommitHooks = new ConcurrentHashMap<Transaction, TxCommitHook>();

    private final TransactionManager transactionManager;
    private final PersistenceManager persistenceManager;
    private final EntityIdGenerator idGenerator;

    private boolean hasAll = false;

    public PropertyIndexManager( TransactionManager transactionManager,
        PersistenceManager persistenceManager, EntityIdGenerator idGenerator )
    {
        this.transactionManager = transactionManager;
        this.persistenceManager = persistenceManager;
        this.idGenerator = idGenerator;
    }

    @Override
    public void init()
    {
    }

    @Override
    public void start()
    {
    }

    @Override
    public void stop()
    {
        indexMap = new ConcurrentHashMap<String, List<PropertyIndex>>();
        idToIndexMap = new ConcurrentHashMap<Integer, PropertyIndex>();
        txCommitHooks.clear();
    }

    @Override
    public void shutdown()
    {
    }

    public Iterable<PropertyIndex> index( String key )
    {
        List<PropertyIndex> list = null;
        if ( key != null )
        {
            list = indexMap.get( key );
            Transaction tx = getTransaction();
            if ( tx != null )
            {
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

    public PropertyIndex getIndexFor( int keyId )
    {
        PropertyIndex index = idToIndexMap.get( keyId );
        if ( index == null )
        {
            Transaction tx = getTransaction();
            if ( tx != null )
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
        catch ( Exception e )
        {
            throw new TransactionFailureException(
                "Unable to get transaction.", e );
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
            hook = new TxCommitHook();
            txCommitHooks.put( tx, hook );
        }
        PropertyIndex index = hook.getIndex( key );
        if ( index != null )
        {
            return index;
        }
        int id = (int) idGenerator.nextId( PropertyIndex.class );
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

    private static class TxCommitHook
    {
        private Map<String,PropertyIndex> createdIndexes =
            new HashMap<String,PropertyIndex>();
        private Map<Integer,PropertyIndex> idToIndex =
            new HashMap<Integer,PropertyIndex>();


        TxCommitHook()
        {
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