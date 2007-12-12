/*
 * Copyright 2002-2007 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import org.neo4j.impl.persistence.IdGenerator;
import org.neo4j.impl.persistence.PersistenceManager;
import org.neo4j.impl.transaction.NotInTransactionException;
import org.neo4j.impl.util.ArrayMap;

public class PropertyIndexManager
{
	private ArrayMap<String,List<PropertyIndex>> indexMap
		= new ArrayMap<String,List<PropertyIndex>>( 5, true, false );
	private ArrayMap<Integer,PropertyIndex> idToIndexMap
		= new ArrayMap<Integer,PropertyIndex>( 9, true, false );

	private ArrayMap<Thread,TxCommitHook> txCommitHooks = 
		new ArrayMap<Thread,TxCommitHook>( 5, true, false );

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
		TxCommitHook hook = txCommitHooks.get( Thread.currentThread() );
		if ( hook != null )
		{
			PropertyIndex index = hook.getIndex( key );
			if ( index != null )
			{
				if ( list == null )
				{
					list = new ArrayList<PropertyIndex>();
				}
				list.add( index );
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
	
	void addPropertyIndexes( RawPropertyIndex[] indexes )
	{
		for ( RawPropertyIndex rawIndex : indexes )
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
            TxCommitHook commitHook = txCommitHooks.get( 
                Thread.currentThread() );
            index = commitHook.getIndex( keyId );
            if ( index != null )
            {
                return index;
            }
			String indexString;
            indexString = persistenceManager.loadIndex( keyId );
			if ( indexString == null )
			{
				throw new NotFoundException( "Index not found [" + keyId + 
					"]" );
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
			list = new ArrayList<PropertyIndex>();
			indexMap.put( index.getKey(), list );
		}
		list.add( index );
		idToIndexMap.put( index.getKeyId(), index );
	}
	
	// concurent transactions may create duplicate keys, oh well
	PropertyIndex createPropertyIndex( String key )
	{
		TxCommitHook hook = txCommitHooks.get( Thread.currentThread() );
		if ( hook == null )
		{
			hook = new TxCommitHook();
			txCommitHooks.put( Thread.currentThread(), hook );
			try
			{
				Transaction tx = transactionManager.getTransaction();
				if ( tx == null )
				{
					throw new NotInTransactionException( 
						"Unable to create property index for " + key );
				}
				tx.registerSynchronization( hook );
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
	
	private class TxCommitHook implements Synchronization
	{
		private Map<String,PropertyIndex> createdIndexes = 
			new HashMap<String,PropertyIndex>();
        private Map<Integer,PropertyIndex> idToIndex = 
            new HashMap<Integer,PropertyIndex>();
		
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
        
		public void afterCompletion( int status )
	    {
			try
			{
			if ( status == Status.STATUS_COMMITTED )
			{
				for ( PropertyIndex index : createdIndexes.values() )
				{
					addPropertyIndex( index );
				}
			}
			txCommitHooks.remove( Thread.currentThread() );
			}
			catch ( Throwable t )
			{
				t.printStackTrace();
			}
	    }
	
		public void beforeCompletion()
	    {
	    }
	}
}