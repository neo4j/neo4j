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

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import org.neo4j.impl.event.Event;
import org.neo4j.impl.event.EventData;
import org.neo4j.impl.event.EventManager;
import org.neo4j.impl.persistence.IdGenerator;
import org.neo4j.impl.persistence.PersistenceException;
import org.neo4j.impl.persistence.PersistenceManager;
import org.neo4j.impl.transaction.NotInTransactionException;
import org.neo4j.impl.transaction.TransactionFactory;
import org.neo4j.impl.util.ArrayMap;

// TODO: make LRU x elements
public class PropertyIndex
{
	private static final TransactionManager transactionManager = 
		TransactionFactory.getTransactionManager();
	
	private static ArrayMap<String,List<PropertyIndex>> indexMap
		= new ArrayMap<String,List<PropertyIndex>>( 5, true, false );
	private static ArrayMap<Integer,PropertyIndex> idToIndexMap
		= new ArrayMap<Integer,PropertyIndex>( 9, true, false );
	
	private static ArrayMap<Thread,TxCommitHook> txCommitHooks = 
		new ArrayMap<Thread,TxCommitHook>( 5, true, false );
	
	private static boolean hasAll = false;
	
	static void clear()
	{
		indexMap = new ArrayMap<String,List<PropertyIndex>>( 5, true, false );
		idToIndexMap = new ArrayMap<Integer,PropertyIndex>( 9, true, false );
		txCommitHooks.clear();
	}
	
	public static Iterable<PropertyIndex> index( String key )
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
					list = new LinkedList<PropertyIndex>();
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
	
	static void setHasAll( boolean status )
	{
		hasAll = status;
	}
	
	static boolean hasAll()
	{
		return hasAll;
	}
	
	public static PropertyIndex createDummyIndex( int id, String key )
	{
		return new PropertyIndex( key, id );
	}
	
	public static boolean hasIndexFor( int keyId )
	{
		return idToIndexMap.get( keyId ) != null;
	}
	
	static void addPropertyIndexes( RawPropertyIndex[] indexes )
	{
		for ( RawPropertyIndex rawIndex : indexes )
		{
			addPropertyIndex( new PropertyIndex( rawIndex.getValue(), 
				rawIndex.getKeyId() ) );
		}
	}
	
	public static PropertyIndex getIndexFor( int keyId )
	{
		PropertyIndex index = idToIndexMap.get( keyId );
		if ( index == null )
		{
			String indexString;
            try
            {
	            indexString = PersistenceManager.getManager().loadIndex( 
	            	keyId );
				if ( indexString == null )
				{
					throw new NotFoundException( "Index not found [" + keyId + 
						"]" );
				}
				index = new PropertyIndex( indexString, keyId );
				addPropertyIndex( index );
            }
            catch ( PersistenceException e )
            {
	            e.printStackTrace();
            }
		}
		return index;
	}
	
	// need synch here so we don't create multiple lists
	private static synchronized void addPropertyIndex( PropertyIndex index )
	{
		List<PropertyIndex> list = indexMap.get( index.getKey() );
		if ( list == null )
		{
			list = new LinkedList<PropertyIndex>();
			indexMap.put( index.getKey(), list );
		}
		// indexMap.put( index.getKey(), index );
		list.add( index );
		idToIndexMap.put( index.getKeyId(), index );
	}
	
//	private static void removePropertyIndex( PropertyIndex index )
//	{
//		List<PropertyIndex> list = indexMap.get( index.getKey() );
//		if ( list != null )
//		{
//			Iterator<PropertyIndex> itr = list.iterator();
//			while ( itr.hasNext() )
//			{
//				PropertyIndex element = itr.next();
//				if ( element.getKeyId() == index.getKeyId() )
//				{
//					itr.remove();
//					break;
//				}
//			}
//			if ( list.size() == 0 )
//			{
//				indexMap.remove( index.getKey() );
//			}
//		}
//		idToIndexMap.remove( index.getKeyId() );
//	}
	
	// concurent transactions may create duplicate keys, oh well
	static PropertyIndex createPropertyIndex( String key )
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
		int id = IdGenerator.getGenerator().nextId( PropertyIndex.class );
		index = new PropertyIndex( key, id );
		hook.addIndex( index );
		EventManager em = EventManager.getManager();
		EventData eventData = new EventData( new PropIndexOpData( index ) );
		if ( !em.generateProActiveEvent( Event.PROPERTY_INDEX_CREATE, 
			eventData ) )
		{
			setRollbackOnly();
			throw new CreateException( "Unable to create property index, " +
				"pro-active event failed." );
		}
		em.generateReActiveEvent( Event.PROPERTY_INDEX_CREATE, eventData );
		return index;
	}
	
	private static class TxCommitHook implements Synchronization
	{
		private Map<String,PropertyIndex> createdIndexes = 
			new HashMap<String,PropertyIndex>();
		
		void addIndex( PropertyIndex index )
		{
			assert !createdIndexes.containsKey( index.getKey() );
			createdIndexes.put( index.getKey(), index );
		}
		
		PropertyIndex getIndex( String key )
		{
			return createdIndexes.get( key );
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
	
	private final String key;
	private final int keyId;
	
	PropertyIndex( String key, int keyId )
	{
		this.key = key;
		this.keyId = keyId;
	}
	
	public String getKey()
	{
		return key;
	}
	
	@Override
	public int hashCode()
	{
		return keyId;
	}
	
	public int getKeyId()
	{
		return this.keyId;
	}
	
	@Override
	public boolean equals( Object o )
	{
		if ( o instanceof PropertyIndex )
		{
			return keyId == ((PropertyIndex ) o).getKeyId();
		}
		return false;
	}

	private static void setRollbackOnly()
	{
		try
		{
			TransactionFactory.getTransactionManager().setRollbackOnly();
		}
		catch ( javax.transaction.SystemException se )
		{
			se.printStackTrace();
		}
	}

//	public static void removeIndex( int id )
//    {
//		PropertyIndex index = idToIndexMap.remove( id );
//		if ( index != null )
//		{
//			removePropertyIndex( index );
//		}
//    }
	
	static class PropIndexOpData implements PropertyIndexOperationEventData
	{
		private final PropertyIndex index;
		
		PropIndexOpData( PropertyIndex index )
		{
			this.index = index;
		}
		
		public PropertyIndex getIndex()
        {
			return index;
        }

		public Object getEntity()
        {
			return index;
        }
	}
}
