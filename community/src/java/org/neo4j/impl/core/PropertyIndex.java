package org.neo4j.impl.core;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
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
	
	private static boolean hasAll = false;
	
	static void clear()
	{
		indexMap = new ArrayMap<String,List<PropertyIndex>>( 5, true, false );
		idToIndexMap = new ArrayMap<Integer,PropertyIndex>( 9, true, false );
	}
	
	public static Iterable<PropertyIndex> index( String key )
	{
//		if ( key == null )
//		{
//			throw new IllegalArgumentException( "null key" );
//		}
		List<PropertyIndex> list = indexMap.get( key );
		if ( list != null )
		{
			return list;
		}
		return Collections.EMPTY_LIST;
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
		int id = IdGenerator.getGenerator().nextId( PropertyIndex.class );
		PropertyIndex index = new PropertyIndex( key, id );

		EventManager em = EventManager.getManager();
		EventData eventData = new EventData( new PropIndexOpData( index ) );
		if ( !em.generateProActiveEvent( Event.PROPERTY_INDEX_CREATE, 
			eventData ) )
		{
			setRollbackOnly();
			throw new CreateException( "Unable to create property index, " +
				"pro-active event failed." );
		}
		Transaction tx = null;
		try
		{
			tx = transactionManager.getTransaction();
			if ( tx == null )
			{
				throw new NotInTransactionException( 
					"Unable to create property index for " + 
					index.getKey() );
			}
			
			tx.registerSynchronization( new TxCommitHook( index ) );
		}
		catch ( javax.transaction.SystemException e )
		{
			throw new NotInTransactionException( e );
		}
		catch ( Exception e )
		{
			throw new NotInTransactionException( e );
		}
		em.generateReActiveEvent( Event.PROPERTY_INDEX_CREATE, eventData );
		return index;
	}
	
	private static class TxCommitHook implements Synchronization
	{
		private PropertyIndex createdIndex;
		
		TxCommitHook( PropertyIndex indexToCreate )
		{
			this.createdIndex = indexToCreate;
		}
		
		public void afterCompletion( int status )
        {
			if ( status == Status.STATUS_COMMITTED )
			{
				addPropertyIndex( createdIndex );
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
	
//	public char[] getChars()
//	{
//		if ( chars == null )
//		{
//			int keyLength = key.length();
//			chars = new char[ keyLength ];
//			key.getChars( 0, keyLength, chars, 0 );
//		}
//		return chars;
//	}
	
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
