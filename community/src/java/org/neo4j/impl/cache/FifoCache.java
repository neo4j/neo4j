package org.neo4j.impl.cache;

import java.util.LinkedHashMap;
import java.util.Map;


/** 
 * Simple implementation of First-In-First-Out. The cache 
 * has a <CODE>maxSize</CODE> set and when the number of cached elements 
 * exceeds that limit the oldest inserted element will be removed. 
 */
public class FifoCache<K,E> extends Cache<K,E> 
{
	private String name = null;
	private int maxSize = 1000;

	private Map<K,E> cache = new LinkedHashMap<K,E>()
	{
		protected boolean removeEldestEntry( Map.Entry<K,E> eldest )
		{
			if ( this.size() > maxSize )
			{	
				remove( eldest.getKey() );
				elementCleaned( eldest.getValue() );
			}
			return false;
		}
	};
	
	/**
	 * Creates a FIFO cache. If <CODE>maxSize < 1</CODE> an 
	 * IllegalArgumentException is thrown.
	 *
	 * @param name name of cache
	 * @param maxSize maximum size of this cache
	 */
	public FifoCache( String name, int maxSize )
	{
		if ( name == null || maxSize < 1 )
		{
			throw new IllegalArgumentException( "maxSize=" + maxSize + 
				", name=" + name );
		}
		this.name = name;
		this.maxSize = maxSize;
	}
	
	public String getName()
	{
		return this.name;
	}
	
	public synchronized void add( K key, E element )
	{
		if ( key == null || element == null )
		{
			throw new IllegalArgumentException( "key=" + key + 
				", elmenet=" + element );
		}
		cache.put( key, element );
	}
	
	public synchronized E remove( K key )
	{
		if ( key == null )
		{
			throw new IllegalArgumentException( "Null parameter" );
		}
		return cache.remove( key );
	}
	
	public synchronized E get( K key )
	{
		if ( key == null )
		{
			throw new IllegalArgumentException();
		}
		return cache.get( key );
	}
	
	public synchronized void clear()
	{
		cache.clear();
	}
	
	public synchronized int size()
	{
		return cache.size();
	}
	
	public int maxSize()
	{
		return maxSize;
	}

	public synchronized void resize( int newMaxSize )
	{
		if ( newMaxSize < 1 ) 
		{
			throw new IllegalArgumentException( "newMaxSize=" + newMaxSize );
		}
		if ( newMaxSize > maxSize )
		{
			maxSize = newMaxSize;
		}
		else
		{
			maxSize = newMaxSize;
			java.util.Iterator<Map.Entry<K,E>> 
				itr = cache.entrySet().iterator();
			cache = new LinkedHashMap<K,E>()
			{
				protected boolean removeEldestEntry( Map.Entry<K,E> eldest )
				{
					if ( this.size() > maxSize )
					{	
						remove( eldest.getKey() );
						elementCleaned( eldest.getValue() );
					}
					return false;
				}
			};
			
			while ( itr.hasNext() )
			{
				Map.Entry<K,E> mapEntry = itr.next();
				cache.put( mapEntry.getKey(), mapEntry.getValue() );
			}
		}
	}
}	

