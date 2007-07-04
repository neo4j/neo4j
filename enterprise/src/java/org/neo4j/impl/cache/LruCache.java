package org.neo4j.impl.cache;

import java.util.LinkedHashMap;
import java.util.Map;


/** 
 * Simple implementation of Least-recently-used cache. The cache 
 * has a <CODE>maxSize</CODE> set and when the number of cached elements 
 * exceeds that limit the least recently used element will be removed. 
 */
public class LruCache<K,E> extends Cache<K,E> 
{
	private String name = null;
	int maxSize = 1000;
	private boolean resizing = false;

	private Map<K,E> cache = new LinkedHashMap<K,E>( 500, 0.75f, true )
	{
		protected boolean removeEldestEntry( Map.Entry<K,E> eldest )
		{
			if ( super.size() > maxSize )
			{	
				if ( isAdaptive() && !isResizing()  )
				{
					adaptCache();
				}
				super.remove( eldest.getKey() );
				elementCleaned( eldest.getValue() );
			}
			return false;
		}
	};
	
	void adaptCache()
	{
		AdaptiveCacheManager.getManager().adaptCache( this );
	}
	
	/**
	 * Creates a LRU cache. If <CODE>maxSize < 1</CODE> an 
	 * IllegalArgumentException is thrown.
	 *
	 * @param name name of cache
	 * @param maxSize maximum size of this cache
	 */
	public LruCache( String name, int maxSize )
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
	
	/**
	 * Returns the maximum size of this cache.
	 *
	 * @return maximum size
	 */
	public int maxSize()
	{
		return maxSize;
	}

	/**
	 * Changes the max size of the cache. If <CODE>newMaxSize</CODE> is 
	 * greater then <CODE>maxSize()</CODE> next invoke to <CODE>maxSize()</CODE>
	 * will return <CODE>newMaxSize</CODE> and the entries in cache will not be 
	 * modified. If <CODE>newMaxSize</CODE> is less then <CODE>size()</CODE> 
	 * the cache will shrink itself removing least recently used element untill 
	 * <CODE>size()</CODE> equals <CODE>newMaxSize</CODE>. 
	 * For each element removed the {@link #elementCleaned} method is invoked.
	 * <p>
	 * If <CODE>newMaxSize</CODE> is less then <CODE>1</CODE> an 
	 * {@link IllegalArgumentException} is thrown.  
	 *
	 * @param newMaxSize the new maximum size of the cache
	 */
	public synchronized void resize( int newMaxSize )
	{
		resizing = true;
		try
		{
			if ( newMaxSize < 1 ) 
			{
				throw new IllegalArgumentException( "newMaxSize=" + newMaxSize );
			}
			if ( newMaxSize >= size() )
			{
				maxSize = newMaxSize;
			}
			else
			{
				maxSize = newMaxSize;
				java.util.Iterator<Map.Entry<K,E>> 
					itr = cache.entrySet().iterator();
				while ( itr.hasNext() && cache.size() > maxSize )
				{
					E element =  itr.next().getValue();
					itr.remove();
					elementCleaned( element );
				}
			}
		}
		finally
		{
			resizing = false;
		}
	}
	
	boolean isResizing()
	{
		return resizing;
	}
}	

