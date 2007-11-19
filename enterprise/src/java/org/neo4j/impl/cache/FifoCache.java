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
	private final String name;
	int maxSize = 1000;

	private Map<K,E> cache = new LinkedHashMap<K,E>()
	{
		protected boolean removeEldestEntry( Map.Entry<K,E> eldest )
		{
			if ( super.size() > maxSize )
			{	
				super.remove( eldest.getKey() );
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
					if ( super.size() > maxSize )
					{	
						super.remove( eldest.getKey() );
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

