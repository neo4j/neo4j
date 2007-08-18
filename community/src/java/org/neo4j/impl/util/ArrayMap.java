package org.neo4j.impl.util;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class ArrayMap<K,V>
{
	private ArrayEntry<K,V>[] arrayEntries;
	
	private int arrayCount = 0;
	private int toMapThreshold = 5;
	private Map<K,V> propertyMap = null;
	private boolean useThreadSafeMap = false;
	private boolean switchBackToArray = false;
	
	public ArrayMap()
	{
		arrayEntries = new ArrayEntry[toMapThreshold];
	}
	
	public ArrayMap( int mapThreshold, boolean threadSafe, 
		boolean shrinkToArray )
	{
		this.toMapThreshold = mapThreshold;
		this.useThreadSafeMap = threadSafe;
		this.switchBackToArray = shrinkToArray;
		arrayEntries = new ArrayEntry[toMapThreshold];
	}
	
	public void put( K key, V value )
	{
		for ( int i = 0; i < arrayCount; i++ )
		{
			if ( arrayEntries[i].getKey().equals( key ) )
			{
				arrayEntries[i].setNewValue( value );
				return;
			}
		}
		if ( arrayCount != -1 )
		{
			if ( arrayCount < arrayEntries.length )
			{
				arrayEntries[ arrayCount++ ] = new ArrayEntry<K,V>( key, 
					value );
			}
			else
			{
				if ( useThreadSafeMap )
				{
					propertyMap = new ConcurrentHashMap<K,V>();
				}
				else
				{
					propertyMap = new HashMap<K,V>();
				}
				for ( int i = 0; i < arrayCount; i++ )
				{
					propertyMap.put( arrayEntries[i].getKey(), 
						arrayEntries[i].getValue() );
				}
				arrayEntries = null;
				arrayCount = -1;
				propertyMap.put( key, value );
			}
		}
		else
		{
			propertyMap.put( key, value );
		}
	}
	
	public V get( K key )
	{
		for ( int i = 0; i < arrayCount; i++ )
		{
			if ( arrayEntries[i].getKey().equals( key ) )
			{
				return arrayEntries[i].getValue();
			}
		}
		if ( arrayCount == -1 )
		{
			return propertyMap.get( key );
		}
		return null;
	}
	
	public V remove( K key )
	{
		for ( int i = 0; i < arrayCount; i++ )
		{
			if ( arrayEntries[i].getKey().equals( key ) )
			{
				V removedProperty = arrayEntries[i].getValue();
				if ( useThreadSafeMap )
				{
					ArrayEntry<K,V>[] newEntries = 
						new ArrayEntry[toMapThreshold];
					System.arraycopy( arrayEntries, 0, newEntries, 0, i );
					arrayCount--;
					System.arraycopy( arrayEntries, i+1, newEntries, i, 
						arrayCount - i );
					arrayEntries = newEntries;
				}
				else
				{
					arrayCount--;
					System.arraycopy( arrayEntries, i+1, arrayEntries, i, 
						arrayCount - i );
				}
				return removedProperty;
			}
		}
		if ( arrayCount == -1 )
		{
			V value = propertyMap.remove( key );
			if ( switchBackToArray && propertyMap.size() < toMapThreshold )
			{
				arrayCount = 0;
				arrayEntries = new ArrayEntry[toMapThreshold];
				for ( Entry<K,V> entry : propertyMap.entrySet() )
				{
					arrayEntries[arrayCount++] = 
						new ArrayEntry<K,V>( entry.getKey(), entry.getValue() );
				}
			}
			return value;
		}
		return null;
	}
	
	static class ArrayEntry<K,V>
	{
		private K key;
		private V value;
		
		ArrayEntry( K key, V value )
		{
			this.key = key;
			this.value = value;
		}
		
		K getKey()
		{
			return key;
		}
		
		V getValue()
		{
			return value;
		}
		
		void setNewValue( V value )
		{
			this.value = value;
		}
	}

	public Iterable<K> keySet()
    {
	    if ( arrayCount == -1 )
	    {
	    	return propertyMap.keySet();
	    }
	    List<K> keys = new LinkedList<K>();
	    for ( int i = 0; i < arrayCount; i++ )
	    {
	    	keys.add( arrayEntries[i].getKey() );
	    }
	    return keys;
    }
	
	public Iterable<V> values()
	{
	    if ( arrayCount == -1 )
	    {
	    	return propertyMap.values();
	    }
	    List<V> values = new LinkedList<V>();
	    for ( int i = 0; i < arrayCount; i++ )
	    {
	    	values.add( arrayEntries[i].getValue() );
	    }
	    return values;
	}
	
	public int size()
	{
		if ( arrayCount != -1 )
		{
			return arrayCount;
		}
		return propertyMap.size();
	}
}
