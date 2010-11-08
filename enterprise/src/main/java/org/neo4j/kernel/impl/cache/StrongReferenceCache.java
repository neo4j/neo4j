package org.neo4j.kernel.impl.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StrongReferenceCache<K,V> implements Cache<K,V>
{
    private final String name;
    private final ConcurrentHashMap<K, V> cache = new ConcurrentHashMap<K, V>();

    public StrongReferenceCache( String name )
    {
        this.name = name;
    }
    
    public void clear()
    {
        cache.clear();
    }

    public void elementCleaned( V value )
    {
    }

    public V get( K key )
    {
        return cache.get( key );
    }

    public String getName()
    {
        return name;
    }

    public boolean isAdaptive()
    {
        return false;
    }

    public int maxSize()
    {
        return Integer.MAX_VALUE;
    }

    public void put( K key, V value )
    {
        cache.put( key, value );
    }

    public void putAll( Map<K, V> map )
    {
        cache.putAll( map );
    }

    public V remove( K key )
    {
        return cache.remove( key );
    }

    public void resize( int newSize )
    {
    }

    public void setAdaptiveStatus( boolean status )
    {
    }

    public int size()
    {
        return cache.size();
    }
}
