package org.neo4j.impl.cache;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.concurrent.ConcurrentHashMap;

public class SoftLruCache<K,V> implements Cache<K,V>
{
    private final ConcurrentHashMap<K,SoftValue<K,V>> cache =
        new ConcurrentHashMap<K,SoftValue<K,V>>();
    
    private final SoftReferenceQueue<K,V> refQueue = 
        new SoftReferenceQueue<K,V>();
    
    private final String name;
    
    public SoftLruCache( String name )
    {
        this.name = name;
    }
    
    public void put( K key, V value )
    {
        SoftValue<K,V> ref = 
            new SoftValue<K,V>( key, value, (ReferenceQueue<V>) refQueue ); 
        cache.put( key, ref );
    }
    
    public V get( K key )
    {
        SoftReference<V> ref = cache.get( key );
        if ( ref != null )
        {
            return ref.get();
        }
        return null;
    }
    
    public V remove( K key )
    {
        SoftReference<V> ref = cache.remove( key );
        if ( ref != null )
        {
            return ref.get();
        }
        return null;
    }
    
    
    public int size()
    {
        return cache.size();
    }
    
    public void pollAll()
    {
        SoftValue cv;
        while ( ( cv = (SoftValue) refQueue.poll() ) != null )
        {
            cache.remove( cv.key );
        }
    }
    
    public void clear()
    {
        cache.clear();
    }

    public void elementCleaned( V value )
    {
    }

    public String getName()
    {
        return name;
    }

    public boolean isAdaptive()
    {
        return true;
    }

    public int maxSize()
    {
        return -1;
    }

    public void resize( int newSize )
    {
    }

    public void setAdaptiveStatus( boolean status )
    {
    }
}