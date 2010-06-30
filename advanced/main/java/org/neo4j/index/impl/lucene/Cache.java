package org.neo4j.index.impl.lucene;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.cache.LruCache;

public class Cache
{
    private final Map<IndexIdentifier, Map<String,LruCache<String,Collection<Long>>>> caching = 
            Collections.synchronizedMap( 
                    new HashMap<IndexIdentifier, Map<String,LruCache<String,Collection<Long>>>>() );
    
    public void setCapacity( IndexIdentifier identifier, String key, int size )
    {
        Map<String, LruCache<String, Collection<Long>>> map = caching.get( identifier );
        if ( map == null )
        {
            map = new HashMap<String, LruCache<String,Collection<Long>>>();
            caching.put( identifier, map );
        }
        map.put( key, new LruCache<String, Collection<Long>>( key, size, null ) );
    }
    
    public LruCache<String, Collection<Long>> get( IndexIdentifier identifier, String key )
    {
        Map<String, LruCache<String, Collection<Long>>> map = caching.get( identifier );
        return map != null ? map.get( key ) : null;
    }
    
    public void disable( IndexIdentifier identifier, String key )
    {
        Map<String, LruCache<String, Collection<Long>>> map = caching.get( identifier );
        if ( map != null )
        {
            map.remove( key );
        }
    }
    
    public void disable( IndexIdentifier identifier )
    {
        Map<String, LruCache<String, Collection<Long>>> map = caching.get( identifier );
        if ( map != null )
        {
            map.clear();
        }
    }
}
