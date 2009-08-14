/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
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
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.cache;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Simple implementation of Least-recently-used cache.
 * 
 * The cache has a <CODE>maxSize</CODE> set and when the number of cached
 * elements exceeds that limit the least recently used element will be removed.
 */
public class LruCache<K,E> implements Cache<K,E>
{
    private final String name;
    int maxSize = 1000;
    private boolean resizing = false;
    private boolean adaptive = false;
    
    private final AdaptiveCacheManager cacheManager;

    private Map<K,E> cache = new LinkedHashMap<K,E>( 500, 0.75f, true )
    {
        protected boolean removeEldestEntry( Map.Entry<K,E> eldest )
        {
            // synchronization miss with old value on maxSize here is ok
            if ( super.size() > maxSize )
            {
                if ( isAdaptive() && !isResizing() )
                {
                    adaptCache();
                }
                else
                {
                    super.remove( eldest.getKey() );
                    elementCleaned( eldest.getValue() );
                }
            }
            return false;
        }
    };

    void adaptCache()
    {
        if ( cacheManager != null )
        {
            cacheManager.adaptCache( this );
        }
    }

    public void setMaxSize( int maxSize )
    {
        this.maxSize = maxSize;
    }

    /**
     * Creates a LRU cache. If <CODE>maxSize < 1</CODE> an
     * IllegalArgumentException is thrown.
     * 
     * @param name
     *            name of cache
     * @param maxSize
     *            maximum size of this cache
     * @param cacheManager
     *            adaptive cache manager or null if adaptive caching not needed
     */
    public LruCache( String name, int maxSize, AdaptiveCacheManager cacheManager )
    {
        this.cacheManager = cacheManager;
        if ( name == null || maxSize < 1 )
        {
            throw new IllegalArgumentException( "maxSize=" + maxSize
                + ", name=" + name );
        }
        this.name = name;
        this.maxSize = maxSize;
    }

    public String getName()
    {
        return this.name;
    }

    public synchronized void put( K key, E element )
    {
        if ( key == null || element == null )
        {
            throw new IllegalArgumentException( "key=" + key + ", element="
                + element );
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
     * will return <CODE>newMaxSize</CODE> and the entries in cache will not
     * be modified. 
     * <p>
     * If <CODE>newMaxSize</CODE> is less then <CODE>size()</CODE>
     * the cache will shrink itself removing least recently used element until
     * <CODE>size()</CODE> equals <CODE>newMaxSize</CODE>. For each element
     * removed the {@link #elementCleaned} method is invoked.
     * <p>
     * If <CODE>newMaxSize</CODE> is less then <CODE>1</CODE> an
     * {@link IllegalArgumentException} is thrown.
     * 
     * @param newMaxSize
     *            the new maximum size of the cache
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
                java.util.Iterator<Map.Entry<K,E>> itr = cache.entrySet()
                    .iterator();
                while ( itr.hasNext() && cache.size() > maxSize )
                {
                    E element = itr.next().getValue();
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

    public void elementCleaned( E element )
    {
    }

    public boolean isAdaptive()
    {
        return adaptive;
    }

    public void setAdaptiveStatus( boolean status )
    {
        this.adaptive = status;
    }
}
