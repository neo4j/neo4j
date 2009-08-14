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