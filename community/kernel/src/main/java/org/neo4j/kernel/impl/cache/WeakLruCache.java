/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.cache;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WeakLruCache<K,V> extends ReferenceCache<K,V>
{
    private final ConcurrentHashMap<K,WeakValue<K,V>> cache =
        new ConcurrentHashMap<K,WeakValue<K,V>>();

    private final WeakReferenceQueue<K,V> refQueue =
        new WeakReferenceQueue<K,V>();

    private final String name;

    public WeakLruCache( String name )
    {
        this.name = name;
    }

    public void put( K key, V value )
    {
        WeakValue<K,V> ref =
            new WeakValue<K,V>( key, value, (ReferenceQueue<V>) refQueue );
        cache.put( key, ref );
        pollClearedValues();
    }

    public void putAll( Map<K,V> map )
    {
        Map<K,WeakValue<K,V>> softMap = new HashMap<K,WeakValue<K,V>>( map.size() * 2 );
        for ( Map.Entry<K, V> entry : map.entrySet() )
        {
            WeakValue<K,V> ref =
                new WeakValue<K,V>( entry.getKey(), entry.getValue(), (ReferenceQueue<V>) refQueue );
            softMap.put( entry.getKey(), ref );
        }
        cache.putAll( softMap );
        pollClearedValues();
    }

    public V get( K key )
    {
        WeakReference<V> ref = cache.get( key );
        if ( ref != null )
        {
            if ( ref.get() == null )
            {
                cache.remove( key );
            }
            return counter.count( ref.get() );
        }
        return counter.<V>count( null );
    }

    public V remove( K key )
    {
        WeakReference<V> ref = cache.remove( key );
        if ( ref != null )
        {
            return ref.get();
        }
        return null;
    }

    @Override
    protected void pollClearedValues()
    {
        WeakValue<K,V> clearedValue = refQueue.safePoll();
        while ( clearedValue != null )
        {
            cache.remove( clearedValue.key );
            clearedValue = refQueue.safePoll();
        }
    }

    public int size()
    {
        return cache.size();
    }

    public void clear()
    {
        cache.clear();
    }

    private final HitCounter counter = HitCounter.create();

    @Override
    public long hitCount()
    {
        return counter.getHitsCount();
    }

    @Override
    public long missCount()
    {
        return counter.getMissCount();
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