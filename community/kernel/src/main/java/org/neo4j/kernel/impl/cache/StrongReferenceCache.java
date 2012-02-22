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
        return counter.count( cache.get( key ) );
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

    private final HitCounter counter = new HitCounter();

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
