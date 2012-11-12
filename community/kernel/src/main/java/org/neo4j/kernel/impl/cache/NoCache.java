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
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public class NoCache<K,V> implements Cache<K,V>
{
    private final String name;
    private volatile long misses;
    private static final AtomicLongFieldUpdater<NoCache> MISSES = AtomicLongFieldUpdater.newUpdater( NoCache.class,
            "misses" );

    public NoCache( String name )
    {
        this.name = name;
    }

    public void put( K key, V value )
    {
    }

    public void putAll( Map<K,V> map )
    {
    }

    public V get( K key )
    {
        MISSES.incrementAndGet( this );
        return null;
    }

    public V remove( K key )
    {
        return null;
    }

    @Override
    public long hitCount()
    {
        return 0;
    }

    @Override
    public long missCount()
    {
        return misses;
    }

    public int size()
    {
        return 0;
    }

    public void clear()
    {
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
