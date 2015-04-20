/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class StrongReferenceCache<E extends EntityWithSizeObject> extends Cache.Adapter<E>
{
    private final ConcurrentHashMap<Long,E> cache = new ConcurrentHashMap<>();
    private final String name;
    private final HitCounter counter = new HitCounter();

    public StrongReferenceCache( String name )
    {
        this.name = name;
    }

    @Override
    public void clear()
    {
        cache.clear();
    }

    @Override
    public E get( long key )
    {
        return counter.count( cache.get( key ) );
    }

    @Override
    public String getName()
    {
        return name;
    }

    public int maxSize()
    {
        return Integer.MAX_VALUE;
    }

    @Override
    public E put( E value, boolean force )
    {
        E old = cache.put( value.getId(), value );
        return old == null ? value : old;
    }

    public void putAll( List<E> list )
    {
        for ( E entity : list )
        {
            cache.put( entity.getId(), entity );
        }
    }

    @Override
    public E remove( long key )
    {
        return cache.remove( key );
    }

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

    @Override
    public long size()
    {
        return cache.size();
    }

    @Override
    public void putAll( Collection<E> values )
    {
        for ( E entity : values )
        {
            cache.put( entity.getId(), entity );
        }
    }

    @Override
    public void updateSize( E entity, int newSize )
    {
        // do nothing
    }

    @Override
    public void printStatistics()
    {
        // do nothing
    }
}
