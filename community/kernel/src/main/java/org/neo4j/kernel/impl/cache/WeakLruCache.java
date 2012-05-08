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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WeakLruCache<E extends EntityWithSize> extends ReferenceCache<E>
{
    private final ConcurrentHashMap<Long,WeakValue<Long,E>> cache =
        new ConcurrentHashMap<Long,WeakValue<Long,E>>();

    private final WeakReferenceQueue<Long,E> refQueue =
        new WeakReferenceQueue<Long,E>();

    private final String name;

    private final HitCounter counter = new HitCounter();

    public WeakLruCache( String name )
    {
        this.name = name;
    }

    public void put( E value )
    {
        Long key = value.getId();
        WeakValue<Long,E> ref =
            new WeakValue<Long,E>( key, value, (ReferenceQueue) refQueue );
        cache.put( key, ref );
        pollClearedValues();
    }

    public void putAll( Collection<E> entities )
    {
        Map<Long,WeakValue<Long,E>> softMap = new HashMap<Long,WeakValue<Long,E>>( entities.size() * 2 );
        for ( E entity : entities )
        {
            Long key = entity.getId();
            WeakValue<Long,E> ref =
                new WeakValue<Long,E>( key, entity, (ReferenceQueue) refQueue );
            softMap.put( key, ref );
        }
        cache.putAll( softMap );
        pollClearedValues();
    }

    public E get( long key )
    {
        WeakReference<E> ref = cache.get( key );
        if ( ref != null )
        {
            if ( ref.get() == null )
            {
                cache.remove( key );
            }
            return counter.count( ref.get() );
        }
        return counter.<E>count( null );
    }

    public E remove( long key )
    {
        WeakReference<E> ref = cache.remove( key );
        if ( ref != null )
        {
            return ref.get();
        }
        return null;
    }

    @Override
    protected void pollClearedValues()
    {
        WeakValue<Long,E> clearedValue = refQueue.safePoll();
        while ( clearedValue != null )
        {
            cache.remove( clearedValue.key );
            clearedValue = refQueue.safePoll();
        }
    }

    public long size()
    {
        return cache.size();
    }

    public void clear()
    {
        cache.clear();
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

    public String getName()
    {
        return name;
    }
}
