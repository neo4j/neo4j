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

import java.lang.ref.ReferenceQueue;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ReferenceCache<E extends EntityWithSizeObject> extends Cache.Adapter<E>
{
    final static int MAX_NUM_PUT_BEFORE_POLL = 5000;

    private final ConcurrentHashMap<Long, ReferenceWithKey<Long, E>> cache = new ConcurrentHashMap<>();
    private final ReferenceWithKeyQueue<Long, E> refQueue = new ReferenceWithKeyQueue<>();
    private final String name;

    private final HitCounter hitCounter = new HitCounter();
    private final AtomicInteger putCounter = new AtomicInteger();

    private final ReferenceWithKey.Factory referenceFactory;

    ReferenceCache( String name, ReferenceWithKey.Factory referenceFactory )
    {
        this.name = name;
        this.referenceFactory = referenceFactory;
    }

    @Override
    public E put( E value, boolean force )
    {
        Long key = value.getId();
        ReferenceWithKey<Long, E> ref = referenceFactory.<Long, E>newReference( key, value, (ReferenceQueue) refQueue );

        try
        {
            // The block below retries until successful. The reason it needs to retry is that we are racing against GC
            // collecting the weak reference, and need to account for that happening at any time.
            do
            {
                ReferenceWithKey<Long, E> previous = force ?
                        cache.put( key, ref ) :
                        cache.putIfAbsent( key, ref );

                if ( previous != null )
                {
                    E prevValue = previous.get();
                    if ( prevValue == null )
                    {
                        pollClearedValues();
                        // Re-run the loop body, re-attempting to get-or-set the reference in the cache.
                        continue;
                    }

                    return prevValue;
                }
                else
                {
                    return value;
                }
            } while ( true );
        }
        finally
        {
            recordPutAndPollIfNeeded( 1 );
        }
    }
    
    @Override
    public void putAll( Collection<E> entities )
    {
        Map<Long, ReferenceWithKey<Long, E>> softMap = new HashMap<>( entities.size() * 2 );
        for ( E entity : entities )
        {
            Long key = entity.getId();
            ReferenceWithKey<Long, E> ref = referenceFactory.newReference( key, entity, (ReferenceQueue) refQueue );
            softMap.put( key, ref );
        }
        cache.putAll( softMap );
        recordPutAndPollIfNeeded( softMap.size() );
    }

    @Override
    public E get( long key )
    {
        ReferenceWithKey<Long, E> ref = cache.get( key );
        if ( ref != null )
        {
            E value = ref.get();
            if ( value != null )
            {
                return hitCounter.count( value );
            }
            pollClearedValues();
        }
        return hitCounter.count( null );
    }

    @Override
    public E remove( long key )
    {
        ReferenceWithKey<Long, E> ref = cache.remove( key );
        if ( ref != null )
        {
            E value = ref.get();
            if ( value != null )
            {
                return value;
            }
            pollClearedValues();
        }
        return null;
    }

    @Override
    public long size()
    {
        return cache.size();
    }

    @Override
    public void clear()
    {
        cache.clear();
    }

    @Override
    public long hitCount()
    {
        return hitCounter.getHitsCount();
    }

    @Override
    public long missCount()
    {
        return hitCounter.getMissCount();
    }

    @Override
    public String getName()
    {
        return name;
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

    private void recordPutAndPollIfNeeded( int elementsCount )
    {
        int count = putCounter.addAndGet( elementsCount );
        if ( count > MAX_NUM_PUT_BEFORE_POLL )
        {
            pollClearedValues();
        }
    }

    private void pollClearedValues()
    {
        putCounter.set( 0 );
        ReferenceWithKey<Long, E> clearedValue = refQueue.safePoll();
        while ( clearedValue != null )
        {
            cache.remove( clearedValue.key() );
            clearedValue = refQueue.safePoll();
        }
    }
}
