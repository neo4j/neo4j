/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.concurrent;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks an (approximate) set of recently seen unique elements in a stream, based on a concurrent LRU implementation.
 *
 * This class is thread safe. For the common case of items that are recently seen being seen again, this class is
 * lock free. 
 *
 * @param <Type> the entry type stored
 */
public class RecentK<Type> implements Iterable<Type>
{
    private final int maxItems;

    /**
     * Source of truth - the keys in this map are the "recent set". For each value, we also track a counter for
     * the number of times we've seen it, which is used to evict older and less used values.
     */
    private final ConcurrentHashMap<Type, AtomicLong> recentItems = new ConcurrentHashMap<>();

    /**
     * @param maxItems is the size of the item set to track
     */
    public RecentK( int maxItems )
    {
        this.maxItems = maxItems;
    }

    /**
     * @param item a new item to the tracked set.
     */
    public void add( Type item )
    {
        AtomicLong counter = recentItems.get( item );
        if(counter != null)
        {
            counter.incrementAndGet();
        }
        else
        {
            // Double-checked locking ahead: Check if there is space for our item
            if( recentItems.size() >= maxItems )
            {
                // If not, synchronize and check again (this will happen if there is > maxItems in the current set)
                synchronized ( recentItems )
                {
                    // Proper check under lock, make space in the set for our new item
                    while( recentItems.size() >= maxItems )
                    {
                        removeItemWithLowestCount();
                    }

                    halveCounts();
                    recentItems.putIfAbsent( item, new AtomicLong( 1 ) );
                }
            }
            else
            {
                // There were < maxItems in the set. This is racy as multiple clients may have hit this branch
                // simultaneously. We accept going above max items here. The set will recover next time an item
                // is added, since the synchronized block above will bring the set to maxItems items again.
                recentItems.putIfAbsent( item, new AtomicLong( 1 ) );
            }
        }

    }

    /**
     * In order to give lower-and-lower priority to keys we've seen a lot in the past, but don't see much anymore,
     * we cut all key counts in half after we've run an eviction cycle.
     */
    private void halveCounts()
    {
        for ( AtomicLong count : recentItems.values() )
        {
            long prev, next;
            do {
                prev = count.get();
                next = Math.max( prev / 2, 1 );
            } while (!count.compareAndSet(prev, next));

        }
    }

    private void removeItemWithLowestCount()
    {
        Type lowestCountKey = null;
        long lowestCount = Long.MAX_VALUE;
        for ( Map.Entry<Type,AtomicLong> entry : recentItems.entrySet() )
        {
            long currentCount = entry.getValue().get();
            if( currentCount < lowestCount)
            {
                lowestCount = currentCount;
                lowestCountKey = entry.getKey();
            }
        }

        if( lowestCountKey != null )
        {
            recentItems.remove( lowestCountKey );
        }
    }

    public Set<Type> recentItems()
    {
        return recentItems.keySet();
    }

    @Override
    public Iterator<Type> iterator()
    {
        return recentItems().iterator();
    }
}
