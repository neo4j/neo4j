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

/**
 * Cache that know itself how to load entities into the cache when requested.
 *
 * @param <E> type of entity objects in the cache.
 */
public class AutoLoadingCache<E extends EntityWithSizeObject> extends Cache.Adapter<E>
{
    private final Cache<E> actual;
    private final Loader<E> loader;

    public interface Loader<E>
    {
        /**
         * Load the entity, or null if no entity exists.
         */
        E loadById( long id );
    }

    public AutoLoadingCache( Cache<E> actual, Loader<E> loader )
    {
        this.loader = loader;
        this.actual = actual;
    }

    @Override
    public String getName()
    {
        return actual.getName();
    }

    @Override
    public E put( E value, boolean force )
    {
        return actual.put( value, force );
    }

    @Override
    public E remove( long key )
    {
        return actual.remove( key );
    }

    @Override
    public E get( long key )
    {
        E result = actual.get( key );
        if ( result != null )
        {
            return result;
        }

        /* MP/JS | A note about locking:
         * Previously this block of code below was wrapped in a lock, striped on the key where there were
         * an arbitrary number of stripes. The lock would prevent multiple threads to load the same entity
         * at the same time, or more specifically prevent multiple threads to put two versions of the same entity
         * into the cache at the same time.
         *   So without the lock there can be thread T1 loading an entity into an entity object E1 at the same time
         * as thread T2 loading the same entity into another entity object E2. E1 and E2 represent the same entity E.
         * This race would have one of the threads win and have its version put in the cache last,
         * the other overwritten. Consider the similarities of that race with cache eviction coming into play,
         * where T1 loads E1 and puts it in cache and returns it. After that and before T2 comes in
         * and wants that same entity there has been an eviction of E1. T2 would then load E2 and
         * put into cache resulting in two "live" versions of E as well. It would seem that having the lock would
         * reduce the chance for this happening, but not prevent it.
         *   There doesn't seem to be any reason as to why having two versions of the same entity object would cause
         * problems (keep in mind that there will only be one version in the cache). Also, the overhead of
         * locking grows with the number of threads/cores to eventually become a bottle neck.
         *
         * Based on that the locking was removed. */
        result = loader.loadById( key );
        if ( result == null )
        {
            return null;
        }
        return actual.put( result );
    }

    public E getIfCached( long key )
    {
        return actual.get( key );
    }

    @Override
    public void clear()
    {
        actual.clear();
    }

    @Override
    public long size()
    {
        return actual.size();
    }

    @Override
    public void putAll( Collection values )
    {
        actual.putAll( values );
    }

    @Override
    public long hitCount()
    {
        return actual.hitCount();
    }

    @Override
    public long missCount()
    {
        return actual.missCount();
    }

    @Override
    public void updateSize( E entity, int newSize )
    {
        actual.updateSize( entity, newSize );
    }

    @Override
    public void printStatistics()
    {
        actual.printStatistics();
    }
}
