/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.server.security.enterprise.auth;

import java.time.Clock;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import org.apache.shiro.cache.Cache;
import org.apache.shiro.cache.CacheException;

import org.neo4j.helpers.collection.Pair;

import static java.util.stream.Collectors.toList;

public class AuthCache<K, V> implements Cache<K,V>
{
    private final ConcurrentLinkedHashMap<K,Pair<Long,V>> cacheMap;
    private final Clock clock;
    private final long TTL;
    private long lastValidation;
    private final ExecutorService executorService;

    public AuthCache( Clock clock, long ttl, long maxCapacity )
    {
        this.clock = clock;
        this.TTL = ttl;
        this.lastValidation = clock.millis();
        this.executorService = Executors.newSingleThreadExecutor();
        cacheMap = new ConcurrentLinkedHashMap.Builder<K,Pair<Long,V>>()
                .maximumWeightedCapacity( maxCapacity )
                .build();
    }

    @Override
    public V get( K key ) throws CacheException
    {
        return get( key, ignored -> {} );
    }

    V get( K key, final Consumer<Boolean> onComplete ) throws CacheException
    {
        if ( needsValidation() )
        {
            executorService.submit( new Callable<Void>()
            {
                @Override
                public Void call() throws Exception
                {
                    onComplete.accept( validateCache() );
                    return null;
                }
            } );

        }
        Pair<Long,V> longVPair = cacheMap.get( key );
        if ( longVPair == null)
        {
            return null;
        }
        else if ( passedTTL( longVPair ) )
        {
            cacheMap.remove( key );
            return null;
        }
        return longVPair.other();
    }

    private boolean passedTTL(Pair<Long, V> value)
    {
        return value.first() + TTL < clock.millis();
    }

    /**
     * Returns true if 5% of TTL has passed since last validation.
     */
    private boolean needsValidation()
    {
        return lastValidation + TTL * 0.05 < clock.millis();
    }

    @Override
    public V put( K key, V value ) throws CacheException
    {
        Pair<Long,V> prevValue = cacheMap.put( key, Pair.of( clock.millis(), value ) );
        return prevValue == null || passedTTL( prevValue ) ? null : prevValue.other();
    }

    @Override
    public V remove( K key ) throws CacheException
    {
        Pair<Long,V> prevValue = cacheMap.remove( key );
        return prevValue == null ? null : prevValue.other();
    }

    @Override
    public void clear() throws CacheException
    {
        cacheMap.clear();
    }

    @Override
    public int size()
    {
        validateCache();
        return cacheMap.size();
    }

    @Override
    public Set<K> keys()
    {
        validateCache();
        return cacheMap.keySet();
    }

    @Override
    public Collection<V> values()
    {
        validateCache();
        return cacheMap.values().stream().map( Pair::other ).collect( toList() );
    }

    // Validates the cache by checking all values.
    private boolean validateCache()
    {
        if ( needsValidation() )
        {
            cacheMap.ascendingMap().entrySet().forEach( entry ->
            {
                if ( passedTTL( entry.getValue() ) )
                {
                    cacheMap.remove( entry.getKey() );
                }
            } );
            lastValidation = clock.millis();
            return true;
        }
        return false;
    }

    int innerSize()
    {
        return cacheMap.size();
    }
}
