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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import org.apache.shiro.cache.Cache;
import org.apache.shiro.cache.CacheException;
import org.apache.shiro.cache.CacheManager;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

class ShiroCaffeineCache<K, V> implements Cache<K,V>
{
    private final com.github.benmanes.caffeine.cache.Cache<K,V> cacheMap;

    ShiroCaffeineCache( Ticker ticker, long ttl, int maxCapacity )
    {
        if ( ttl <= 0 )
        {
            throw new IllegalArgumentException( "TTL must be larger than zero." );
        }
        cacheMap = Caffeine.newBuilder()
                .maximumSize( maxCapacity )
                .expireAfterWrite( ttl, TimeUnit.MILLISECONDS )
                .ticker( ticker )
                .build();
    }

    @Override
    public V get( K key ) throws CacheException
    {
        return cacheMap.getIfPresent( key );
    }

    @Override
    public V put( K key, V value ) throws CacheException
    {
        V oldValue = cacheMap.getIfPresent( key );
        cacheMap.put( key, value );
        return oldValue;
    }

    @Override
    public V remove( K key ) throws CacheException
    {
        V oldValue = cacheMap.getIfPresent( key );
        cacheMap.invalidate( key );
        return oldValue;
    }

    @Override
    public void clear() throws CacheException
    {
        cacheMap.invalidateAll();
    }

    @Override
    public int size()
    {
        return Math.toIntExact( cacheMap.estimatedSize() );
    }

    @Override
    public Set<K> keys()
    {
        return cacheMap.asMap().keySet();
    }

    @Override
    public Collection<V> values()
    {
        return cacheMap.asMap().values();
    }

    static class Manager implements CacheManager
    {
        private final Map<String,Cache<?,?>> cacheMap;
        private final Ticker ticker;
        private final long ttl;
        private final int maxCapacity;

        Manager( Ticker ticker, long ttl, int maxCapacity )
        {
            this.ticker = ticker;
            this.ttl = ttl;
            this.maxCapacity = maxCapacity;
            cacheMap = new HashMap<>();
        }

        @Override
        public <K, V> Cache<K,V> getCache( String s ) throws CacheException
        {
            if ( !cacheMap.containsKey( s ) )
            {
                cacheMap.put( s, ttl <= 0 ? new NullCache() : new ShiroCaffeineCache<K,V>( ticker, ttl, maxCapacity ) );
            }
            //noinspection unchecked
            return (Cache<K,V>) cacheMap.get( s );
        }
    }

    private static class NullCache<K, V> implements Cache<K, V>
    {

        @Override
        public V get( K key ) throws CacheException
        {
            return null;
        }

        @Override
        public V put( K key, V value ) throws CacheException
        {
            return null;
        }

        @Override
        public V remove( K key ) throws CacheException
        {
            return null;
        }

        @Override
        public void clear() throws CacheException
        {

        }

        @Override
        public int size()
        {
            return 0;
        }

        @Override
        public Set<K> keys()
        {
            return Collections.emptySet();
        }

        @Override
        public Collection<V> values()
        {
            return Collections.emptySet();
        }
    }
}
