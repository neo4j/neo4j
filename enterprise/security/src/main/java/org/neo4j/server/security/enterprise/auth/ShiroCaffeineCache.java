/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

class ShiroCaffeineCache<K, V> implements Cache<K,V>
{
    private final com.github.benmanes.caffeine.cache.Cache<K,V> caffCache;

    ShiroCaffeineCache( Ticker ticker, long ttl, int maxCapacity )
    {
        this( ticker, ForkJoinPool.commonPool(), ttl, maxCapacity );
    }

    ShiroCaffeineCache( Ticker ticker, Executor maintenanceExecutor, long ttl, int maxCapacity )
    {
        if ( ttl <= 0 )
        {
            throw new IllegalArgumentException( "TTL must be larger than zero." );
        }
        caffCache = Caffeine.newBuilder()
                .maximumSize( maxCapacity )
                .expireAfterWrite( ttl, TimeUnit.MILLISECONDS )
                .executor( maintenanceExecutor )
                .ticker( ticker )
                .build();
    }

    @Override
    public V get( K key ) throws CacheException
    {
        return caffCache.getIfPresent( key );
    }

    @Override
    public V put( K key, V value ) throws CacheException
    {
        return caffCache.asMap().put( key, value );
    }

    @Override
    public V remove( K key ) throws CacheException
    {
        return caffCache.asMap().remove( key );
    }

    @Override
    public void clear() throws CacheException
    {
        caffCache.invalidateAll();
    }

    @Override
    public int size()
    {
        return caffCache.asMap().size();
    }

    @Override
    public Set<K> keys()
    {
        return caffCache.asMap().keySet();
    }

    @Override
    public Collection<V> values()
    {
        return caffCache.asMap().values();
    }

    static class Manager implements CacheManager
    {
        private final Map<String,Cache<?,?>> caches;
        private final Ticker ticker;
        private final long ttl;
        private final int maxCapacity;

        Manager( Ticker ticker, long ttl, int maxCapacity )
        {
            this.ticker = ticker;
            this.ttl = ttl;
            this.maxCapacity = maxCapacity;
            caches = new HashMap<>();
        }

        @Override
        public <K, V> Cache<K,V> getCache( String s ) throws CacheException
        {
            //noinspection unchecked
            return (Cache<K,V>) caches.computeIfAbsent( s, ignored -> ttl <= 0 ?
                    new NullCache() :
                    new ShiroCaffeineCache<K,V>( ticker, ttl, maxCapacity ) );
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
