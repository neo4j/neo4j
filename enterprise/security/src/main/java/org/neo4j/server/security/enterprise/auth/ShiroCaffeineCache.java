/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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

    ShiroCaffeineCache( Ticker ticker, long ttl, int maxCapacity, boolean useTTL )
    {
        this( ticker, ForkJoinPool.commonPool(), ttl, maxCapacity, useTTL );
    }

    ShiroCaffeineCache( Ticker ticker, Executor maintenanceExecutor, long ttl, int maxCapacity, boolean useTTL )
    {
        Caffeine<Object,Object> builder = Caffeine.newBuilder()
                                                  .maximumSize( maxCapacity )
                                                  .executor( maintenanceExecutor );
        if ( useTTL )
        {
            if ( ttl <= 0 )
            {
                throw new IllegalArgumentException( "TTL must be larger than zero." );
            }
            builder.ticker( ticker ).expireAfterWrite( ttl, TimeUnit.MILLISECONDS );
        }
        caffCache = builder.build();
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
        private boolean useTTL;

        Manager( Ticker ticker, long ttl, int maxCapacity, boolean useTTL )
        {
            this.ticker = ticker;
            this.ttl = ttl;
            this.maxCapacity = maxCapacity;
            this.useTTL = useTTL;
            caches = new HashMap<>();
        }

        @Override
        public <K, V> Cache<K,V> getCache( String s ) throws CacheException
        {
            //noinspection unchecked
            return (Cache<K,V>) caches.computeIfAbsent( s,
                    ignored -> useTTL && ttl <= 0 ? new NullCache() : new ShiroCaffeineCache<K,V>( ticker, ttl, maxCapacity, useTTL ) );
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
