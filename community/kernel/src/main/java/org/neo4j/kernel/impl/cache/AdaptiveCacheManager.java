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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.DaemonThreadFactory;
import org.neo4j.kernel.Lifecycle;
import org.neo4j.kernel.configuration.Config;

public class AdaptiveCacheManager
    implements Lifecycle
{
    public static class Configuration
    {
        public static final GraphDatabaseSetting.IntegerSetting adaptive_cache_worker_sleep_time = GraphDatabaseSettings.adaptive_cache_worker_sleep_time;

        public static final GraphDatabaseSetting.FloatSetting adaptive_cache_manager_decrease_ratio = GraphDatabaseSettings.adaptive_cache_manager_decrease_ratio;
        public static final GraphDatabaseSetting.FloatSetting adaptive_cache_manager_increase_ratio = GraphDatabaseSettings.adaptive_cache_manager_increase_ratio;
    }

    private Config config;
    private ScheduledExecutorService executor;

    public AdaptiveCacheManager(Config config)
    {
        this.config = config;
    }

    private static final Logger log = Logger
        .getLogger( AdaptiveCacheManager.class.getName() );

    private float decreaseRatio;
    private float increaseRatio;

    private final List<AdaptiveCacheElement> caches = 
        new LinkedList<AdaptiveCacheElement>();

    private final List<ReferenceCache<?,?>> referenceCaches = 
        new ArrayList<ReferenceCache<?,?>>();
    
    public synchronized void registerCache( Cache<?,?> cache, float ratio,
        int minSize )
    {
        if ( cache == null || ratio >= 1 || ratio <= 0 || minSize < 0 )
        {
            throw new IllegalArgumentException( " cache=" + cache + " ratio ="
                + ratio + " minSize=" + minSize );
        }
        
        if ( cache instanceof ReferenceCache<?,?> )
        {
            referenceCaches.add( (ReferenceCache<?,?>) cache );
            return;
        }
        
        for ( AdaptiveCacheElement element : caches )
        {
            if ( element.getCache() == cache )
            {
                log.fine( "Cache[" + cache.getName() + 
                    "] already registered." );
                return;
            }
            
        }
        AdaptiveCacheElement element = new AdaptiveCacheElement( cache, ratio,
            minSize );
        caches.add( element );
        cache.setAdaptiveStatus( true );
        log.fine( "Cache[" + cache.getName() + "] threshold=" + ratio
            + "minSize=" + minSize + " registered." );
    }

    public synchronized void unregisterCache( Cache<?,?> cache )
    {
        if ( cache == null )
        {
            throw new IllegalArgumentException( "Null cache" );
        }
        if ( cache instanceof SoftLruCache<?,?> )
        {
            referenceCaches.remove( cache );
            return;
        }
        Iterator<AdaptiveCacheElement> itr = caches.iterator();
        while ( itr.hasNext() )
        {
            AdaptiveCacheElement element = itr.next();
            if ( element.getCache() == cache )
            {
                itr.remove();
                break;
            }
        }
        log.fine( "Cache[" + cache.getName() + "] removed." );
    }

    synchronized AdaptiveCacheElement getAdaptiveCacheElementIndex( 
        Cache<?,?> cache )
    {
        for ( AdaptiveCacheElement element : caches )
        {
            if ( element.getCache() == cache )
            {
                return element;
            }
        }
        return null;
    }

    @Override
    public void init()
    {
    }

    @Override
    public void start()
    {
        decreaseRatio = config.getFloat( Configuration.adaptive_cache_manager_decrease_ratio );
        increaseRatio = config.getFloat( Configuration.adaptive_cache_manager_increase_ratio );

        executor = Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("Adaptive cache manager"));
        executor.schedule( new AdaptiveCacheJob(), config.getInteger( Configuration.adaptive_cache_worker_sleep_time), TimeUnit.MILLISECONDS );
    }

    @Override
    public void stop()
    {
        executor.shutdownNow();
    }

    @Override
    public void shutdown()
    {
    }

    private class AdaptiveCacheJob
        implements Runnable
    {
        public void run()
        {
            adaptReferenceCaches();
            adaptCaches();
        }
    }
    
    public void adaptCaches()
    {
        List<AdaptiveCacheElement> copy = 
            new LinkedList<AdaptiveCacheElement>();
        synchronized ( this )
        {
            copy.addAll( caches );
        }
        for ( AdaptiveCacheElement element : copy )
        {
            adaptCache( element );
        }
    }

    public synchronized void adaptReferenceCaches()
    {
        for ( ReferenceCache<?,?> cache : referenceCaches )
        {
            cache.pollClearedValues();
        }
    }
    
    public void adaptCache( Cache<?,?> cache )
    {
        if ( cache == null )
        {
            throw new IllegalArgumentException( "Null cache" );
        }
        
        AdaptiveCacheElement element = getAdaptiveCacheElementIndex( cache );
        if ( element != null )
        {
            adaptCache( element );
        }
    }
    

    private void adaptCache( AdaptiveCacheElement element )
    {
        long max = Runtime.getRuntime().maxMemory();
        long total = Runtime.getRuntime().totalMemory();
        long free = Runtime.getRuntime().freeMemory();

        float ratio = (float) (max - free) / max;
        float allocationRatio = (float) total / max;
        if ( allocationRatio < element.getRatio() )
        {
            // allocation ratio < 1 means JVM can still increase heap size
            // we won't decrease caches until element ratio is less
            // then allocationRatio
            ratio = 0;
        }

        if ( ratio > element.getRatio() )
        {
            // decrease cache size
            // after decrease we resize again with +1000 to avoid
            // spam of this method
            Cache<?,?> cache = element.getCache();
            int newCacheSize = (int) ( cache.maxSize() / decreaseRatio / 
                (1 + (ratio - element.getRatio())) );
            int minSize = element.minSize();
            if ( newCacheSize < minSize )
            {
                log.fine( "Cache[" + cache.getName() + 
                    "] cannot decrease under " + minSize + 
                    " (allocation ratio=" + allocationRatio + 
                    " threshold status=" + ratio + ")" );
                cache.resize( minSize );
                cache.resize( minSize + 1000 );
                return;
            }
            if ( newCacheSize + 1200 > cache.size() )
            {
                if ( cache.size() - 1200 >= minSize )
                {
                    newCacheSize = cache.size() - 1200;
                }
                else
                {
                    newCacheSize = minSize;
                }
            }
            log.fine( "Cache[" + cache.getName() + "] decreasing from " + 
                cache.size() + " to " + newCacheSize + " (allocation ratio=" + 
                allocationRatio + " threshold status=" + ratio + ")" );
            if ( newCacheSize <= 1000 )
            {
                cache.clear();
            }
            else
            {
                cache.resize( newCacheSize );
            }
            cache.resize( newCacheSize + 1000 );
        }
        else
        {
            // increase cache size
            Cache<?,?> cache = element.getCache();
            if ( cache.size() / (float) cache.maxSize() < 0.9f )
            {
                return;
            }
            int newCacheSize = (int) (cache.maxSize() * increaseRatio);
            log.fine( "Cache[" + cache.getName() + "] increasing from " + 
                cache.size() + " to " + newCacheSize + " (allocation ratio=" + 
                allocationRatio + " threshold status=" + ratio + ")" );
            cache.resize( newCacheSize );
        }

    }

    private static class AdaptiveCacheElement
    {
        private final Cache<?,?> cache;
        private final float ratio;
        private final int minSize;

        AdaptiveCacheElement( Cache<?,?> cache, float ratio, int minSize )
        {
            this.cache = cache;
            this.ratio = ratio;
            this.minSize = minSize;
        }

        Cache<?,?> getCache()
        {
            return cache;
        }

        float getRatio()
        {
            return ratio;
        }

        int minSize()
        {
            return minSize;
        }
    }
}