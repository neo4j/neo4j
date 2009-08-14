/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.impl.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class AdaptiveCacheManager
{
    private static final Logger log = Logger
        .getLogger( AdaptiveCacheManager.class.getName() );

    private float decreaseRatio = 1.15f;
    private float increaseRatio = 1.1f;

    private final List<AdaptiveCacheElement> caches = 
        new LinkedList<AdaptiveCacheElement>();

    private final List<SoftLruCache<?,?>> softCaches = 
        new ArrayList<SoftLruCache<?,?>>();
    
    private AdaptiveCacheWorker workerThread;

    public synchronized void registerCache( Cache<?,?> cache, float ratio,
        int minSize )
    {
        if ( cache == null || ratio >= 1 || ratio <= 0 || minSize < 0 )
        {
            throw new IllegalArgumentException( " cache=" + cache + " ratio ="
                + ratio + " minSize=" + minSize );
        }
        
        if ( cache instanceof SoftLruCache )
        {
            softCaches.add( (SoftLruCache<?,?>) cache );
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
        if ( cache instanceof SoftLruCache )
        {
            softCaches.remove( cache );
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

    private void parseParams( Map<Object,Object> params )
    {
        if ( params == null )
        {
            return;
        }
        if ( params.containsKey( "adaptive_cache_worker_sleep_time" ) )
        {
            Object value = params.get( "adaptive_cache_worker_sleep_time" );
            int sleepTime = 3000;
            try
            {
                sleepTime = Integer.parseInt( (String) value );
            }
            catch ( NumberFormatException e )
            {
                log.warning( 
                    "Unable to parse apdaptive_cache_worker_sleep_time " + 
                    value );
            }
            workerThread.setSleepTime( sleepTime );
        }
        if ( params.containsKey( "adaptive_cache_manager_decrease_ratio" ) )
        {
            Object value = params.get( 
                "adaptive_cache_manager_decrease_ratio" );
            try
            {
                decreaseRatio = Float.parseFloat( (String) value );
            }
            catch ( NumberFormatException e )
            {
                log.warning( 
                    "Unable to parse adaptive_cache_manager_decrease_ratio " + 
                    value );
            }
            if ( decreaseRatio < 1 )
            {
                decreaseRatio = 1.0f;
            }
        }
        if ( params.containsKey( "adaptive_cache_manager_increase_ratio" ) )
        {
            Object value = params.get( 
                "adaptive_cache_manager_increase_ratio" );
            try
            {
                increaseRatio = Float.parseFloat( (String) value );
            }
            catch ( NumberFormatException e )
            {
                log.warning( 
                    "Unable to parse adaptive_cache_manager_increase_ratio " + 
                    value );
            }
            if ( increaseRatio < 1 )
            {
                increaseRatio = 1.0f;
            }
        }
    }

    public void start( Map<Object,Object> params )
    {
        workerThread = new AdaptiveCacheWorker();
        parseParams( params );
        workerThread.start();
    }

    public void stop()
    {
        workerThread.markDone();
        workerThread = null;
    }

    Collection<AdaptiveCacheElement> getCaches()
    {
        return caches;
    }

    private class AdaptiveCacheWorker extends Thread
    {
        private boolean done = false;
        private int sleepTime = 3000;

        AdaptiveCacheWorker()
        {
            super( "AdaptiveCacheWorker" );

        }

        void setSleepTime( int sleepTime )
        {
            this.sleepTime = sleepTime;
        }

        public synchronized void run()
        {
            while ( !done )
            {
                try
                {
                    adaptCaches();
                    adaptSoftCaches();
                    this.wait( sleepTime );
                }
                catch ( InterruptedException e )
                { 
                    Thread.interrupted();
                }
            }
        }

        void markDone()
        {
            done = true;
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

    public synchronized void adaptSoftCaches()
    {
        for ( SoftLruCache<?,?> cache : softCaches )
        {
            cache.pollAll();
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
        // System.out.println( "max= " + max + " total=" + total +
        // " ratio= " + ratio + " ... free=" + free );
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

        String getName()
        {
            return cache.getName();
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