/*
 * Copyright 2002-2007 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.cache;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Logger;

public class AdaptiveCacheManager 
{
	private static Logger log = 
		Logger.getLogger( AdaptiveCacheManager.class.getName() );

//	private static final AdaptiveCacheManager instance = 
//		new AdaptiveCacheManager();
//	
//	
//	public static AdaptiveCacheManager getManager()
//	{
//		return instance;
//	}
	
	public AdaptiveCacheManager()
	{
		
	}
	
	private final List<AdaptiveCacheElement> caches = 
		new CopyOnWriteArrayList<AdaptiveCacheElement>();
	private AdaptiveCacheWorker workerThread;
	
	public synchronized void registerCache( Cache<?,?> cache, 
		float ratio, int minSize )
	{
		if ( cache == null || ratio >= 1 || ratio <= 0 || 
			minSize < 0 )
		{
			throw new IllegalArgumentException( 
				" cache=" + cache + " ratio =" + ratio + " minSize=" + 
				minSize );
		}
		AdaptiveCacheElement element = new AdaptiveCacheElement( 
			cache, ratio, minSize );
		int elementIndex = getAdaptiveCacheElementIndex( cache );
		if ( elementIndex == -1 )
		{
			caches.add( element );
		}
		cache.setAdaptiveStatus( true );
		log.fine( "Cache[" + cache.getName() + "] threshold=" + ratio + 
			"minSize=" + minSize + " registered." );
	}
	
	public synchronized void unregisterCache( Cache<?,?> cache )
	{
		if ( cache == null )
		{
			throw new IllegalArgumentException( "Null cache" );
		}
		int elementIndex = getAdaptiveCacheElementIndex( cache );
		if ( elementIndex != -1 )
		{
			caches.remove( elementIndex );
		}
		cache.setAdaptiveStatus( false );
		log.fine( "Cache[" + cache.getName() + "] removed." );
	}
	
	int getAdaptiveCacheElementIndex( Cache<?,?> cache )
	{
		int i = 0;
		for ( AdaptiveCacheElement element : caches )
		{
			if ( element.getCache() == cache )
			{
				return i;
			}
			i++;
		}
		return -1;
	}
	
	public void start()
	{
		workerThread = new AdaptiveCacheWorker();
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
		
		AdaptiveCacheWorker()
		{
			super( "AdaptiveCacheWorker" );
		}
		
		public void run()
		{
			while ( !done )
			{
				try 
				{
					adaptCaches();
					Thread.sleep( 3000 );
				} 
				catch (InterruptedException e) 
				{ // ok
				}
			}
		}

		private void adaptCaches()
		{
			for ( AdaptiveCacheElement element : getCaches() )
			{
				adaptCache( element.getCache() );
			}
		}
		
		void markDone()
		{
			done = true;
		}
	}
	
	public void adaptCache( Cache<?,?> cache )
	{
		if ( cache == null )
		{
			throw new IllegalArgumentException( "Null cache" );
		}
		int elementIndex = getAdaptiveCacheElementIndex( cache );
		if ( elementIndex != -1 )
		{
			 adaptCache( caches.get( elementIndex ) );
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
//		System.out.println( "max= " + max + " total=" + total + 
//			" ratio= " + ratio + " ... free=" + free );
		if ( ratio > element.getRatio() )
		{
			// decrease cache size
			// after decrease we resize again with +1000 to avoid
			// spam of this method
			Cache<?,?> cache = element.getCache();
			int newCacheSize = (int) ( cache.maxSize() / 1.15 / 
                (1 + (ratio - element.getRatio())));
			int minSize = element.minSize();
			if ( newCacheSize < minSize )
			{
				log.fine( "Cache[" + cache.getName() + 
					"] cannot decrease under " + 
					minSize + " (allocation ratio=" + allocationRatio + 
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
				cache.size() + " to " + newCacheSize + 
				" (allocation ratio=" + allocationRatio + 
				" threshold status=" + ratio + ")" );
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
			if ( cache.size() / 
					(float) cache.maxSize() < 0.9f )
			{
				return;
			}
			int newCacheSize =  (int) (cache.maxSize() * 1.1);
			log.fine( "Cache[" + cache.getName() + "] increasing from " + 
				cache.size() + " to " + newCacheSize + 
				" (allocation ratio=" + allocationRatio + 
				" threshold status=" + ratio + ")" );
			cache.resize( newCacheSize );
		}
		
	}
	
	private static class AdaptiveCacheElement
	{
		private final Cache<?,?> cache;
		private final float ratio;
		private final int minSize;
		
		AdaptiveCacheElement( Cache<?,?> cache, float ratio, 
			int minSize )
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
