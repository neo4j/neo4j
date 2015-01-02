/**
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
package org.neo4j.kernel.impl.pagecache;

import java.io.File;
import java.io.IOException;

import org.neo4j.helpers.Settings;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.monitoring.PageCacheMonitor;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.RunnablePageCache;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.mapped_memory_page_size;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.mapped_memory_total_size;

public class LifecycledPageCache extends LifecycleAdapter implements PageCache
{
    private final PageSwapperFactory swapperFactory;
    private final JobScheduler scheduler;
    private final Config config;
    private final PageCacheMonitor monitor;

    private RunnablePageCache pageCache;
    private boolean stopped;
    private JobScheduler.JobHandle pageEvictionJobHandle;

    public LifecycledPageCache(
            PageSwapperFactory swapperFactory,
            JobScheduler scheduler,
            Config config,
            PageCacheMonitor monitor )
    {
        this.swapperFactory = swapperFactory;
        this.scheduler = scheduler;
        this.config = config;
        this.monitor = monitor;
        initialisePageCache();
    }

    private void initialisePageCache()
    {
        pageCache = new MuninnPageCache(
                swapperFactory,
                calculateMaxPages( config ),
                calculatePageSize( config ),
                monitor );
    }

    private static int calculateMaxPages( Config config )
    {
        long availableMemory = config.get( mapped_memory_total_size );
        long pageSize = config.get( mapped_memory_page_size );
        long pageCount = availableMemory / pageSize;
        return (int) Math.min( Integer.MAX_VALUE, pageCount );
    }

    private static int calculatePageSize( Config config )
    {
        return config.get( mapped_memory_page_size ).intValue();
    }

    @Override
    public synchronized void start()
    {
        if ( stopped )
        {
            initialisePageCache();
            stopped = false;
        }
        pageEvictionJobHandle = scheduler.schedule( JobScheduler.Group.pageCacheEviction, pageCache );
    }

    @Override
    public synchronized void stop() throws IOException
    {
        JobScheduler.JobHandle handle = pageEvictionJobHandle;
        if ( handle != null )
        {
            handle.cancel( true );
        }
        pageCache.close();
        stopped = true;
    }

    @Override
    public void close() throws IOException
    {
        throw new UnsupportedOperationException( "This page cache is life-cycled and cannot be directly closed." );
    }

    @Override
    public PagedFile map( File file, int filePageSize ) throws IOException
    {
        return pageCache.map( file, filePageSize );
    }

    @Override
    public void flush() throws IOException
    {
        pageCache.flush();
    }

    @Override
    public int pageSize()
    {
        return pageCache.pageSize();
    }

    @Override
    public int maxCachedPages()
    {
        return pageCache.maxCachedPages();
    }

    public void dumpConfiguration( StringLogger messagesLog )
    {
        long totalPhysicalMemMb = Settings.DirectMemoryUsage.totalPhysicalMemory() / 1024 / 1024;
        long maxVmUsageMb = Runtime.getRuntime().maxMemory() / 1024 / 1024;
        long pageCacheMb = (maxCachedPages() * pageSize()) / 1024 / 1024;
        String msg = "Physical mem: " + totalPhysicalMemMb + "MB," +
                " Heap size: " + maxVmUsageMb + "MB," +
                " Page cache size: " + pageCacheMb + "MB.";

        messagesLog.info( msg );
    }

    public PageCache unwrap()
    {
        return pageCache;
    }
}
