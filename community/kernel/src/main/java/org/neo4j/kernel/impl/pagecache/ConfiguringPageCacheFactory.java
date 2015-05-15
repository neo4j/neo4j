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
package org.neo4j.kernel.impl.pagecache;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.mapped_memory_page_size;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_memory;

public class ConfiguringPageCacheFactory
{
    private final PageSwapperFactory swapperFactory;
    private final Config config;
    private final PageCacheTracer tracer;
    private final Log log;
    private PageCache pageCache;

    public ConfiguringPageCacheFactory(
            FileSystemAbstraction fs, Config config, PageCacheTracer tracer, Log log )
    {
        this.swapperFactory = new SingleFilePageSwapperFactory( fs );
        this.config = config;
        this.tracer = tracer;
        this.log = log;
    }

    public synchronized PageCache getOrCreatePageCache()
    {
        if ( pageCache == null )
        {
            pageCache = createPageCache();
        }
        return pageCache;
    }

    protected PageCache createPageCache()
    {
        return new MuninnPageCache(
                swapperFactory,
                calculateMaxPages( config ),
                calculatePageSize( config ),
                tracer );
    }

    public int calculateMaxPages( Config config )
    {
        long pageCacheMemory = config.get( pagecache_memory );
        long maxHeap = Runtime.getRuntime().maxMemory();
        if ( pageCacheMemory / maxHeap > 100 )
        {
            log.warn( "The memory configuration looks unbalanced. It is generally recommended to have at least " +
                      "10 KiB of heap memory, for every 1 MiB of page cache memory. The current configuration is " +
                      "allocating %s bytes for the page cache, and %s bytes for the heap.", pageCacheMemory, maxHeap );
        }
        long pageSize = config.get( mapped_memory_page_size );
        long pageCount = pageCacheMemory / pageSize;
        return (int) Math.min( Integer.MAX_VALUE - 2000, pageCount );
    }

    public int calculatePageSize( Config config )
    {
        return config.get( mapped_memory_page_size ).intValue();
    }

    public void dumpConfiguration()
    {
        long totalPhysicalMemory = totalPhysicalMemory();
        String totalPhysicalMemMb = totalPhysicalMemory == -1? "?" : "" + totalPhysicalMemory / 1024 / 1024;
        long maxVmUsageMb = Runtime.getRuntime().maxMemory() / 1024 / 1024;
        long pageCacheMb = (calculateMaxPages( config ) * calculatePageSize( config )) / 1024 / 1024;
        String msg = "Physical mem: " + totalPhysicalMemMb + " MiB," +
                     " Heap size: " + maxVmUsageMb + " MiB," +
                     " Page cache size: " + pageCacheMb + " MiB.";

        log.info( msg );
    }

    public long totalPhysicalMemory()
    {
        try
        {
            Class<?> beanClass = Thread.currentThread().getContextClassLoader().loadClass(
                    "com.sun.management.OperatingSystemMXBean" );
            Method method = beanClass.getMethod( "getTotalPhysicalMemorySize" );
            return (long) method.invoke( ManagementFactory.getOperatingSystemMXBean() );
        }
        catch ( Exception | LinkageError e )
        {
            // We tried, but at this point we actually have no idea.
            return -1;
        }
    }
}
