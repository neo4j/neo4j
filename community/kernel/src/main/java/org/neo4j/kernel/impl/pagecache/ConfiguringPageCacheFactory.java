/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.helpers.Service;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.mem.MemoryAllocator;
import org.neo4j.io.os.OsBeanUtil;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.memory.GlobalMemoryTracker;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.mapped_memory_page_size;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_swapper;
import static org.neo4j.kernel.configuration.Settings.BYTES;

public class ConfiguringPageCacheFactory
{
    private PageSwapperFactory swapperFactory;
    private final FileSystemAbstraction fs;
    private final Config config;
    private final PageCacheTracer pageCacheTracer;
    private final Log log;
    private final VersionContextSupplier versionContextSupplier;
    private PageCache pageCache;
    private PageCursorTracerSupplier pageCursorTracerSupplier;

    /**
     * Construct configuring page cache factory
     * @param fs fileSystem file system that page cache will be based on
     * @param config page swapper configuration
     * @param pageCacheTracer global page cache tracer
     * @param pageCursorTracerSupplier supplier of thread local (transaction local) page cursor tracer that will provide
     * thread local page cache statistics
     * @param log page cache factory log
     * @param versionContextSupplier cursor context factory
     */
    public ConfiguringPageCacheFactory( FileSystemAbstraction fs, Config config, PageCacheTracer pageCacheTracer,
            PageCursorTracerSupplier pageCursorTracerSupplier, Log log,
            VersionContextSupplier versionContextSupplier )
    {
        this.fs = fs;
        this.versionContextSupplier = versionContextSupplier;
        this.config = config;
        this.pageCacheTracer = pageCacheTracer;
        this.log = log;
        this.pageCursorTracerSupplier = pageCursorTracerSupplier;
    }

    public synchronized PageCache getOrCreatePageCache()
    {
        if ( pageCache == null )
        {
            this.swapperFactory = createAndConfigureSwapperFactory( fs, config, log );
            this.pageCache = createPageCache();
        }
        return pageCache;
    }

    protected PageCache createPageCache()
    {
        checkPageSize( config );
        MemoryAllocator memoryAllocator = buildMemoryAllocator( config );
        return new MuninnPageCache( swapperFactory, memoryAllocator, pageCacheTracer, pageCursorTracerSupplier,
                versionContextSupplier );
    }

    private MemoryAllocator buildMemoryAllocator( Config config )
    {
        String pageCacheMemorySetting = config.get( pagecache_memory );
        if ( pageCacheMemorySetting == null )
        {
            long heuristic = defaultHeuristicPageCacheMemory();
            log.warn( "The " + pagecache_memory.name() + " setting has not been configured. It is recommended that this " +
                      "setting is always explicitly configured, to ensure the system has a balanced configuration. " +
                      "Until then, a computed heuristic value of " + heuristic + " bytes will be used instead. " +
                      "Run `neo4j-admin memrec` for memory configuration suggestions." );
            pageCacheMemorySetting = "" + heuristic;
        }

        return MemoryAllocator.createAllocator( pageCacheMemorySetting, GlobalMemoryTracker.INSTANCE );
    }

    public static long defaultHeuristicPageCacheMemory()
    {
        // First check if we have a default override...
        String defaultMemoryOverride = System.getProperty( "dbms.pagecache.memory.default.override" );
        if ( defaultMemoryOverride != null )
        {
            return BYTES.apply( defaultMemoryOverride );
        }

        double ratioOfFreeMem = 0.50;
        String defaultMemoryRatioOverride = System.getProperty( "dbms.pagecache.memory.ratio.default.override" );
        if ( defaultMemoryRatioOverride != null )
        {
            ratioOfFreeMem = Double.parseDouble( defaultMemoryRatioOverride );
        }

        // Try to compute (RAM - maxheap) * 0.50 if we can get reliable numbers...
        long maxHeapMemory = Runtime.getRuntime().maxMemory();
        if ( 0 < maxHeapMemory && maxHeapMemory < Long.MAX_VALUE )
        {
            try
            {
                long physicalMemory = OsBeanUtil.getTotalPhysicalMemory();
                if ( 0 < physicalMemory && physicalMemory < Long.MAX_VALUE && maxHeapMemory < physicalMemory )
                {
                    long heuristic = (long) ((physicalMemory - maxHeapMemory) * ratioOfFreeMem);
                    long min = ByteUnit.mebiBytes( 32 ); // We'd like at least 32 MiBs.
                    long max = Math.min( maxHeapMemory * 70, ByteUnit.gibiBytes( 20 ) );
                    // Don't heuristically take more than 20 GiBs, and don't take more than 70 times our max heap.
                    // 20 GiBs of page cache memory is ~2.6 million 8 KiB pages. If each page has an overhead of
                    // 72 bytes, then this will take up ~175 MiBs of heap memory. We should be able to tolerate that
                    // in most environments. The "no more than 70 times heap" heuristic is based on the page size over
                    // the per page overhead, 8192 / 72 ~= 114, plus leaving some extra room on the heap for the rest
                    // of the system. This means that we won't heuristically try to create a page cache that is too
                    // large to fit on the heap.
                    return Math.min( max, Math.max( min, heuristic ) );
                }
            }
            catch ( Exception ignore )
            {
            }
        }
        // ... otherwise we just go with 2 GiBs.
        return ByteUnit.gibiBytes( 2 );
    }

    public void checkPageSize( Config config )
    {
        if ( config.get( mapped_memory_page_size ).intValue() != 0 )
        {
            log.warn( "The setting unsupported.dbms.memory.pagecache.pagesize does not have any effect. It is " +
                    "deprecated and will be removed in a future version." );
        }
    }

    public void dumpConfiguration()
    {
        checkPageSize( config );
        String pageCacheMemory = config.get( pagecache_memory );
        long totalPhysicalMemory = OsBeanUtil.getTotalPhysicalMemory();
        String totalPhysicalMemMb = (totalPhysicalMemory == OsBeanUtil.VALUE_UNAVAILABLE)
                                    ? "?" : "" + ByteUnit.Byte.toMebiBytes( totalPhysicalMemory );
        long maxVmUsageMb = ByteUnit.Byte.toMebiBytes( Runtime.getRuntime().maxMemory() );
        String msg = "Physical mem: " + totalPhysicalMemMb + " MiB," +
                     " Heap size: " + maxVmUsageMb + " MiB," +
                     " Page cache: " + pageCacheMemory + ".";

        log.info( msg );
    }

    private static PageSwapperFactory createAndConfigureSwapperFactory( FileSystemAbstraction fs, Config config, Log log )
    {
        PageSwapperFactory factory = getPageSwapperFactory( config, log );
        factory.open( fs, config );
        return factory;
    }

    private static PageSwapperFactory getPageSwapperFactory( Config config, Log log )
    {
        String desiredImplementation = config.get( pagecache_swapper );
        if ( desiredImplementation != null )
        {
            for ( PageSwapperFactory factory : Service.load( PageSwapperFactory.class ) )
            {
                if ( factory.implementationName().equals( desiredImplementation ) )
                {
                    log.info( "Configured " + pagecache_swapper.name() + ": " + desiredImplementation );
                    return factory;
                }
            }
            throw new IllegalArgumentException( "Cannot find PageSwapperFactory: " + desiredImplementation );
        }
        return new SingleFilePageSwapperFactory();
    }
}
