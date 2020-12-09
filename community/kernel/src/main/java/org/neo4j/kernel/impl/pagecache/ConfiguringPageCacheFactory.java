/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.configuration.Config;
import org.neo4j.configuration.pagecache.ConfigurableIOBufferFactory;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.mem.MemoryAllocator;
import org.neo4j.io.os.OsBeanUtil;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.logging.Log;
import org.neo4j.memory.MachineMemory;
import org.neo4j.memory.MemoryPools;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.SystemNanoClock;

import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.configuration.SettingValueParsers.BYTES;
import static org.neo4j.io.mem.MemoryAllocator.createAllocator;
import static org.neo4j.memory.MemoryGroup.PAGE_CACHE;

public class ConfiguringPageCacheFactory
{
    private PageSwapperFactory swapperFactory;
    private final FileSystemAbstraction fs;
    private final Config config;
    private final PageCacheTracer pageCacheTracer;
    private final Log log;
    private final VersionContextSupplier versionContextSupplier;
    private PageCache pageCache;
    private final JobScheduler scheduler;
    private final SystemNanoClock clock;
    private final MemoryPools memoryPools;

    /**
     * Construct configuring page cache factory
     * @param fs fileSystem file system that page cache will be based on
     * @param config page swapper configuration
     * @param pageCacheTracer global page cache tracer
     * @param log page cache factory log
     * @param versionContextSupplier cursor context factory
     * @param scheduler job scheduler to execute page cache jobs
     * @param clock the clock source used by the page cache.
     */
    public ConfiguringPageCacheFactory( FileSystemAbstraction fs, Config config, PageCacheTracer pageCacheTracer, Log log,
            VersionContextSupplier versionContextSupplier, JobScheduler scheduler, SystemNanoClock clock, MemoryPools memoryPools )
    {
        this.fs = fs;
        this.versionContextSupplier = versionContextSupplier;
        this.config = config;
        this.pageCacheTracer = pageCacheTracer;
        this.log = log;
        this.scheduler = scheduler;
        this.clock = clock;
        this.memoryPools = memoryPools;
    }

    public synchronized PageCache getOrCreatePageCache()
    {
        if ( pageCache == null )
        {
            this.swapperFactory = createAndConfigureSwapperFactory( fs );
            this.pageCache = createPageCache();
        }
        return pageCache;
    }

    protected PageCache createPageCache()
    {
        long pageCacheMaxMemory = getPageCacheMaxMemory( config );
        var memoryPool = memoryPools.pool( PAGE_CACHE, pageCacheMaxMemory, false, null );
        var memoryTracker = memoryPool.getPoolMemoryTracker();
        MemoryAllocator memoryAllocator = buildMemoryAllocator( pageCacheMaxMemory, memoryTracker );
        var bufferFactory = new ConfigurableIOBufferFactory( config, memoryTracker );
        MuninnPageCache.Configuration configuration = MuninnPageCache.config( memoryAllocator )
                .memoryTracker( memoryTracker )
                .bufferFactory( bufferFactory )
                .clock( clock )
                .pageCacheTracer( pageCacheTracer )
                .versionContextSupplier( versionContextSupplier );
        return new MuninnPageCache( swapperFactory, scheduler, configuration );
    }

    private MemoryAllocator buildMemoryAllocator( long pageCacheMaxMemory, MemoryTracker memoryTracker )
    {
        return createAllocator( pageCacheMaxMemory, memoryTracker );
    }

    private long getPageCacheMaxMemory( Config config )
    {
        String pageCacheMemorySetting = config.get( pagecache_memory );
        if ( pageCacheMemorySetting == null )
        {
            long heuristic = defaultHeuristicPageCacheMemory( MachineMemory.DEFAULT );
            log.warn( "The " + pagecache_memory.name() + " setting has not been configured. It is recommended that this " +
                      "setting is always explicitly configured, to ensure the system has a balanced configuration. " +
                      "Until then, a computed heuristic value of " + heuristic + " bytes will be used instead. " +
                      "Run `neo4j-admin memrec` for memory configuration suggestions." );
            pageCacheMemorySetting = "" + heuristic;
        }
        return ByteUnit.parse( pageCacheMemorySetting );
    }

    public static long defaultHeuristicPageCacheMemory( MachineMemory machineMemory )
    {
        // First check if we have a default override...
        String defaultMemoryOverride = System.getProperty( "dbms.pagecache.memory.default.override" );
        if ( defaultMemoryOverride != null )
        {
            return BYTES.parse( defaultMemoryOverride );
        }

        double ratioOfFreeMem = 0.50;
        String defaultMemoryRatioOverride = System.getProperty( "dbms.pagecache.memory.ratio.default.override" );
        if ( defaultMemoryRatioOverride != null )
        {
            ratioOfFreeMem = Double.parseDouble( defaultMemoryRatioOverride );
        }

        // Try to compute (RAM - maxheap) * 0.50 if we can get reliable numbers...
        long maxHeapMemory = machineMemory.getHeapMemoryUsage().getMax();
        if ( 0 < maxHeapMemory && maxHeapMemory < Long.MAX_VALUE )
        {
            try
            {
                long physicalMemory = machineMemory.getTotalPhysicalMemory();
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

    public void dumpConfiguration()
    {
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

    private static PageSwapperFactory createAndConfigureSwapperFactory( FileSystemAbstraction fs )
    {
        return new SingleFilePageSwapperFactory( fs );
    }
}
