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
package org.neo4j.kernel.impl.pagecache;

import java.util.function.Supplier;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.state.NeoStoreFileListing;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.JobScheduler;

class PageCacheWarmerKernelExtension extends LifecycleAdapter
{
    private final AvailabilityGuard availabilityGuard;
    private final Supplier<NeoStoreFileListing> fileListing;
    private final Config config;
    private final PageCacheWarmer pageCacheWarmer;
    private final WarmupAvailabilityListener availabilityListener;
    private volatile boolean started;

    PageCacheWarmerKernelExtension(
            JobScheduler scheduler, AvailabilityGuard availabilityGuard, PageCache pageCache, FileSystemAbstraction fs,
            Supplier<NeoStoreFileListing> fileListing, Log log, PageCacheWarmerMonitor monitor, Config config )
    {
        this.availabilityGuard = availabilityGuard;
        this.fileListing = fileListing;
        this.config = config;
        pageCacheWarmer = new PageCacheWarmer( fs, pageCache, scheduler );
        availabilityListener = new WarmupAvailabilityListener( scheduler, pageCacheWarmer, config, log, monitor );
    }

    @Override
    public void start()
    {
        if ( config.get( GraphDatabaseSettings.pagecache_warmup_enabled ) )
        {
            pageCacheWarmer.start();
            availabilityGuard.addListener( availabilityListener );
            fileListing.get().registerStoreFileProvider( pageCacheWarmer );
            started = true;
        }
    }

    @Override
    public void stop() throws Throwable
    {
        if ( started )
        {
            availabilityGuard.removeListener( availabilityListener );
            availabilityListener.unavailable(); // Make sure scheduled jobs get cancelled.
            pageCacheWarmer.stop();
            started = false;
        }
    }
}
