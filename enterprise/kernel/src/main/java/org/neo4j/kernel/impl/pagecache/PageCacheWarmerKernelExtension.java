/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.pagecache;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Format;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.state.NeoStoreFileListing;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.JobScheduler;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.neo4j.scheduler.JobScheduler.Groups.pageCacheIOHelper;

class PageCacheWarmerKernelExtension extends LifecycleAdapter
{
    private final JobScheduler scheduler;
    private final AvailabilityGuard availabilityGuard;
    private final Supplier<NeoStoreFileListing> fileListing;
    private final Log log;
    private final PageCacheWarmerMonitor monitor;
    private final Config config;
    private final PageCacheWarmer pageCacheWarmer;
    private volatile JobScheduler.JobHandle profileHandle;
    private volatile boolean started;

    PageCacheWarmerKernelExtension(
            JobScheduler scheduler, AvailabilityGuard availabilityGuard, PageCache pageCache, FileSystemAbstraction fs,
            Supplier<NeoStoreFileListing> fileListing, Log log, PageCacheWarmerMonitor monitor, Config config )
    {
        this.scheduler = scheduler;
        this.availabilityGuard = availabilityGuard;
        this.fileListing = fileListing;
        this.log = log;
        this.monitor = monitor;
        this.config = config;
        pageCacheWarmer = new PageCacheWarmer( fs, pageCache, scheduler );
    }

    @Override
    public void start()
    {
        if ( config.get( GraphDatabaseSettings.pagecache_warmup_enabled ) )
        {
            pageCacheWarmer.start();
            scheduleTryReheat();
            fileListing.get().registerStoreFileProvider( pageCacheWarmer );
            started = true;
        }
    }

    private void scheduleTryReheat()
    {
        scheduler.schedule( pageCacheIOHelper, this::tryReheat, 100, TimeUnit.MILLISECONDS );
    }

    private void tryReheat()
    {
        if ( availabilityGuard.isAvailable() )
        {
            doReheat();
            scheduleProfile();
        }
        else if ( !availabilityGuard.isShutdown() )
        {
            scheduleTryReheat();
        }
    }

    private void doReheat()
    {
        try
        {
            long start = System.nanoTime();
            pageCacheWarmer.reheat().ifPresent( pagesLoaded ->
            {
                long elapsedMillis = NANOSECONDS.toMillis( System.nanoTime() - start );
                monitor.warmupCompleted( pagesLoaded, elapsedMillis );
                log.debug( "Active page cache warmup took " + Format.duration( elapsedMillis ) +
                           " to load " + pagesLoaded + " pages." );
            } );
        }
        catch ( Exception e )
        {
            log.debug( "Active page cache warmup failed, " +
                       "so it may take longer for the cache to be populated with hot data.", e );
        }
    }

    private void scheduleProfile()
    {
        long frequencyMillis = config.get( GraphDatabaseSettings.pagecache_warmup_profiling_interval ).toMillis();
        profileHandle = scheduler.scheduleRecurring(
                pageCacheIOHelper, this::doProfile, frequencyMillis, TimeUnit.MILLISECONDS );
    }

    private void doProfile()
    {
        try
        {
            long start = System.nanoTime();
            pageCacheWarmer.profile().ifPresent( pagesInMemory ->
            {
                long elapsedMillis = NANOSECONDS.toMillis( System.nanoTime() - start );
                monitor.profileCompleted( elapsedMillis, pagesInMemory );
                log.debug( "Profiled page cache in " + Format.duration( elapsedMillis ) +
                           ", and found " + pagesInMemory + " pages in memory." );
            });
        }
        catch ( Exception e )
        {
            log.debug( "Page cache profiling failed, so no new profile of what data is hot or not was produced. " +
                       "This may reduce the effectiveness of a future page cache warmup process.", e );
        }
    }

    @Override
    public void stop() throws Throwable
    {
        if ( started )
        {
            JobScheduler.JobHandle handle = profileHandle;
            if ( handle != null )
            {
                handle.cancel( false );
            }
            pageCacheWarmer.stop();
        }
    }
}
