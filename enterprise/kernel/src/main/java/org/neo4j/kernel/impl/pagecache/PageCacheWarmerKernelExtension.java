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

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.neo4j.util.FeatureToggles;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.neo4j.scheduler.JobScheduler.Groups.pageCacheIOHelper;

class PageCacheWarmerKernelExtension extends LifecycleAdapter
{
    private static final boolean ENABLED = FeatureToggles.flag( PageCacheWarmerKernelExtension.class, "enabled", true );

    private final JobScheduler scheduler;
    private final AvailabilityGuard availabilityGuard;
    private final Supplier<NeoStoreFileListing> fileListing;
    private final Log log;
    private final PageCacheWarmerMonitor monitor;
    private final Config config;
    private final PageCacheWarmer pageCacheWarmer;
    private final AtomicBoolean profilingStarted;
    private volatile JobScheduler.JobHandle profileHandle;

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
        profilingStarted = new AtomicBoolean();
    }

    @Override
    public void start() throws Throwable
    {
        if ( ENABLED )
        {
            scheduleTryReheat();
            fileListing.get().registerStoreFileProvider( pageCacheWarmer );
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
        catch ( IOException e )
        {
            log.debug( "Active page cache warmup failed, " +
                       "so it may take longer for the cache to be populated with hot data.", e );
        }
    }

    private void scheduleProfile()
    {
        long frequencyMillis = config.get( GraphDatabaseSettings.pagecache_warmup_profiling_interval ).toMillis();
        profileHandle = scheduler.schedule(
                pageCacheIOHelper, this::tryStartProfile, frequencyMillis, TimeUnit.MILLISECONDS );
    }

    private void tryStartProfile()
    {
        if ( profilingStarted.compareAndSet( false, true ) )
        {
            // At this point, we are currently executing inside a *scheduled* executor thread. There are, at this time
            // of writing, only two executor threads. Therefor, we *must* hand off the actual profiling work to another
            // thread, so we don't cause congestion in the scheduler. Congestion could for instance arise if the
            // profile gets stuck on some lock in the page cache. We cannot allow this to block a scheduler thread for
            // an extended period of time, since there are many other services that rely on timely activation from the
            // scheduler. For instance, catchup in causal cluster is relying the timeliness of the scheduler.
            scheduler.schedule( pageCacheIOHelper, this::doProfile );
        }
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
        catch ( IOException e )
        {
            log.debug( "Page cache profiling failed, so no new profile of what data is hot or not was produced. " +
                       "This may reduce the effectiveness of a future page cache warmup process.", e );
        }
        finally
        {
            profilingStarted.set( false );
            scheduleProfile();
        }
    }

    @Override
    public void stop() throws Throwable
    {
        JobScheduler.JobHandle handle = profileHandle;
        if ( handle != null )
        {
            handle.cancel( false );
        }
        pageCacheWarmer.stop();
    }
}
