/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.impl.pagecache;

import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.AvailabilityGuard.AvailabilityListener;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.scheduler.JobScheduler.Groups.pageCacheIOHelper;

class WarmupAvailabilityListener implements AvailabilityListener
{
    private final JobScheduler scheduler;
    private final PageCacheWarmer pageCacheWarmer;
    private final Config config;
    private final Log log;
    private final PageCacheWarmerMonitor monitor;

    // We use the monitor lock to guard the job handle. However, it could happen that a job has already started, ends
    // up waiting for the lock while it's being held by another thread calling `unavailable()`. In that case, we need
    // to make sure that the signal to stop is not lost. Cancelling a job handle only works on jobs that haven't
    // started yet, since we don't propagate an interrupt. This is why we check the `available` field in the
    // `scheduleProfile` method.
    private volatile boolean available;
    private JobScheduler.JobHandle jobHandle; // Guarded by `this`.

    WarmupAvailabilityListener( JobScheduler scheduler, PageCacheWarmer pageCacheWarmer,
                                Config config, Log log, PageCacheWarmerMonitor monitor )
    {
        this.scheduler = scheduler;
        this.pageCacheWarmer = pageCacheWarmer;
        this.config = config;
        this.log = log;
        this.monitor = monitor;
    }

    @Override
    public synchronized void available()
    {
        available = true;
        jobHandle = scheduler.schedule( pageCacheIOHelper, this::startWarmup );
    }

    private void startWarmup()
    {
        if ( !available )
        {
            return;
        }
        try
        {
            pageCacheWarmer.reheat().ifPresent( monitor::warmupCompleted );
        }
        catch ( Exception e )
        {
            log.debug( "Active page cache warmup failed, " +
                       "so it may take longer for the cache to be populated with hot data.", e );
        }

        scheduleProfile();
    }

    private synchronized void scheduleProfile()
    {
        if ( !available )
        {
            return;
        }
        long frequencyMillis = config.get( GraphDatabaseSettings.pagecache_warmup_profiling_interval ).toMillis();
        jobHandle = scheduler.scheduleRecurring(
                pageCacheIOHelper, this::doProfile, frequencyMillis, TimeUnit.MILLISECONDS );
    }

    private void doProfile()
    {
        try
        {
            pageCacheWarmer.profile().ifPresent( monitor::profileCompleted );
        }
        catch ( Exception e )
        {
            log.debug( "Page cache profiling failed, so no new profile of what data is hot or not was produced. " +
                       "This may reduce the effectiveness of a future page cache warmup process.", e );
        }
    }

    @Override
    public synchronized void unavailable()
    {
        available = false;
        if ( jobHandle != null )
        {
            jobHandle.cancel( false );
            jobHandle = null;
        }
    }
}
