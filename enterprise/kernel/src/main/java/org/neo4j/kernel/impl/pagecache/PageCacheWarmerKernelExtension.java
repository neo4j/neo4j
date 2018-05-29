/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;

class PageCacheWarmerKernelExtension extends LifecycleAdapter
{
    private final AvailabilityGuard availabilityGuard;
    private final Config config;
    private final PageCacheWarmer pageCacheWarmer;
    private final WarmupAvailabilityListener availabilityListener;
    private volatile boolean started;

    PageCacheWarmerKernelExtension(
            JobScheduler scheduler, AvailabilityGuard availabilityGuard, PageCache pageCache,
            Log log, PageCacheWarmerMonitor monitor, Config config )
    {
        this.availabilityGuard = availabilityGuard;
        this.config = config;
        pageCacheWarmer = new PageCacheWarmer( pageCache, scheduler );
        availabilityListener = new WarmupAvailabilityListener( scheduler, pageCacheWarmer, log, monitor );
    }

    @Override
    public void start()
    {
        if ( config.get( GraphDatabaseSettings.pagecache_warmup_enabled ) )
        {
            pageCacheWarmer.start();
            availabilityGuard.addListener( availabilityListener );
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
