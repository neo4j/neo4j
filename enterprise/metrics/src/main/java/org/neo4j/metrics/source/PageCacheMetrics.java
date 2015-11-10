/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.metrics.source;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.io.IOException;

import org.neo4j.io.pagecache.monitoring.PageCacheMonitor;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.metrics.MetricsSettings;

import static com.codahale.metrics.MetricRegistry.name;

public class PageCacheMetrics extends LifecycleAdapter
{
    private static final String PAGE_CACHE_PREFIX = "neo4j.page_cache";
    private static final String PC_EVICTION_EXCEPTIONS = name( PAGE_CACHE_PREFIX, "eviction_exceptions" );
    private static final String PC_FLUSHES = name( PAGE_CACHE_PREFIX, "flushes" );
    private static final String PC_UNPINS = name( PAGE_CACHE_PREFIX, "unpins" );
    private static final String PC_PINS = name( PAGE_CACHE_PREFIX, "pins" );
    private static final String PC_EVICTIONS = name( PAGE_CACHE_PREFIX, "evictions" );
    private static final String PC_PAGE_FAULTS = name( PAGE_CACHE_PREFIX, "page_faults" );

    private final MetricRegistry registry;
    private final Config config;
    private final PageCacheMonitor pageCacheCounters;

    public PageCacheMetrics( MetricRegistry registry, Config config, PageCacheMonitor pageCacheCounters )
    {
        this.registry = registry;
        this.config = config;
        this.pageCacheCounters = pageCacheCounters;
    }

    @Override
    public void start() throws Throwable
    {
        if ( config.get( MetricsSettings.neoPageCacheEnabled ) )
        {
            registry.register( PC_PAGE_FAULTS, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return pageCacheCounters.countFaults();
                }
            } );

            registry.register( PC_EVICTIONS, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return pageCacheCounters.countEvictions();
                }
            } );

            registry.register( PC_PINS, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return pageCacheCounters.countPins();
                }
            } );

            registry.register( PC_UNPINS, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return pageCacheCounters.countUnpins();
                }
            } );

            registry.register( PC_FLUSHES, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return pageCacheCounters.countFlushes();
                }
            } );

            registry.register( PC_EVICTION_EXCEPTIONS, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return pageCacheCounters.countEvictionExceptions();
                }
            } );
        }
    }

    @Override
    public void stop() throws IOException
    {
        if ( config.get( MetricsSettings.neoPageCacheEnabled ) )
        {
            registry.remove( PC_PAGE_FAULTS );
            registry.remove( PC_EVICTIONS );
            registry.remove( PC_PINS );
            registry.remove( PC_UNPINS );
            registry.remove( PC_FLUSHES );
            registry.remove( PC_EVICTION_EXCEPTIONS );
        }
    }
}
