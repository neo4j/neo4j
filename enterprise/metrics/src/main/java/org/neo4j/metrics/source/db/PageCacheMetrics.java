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
package org.neo4j.metrics.source.db;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import org.neo4j.io.pagecache.monitoring.PageCacheCounters;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Database page cache metrics" )
public class PageCacheMetrics extends LifecycleAdapter
{
    private static final String PAGE_CACHE_PREFIX = "neo4j.page_cache";

    @Documented( "The total number of exceptions seen during the eviction process in the page cache" )
    public static final String PC_EVICTION_EXCEPTIONS = name( PAGE_CACHE_PREFIX, "eviction_exceptions" );
    @Documented( "The total number of flushes executed by the page cache" )
    public static final String PC_FLUSHES = name( PAGE_CACHE_PREFIX, "flushes" );
    @Documented( "The total number of page unpins executed by the page cache" )
    public static final String PC_UNPINS = name( PAGE_CACHE_PREFIX, "unpins" );
    @Documented( "The total number of page pins executed by the page cache" )
    public static final String PC_PINS = name( PAGE_CACHE_PREFIX, "pins" );
    @Documented( "The total number of page evictions executed by the page cache" )
    public static final String PC_EVICTIONS = name( PAGE_CACHE_PREFIX, "evictions" );
    @Documented( "The total number of page faults happened in the page cache" )
    public static final String PC_PAGE_FAULTS = name( PAGE_CACHE_PREFIX, "page_faults" );
    @Documented( "The total number of page hits happened in the page cache" )
    public static final String PC_HITS = name( PAGE_CACHE_PREFIX, "hits" );
    @Documented( "The ratio of hits to the total number of lookups in the page cache" )
    public static final String PC_HIT_RATIO = name( PAGE_CACHE_PREFIX, "hit_ratio" );
    @Documented( "The ratio of number of used pages to total number of available pages" )
    public static final String PC_USAGE_RATIO = name( PAGE_CACHE_PREFIX, "usage_ratio" );

    private final MetricRegistry registry;
    private final PageCacheCounters pageCacheCounters;

    public PageCacheMetrics( MetricRegistry registry, PageCacheCounters pageCacheCounters )
    {
        this.registry = registry;
        this.pageCacheCounters = pageCacheCounters;
    }

    @Override
    public void start()
    {
        registry.register( PC_PAGE_FAULTS, (Gauge<Long>) pageCacheCounters::faults );
        registry.register( PC_EVICTIONS, (Gauge<Long>) pageCacheCounters::evictions );
        registry.register( PC_PINS, (Gauge<Long>) pageCacheCounters::pins );
        registry.register( PC_UNPINS, (Gauge<Long>) pageCacheCounters::unpins );
        registry.register( PC_HITS, (Gauge<Long>) pageCacheCounters::hits );
        registry.register( PC_FLUSHES, (Gauge<Long>) pageCacheCounters::flushes );
        registry.register( PC_EVICTION_EXCEPTIONS, (Gauge<Long>) pageCacheCounters::evictionExceptions );
        registry.register( PC_HIT_RATIO, (Gauge<Double>) pageCacheCounters::hitRatio );
        registry.register( PC_USAGE_RATIO, (Gauge<Double>) pageCacheCounters::usageRatio );
    }

    @Override
    public void stop()
    {
        registry.remove( PC_PAGE_FAULTS );
        registry.remove( PC_EVICTIONS );
        registry.remove( PC_PINS );
        registry.remove( PC_UNPINS );
        registry.remove( PC_HITS );
        registry.remove( PC_FLUSHES );
        registry.remove( PC_EVICTION_EXCEPTIONS );
        registry.remove( PC_HIT_RATIO );
        registry.remove( PC_USAGE_RATIO );
    }
}
