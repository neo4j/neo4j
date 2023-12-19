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
package org.neo4j.metrics.source.db;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import org.neo4j.cypher.PlanCacheMetricsMonitor;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Cypher metrics" )
public class CypherMetrics extends LifecycleAdapter
{
    private static final String NAME_PREFIX = "neo4j.cypher";

    @Documented( "The total number of times Cypher has decided to re-plan a query" )
    public static final String REPLAN_EVENTS = name( NAME_PREFIX, "replan_events" );

    @Documented( "The total number of seconds waited between query replans" )
    public static final String REPLAN_WAIT_TIME = name( NAME_PREFIX, "replan_wait_time" );

    private final MetricRegistry registry;
    private final Monitors monitors;
    private final PlanCacheMetricsMonitor cacheMonitor = new PlanCacheMetricsMonitor();

    public CypherMetrics( MetricRegistry registry, Monitors monitors )
    {
        this.registry = registry;
        this.monitors = monitors;
    }

    @Override
    public void start()
    {
        monitors.addMonitorListener( cacheMonitor );
        registry.register( REPLAN_EVENTS, (Gauge<Long>) cacheMonitor::numberOfReplans );
        registry.register( REPLAN_WAIT_TIME, (Gauge<Long>) cacheMonitor::replanWaitTime );
    }

    @Override
    public void stop()
    {
        registry.remove( REPLAN_EVENTS );
        registry.remove( REPLAN_WAIT_TIME );
        monitors.removeMonitorListener( cacheMonitor );
    }
}

