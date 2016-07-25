/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.metrics.source.db;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import org.neo4j.cypher.PlanCacheMetricsMonitor;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Cypher Metrics" )
public class CypherMetrics extends LifecycleAdapter
{
    private static final String NAME_PREFIX = "neo4j.cypher";

    @Documented( "The total number of times Cypher has decided to re-plan a query" )
    public static final String REPLAN_EVENTS = name( NAME_PREFIX, "replan_events" );

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
    }

    @Override
    public void stop()
    {
        registry.remove( REPLAN_EVENTS );
        monitors.removeMonitorListener( cacheMonitor );
    }
}

