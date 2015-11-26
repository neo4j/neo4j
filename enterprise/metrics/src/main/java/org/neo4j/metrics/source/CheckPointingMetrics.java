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

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.impl.api.LogRotationMonitor;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerMonitor;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.metrics.MetricsSettings;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Database CheckPointing Metrics" )
public class CheckPointingMetrics extends LifecycleAdapter
{
    private static final String CHECK_POINT_PREFIX = "neo4j.check_point";
    private static final String LOG_ROTATION_PREFIX = "neo4j.log_rotation";

    @Documented( "The total number of check point events executed so far" )
    public static final String CHECK_POINT_EVENTS = name( CHECK_POINT_PREFIX, "events" );
    @Documented( "The total time spent in check pointing so far" )
    public static final String CHECK_POINT_TOTAL_TIME = name( CHECK_POINT_PREFIX, "total_time" );

    @Documented( "The total number of transaction log rotations executed so far" )
    public static final String LOG_ROTATION_EVENTS = name( LOG_ROTATION_PREFIX, "events" );
    @Documented( "The total time spent in rotating transaction logs so far" )
    public static final String LOG_ROTATION_TOTAL_TIME = name( LOG_ROTATION_PREFIX, "total_time" );

    private final MetricRegistry registry;
    private final Config config;
    private final CheckPointerMonitor checkPointerMonitor;
    private final LogRotationMonitor logRotationMonitor;

    public CheckPointingMetrics( MetricRegistry registry, Config config, CheckPointerMonitor checkPointerMonitor,
            LogRotationMonitor logRotationMonitor )
    {
        this.registry = registry;
        this.config = config;
        this.checkPointerMonitor = checkPointerMonitor;
        this.logRotationMonitor = logRotationMonitor;
    }

    @Override
    public void start() throws Throwable
    {
        // check pointing
        if ( config.get( MetricsSettings.neoCheckPointingEnabled ) )
        {
            registry.register( CHECK_POINT_EVENTS, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return checkPointerMonitor.numberOfCheckPointEvents();
                }
            } );

            registry.register( CHECK_POINT_TOTAL_TIME, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return checkPointerMonitor.checkPointAccumulatedTotalTimeMillis();
                }
            } );
        }

        // log rotation
        if ( config.get( MetricsSettings.neoLogRotationEnabled ) )
        {
            registry.register( LOG_ROTATION_EVENTS, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return logRotationMonitor.numberOfLogRotationEvents();
                }
            } );

            registry.register( LOG_ROTATION_TOTAL_TIME, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return logRotationMonitor.logRotationAccumulatedTotalTimeMillis();
                }
            } );
        }
    }

    @Override
    public void stop() throws IOException
    {
        // check pointing
        if ( config.get( MetricsSettings.neoCheckPointingEnabled ) )
        {
            registry.remove( CHECK_POINT_EVENTS );
            registry.remove( CHECK_POINT_TOTAL_TIME );
        }

        // log rotation
        if ( config.get( MetricsSettings.neoLogRotationEnabled ) )
        {
            registry.remove( LOG_ROTATION_EVENTS );
            registry.remove( LOG_ROTATION_TOTAL_TIME );
        }
    }
}
