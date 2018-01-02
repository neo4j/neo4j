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
package org.neo4j.metrics.source;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.SlaveUpdatePuller;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.metrics.MetricsSettings;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Cluster Metrics" )
public class ClusterMetrics extends LifecycleAdapter
{
    private static final String NAME_PREFIX = "neo4j.cluster";
    @Documented( "The total number of update pulls executed by this instance" )
    public static final String SLAVE_PULL_UPDATES = name( NAME_PREFIX, "slave_pull_updates" );
    @Documented( "The highest transaction id that has been pulled in the last pull updates by this instance" )
    public static final String SLAVE_PULL_UPDATE_UP_TO_TX = name( NAME_PREFIX, "slave_pull_update_up_to_tx" );

    private final Config config;
    private final Monitors monitors;
    private final MetricRegistry registry;
    private final SlaveUpdatePullerMonitor monitor = new SlaveUpdatePullerMonitor();

    public ClusterMetrics( Config config, Monitors monitors, MetricRegistry registry )
    {
        this.config = config;
        this.monitors = monitors;
        this.registry = registry;
    }

    @Override
    public void start() throws Throwable
    {
        if ( config.get( MetricsSettings.neoClusterEnabled ) )
        {
            monitors.addMonitorListener( monitor );

            registry.register( SLAVE_PULL_UPDATES, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return monitor.events.get();
                }
            } );

            registry.register( SLAVE_PULL_UPDATE_UP_TO_TX, new Gauge<Long>()
            {
                @Override
                public Long getValue()
                {
                    return monitor.lastAppliedTxId;
                }
            } );
        }
    }

    @Override
    public void stop() throws IOException
    {
        if ( config.get( MetricsSettings.neoClusterEnabled ) )
        {
            registry.remove( SLAVE_PULL_UPDATES );
            registry.remove( SLAVE_PULL_UPDATE_UP_TO_TX );

            monitors.removeMonitorListener( monitor );
        }
    }

    private static class SlaveUpdatePullerMonitor implements SlaveUpdatePuller.Monitor
    {
        private AtomicLong events = new AtomicLong();
        private volatile long lastAppliedTxId;

        @Override
        public void pulledUpdates( long lastAppliedTxId )
        {
            events.incrementAndGet();
            this.lastAppliedTxId = lastAppliedTxId;
        }
    }
}
