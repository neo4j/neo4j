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
package org.neo4j.metrics.source.cluster;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.neo4j.kernel.ha.SlaveUpdatePuller;
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;

import static com.codahale.metrics.MetricRegistry.name;
import static org.neo4j.kernel.ha.cluster.modeswitch.HighAvailabilityModeSwitcher.MASTER;
import static org.neo4j.kernel.ha.cluster.modeswitch.HighAvailabilityModeSwitcher.UNKNOWN;

@Documented( ".Cluster Metrics" )
public class ClusterMetrics extends LifecycleAdapter
{
    private static final String NAME_PREFIX = "neo4j.cluster";

    @Documented( "The total number of update pulls executed by this instance" )
    public static final String SLAVE_PULL_UPDATES = name( NAME_PREFIX, "slave_pull_updates" );
    @Documented( "The highest transaction id that has been pulled in the last pull updates by this instance" )
    public static final String SLAVE_PULL_UPDATE_UP_TO_TX = name( NAME_PREFIX, "slave_pull_update_up_to_tx" );
    @Documented( "Whether or not this instance is the master in the cluster" )
    public static final String IS_MASTER = name( NAME_PREFIX, "is_master" );
    @Documented( "Whether or not this instance is available in the cluster" )
    public static final String IS_AVAILABLE = name( NAME_PREFIX, "is_available" );

    private final Monitors monitors;
    private final MetricRegistry registry;
    private final SlaveUpdatePullerMonitor monitor = new SlaveUpdatePullerMonitor();
    private final Supplier<ClusterMembers> clusterMembers;

    public ClusterMetrics( Monitors monitors, MetricRegistry registry, Supplier<ClusterMembers> clusterMembers )
    {
        this.monitors = monitors;
        this.registry = registry;
        this.clusterMembers = clusterMembers;
    }

    @Override
    public void start()
    {
        monitors.addMonitorListener( monitor );

        registry.register( IS_MASTER, new RoleGauge( MASTER::equals ) );
        registry.register( IS_AVAILABLE, new RoleGauge( s -> !UNKNOWN.equals( s ) ) );

        registry.register( SLAVE_PULL_UPDATES, (Gauge<Long>) () -> monitor.events.get() );
        registry.register( SLAVE_PULL_UPDATE_UP_TO_TX, (Gauge<Long>) () -> monitor.lastAppliedTxId );
    }

    @Override
    public void stop()
    {
        registry.remove( SLAVE_PULL_UPDATES );
        registry.remove( SLAVE_PULL_UPDATE_UP_TO_TX );

        registry.remove( IS_MASTER );
        registry.remove( IS_AVAILABLE );

        monitors.removeMonitorListener( monitor );
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

    private class RoleGauge implements Gauge<Integer>
    {
        private Predicate<String> rolePredicate;

        RoleGauge( Predicate<String> rolePredicate )
        {
            this.rolePredicate = rolePredicate;
        }

        @Override
        public Integer getValue()
        {
            ClusterMembers clusterMembers = ClusterMetrics.this.clusterMembers.get();
            return clusterMembers != null && rolePredicate.test( clusterMembers.getCurrentMemberRole() ) ? 1 : 0;
        }
    }
}
