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
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.function.Predicate;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.kernel.ha.SlaveUpdatePuller;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.metrics.MetricsSettings;

import static com.codahale.metrics.MetricRegistry.name;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher.MASTER;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher.UNKNOWN;


public class ClusterMetrics extends LifecycleAdapter
{
    private static final String NAME_PREFIX = "neo4j.cluster";
    private static final String SLAVE_PULL_UPDATES = name( NAME_PREFIX, "slave_pull_updates" );
    private static final String SLAVE_PULL_UPDATE_UP_TO_TX = name( NAME_PREFIX, "slave_pull_update_up_to_tx" );
    static final String IS_MASTER = name( NAME_PREFIX, "is_master" );
    static final String IS_AVAILABLE = name( NAME_PREFIX, "is_available" );

    private final Config config;
    private final Monitors monitors;
    private final MetricRegistry registry;
    private final DependencyResolver dependencyResolver;
    private final LogService logService;
    private final SlaveUpdatePullerMonitor monitor = new SlaveUpdatePullerMonitor();
    private ClusterMembers clusterMembers = null;

    public ClusterMetrics( Config config, Monitors monitors, MetricRegistry registry,
            DependencyResolver dependencyResolver, LogService logService )
    {
        this.config = config;
        this.monitors = monitors;
        this.registry = registry;
        this.dependencyResolver = dependencyResolver;
        this.logService = logService;
    }

    @Override
    public void start() throws Throwable
    {
        if ( config.get( MetricsSettings.neoClusterEnabled ) && resolveClusterMembersDependencyOrLogWarning() )
        {
            monitors.addMonitorListener( monitor );

            registry.register( IS_MASTER, new RoleGauge( Predicates.equalTo( MASTER ) ) );
            registry.register( IS_AVAILABLE, new RoleGauge( Predicates.not( Predicates.equalTo( UNKNOWN ) ) ) );

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

    private boolean resolveClusterMembersDependencyOrLogWarning()
    {
        try
        {
            clusterMembers = dependencyResolver.resolveDependency( ClusterMembers.class );
            return true;
        }
        catch ( IllegalArgumentException e )
        {
            logService.getUserLog( getClass() ).warn( "Cluster metrics was enabled but the graph database" +
                    "is not in HA mode." );
            return false;
        }
    }

    @Override
    public void stop() throws IOException
    {
        if ( config.get( MetricsSettings.neoClusterEnabled ) && (clusterMembers != null) )
        {
            registry.remove( SLAVE_PULL_UPDATES );
            registry.remove( SLAVE_PULL_UPDATE_UP_TO_TX );

            registry.remove( IS_MASTER );
            registry.remove( IS_AVAILABLE );

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

    private class RoleGauge implements Gauge<Integer>
    {
        private Predicate<String> rolePredicate;

        public RoleGauge( Predicate<String> rolePredicate )
        {
            this.rolePredicate = rolePredicate;
        }

        public Integer getValue()
        {
            int value = 0;
            if ( clusterMembers != null )
            {
                value = rolePredicate.test( clusterMembers.getCurrentMemberRole() ) ? 1 : 0;
            }
            return value;
        }
    }
}
