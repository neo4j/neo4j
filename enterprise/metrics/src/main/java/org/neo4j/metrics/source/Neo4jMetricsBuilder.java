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

import com.codahale.metrics.MetricRegistry;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.io.pagecache.monitoring.PageCacheMonitor;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.kernel.impl.api.LogRotationMonitor;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.transaction.TransactionCounters;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerMonitor;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.metrics.MetricsSettings;
import org.neo4j.metrics.output.EventReporter;
import org.neo4j.metrics.source.cluster.ClusterMetrics;
import org.neo4j.metrics.source.cluster.NetworkMetrics;
import org.neo4j.metrics.source.db.CheckPointingMetrics;
import org.neo4j.metrics.source.db.CypherMetrics;
import org.neo4j.metrics.source.db.EntityCountMetrics;
import org.neo4j.metrics.source.db.LogRotationMetrics;
import org.neo4j.metrics.source.db.PageCacheMetrics;
import org.neo4j.metrics.source.db.TransactionMetrics;
import org.neo4j.metrics.source.jvm.GCMetrics;
import org.neo4j.metrics.source.jvm.MemoryBuffersMetrics;
import org.neo4j.metrics.source.jvm.MemoryPoolMetrics;
import org.neo4j.metrics.source.jvm.ThreadMetrics;

public class Neo4jMetricsBuilder
{
    private final MetricRegistry registry;
    private final LifeSupport life;
    private final EventReporter reporter;
    private final Config config;
    private final LogService logService;
    private final Dependencies deps;

    public interface Dependencies
    {
        Monitors monitors();

        DataSourceManager dataSourceManager();

        TransactionCounters transactionCounters();

        PageCacheMonitor pageCacheCounters();

        CheckPointerMonitor checkPointerMonitor();

        LogRotationMonitor logRotationMonitor();

        @SuppressWarnings( "deprecation" )
        IdGeneratorFactory idGeneratorFactory();

        DependencyResolver getDependencyResolver();
    }

    public Neo4jMetricsBuilder( MetricRegistry registry, EventReporter reporter, Config config, LogService logService,
            Dependencies deps, LifeSupport life )
    {
        this.registry = registry;
        this.reporter = reporter;
        this.config = config;
        this.logService = logService;
        this.deps = deps;
        this.life = life;
    }

    public boolean build()
    {
        boolean result = false;
        if ( config.get( MetricsSettings.neoTxEnabled ) )
        {
            life.add( new TransactionMetrics( registry, deps.dataSourceManager(), deps.transactionCounters() ) );
            result = true;
        }

        if ( config.get( MetricsSettings.neoPageCacheEnabled ) )
        {

            life.add( new PageCacheMetrics( registry, deps.pageCacheCounters() ) );
            result = true;
        }

        if ( config.get( MetricsSettings.neoCheckPointingEnabled ) )
        {
            life.add( new CheckPointingMetrics( reporter, registry,
                    deps.monitors(), deps.checkPointerMonitor() ) );
            result = true;
        }

        if ( config.get( MetricsSettings.neoLogRotationEnabled ) )
        {
            life.add( new LogRotationMetrics( reporter, registry,
                    deps.monitors(), deps.logRotationMonitor() ) );
            result = true;
        }

        if ( config.get( MetricsSettings.neoCountsEnabled ) )
        {
            life.add( new EntityCountMetrics( registry, deps.idGeneratorFactory() ) );
            result = true;
        }

        if ( config.get( MetricsSettings.neoNetworkEnabled ) )
        {
            life.add( new NetworkMetrics( registry, deps.monitors() ) );
            result = true;
        }

        if ( config.get( MetricsSettings.neoClusterEnabled ) )
        {
            final ClusterMembers clusterMembers =
                    resolveClusterMembersOrNull( deps.getDependencyResolver(), logService );
            if ( clusterMembers != null )
            {
                life.add( new ClusterMetrics( deps.monitors(), registry, clusterMembers ) );
                result = true;
            }
        }

        if ( config.get( MetricsSettings.cypherPlanningEnabled ) )
        {
            life.add( new CypherMetrics( registry, deps.monitors() ) );
            result = true;
        }

        if ( config.get( MetricsSettings.jvmGcEnabled ) )
        {
            life.add( new GCMetrics( registry ) );
            result = true;
        }

        if ( config.get( MetricsSettings.jvmThreadsEnabled ) )
        {
            life.add( new ThreadMetrics( registry ) );
            result = true;
        }

        if ( config.get( MetricsSettings.jvmMemoryEnabled ) )
        {
            life.add( new MemoryPoolMetrics( registry ) );
            result = true;
        }

        if ( config.get( MetricsSettings.jvmBuffersEnabled ) )
        {
            life.add( new MemoryBuffersMetrics( registry ) );
            result = true;
        }

        return result;
    }

    private ClusterMembers resolveClusterMembersOrNull( DependencyResolver dependencyResolver, LogService logService )
    {
        try
        {
            return dependencyResolver.resolveDependency( ClusterMembers.class );
        }
        catch ( IllegalArgumentException e )
        {
            logService.getUserLog( getClass() )
                    .warn( "Cluster metrics was enabled but the graph database is not in HA mode." );
            return null;
        }
    }
}
