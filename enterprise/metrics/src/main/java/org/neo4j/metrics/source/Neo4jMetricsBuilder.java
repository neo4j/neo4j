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
package org.neo4j.metrics.source;

import com.codahale.metrics.MetricRegistry;

import java.util.function.Supplier;

import org.neo4j.causalclustering.core.consensus.CoreMetaData;
import org.neo4j.io.pagecache.monitoring.PageCacheCounters;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.kernel.impl.api.LogRotationMonitor;
import org.neo4j.kernel.impl.factory.Edition;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.store.stats.StoreEntityCounters;
import org.neo4j.kernel.impl.transaction.TransactionCounters;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerMonitor;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.metrics.MetricsSettings;
import org.neo4j.metrics.output.EventReporter;
import org.neo4j.metrics.source.causalclustering.CatchUpMetrics;
import org.neo4j.metrics.source.causalclustering.CoreMetrics;
import org.neo4j.metrics.source.causalclustering.ReadReplicaMetrics;
import org.neo4j.metrics.source.cluster.ClusterMetrics;
import org.neo4j.metrics.source.cluster.NetworkMetrics;
import org.neo4j.metrics.source.db.BoltMetrics;
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
import org.neo4j.metrics.source.server.ServerMetrics;

public class Neo4jMetricsBuilder
{
    private final MetricRegistry registry;
    private final LifeSupport life;
    private final EventReporter reporter;
    private final Config config;
    private final LogService logService;
    private final KernelContext kernelContext;
    private final Dependencies dependencies;

    public interface Dependencies
    {
        Monitors monitors();

        TransactionCounters transactionCounters();

        PageCacheCounters pageCacheCounters();

        CheckPointerMonitor checkPointerMonitor();

        LogRotationMonitor logRotationMonitor();

        StoreEntityCounters entityCountStats();

        Supplier<ClusterMembers> clusterMembers();

        Supplier<CoreMetaData> raft();

        Supplier<TransactionIdStore> transactionIdStore();
    }

    public Neo4jMetricsBuilder( MetricRegistry registry, EventReporter reporter, Config config, LogService logService,
            KernelContext kernelContext, Dependencies dependencies, LifeSupport life )
    {
        this.registry = registry;
        this.reporter = reporter;
        this.config = config;
        this.logService = logService;
        this.kernelContext = kernelContext;
        this.dependencies = dependencies;
        this.life = life;
    }

    public boolean build()
    {
        boolean result = false;
        if ( config.get( MetricsSettings.neoTxEnabled ) )
        {
            life.add( new TransactionMetrics( registry, dependencies.transactionIdStore(),
                    dependencies.transactionCounters() ) );
            result = true;
        }

        if ( config.get( MetricsSettings.neoPageCacheEnabled ) )
        {
            life.add( new PageCacheMetrics( registry, dependencies.pageCacheCounters() ) );
            result = true;
        }

        if ( config.get( MetricsSettings.neoCheckPointingEnabled ) )
        {
            life.add( new CheckPointingMetrics( reporter, registry, dependencies.monitors(),
                    dependencies.checkPointerMonitor() ) );
            result = true;
        }

        if ( config.get( MetricsSettings.neoLogRotationEnabled ) )
        {
            life.add( new LogRotationMetrics( reporter, registry, dependencies.monitors(),
                    dependencies.logRotationMonitor() ) );
            result = true;
        }

        if ( config.get( MetricsSettings.neoCountsEnabled ) )
        {
            if ( kernelContext.databaseInfo().edition != Edition.community &&
                    kernelContext.databaseInfo().edition != Edition.unknown )
            {
                life.add( new EntityCountMetrics( registry, dependencies.entityCountStats() ) );
                result = true;
            }
        }

        if ( config.get( MetricsSettings.neoNetworkEnabled ) )
        {
            life.add( new NetworkMetrics( registry, dependencies.monitors() ) );
            result = true;
        }

        if ( config.get( MetricsSettings.neoClusterEnabled ) )
        {
            if ( kernelContext.databaseInfo().operationalMode == OperationalMode.ha )
            {
                life.add( new ClusterMetrics( dependencies.monitors(), registry, dependencies.clusterMembers() ) );
                result = true;
            }
        }

        if ( config.get( MetricsSettings.cypherPlanningEnabled ) )
        {
            life.add( new CypherMetrics( registry, dependencies.monitors() ) );
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

        if ( config.get( MetricsSettings.boltMessagesEnabled ) )
        {
            life.add( new BoltMetrics( registry, dependencies.monitors() ) );
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

        if ( config.get( MetricsSettings.causalClusteringEnabled ) )
        {
            OperationalMode mode = kernelContext.databaseInfo().operationalMode;
            if ( mode == OperationalMode.core )
            {
                life.add( new CoreMetrics( dependencies.monitors(), registry, dependencies.raft() ) );
                life.add( new CatchUpMetrics( dependencies.monitors(), registry ) );
                result = true;
            }
            else if ( mode == OperationalMode.read_replica )
            {
                life.add( new ReadReplicaMetrics( dependencies.monitors(), registry ) );
                life.add( new CatchUpMetrics( dependencies.monitors(), registry ) );
                result = true;
            }
        }

        if ( config.get( MetricsSettings.neoServerEnabled ) )
        {
            life.add( new ServerMetrics( registry, logService, kernelContext.dependencySatisfier() ) );
            result = true;
        }

        return result;
    }
}
