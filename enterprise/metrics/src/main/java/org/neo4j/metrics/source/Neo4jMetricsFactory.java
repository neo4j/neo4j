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

import java.io.IOException;

import org.neo4j.function.Factory;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.io.pagecache.monitoring.PageCacheMonitor;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.LogRotationMonitor;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.transaction.TransactionCounters;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerMonitor;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;

public class Neo4jMetricsFactory implements Factory<Lifecycle>
{
    private final MetricRegistry registry;
    private final Config config;
    private final Monitors monitors;
    private final DataSourceManager dataSourceManager;
    private final TransactionCounters transactionCounters;
    private final PageCacheMonitor pageCacheCounters;
    private final CheckPointerMonitor checkPointerMonitor;
    private final LogRotationMonitor logRotationMonitor;
    private final IdGeneratorFactory idGeneratorFactory;
    private final DependencyResolver dependencyResolver;
    private final LogService logService;

    public Neo4jMetricsFactory( MetricRegistry registry, Config config, Monitors monitors,
            DataSourceManager dataSourceManager, TransactionCounters transactionCounters,
            PageCacheMonitor pageCacheCounters, CheckPointerMonitor checkPointerMonitor,
            LogRotationMonitor logRotationMonitor, IdGeneratorFactory idGeneratorFactory,
            DependencyResolver dependencyResolver, LogService logService )
    {
        this.registry = registry;
        this.config = config;
        this.monitors = monitors;
        this.dataSourceManager = dataSourceManager;
        this.transactionCounters = transactionCounters;
        this.pageCacheCounters = pageCacheCounters;
        this.checkPointerMonitor = checkPointerMonitor;
        this.logRotationMonitor = logRotationMonitor;
        this.idGeneratorFactory = idGeneratorFactory;
        this.dependencyResolver = dependencyResolver;
        this.logService = logService;
    }

    @Override
    public Lifecycle newInstance()
    {
        final TransactionMetrics transactionMetrics =
                new TransactionMetrics( registry, config, dataSourceManager, transactionCounters );
        final PageCacheMetrics pageCacheMetrics = new PageCacheMetrics( registry, config, pageCacheCounters );
        final CheckPointingMetrics checkPointingMetrics =
                new CheckPointingMetrics( registry, config, checkPointerMonitor, logRotationMonitor );
        final EntityCountMetrics entityCountMetrics = new EntityCountMetrics( registry, config, idGeneratorFactory );

        final NetworkMetrics networkMetrics = new NetworkMetrics( config, monitors, registry );
        final ClusterMetrics clusterMetrics = new ClusterMetrics( config, monitors, registry, dependencyResolver,
                logService );
        final CypherMetrics cypherMetrics = new CypherMetrics( config, monitors, registry );
        final JvmMetrics jvmMetrics = new JvmMetrics( config, registry );
        return new LifecycleAdapter()
        {
            @Override
            public void start() throws Throwable
            {
                transactionMetrics.start();
                pageCacheMetrics.start();
                checkPointingMetrics.start();
                entityCountMetrics.start();
                networkMetrics.start();
                clusterMetrics.start();
                jvmMetrics.start();
                cypherMetrics.start();
            }

            @Override
            public void stop() throws IOException
            {
                transactionMetrics.stop();
                pageCacheMetrics.stop();
                checkPointingMetrics.stop();
                entityCountMetrics.stop();
                networkMetrics.stop();
                clusterMetrics.stop();
                jvmMetrics.stop();
                cypherMetrics.stop();
            }
        };
    }
}
