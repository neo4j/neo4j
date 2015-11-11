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
package org.neo4j.metrics;

import com.codahale.metrics.MetricRegistry;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.io.pagecache.monitoring.PageCacheMonitor;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.LogRotationMonitor;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.transaction.TransactionCounters;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerMonitor;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.metrics.output.OutputBuilder;
import org.neo4j.metrics.source.Neo4jMetricsBuilder;

public class MetricsExtension implements Lifecycle
{
    private final LifeSupport life = new LifeSupport();
    private final LogService logService;
    private final Config configuration;
    private final Monitors monitors;
    private final TransactionCounters transactionCounters;
    private final PageCacheMonitor pageCacheCounters;
    private final CheckPointerMonitor checkPointerMonitor;
    private final IdGeneratorFactory idGeneratorFactory;
    private final LogRotationMonitor logRotationMonitor;
    private final DataSourceManager dataSourceManager;
    private final DependencyResolver dependencyResolver;
    private final KernelContext kernelContext;

    public MetricsExtension( MetricsKernelExtensionFactory.Dependencies dependencies )
    {
        logService = dependencies.logService();
        configuration = dependencies.configuration();
        monitors = dependencies.monitors();
        dataSourceManager = dependencies.dataSourceManager();
        transactionCounters = dependencies.transactionCounters();
        pageCacheCounters = dependencies.pageCacheCounters();
        checkPointerMonitor = dependencies.checkPointerCounters();
        logRotationMonitor = dependencies.logRotationCounters();
        idGeneratorFactory = dependencies.idGeneratorFactory();
        dependencyResolver = dependencies.getDependencyResolver();
        kernelContext = dependencies.kernelContext();
    }

    @Override
    public void init()
    {
        Log logger = logService.getUserLog( getClass() );
        logger.info( "Initiating metrics..." );

        // Setup metrics
        final MetricRegistry registry = new MetricRegistry();

        // Setup output
        boolean outputBuilt = new OutputBuilder( configuration, registry, logger, kernelContext, life ).build();

        if ( outputBuilt )
        {
            // Setup metric gathering
            boolean metricsBuilt = new Neo4jMetricsBuilder( registry, configuration, monitors,
                    dataSourceManager, transactionCounters, pageCacheCounters, checkPointerMonitor, logRotationMonitor,
                    idGeneratorFactory, dependencyResolver, logService, life ).build();
        }

        life.init();
    }

    @Override
    public void start()
    {
        life.start();
    }

    @Override
    public void stop()
    {
        life.stop();
    }

    @Override
    public void shutdown()
    {
        life.shutdown();
    }
}
