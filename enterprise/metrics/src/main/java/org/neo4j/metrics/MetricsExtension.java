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
package org.neo4j.metrics;

import com.codahale.metrics.MetricRegistry;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.io.pagecache.monitoring.PageCacheMonitor;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.LogRotationMonitor;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.transaction.TransactionCounters;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointerMonitor;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.metrics.output.CsvOutput;
import org.neo4j.metrics.output.GangliaOutput;
import org.neo4j.metrics.output.GraphiteOutput;
import org.neo4j.metrics.source.Neo4jMetricsFactory;

public class MetricsExtension implements Lifecycle
{
    private final LifeSupport life;
    private final LogService logService;
    private final Config configuration;
    private final Monitors monitors;
    private final TransactionCounters transactionCounters;
    private final PageCacheMonitor pageCacheCounters;
    private final CheckPointerMonitor checkPointerMonitor;
    private final IdGeneratorFactory idGeneratorFactory;
    private final LogRotationMonitor logRotationMonitor;
    private final KernelContext kernelContext;

    public MetricsExtension( MetricsKernelExtensionFactory.Dependencies dependencies )
    {
        life = new LifeSupport();
        logService = dependencies.logService();
        configuration = dependencies.configuration();
        monitors = dependencies.monitors();
        transactionCounters = dependencies.transactionCounters();
        pageCacheCounters = dependencies.pageCacheCounters();
        checkPointerMonitor = dependencies.checkPointerCounters();
        logRotationMonitor = dependencies.logRotationCounters();
        idGeneratorFactory = dependencies.idGeneratorFactory();
        kernelContext = dependencies.kernelContext();
    }

    @Override
    public void init() throws Throwable
    {
        Log logger = logService.getUserLog( getClass() );

        // Setup metrics
        final MetricRegistry registry = new MetricRegistry();

        logger.info( "Initiating metrics.." );

        // Setup output
        String prefix = computePrefix( configuration );

        life.add( new CsvOutput( configuration, registry, logger, kernelContext ) );
        life.add( new GraphiteOutput( configuration, registry, logger, prefix ) );
        life.add( new GangliaOutput( configuration, registry, logger, prefix ) );

        // Setup metric gathering
        Neo4jMetricsFactory factory = new Neo4jMetricsFactory( registry, configuration, monitors,
                transactionCounters, pageCacheCounters, checkPointerMonitor, logRotationMonitor, idGeneratorFactory );
        life.add( factory.newInstance() );

        life.init();
    }

    @Override
    public void start() throws Throwable
    {
        life.start();
    }

    @Override
    public void stop() throws Throwable
    {
        life.stop();
    }

    @Override
    public void shutdown() throws Throwable
    {
        life.shutdown();
    }

    private String computePrefix( Config config )
    {
        String prefix = config.get( MetricsSettings.metricsPrefix );

        if ( prefix.equals( MetricsSettings.metricsPrefix.getDefaultValue() ) )
        {
            // If default name and in HA, try to figure out a nicer name
            if ( config.getParams().containsKey( ClusterSettings.server_id.name() ) )
            {
                prefix += "." + config.get( ClusterSettings.cluster_name );
                prefix += "." + config.get( ClusterSettings.server_id );
            }
        }
        return prefix;
    }
}
