/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.metrics.output.CompositeEventReporter;
import org.neo4j.metrics.output.EventReporterBuilder;
import org.neo4j.metrics.source.Neo4jMetricsBuilder;
import org.neo4j.scheduler.JobScheduler;

public class MetricsExtension implements Lifecycle
{
    private final LifeSupport life = new LifeSupport();
    private Log logger;
    private CompositeEventReporter reporter;
    private boolean metricsBuilt;

    MetricsExtension( KernelContext kernelContext, MetricsKernelExtensionFactory.Dependencies dependencies )
    {
        LogService logService = dependencies.logService();
        Config configuration = dependencies.configuration();
        FileSystemAbstraction fileSystem = dependencies.fileSystemAbstraction();
        JobScheduler scheduler = dependencies.scheduler();
        logger = logService.getUserLog( getClass() );

        MetricRegistry registry = new MetricRegistry();
        reporter = new EventReporterBuilder( configuration, registry, logger, kernelContext, life, fileSystem,
                scheduler ).build();
        metricsBuilt = new Neo4jMetricsBuilder( registry, reporter, configuration, logService, kernelContext,
                                                dependencies, life ).build();
    }

    @Override
    public void init()
    {
        logger.info( "Initiating metrics..." );
        if ( metricsBuilt && reporter.isEmpty() )
        {
            logger.warn( "Several metrics were enabled but no exporting option was configured to report values to. " +
                         "Disabling kernel metrics extension." );
            life.clear();
        }

        if ( !reporter.isEmpty() && !metricsBuilt )
        {
            logger.warn( "Exporting tool have been configured to report values to but no metrics were enabled. " +
                         "Disabling kernel metrics extension." );
            life.clear();
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
