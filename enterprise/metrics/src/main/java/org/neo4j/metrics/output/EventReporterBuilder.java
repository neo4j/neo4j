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
package org.neo4j.metrics.output;

import com.codahale.metrics.MetricRegistry;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.Log;
import org.neo4j.metrics.MetricsSettings;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.metrics.MetricsSettings.csvEnabled;
import static org.neo4j.metrics.MetricsSettings.graphiteEnabled;
import static org.neo4j.metrics.MetricsSettings.graphiteInterval;
import static org.neo4j.metrics.MetricsSettings.graphiteServer;
import static org.neo4j.metrics.MetricsSettings.prometheusEnabled;
import static org.neo4j.metrics.MetricsSettings.prometheusEndpoint;

public class EventReporterBuilder
{
    private final Config config;
    private final MetricRegistry registry;
    private final Log logger;
    private final KernelContext kernelContext;
    private final LifeSupport life;
    private FileSystemAbstraction fileSystem;
    private JobScheduler scheduler;

    public EventReporterBuilder( Config config, MetricRegistry registry, Log logger, KernelContext kernelContext,
            LifeSupport life, FileSystemAbstraction fileSystem, JobScheduler scheduler )
    {
        this.config = config;
        this.registry = registry;
        this.logger = logger;
        this.kernelContext = kernelContext;
        this.life = life;
        this.fileSystem = fileSystem;
        this.scheduler = scheduler;
    }

    public CompositeEventReporter build()
    {
        CompositeEventReporter reporter = new CompositeEventReporter();
        final String prefix = createMetricsPrefix( config );
        if ( config.get( csvEnabled ) )
        {
            CsvOutput csvOutput = new CsvOutput( config, registry, logger, kernelContext, fileSystem, scheduler );
            reporter.add( csvOutput );
            life.add( csvOutput );
        }

        if ( config.get( graphiteEnabled ) )
        {
            HostnamePort server = config.get( graphiteServer );
            long period = config.get( graphiteInterval ).toMillis();
            GraphiteOutput graphiteOutput = new GraphiteOutput( server, period, registry, logger, prefix );
            reporter.add( graphiteOutput );
            life.add( graphiteOutput );
        }

        if ( config.get( prometheusEnabled ) )
        {
            HostnamePort server = config.get( prometheusEndpoint );
            PrometheusOutput prometheusOutput = new PrometheusOutput( server, registry, logger );
            reporter.add( prometheusOutput );
            life.add( prometheusOutput );
        }

        return reporter;
    }

    private String createMetricsPrefix( Config config )
    {
        String prefix = config.get( MetricsSettings.metricsPrefix );

        if ( prefix.equals( MetricsSettings.metricsPrefix.getDefaultValue() ) )
        {
            // If default name and in HA, try to figure out a nicer name
            if ( config.isConfigured( ClusterSettings.server_id ) )
            {
                prefix += "." + config.get( ClusterSettings.cluster_name );
                prefix += "." + config.get( ClusterSettings.server_id );
            }
        }
        return prefix;
    }
}
