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
package org.neo4j.metrics.output;

import com.codahale.metrics.MetricRegistry;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.Log;
import org.neo4j.metrics.MetricsSettings;

import static org.neo4j.metrics.MetricsSettings.csvEnabled;
import static org.neo4j.metrics.MetricsSettings.graphiteEnabled;
import static org.neo4j.metrics.MetricsSettings.graphiteInterval;
import static org.neo4j.metrics.MetricsSettings.graphiteServer;

public class EventReporterBuilder
{
    private final Config config;
    private final MetricRegistry registry;
    private final Log logger;
    private final KernelContext kernelContext;
    private final LifeSupport life;

    public EventReporterBuilder( Config config, MetricRegistry registry, Log logger, KernelContext kernelContext,
            LifeSupport life )
    {
        this.config = config;
        this.registry = registry;
        this.logger = logger;
        this.kernelContext = kernelContext;
        this.life = life;
    }

    public CompositeEventReporter build()
    {
        CompositeEventReporter reporter = new CompositeEventReporter();
        final String prefix = createMetricsPrefix( config );
        if ( config.get( csvEnabled ) )
        {
            CsvOutput csvOutput = new CsvOutput( config, registry, logger, kernelContext );
            reporter.add( csvOutput );
            life.add( csvOutput );
        }

        if ( config.get( graphiteEnabled ) )
        {
            HostnamePort server = config.get( graphiteServer );
            long period = config.get( graphiteInterval );
            GraphiteOutput graphiteOutput = new GraphiteOutput( server, period, registry, logger, prefix );
            reporter.add( graphiteOutput );
            life.add( graphiteOutput );
        }

        return reporter;
    }

    private String createMetricsPrefix( Config config )
    {
        String prefix = config.get( MetricsSettings.metricsPrefix );

        if ( prefix.equals( MetricsSettings.metricsPrefix.getDefaultValue() ) )
        {
            // If default name and in HA, try to figure out a nicer name
            if ( config.getRaw( ClusterSettings.server_id.name() ).isPresent() )
            {
                prefix += "." + config.get( ClusterSettings.cluster_name );
                prefix += "." + config.get( ClusterSettings.server_id );
            }
        }
        return prefix;
    }
}
