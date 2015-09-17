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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.metrics.output.CsvOutput;
import org.neo4j.metrics.output.GangliaOutput;
import org.neo4j.metrics.output.GraphiteOutput;
import org.neo4j.metrics.source.Neo4jMetrics;

public class MetricsExtension extends LifecycleAdapter
{
    private MetricsKernelExtensionFactory.Dependencies dependencies;
    private final Log logger;

    private Config config;

    private List<Closeable> services = new ArrayList<>();

    public MetricsExtension( MetricsKernelExtensionFactory.Dependencies dependencies )
    {
        this.dependencies = dependencies;
        logger = dependencies.logService().getUserLog( getClass() );
        config = dependencies.configuration();
    }

    public void start() throws Throwable
    {
        // Setup metrics
        final MetricRegistry registry = new MetricRegistry();

        logger.info( "Initiating metrics.." );

        // Setup output
        String prefix = computePrefix();

        services.add( new CsvOutput( config, registry, logger ) );
        services.add( new GraphiteOutput( config, registry, logger, prefix ) );
        services.add( new GangliaOutput( config, registry, logger, prefix ) );

        // Setup metric gathering
        services.add( new Neo4jMetrics( registry, dependencies ) );
    }

    public void stop() throws Throwable
    {
        for ( Closeable service : services )
        {
            try
            {
                service.close();
            }
            catch ( IOException e )
            {
                logger.warn( "Could not close", e );
            }
        }
    }

    private String computePrefix()
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
