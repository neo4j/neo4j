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
package org.neo4j.metrics.output;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ganglia.GangliaReporter;
import info.ganglia.gmetric4j.gmetric.GMetric;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;

import static org.neo4j.metrics.MetricsSettings.gangliaEnabled;
import static org.neo4j.metrics.MetricsSettings.gangliaInterval;
import static org.neo4j.metrics.MetricsSettings.gangliaServer;

public class GangliaOutput implements Closeable
{
    private GangliaReporter gangliaReporter;

    public GangliaOutput( Config config, MetricRegistry registry, Log logger, String prefix )
    {
        try
        {
            if ( config.get( gangliaEnabled ) )
            {
                // Setup Ganglia reporting

                final HostnamePort hostnamePort = config.get( gangliaServer );
                final GMetric ganglia = new GMetric(
                        hostnamePort.getHost(),
                        hostnamePort.getPort(),
                        GMetric.UDPAddressingMode.MULTICAST,
                        1 );

                gangliaReporter = GangliaReporter.forRegistry( registry )
                        .prefixedWith( prefix )
                        .convertRatesTo( TimeUnit.SECONDS )
                        .convertDurationsTo( TimeUnit.MILLISECONDS )
                        .filter( MetricFilter.ALL )
                        .build( ganglia );

                // Start Ganglia reporter
                gangliaReporter.start( config.get( gangliaInterval ), TimeUnit.MILLISECONDS );

                logger.info( "Sending metrics to Ganglia server at " + hostnamePort );
            }
        }
        catch ( IOException e )
        {
            logger.error( "Could not start Ganglia output", e );
        }
    }

    public void close() throws IOException
    {
        if ( gangliaReporter != null )
        {
            gangliaReporter.close();
            gangliaReporter = null;
        }
    }
}
