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
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;

import static org.neo4j.metrics.MetricsSettings.graphiteEnabled;
import static org.neo4j.metrics.MetricsSettings.graphiteInterval;
import static org.neo4j.metrics.MetricsSettings.graphiteServer;

public class GraphiteOutput implements Closeable
{
    private GraphiteReporter graphiteReporter;

    public GraphiteOutput( Config config, MetricRegistry registry, Log logger, String prefix )
    {
        if ( config.get( graphiteEnabled ) )
        {
            // Setup Graphite reporting

            final Graphite graphite = new Graphite( new InetSocketAddress( config.get( graphiteServer ).getHost(),
                    config.get( graphiteServer ).getPort() ) );
            graphiteReporter = GraphiteReporter.forRegistry( registry )
                    .prefixedWith( prefix )
                    .convertRatesTo( TimeUnit.SECONDS )
                    .convertDurationsTo( TimeUnit.MILLISECONDS )
                    .filter( MetricFilter.ALL )
                    .build( graphite );
            // Start Graphite reporter
            graphiteReporter.start( config.get( graphiteInterval ), TimeUnit.MILLISECONDS );

            logger.info( "Sending metrics to Graphite server at " + config.get( graphiteServer ) );
        }
    }

    public void close() throws IOException
    {
        if ( graphiteReporter != null )
        {
            graphiteReporter.close();
            graphiteReporter = null;
        }
    }
}
