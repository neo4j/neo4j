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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;

import java.io.IOException;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;

public class GraphiteOutput implements Lifecycle, EventReporter
{
    private final HostnamePort hostnamePort;
    private final long period;
    private final MetricRegistry registry;
    private final Log logger;
    private final String prefix;

    private GraphiteReporter graphiteReporter;

    public GraphiteOutput( HostnamePort hostnamePort, long period, MetricRegistry registry, Log logger, String prefix )
    {
        this.hostnamePort = hostnamePort;
        this.period = period;
        this.registry = registry;
        this.logger = logger;
        this.prefix = prefix;
    }

    @Override
    public void init()
    {
        // Setup Graphite reporting
        final Graphite graphite = new Graphite( hostnamePort.getHost(), hostnamePort.getPort() );

        graphiteReporter = GraphiteReporter.forRegistry( registry )
                .prefixedWith( prefix )
                .convertRatesTo( TimeUnit.SECONDS )
                .convertDurationsTo( TimeUnit.MILLISECONDS )
                .filter( MetricFilter.ALL )
                .build( graphite );
    }

    @Override
    public void start()
    {
        graphiteReporter.start( period, TimeUnit.MILLISECONDS );
        logger.info( "Sending metrics to Graphite server at " + hostnamePort );
    }

    @Override
    public void stop() throws IOException
    {
        graphiteReporter.close();
    }

    @Override
    public void shutdown()
    {
        graphiteReporter = null;
    }

    @Override
    public void report( SortedMap<String,Gauge> gauges, SortedMap<String,Counter> counters,
            SortedMap<String,Histogram> histograms, SortedMap<String,Meter> meters, SortedMap<String,Timer> timers )
    {
        /*
         * The synchronized is needed here since the `report` method called below is also called by the recurring
         * scheduled thread.  In order to avoid races with that thread we synchronize on the same monitor
         * before reporting.
         */
        synchronized ( graphiteReporter )
        {
            graphiteReporter.report( gauges, counters, histograms, meters, timers );
        }
    }
}
