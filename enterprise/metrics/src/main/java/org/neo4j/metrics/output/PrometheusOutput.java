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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.HTTPServer;

import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;

/**
 * Prometheus poll data from clients, this exposes a HTTP endpoint at a configurable port.
 */
public class PrometheusOutput implements Lifecycle, EventReporter
{
    private final HostnamePort hostnamePort;
    private final MetricRegistry registry;
    private final Log logger;

    private HTTPServer server;
    private Map<String,Object> registeredEvents = new ConcurrentHashMap<>();
    private final MetricRegistry eventRegistry;

    public PrometheusOutput( HostnamePort hostnamePort, MetricRegistry registry, Log logger )
    {
        this.hostnamePort = hostnamePort;
        this.registry = registry;
        this.logger = logger;
        eventRegistry = new MetricRegistry();
    }

    @Override
    public void init()
    {
        // Setup prometheus collector
        CollectorRegistry.defaultRegistry.register( new DropwizardExports( registry ) );

        // We have to have a separate registry to not pollute the default one
        CollectorRegistry.defaultRegistry.register( new DropwizardExports( eventRegistry ) );
    }

    @Override
    public void start() throws Throwable
    {
        if ( server == null )
        {
            server = new HTTPServer( hostnamePort.getHost(), hostnamePort.getPort(), true );
            logger.info( "Started publishing Prometheus metrics at http://" + hostnamePort + "/metrics" );
        }
    }

    @Override
    public void stop()
    {
        if ( server != null )
        {
            server.stop();
            server = null;
            logger.info( "Stopped Prometheus endpoint at http://" + hostnamePort + "/metrics" );
        }
    }

    @Override
    public void shutdown()
    {
        this.stop();
    }

    @Override
    public void report( SortedMap<String,Gauge> gauges, SortedMap<String,Counter> counters,
            SortedMap<String,Histogram> histograms, SortedMap<String,Meter> meters, SortedMap<String,Timer> timers )
    {
        // Prometheus does not support events, just output the latest event that occurred
        String metricKey = gauges.firstKey();
        if ( !registeredEvents.containsKey( metricKey ) )
        {
            eventRegistry.register( metricKey, (Gauge) () -> registeredEvents.get( metricKey ) );
        }

        registeredEvents.put( metricKey, gauges.get( metricKey ).getValue() );
    }
}
