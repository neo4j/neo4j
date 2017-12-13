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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.function.LongConsumer;

import org.neo4j.helpers.HostnamePort;
import org.neo4j.logging.Log;
import org.neo4j.ports.allocation.PortAuthority;

import static java.util.Collections.emptySortedMap;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

public class PrometheusOutputTest
{
    @Test
    public void eventsShouldBeRedirectedToGauges() throws Throwable
    {
        String serverAddress = "localhost:" + PortAuthority.allocatePort();
        MetricRegistry registry = new MetricRegistry();
        PrometheusOutput prometheusOutput =
                new PrometheusOutput( new HostnamePort( serverAddress ), registry, Mockito.mock( Log.class ) );

        LongConsumer callback = l ->
        {
            TreeMap<String,Gauge> gauges = new TreeMap<>();
            gauges.put( "my.event", () -> l );
            prometheusOutput.report( gauges, emptySortedMap(), emptySortedMap(), emptySortedMap(), emptySortedMap() );
        };

        callback.accept( 10 );

        prometheusOutput.init();
        prometheusOutput.start();

        assertTrue( getResponse( serverAddress ).contains( "my_event 10.0" ) );
        assertTrue( getResponse( serverAddress ).contains( "my_event 10.0" ) );

        callback.accept( 20 );
        assertTrue( getResponse( serverAddress ).contains( "my_event 20.0" ) );
        assertTrue( getResponse( serverAddress ).contains( "my_event 20.0" ) );
    }

    @Test
    public void metricsRegisteredAfterStartShouldBeIncluded() throws Throwable
    {
        String serverAddress = "localhost:" + PortAuthority.allocatePort();
        MetricRegistry registry = new MetricRegistry();
        PrometheusOutput prometheusOutput =
                new PrometheusOutput( new HostnamePort( serverAddress ), registry, Mockito.mock( Log.class ) );

        LongConsumer callback = l ->
        {
            TreeMap<String,Gauge> gauges = new TreeMap<>();
            gauges.put( "my.event", () -> l );
            prometheusOutput.report( gauges, emptySortedMap(), emptySortedMap(), emptySortedMap(), emptySortedMap() );
        };

        registry.register( "my.metric", (Gauge) () -> 10 );

        prometheusOutput.init();
        prometheusOutput.start();

        callback.accept( 20 );

        String response = getResponse( serverAddress );
        assertTrue( response.contains( "my_metric 10.0" ) );
        assertTrue( response.contains( "my_event 20.0" ) );
    }

    private String getResponse( String serverAddress ) throws IOException
    {
        String url = "http://" + serverAddress + "/metrics";
        URLConnection connection = new URL( url ).openConnection();
        connection.setDoOutput( true );
        connection.connect();
        Scanner s = new Scanner( connection.getInputStream(), "UTF-8" ).useDelimiter( "\\A" );

        assertTrue( s.hasNext() );
        String ret = s.next();
        assertFalse( s.hasNext() );
        s.close();
        return ret;
    }
}
