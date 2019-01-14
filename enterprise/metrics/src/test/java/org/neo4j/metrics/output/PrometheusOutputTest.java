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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
