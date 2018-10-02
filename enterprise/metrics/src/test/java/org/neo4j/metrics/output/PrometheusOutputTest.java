/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLConnection;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.function.LongConsumer;

import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.logging.Log;

import static java.util.Collections.emptySortedMap;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class PrometheusOutputTest
{
    @Test
    public void eventsShouldBeRedirectedToGauges() throws Throwable
    {
        MetricRegistry registry = new MetricRegistry();
        DynamicAddressPrometheusOutput dynamicOutput = new DynamicAddressPrometheusOutput( "localhost", registry, mock( Log.class ) );

        LongConsumer callback = l ->
        {
            TreeMap<String,Gauge> gauges = new TreeMap<>();
            gauges.put( "my.event", () -> l );
            dynamicOutput.report( gauges, emptySortedMap(), emptySortedMap(), emptySortedMap(), emptySortedMap() );
        };

        callback.accept( 10 );

        dynamicOutput.init();
        dynamicOutput.start();

        String serverAddress = dynamicOutput.getServerAddress();
        assertTrue( getResponse( serverAddress ).contains( "my_event 10.0" ) );
        assertTrue( getResponse( serverAddress ).contains( "my_event 10.0" ) );

        callback.accept( 20 );
        assertTrue( getResponse( serverAddress ).contains( "my_event 20.0" ) );
        assertTrue( getResponse( serverAddress ).contains( "my_event 20.0" ) );
    }

    @Test
    public void metricsRegisteredAfterStartShouldBeIncluded() throws Throwable
    {
        MetricRegistry registry = new MetricRegistry();
        DynamicAddressPrometheusOutput dynamicOutput = new DynamicAddressPrometheusOutput( "localhost", registry, mock( Log.class ) );

        LongConsumer callback = l ->
        {
            TreeMap<String,Gauge> gauges = new TreeMap<>();
            gauges.put( "my.event", () -> l );
            dynamicOutput.report( gauges, emptySortedMap(), emptySortedMap(), emptySortedMap(), emptySortedMap() );
        };

        registry.register( "my.metric", (Gauge) () -> 10 );

        dynamicOutput.init();
        dynamicOutput.start();

        callback.accept( 20 );

        String serverAddress = dynamicOutput.getServerAddress();
        String response = getResponse( serverAddress );
        assertTrue( response.contains( "my_metric 10.0" ) );
        assertTrue( response.contains( "my_event 20.0" ) );
    }

    private static String getResponse( String serverAddress ) throws IOException
    {
        String url = "http://" + serverAddress + "/metrics";
        URLConnection connection = new URL( url ).openConnection();
        connection.setDoOutput( true );
        connection.connect();
        try ( Scanner s = new Scanner( connection.getInputStream(), "UTF-8" ).useDelimiter( "\\A" ) )
        {
            assertTrue( s.hasNext() );
            String ret = s.next();
            assertFalse( s.hasNext() );
            return ret;
        }
    }

    private static class DynamicAddressPrometheusOutput extends PrometheusOutput
    {
        DynamicAddressPrometheusOutput( String host, MetricRegistry registry, Log logger )
        {
            super( new HostnamePort( host ), registry, logger, mock( ConnectorPortRegister.class ) );
        }

        String getServerAddress()
        {
            InetSocketAddress address = server.getAddress();
            return address.getHostString() + ":" + address.getPort();
        }
    }
}
