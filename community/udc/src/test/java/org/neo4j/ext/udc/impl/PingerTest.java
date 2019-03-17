/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.ext.udc.impl;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpRequest;
import java.util.Map;

import org.neo4j.ext.udc.UdcConstants;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.SocketAddress;
import org.neo4j.io.IOUtils;
import org.neo4j.test.extension.SuppressOutputExtension;

import static java.net.http.HttpClient.newHttpClient;
import static java.net.http.HttpResponse.BodyHandlers.discarding;
import static java.util.Collections.emptyMap;
import static org.eclipse.jetty.http.HttpStatus.OK_200;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.ext.udc.UdcConstants.ID;

@ExtendWith( SuppressOutputExtension.class )
class PingerTest
{
    private static final String EXPECTED_KERNEL_VERSION = "1.0";
    private static final String EXPECTED_STORE_ID = "CAFE";

    private PingerServer server;
    private String serverUrl;

    @BeforeEach
    void beforeEach() throws Exception
    {
        server = new PingerServer();
        serverUrl = "http://" + SocketAddress.format( server.getHost(), server.getPort() );
    }

    @AfterEach
    void afterEach()
    {
        IOUtils.closeAllSilently( server );
    }

    @Test
    void shouldRespondToHttpClientGet() throws Exception
    {
        var request = HttpRequest.newBuilder( URI.create( serverUrl + "/?id=storeId+v=kernelVersion" ) ).GET().build();

        var response = newHttpClient().send( request, discarding() );

        assertEquals( OK_200, response.statusCode() );
    }

    @Test
    void shouldPingServer() throws IOException
    {
        var hostURL = new HostnamePort( server.getHost(), server.getPort() );
        var udcFields = Map.of(
                ID, EXPECTED_STORE_ID,
                UdcConstants.VERSION, EXPECTED_KERNEL_VERSION );

        new Pinger( hostURL, new TestUdcCollector( udcFields ) ).ping();

        var actualQueryMap = server.getQueryMap();
        assertEquals( EXPECTED_STORE_ID, actualQueryMap.get( ID ) );
    }

    @Test
    void shouldIncludePingCountInURI() throws IOException
    {
        var expectedPingCount = 16;
        var hostURL = new HostnamePort( server.getHost(), server.getPort() );

        var pinger = new Pinger( hostURL, new TestUdcCollector( emptyMap() ) );
        for ( int i = 0; i < expectedPingCount; i++ )
        {
            pinger.ping();
        }

        assertEquals( expectedPingCount, pinger.getPingCount() );

        var actualQueryMap = server.getQueryMap();
        assertEquals( Integer.toString( expectedPingCount ), actualQueryMap.get( UdcConstants.PING ) );
    }

    @Test
    void normalPingSequenceShouldBeOneThenTwoThenThreeEtc() throws Exception
    {
        var hostURL = new HostnamePort( server.getHost(), server.getPort() );

        var pinger = new Pinger( hostURL, new TestUdcCollector( emptyMap() ) );

        for ( int i = 1; i < 5; i++ )
        {
            pinger.ping();

            int count = Integer.parseInt( server.getQueryMap().get( UdcConstants.PING ) );
            assertEquals( i, count );
        }
    }

    private static class TestUdcCollector implements UdcInformationCollector
    {
        private final Map<String,String> params;

        TestUdcCollector( Map<String,String> params )
        {
            this.params = params;
        }

        @Override
        public Map<String,String> getUdcParams()
        {
            return params;
        }

        @Override
        public String getStoreId()
        {
            return getUdcParams().get( ID );
        }

    }
}
