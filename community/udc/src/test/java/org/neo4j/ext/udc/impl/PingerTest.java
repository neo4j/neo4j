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

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.localserver.LocalServerTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.ext.udc.UdcConstants;
import org.neo4j.helpers.HostnamePort;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.ext.udc.UdcConstants.ID;

/**
 * Unit tests for the UDC statistics pinger.
 */
public class PingerTest extends LocalServerTestBase
{
    private final String EXPECTED_KERNEL_VERSION = "1.0";
    private final String EXPECTED_STORE_ID = "CAFE";
    private String hostname = "localhost";
    private String serverUrl;

    PingerHandler handler;

    @Before
    @Override
    public void setUp() throws Exception
    {
        super.setUp();
        handler = new PingerHandler();
        this.serverBootstrap.registerHandler( "/*", handler );
        HttpHost target = start();
        hostname = target.getHostName();
        serverUrl = "http://" + hostname + ":" + target.getPort();
    }

    @Override
    @After
    public void shutDown() throws Exception
    {
        if ( httpclient != null )
        {
            httpclient.close();
        }
        if ( server != null )
        {
            server.shutdown( 0, TimeUnit.MILLISECONDS );
        }
    }

    @Test
    public void shouldRespondToHttpClientGet() throws Exception
    {
        try ( DefaultHttpClient httpclient = new DefaultHttpClient() )
        {
            HttpGet httpget = new HttpGet( serverUrl + "/?id=storeId+v=kernelVersion" );
            try ( CloseableHttpResponse response = httpclient.execute( httpget ) )
            {
                HttpEntity entity = response.getEntity();
                if ( entity != null )
                {
                    try ( InputStream instream = entity.getContent() )
                    {
                        byte[] tmp = new byte[2048];
                        while ( (instream.read( tmp )) != -1 )
                        {
                        }
                    }
                }
                assertThat( response, notNullValue() );
                assertThat( response.getStatusLine().getStatusCode(), is( HttpStatus.SC_OK ) );
            }
        }
    }

    @Test
    public void shouldPingServer() throws IOException
    {
        final HostnamePort hostURL = new HostnamePort( hostname, server.getLocalPort() );
        final Map<String,String> udcFields = new HashMap<>();
        udcFields.put( ID, EXPECTED_STORE_ID );
        udcFields.put( UdcConstants.VERSION, EXPECTED_KERNEL_VERSION );

        Pinger p = new Pinger( hostURL, new TestUdcCollector( udcFields ) );
        p.ping();

        Map<String,String> actualQueryMap = handler.getQueryMap();
        assertThat( actualQueryMap, notNullValue() );
        assertThat( actualQueryMap.get( ID ), is( EXPECTED_STORE_ID ) );
    }

    @Test
    public void shouldIncludePingCountInURI() throws IOException
    {
        final int EXPECTED_PING_COUNT = 16;
        final HostnamePort hostURL = new HostnamePort( hostname, server.getLocalPort() );
        final Map<String,String> udcFields = new HashMap<>();

        Pinger p = new Pinger( hostURL, new TestUdcCollector( udcFields ) );
        for ( int i = 0; i < EXPECTED_PING_COUNT; i++ )
        {
            p.ping();
        }

        assertThat( p.getPingCount(), is( equalTo( EXPECTED_PING_COUNT ) ) );

        Map<String,String> actualQueryMap = handler.getQueryMap();
        assertThat( actualQueryMap.get( UdcConstants.PING ), is( Integer.toString( EXPECTED_PING_COUNT ) ) );
    }

    @Test
    public void normalPingSequenceShouldBeOneThenTwoThenThreeEtc() throws Exception
    {
        int[] expectedSequence = {1, 2, 3, 4};
        final HostnamePort hostURL = new HostnamePort( hostname, server.getLocalPort() );
        final Map<String,String> udcFields = new HashMap<>();

        Pinger p = new Pinger( hostURL, new TestUdcCollector( udcFields ) );
        for ( int s : expectedSequence )
        {
            p.ping();
            int count = Integer.parseInt( handler.getQueryMap().get( UdcConstants.PING ) );
            assertEquals( s, count );
        }
    }

    static class TestUdcCollector implements UdcInformationCollector
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
