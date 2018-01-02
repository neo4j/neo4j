/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.localserver.LocalTestServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.ext.udc.UdcConstants;
import org.neo4j.helpers.HostnamePort;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import static org.neo4j.ext.udc.UdcConstants.ID;

/**
 * Unit tests for the UDC statistics pinger.
 */
public class PingerTest
{
    private final String EXPECTED_KERNEL_VERSION = "1.0";
    private final String EXPECTED_STORE_ID = "CAFE";
    private String hostname = "localhost";
    private String serverUrl;

    private LocalTestServer server = null;

    PingerHandler handler;

    @Before
    public void setUp() throws Exception
    {
        handler = new PingerHandler();
        server = new LocalTestServer( null, null );
        server.register( "/*", handler );
        server.start();

        hostname = server.getServiceAddress().getHostName();
        serverUrl = "http://" + hostname + ":" + server.getServiceAddress().getPort();
    }

    /**
     * Test that the LocalTestServer actually works.
     *
     * @throws Exception
     */
    @Test
    public void shouldRespondToHttpClientGet() throws Exception
    {
        HttpClient httpclient = new DefaultHttpClient();
        HttpGet httpget = new HttpGet( serverUrl + "/?id=storeId+v=kernelVersion" );
        HttpResponse response = httpclient.execute( httpget );
        HttpEntity entity = response.getEntity();
        if ( entity != null )
        {
            InputStream instream = entity.getContent();
            int l;
            byte[] tmp = new byte[2048];
            while ( (l = instream.read( tmp )) != -1 )
            {
            }
        }
        assertThat( response, notNullValue() );
        assertThat( response.getStatusLine().getStatusCode(), is( HttpStatus.SC_OK ) );
    }

    static class TestUdcCollector implements UdcInformationCollector
    {
        private final Map<String, String> params;
        private boolean crashed;


        TestUdcCollector( Map<String, String> params )
        {
            this.params = params;
        }

        @Override
        public Map<String, String> getUdcParams()
        {
            return params;
        }

        @Override
        public String getStoreId()
        {
            return getUdcParams().get( ID );
        }

        @Override
        public boolean getCrashPing()
        {
            return crashed;
        }

        public UdcInformationCollector withCrash()
        {
            crashed = true;
            return this;
        }
    }

    @Test
    public void shouldPingServer()
    {
        final HostnamePort hostURL = new HostnamePort( hostname, server.getServiceAddress().getPort() );
        final Map<String, String> udcFields = new HashMap<String, String>();
        udcFields.put( ID, EXPECTED_STORE_ID );
        udcFields.put( UdcConstants.VERSION, EXPECTED_KERNEL_VERSION );

        Pinger p = new Pinger( hostURL, new TestUdcCollector( udcFields ) );
        Exception thrownException = null;
        try
        {
            p.ping();
        }
        catch ( IOException e )
        {
            thrownException = e;
            e.printStackTrace();
        }
        assertThat( thrownException, nullValue() );

        Map<String, String> actualQueryMap = handler.getQueryMap();
        assertThat( actualQueryMap, notNullValue() );
        assertThat( actualQueryMap.get( ID ), is( EXPECTED_STORE_ID ) );

    }

    @Test
    public void shouldIncludePingCountInURI() throws IOException
    {
        final int EXPECTED_PING_COUNT = 16;
        final HostnamePort hostURL = new HostnamePort( hostname, server.getServiceAddress().getPort() );
        final Map<String, String> udcFields = new HashMap<String, String>();

        Pinger p = new Pinger( hostURL, new TestUdcCollector( udcFields ) );
        for ( int i = 0; i < EXPECTED_PING_COUNT; i++ )
        {
            p.ping();
        }

        assertThat( p.getPingCount(), is( equalTo( EXPECTED_PING_COUNT ) ) );

        Map<String, String> actualQueryMap = handler.getQueryMap();
        assertThat( actualQueryMap.get( UdcConstants.PING ), is( Integer.toString( EXPECTED_PING_COUNT ) ) );
    }

    @Test
    public void normalPingSequenceShouldBeOneThenTwoThenThreeEtc() throws Exception
    {
        int[] expectedSequence = {1, 2, 3, 4};
        final HostnamePort hostURL = new HostnamePort( hostname, server.getServiceAddress().getPort() );
        final Map<String, String> udcFields = new HashMap<String, String>();

        Pinger p = new Pinger( hostURL, new TestUdcCollector( udcFields ) );
        for ( int i = 0; i < expectedSequence.length; i++ )
        {
            p.ping();
            int count = Integer.parseInt( handler.getQueryMap().get( UdcConstants.PING ) );
            assertEquals( expectedSequence[i], count );
        }
    }

    @Test
    public void crashPingSequenceShouldBeMinusOneThenTwoThenThreeEtc() throws Exception
    {
        int[] expectedSequence = {-1, 2, 3, 4};
        final HostnamePort hostURL = new HostnamePort( hostname, server.getServiceAddress().getPort() );
        final Map<String, String> udcFields = new HashMap<String, String>();

        Pinger p = new Pinger( hostURL, new TestUdcCollector( udcFields ).withCrash() );
        for ( int i = 0; i < expectedSequence.length; i++ )
        {
            p.ping();
            int count = Integer.parseInt( handler.getQueryMap().get( UdcConstants.PING ) );
            assertEquals( expectedSequence[i], count );
        }
    }

    @After
    public void tearDown() throws Exception
    {
        server.stop();
    }
}
