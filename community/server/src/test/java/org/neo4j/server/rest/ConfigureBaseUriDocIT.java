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
package org.neo4j.server.rest;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import org.neo4j.server.helpers.FunctionalTestHelper;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ConfigureBaseUriDocIT extends AbstractRestFunctionalTestBase
{
    private static FunctionalTestHelper functionalTestHelper;

    @BeforeClass
    public static void setupServer() throws IOException
    {
        functionalTestHelper = new FunctionalTestHelper( server() );
    }

    @Test
    public void shouldForwardHttpAndHost() throws Exception
    {
        URI rootUri = functionalTestHelper.baseUri();

        HttpClient httpclient = new DefaultHttpClient();
        try
        {
            HttpGet httpget = new HttpGet( rootUri );

            httpget.setHeader( "Accept", "application/json" );
            httpget.setHeader( "X-Forwarded-Host", "foobar.com" );
            httpget.setHeader( "X-Forwarded-Proto", "http" );

            HttpResponse response = httpclient.execute( httpget );

            String length = response.getHeaders( "CONTENT-LENGTH" )[0].getValue();
            byte[] data = new byte[Integer.valueOf( length )];
            response.getEntity().getContent().read( data );

            String responseEntityBody = new String( data );

            assertTrue( responseEntityBody.contains( "http://foobar.com" ) );
            assertFalse( responseEntityBody.contains( "localhost" ) );
        }
        finally
        {
            httpclient.getConnectionManager().shutdown();
        }

    }

    @Test
    public void shouldForwardHttpsAndHost() throws Exception
    {
        URI rootUri = functionalTestHelper.baseUri();

        HttpClient httpclient = new DefaultHttpClient();
        try
        {
            HttpGet httpget = new HttpGet( rootUri );

            httpget.setHeader( "Accept", "application/json" );
            httpget.setHeader( "X-Forwarded-Host", "foobar.com" );
            httpget.setHeader( "X-Forwarded-Proto", "https" );

            HttpResponse response = httpclient.execute( httpget );

            String length = response.getHeaders( "CONTENT-LENGTH" )[0].getValue();
            byte[] data = new byte[Integer.valueOf( length )];
            response.getEntity().getContent().read( data );

            String responseEntityBody = new String( data );

            assertTrue( responseEntityBody.contains( "https://foobar.com" ) );
            assertFalse( responseEntityBody.contains( "localhost" ) );
        }
        finally
        {
            httpclient.getConnectionManager().shutdown();
        }
    }

    @Test
    public void shouldForwardHttpAndHostOnDifferentPort() throws Exception
    {

        URI rootUri = functionalTestHelper.baseUri();

        HttpClient httpclient = new DefaultHttpClient();
        try
        {
            HttpGet httpget = new HttpGet( rootUri );

            httpget.setHeader( "Accept", "application/json" );
            httpget.setHeader( "X-Forwarded-Host", "foobar.com:9999" );
            httpget.setHeader( "X-Forwarded-Proto", "http" );

            HttpResponse response = httpclient.execute( httpget );

            String length = response.getHeaders( "CONTENT-LENGTH" )[0].getValue();
            byte[] data = new byte[Integer.valueOf( length )];
            response.getEntity().getContent().read( data );

            String responseEntityBody = new String( data );

            assertTrue( responseEntityBody.contains( "http://foobar.com:9999" ) );
            assertFalse( responseEntityBody.contains( "localhost" ) );
        }
        finally
        {
            httpclient.getConnectionManager().shutdown();
        }
    }

    @Test
    public void shouldForwardHttpAndFirstHost() throws Exception
    {
        URI rootUri = functionalTestHelper.baseUri();

        HttpClient httpclient = new DefaultHttpClient();
        try
        {
            HttpGet httpget = new HttpGet( rootUri );

            httpget.setHeader( "Accept", "application/json" );
            httpget.setHeader( "X-Forwarded-Host", "foobar.com, bazbar.com" );
            httpget.setHeader( "X-Forwarded-Proto", "http" );

            HttpResponse response = httpclient.execute( httpget );

            String length = response.getHeaders( "CONTENT-LENGTH" )[0].getValue();
            byte[] data = new byte[Integer.valueOf( length )];
            response.getEntity().getContent().read( data );

            String responseEntityBody = new String( data );

            assertTrue( responseEntityBody.contains( "http://foobar.com" ) );
            assertFalse( responseEntityBody.contains( "localhost" ) );
        }
        finally
        {
            httpclient.getConnectionManager().shutdown();
        }

    }

    @Test
    public void shouldForwardHttpsAndHostOnDifferentPort() throws Exception
    {
        URI rootUri = functionalTestHelper.baseUri();

        HttpClient httpclient = new DefaultHttpClient();
        try
        {
            HttpGet httpget = new HttpGet( rootUri );

            httpget.setHeader( "Accept", "application/json" );
            httpget.setHeader( "X-Forwarded-Host", "foobar.com:9999" );
            httpget.setHeader( "X-Forwarded-Proto", "https" );

            HttpResponse response = httpclient.execute( httpget );

            String length = response.getHeaders( "CONTENT-LENGTH" )[0].getValue();
            byte[] data = new byte[Integer.valueOf( length )];
            response.getEntity().getContent().read( data );

            String responseEntityBody = new String( data );

            assertTrue( responseEntityBody.contains( "https://foobar.com:9999" ) );
            assertFalse( responseEntityBody.contains( "localhost" ) );
        }
        finally
        {
            httpclient.getConnectionManager().shutdown();
        }
    }


    @Test
    public void shouldUseRequestUriWhenNoXForwardHeadersPresent() throws Exception
    {
        URI rootUri = functionalTestHelper.baseUri();

        HttpClient httpclient = new DefaultHttpClient();
        try
        {
            HttpGet httpget = new HttpGet( rootUri );

            httpget.setHeader( "Accept", "application/json" );

            HttpResponse response = httpclient.execute( httpget );

            String length = response.getHeaders( "CONTENT-LENGTH" )[0].getValue();
            byte[] data = new byte[Integer.valueOf( length )];
            response.getEntity().getContent().read( data );

            String responseEntityBody = new String( data );

            assertFalse( responseEntityBody.contains( "https://foobar.com" ) );
            assertFalse( responseEntityBody.contains( ":0" ) );
            assertTrue( responseEntityBody.contains( "localhost" ) );
        }
        finally
        {
            httpclient.getConnectionManager().shutdown();
        }
    }
}
