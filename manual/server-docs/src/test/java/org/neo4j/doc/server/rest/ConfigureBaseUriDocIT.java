/*
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.neo4j.doc.server.rest;

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

public class ConfigureBaseUriDocIT extends org.neo4j.doc.server.rest.AbstractRestFunctionalTestBase
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
