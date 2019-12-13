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
package org.neo4j.server.rest;

import org.junit.BeforeClass;
import org.junit.Test;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import org.neo4j.server.helpers.FunctionalTestHelper;

import static java.lang.Integer.parseInt;
import static java.net.http.HttpClient.newHttpClient;
import static java.net.http.HttpResponse.BodyHandlers.ofByteArray;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConfigureBaseUriIT extends AbstractRestFunctionalTestBase
{
    private static FunctionalTestHelper functionalTestHelper;
    private static HttpClient httpClient;

    @BeforeClass
    public static void setupServer()
    {
        functionalTestHelper = new FunctionalTestHelper( container() );
        httpClient = newHttpClient();
    }

    @Test
    public void shouldForwardHttpAndHost() throws Exception
    {
        var request = HttpRequest.newBuilder( functionalTestHelper.baseUri() )
                .GET()
                .header( "Accept", "application/json" )
                .header( "X-Forwarded-Host", "foobar.com" )
                .header( "X-Forwarded-Proto", "http" )
                .build();

        var response = httpClient.send( request, ofByteArray() );

        verifyContentLength( response );

        var responseBodyString = new String( response.body() );
        assertThat( responseBodyString, containsString( "http://foobar.com" ) );
        assertThat( responseBodyString, not( containsString( "http://localhost" ) ) );
    }

    @Test
    public void shouldForwardHttpsAndHost() throws Exception
    {
        var request = HttpRequest.newBuilder( functionalTestHelper.baseUri() )
                .GET()
                .header( "Accept", "application/json" )
                .header( "X-Forwarded-Host", "foobar.com" )
                .header( "X-Forwarded-Proto", "https" )
                .build();

        var response = httpClient.send( request, ofByteArray() );

        verifyContentLength( response );

        var responseBodyString = new String( response.body() );
        assertThat( responseBodyString, containsString( "https://foobar.com" ) );
        assertThat( responseBodyString, not( containsString( "https://localhost" ) ) );
    }

    @Test
    public void shouldForwardHttpAndHostOnDifferentPort() throws Exception
    {
        var request = HttpRequest.newBuilder( functionalTestHelper.baseUri() )
                .GET()
                .header( "Accept", "application/json" )
                .header( "X-Forwarded-Host", "foobar.com:9999" )
                .header( "X-Forwarded-Proto", "http" )
                .build();

        var response = httpClient.send( request, ofByteArray() );

        verifyContentLength( response );

        var responseBodyString = new String( response.body() );
        assertThat( responseBodyString, containsString( "http://foobar.com:9999" ) );
        assertThat( responseBodyString, not( containsString( "http://localhost" ) ) );
    }

    @Test
    public void shouldForwardHttpAndFirstHost() throws Exception
    {
        var request = HttpRequest.newBuilder( functionalTestHelper.baseUri() )
                .GET()
                .header( "Accept", "application/json" )
                .header( "X-Forwarded-Host", "foobar.com, bazbar.com" )
                .header( "X-Forwarded-Proto", "http" )
                .build();

        var response = httpClient.send( request, ofByteArray() );

        verifyContentLength( response );

        var responseBodyString = new String( response.body() );
        assertThat( responseBodyString, containsString( "http://foobar.com" ) );
        assertThat( responseBodyString, not( containsString( "http://localhost" ) ) );
    }

    @Test
    public void shouldForwardHttpsAndHostOnDifferentPort() throws Exception
    {
        var request = HttpRequest.newBuilder( functionalTestHelper.baseUri() )
                .GET()
                .header( "Accept", "application/json" )
                .header( "X-Forwarded-Host", "foobar.com:9999" )
                .header( "X-Forwarded-Proto", "https" )
                .build();

        var response = httpClient.send( request, ofByteArray() );

        verifyContentLength( response );

        var responseBodyString = new String( response.body() );
        assertThat( responseBodyString, containsString( "https://foobar.com:9999" ) );
        assertThat( responseBodyString, not( containsString( "https://localhost" ) ) );
    }

    @Test
    public void shouldUseRequestUriWhenNoXForwardHeadersPresent() throws Exception
    {
        var request = HttpRequest.newBuilder( functionalTestHelper.baseUri() )
                .GET()
                .header( "Accept", "application/json" )
                .build();

        var response = httpClient.send( request, ofByteArray() );

        verifyContentLength( response );

        var responseBodyString = new String( response.body() );
        assertThat( responseBodyString, containsString( "http://localhost" ) );
        assertThat( responseBodyString, not( containsString( "https://foobar.com" ) ) );
        assertThat( responseBodyString, not( containsString( ":0" ) ) );
    }

    private static void verifyContentLength( HttpResponse<byte[]> response )
    {
        var contentLengthValue = response.headers().firstValue( "CONTENT-LENGTH" );
        assertTrue( contentLengthValue.isPresent() );
        assertEquals( parseInt( contentLengthValue.get() ), response.body().length );
    }
}
