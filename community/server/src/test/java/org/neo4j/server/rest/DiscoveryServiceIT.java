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

import org.junit.Test;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import org.neo4j.server.rest.domain.JsonHelper;

import static java.net.http.HttpClient.Redirect.NEVER;
import static java.net.http.HttpClient.newHttpClient;
import static java.net.http.HttpResponse.BodyHandlers.discarding;
import static java.net.http.HttpResponse.BodyHandlers.ofString;
import static javax.ws.rs.core.HttpHeaders.ACCEPT;
import static javax.ws.rs.core.HttpHeaders.CONTENT_LENGTH;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_HTML;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DiscoveryServiceIT extends AbstractRestFunctionalTestBase
{
    @Test
    public void shouldRespondWith200WhenRetrievingDiscoveryDocument() throws Exception
    {
        var response = requestDiscovery();
        assertEquals( 200, response.statusCode() );
    }

    @Test
    public void shouldGetContentLengthHeaderWhenRetrievingDiscoveryDocument() throws Exception
    {
        var response = requestDiscovery();
        assertTrue( response.headers().firstValue( CONTENT_LENGTH ).isPresent() );
    }

    @Test
    public void shouldHaveJsonMediaTypeWhenRetrievingDiscoveryDocument() throws Exception
    {
        var response = requestDiscovery();
        assertThat( response.headers().firstValue( CONTENT_TYPE ).orElseThrow(), containsString( APPLICATION_JSON ) );
    }

    @Test
    public void shouldHaveJsonDataInResponse() throws Exception
    {
        var response = requestDiscovery();

        var responseBodyMap = JsonHelper.jsonToMap( response.body() );

        var managementKey = "management";
        assertTrue( responseBodyMap.containsKey( managementKey ) );
        assertNotNull( responseBodyMap.get( managementKey ) );

        var dataKey = "data";
        assertTrue( responseBodyMap.containsKey( dataKey ) );
        assertNotNull( responseBodyMap.get( dataKey ) );
    }

    @Test
    public void shouldRedirectOnHtmlRequest() throws Exception
    {
        var request = HttpRequest.newBuilder( server().baseUri() ).header( ACCEPT, TEXT_HTML ).GET().build();
        var httpClient = HttpClient.newBuilder().followRedirects( NEVER ).build();
        var response = httpClient.send( request, discarding() );

        assertEquals( 303, response.statusCode() );
    }

    private static HttpResponse<String> requestDiscovery() throws IOException, InterruptedException
    {
        var request = HttpRequest.newBuilder( server().baseUri() ).GET().build();
        return newHttpClient().send( request, ofString() );
    }
}
