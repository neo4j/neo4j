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

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;

import org.neo4j.server.rest.domain.JsonHelper;

import static java.net.http.HttpClient.Redirect.NEVER;
import static java.net.http.HttpResponse.BodyHandlers.discarding;
import static javax.ws.rs.core.HttpHeaders.ACCEPT;
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
    public void shouldRespondWith200WhenRetrievingDiscoveryDocument()
    {
        JaxRsResponse response = getDiscoveryDocument();
        assertEquals( 200, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldGetContentLengthHeaderWhenRetrievingDiscoveryDocument()
    {
        JaxRsResponse response = getDiscoveryDocument();
        assertNotNull( response.getHeaders().get( "Content-Length" ) );
        response.close();
    }

    @Test
    public void shouldHaveJsonMediaTypeWhenRetrievingDiscoveryDocument()
    {
        JaxRsResponse response = getDiscoveryDocument();
        assertThat( response.getType().toString(), containsString( APPLICATION_JSON ) );
        response.close();
    }

    @Test
    public void shouldHaveJsonDataInResponse() throws Exception
    {
        JaxRsResponse response = getDiscoveryDocument();

        Map<String,Object> map = JsonHelper.jsonToMap( response.getEntity() );

        String managementKey = "management";
        assertTrue( map.containsKey( managementKey ) );
        assertNotNull( map.get( managementKey ) );

        String dataKey = "data";
        assertTrue( map.containsKey( dataKey ) );
        assertNotNull( map.get( dataKey ) );
        response.close();
    }

    @Test
    public void shouldRedirectOnHtmlRequest() throws Exception
    {
        HttpRequest request = HttpRequest.newBuilder( server().baseUri() ).header( ACCEPT, TEXT_HTML ).GET().build();
        HttpClient client = HttpClient.newBuilder().followRedirects( NEVER ).build();
        HttpResponse<Void> response = client.send( request, discarding() );

        assertEquals( 303, response.statusCode() );
    }

    private static JaxRsResponse getDiscoveryDocument()
    {
        return new RestRequest( server().baseUri() ).get();
    }
}
