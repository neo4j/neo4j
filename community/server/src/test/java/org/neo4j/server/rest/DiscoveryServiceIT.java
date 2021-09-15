/*
 * Copyright (c) "Neo4j"
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

import com.sun.jersey.api.client.Client;
import org.junit.Test;

import java.util.Map;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_HTML_TYPE;
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

        assertJsonResponseBody( response );
    }

    @Test
    public void shouldFigureOutMatchingFormatFromVariousAcceptHeaders() throws Exception
    {

        JaxRsResponse response = new RestRequest( server().baseUri() )
                                 .header( HttpHeaders.ACCEPT, "application/vnd.neo4j.jolt+json-seq; q=1.0" )
                                 .header( HttpHeaders.ACCEPT, "application/json; q=0.9" )
                                 .header( HttpHeaders.ACCEPT, "text/html; q=0.0" )
                                 .get();

        assertEquals( 200, response.getStatus() );
        assertJsonResponseBody( response );
        assertEquals( HttpHeaders.ACCEPT, response.getHeaders().getFirst( HttpHeaders.VARY ) );
    }

    @Test
    public void shouldNotAcceptUnacceptableThings()
    {
        JaxRsResponse response = new RestRequest( server().baseUri() )
                .accept( MediaType.TEXT_PLAIN_TYPE )
                .get();
        assertEquals( Response.Status.NOT_ACCEPTABLE.getStatusCode(), response.getStatus() );
    }

    private void assertJsonResponseBody( JaxRsResponse response ) throws JsonParseException
    {
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
    public void shouldRedirectOnHtmlRequest()
    {
        Client nonRedirectingClient = Client.create();
        nonRedirectingClient.setFollowRedirects( false );

        JaxRsResponse clientResponse =
                new RestRequest( null, nonRedirectingClient ).get( server().baseUri().toString(), TEXT_HTML_TYPE );

        assertEquals( 303, clientResponse.getStatus() );
        assertEquals( HttpHeaders.ACCEPT, clientResponse.getHeaders().getFirst( HttpHeaders.VARY ) );
    }

    private JaxRsResponse getDiscoveryDocument()
    {
        return new RestRequest( server().baseUri() ).get();
    }
}
