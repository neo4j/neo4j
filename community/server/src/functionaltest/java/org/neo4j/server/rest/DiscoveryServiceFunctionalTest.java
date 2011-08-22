/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.server.rest.domain.JsonHelper;

import com.sun.jersey.api.client.Client;

public class DiscoveryServiceFunctionalTest
{

    private static NeoServerWithEmbeddedWebServer server;

    @BeforeClass
    public static void setupServer() throws IOException
    {
        server = ServerHelper.createServer();
    }

    @Before
    public void cleanTheDatabase()
    {
        ServerHelper.cleanTheDatabase( server );
    }

    @AfterClass
    public static void stopServer()
    {
        server.stop();
    }

    @Test
    public void shouldRespondWith200WhenRetrievingDiscoveryDocument() throws Exception
    {
        JaxRsResponse response = getDiscoveryDocument();
        assertEquals( 200, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldGetContentLengthHeaderWhenRetrievingDiscoveryDocument() throws Exception
    {
        JaxRsResponse response = getDiscoveryDocument();
        assertNotNull( response.getHeaders()
                .get( "Content-Length" ) );
        response.close();
    }

    @Test
    public void shouldHaveJsonMediaTypeWhenRetrievingDiscoveryDocument() throws Exception
    {
        JaxRsResponse response = getDiscoveryDocument();
        assertEquals( MediaType.APPLICATION_JSON_TYPE, response.getType() );
        response.close();
    }

    @Test
    public void shouldHaveJsonDataInResponse() throws Exception
    {
        JaxRsResponse response = getDiscoveryDocument();

        Map<String, Object> map = JsonHelper.jsonToMap( response.getEntity( String.class ) );

        String managementKey = "management";
        assertTrue( map.containsKey( managementKey ) );
        assertNotNull( map.get( managementKey ) );

        String dataKey = "data";
        assertTrue( map.containsKey( dataKey ) );
        assertNotNull( map.get( dataKey ) );
        response.close();
    }

    @Test
    public void shouldRedirectToWebadminOnHtmlRequest() throws Exception
    {
        Client nonRedirectingClient = Client.create();
        nonRedirectingClient.setFollowRedirects( false );

        JaxRsResponse clientResponse = new RestRequest(null,nonRedirectingClient).get(server.baseUri().toString(),MediaType.TEXT_HTML_TYPE);

        assertEquals( 303, clientResponse.getStatus() );
    }

    private JaxRsResponse getDiscoveryDocument() throws Exception
    {
        return new RestRequest(server.baseUri()).get();
    }

}
