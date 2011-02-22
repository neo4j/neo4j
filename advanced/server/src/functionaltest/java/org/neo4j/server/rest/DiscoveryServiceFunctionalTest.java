/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.ServerBuilder;
import org.neo4j.server.rest.domain.JsonHelper;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;


public class DiscoveryServiceFunctionalTest {

    private NeoServerWithEmbeddedWebServer server;
    
    @Before
    public void setupServer() throws IOException {
        server = ServerBuilder.server().withRandomDatabaseDir().withPassingStartupHealthcheck().build();
        server.start();
    }

    @After
    public void stopServer() {
        server.stop();
        server = null;
    }
    
    @Test
    public void shouldRespondWith200WhenRetrievingDiscoveryDocument() throws Exception {
        ClientResponse response = getDiscoveryDocument();
        assertEquals( 200, response.getStatus() );
    }

    @Test
    public void shouldGetContentLengthHeaderWhenRetrievingDiscoveryDocument() throws Exception {
        ClientResponse response = getDiscoveryDocument();
        assertNotNull(response.getHeaders().get("Content-Length"));
    }

    @Test
    public void shouldHaveJsonMediaTypeWhenRetrievingDiscoveryDocument() throws Exception {
        ClientResponse response = getDiscoveryDocument();
        assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    }
    
    @Test
    public void shouldHaveJsonDataInResponse() throws Exception {
        ClientResponse response = getDiscoveryDocument();

        Map<String, Object> map = JsonHelper.jsonToMap(response.getEntity(String.class));
        
        String managementKey = "management";
        assertTrue(map.containsKey(managementKey));
        assertNotNull(map.get(managementKey));

        String dataKey = "data";
        assertTrue(map.containsKey(dataKey));
        assertNotNull(map.get(dataKey));
    }
    
    @Test
    public void shouldRedirectToWebadminOnHtmlRequest() throws Exception {
        Client client = Client.create();
        client.setFollowRedirects(false);
        
        ClientResponse clientResponse = client.resource( server.baseUri() ).accept( MediaType.TEXT_HTML ).get( ClientResponse.class );
        
        assertEquals(303, clientResponse.getStatus());
    }
    
    private ClientResponse getDiscoveryDocument() throws Exception {
        return Client.create().resource( server.baseUri() ).accept( MediaType.APPLICATION_JSON ).get( ClientResponse.class );
    }
    
}
