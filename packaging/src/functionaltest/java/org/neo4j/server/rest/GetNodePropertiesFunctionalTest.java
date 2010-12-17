/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

import java.io.IOException;
import java.util.Collections;

import javax.ws.rs.core.MediaType;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.server.NeoServer;
import org.neo4j.server.ServerBuilder;
import org.neo4j.server.rest.domain.JsonHelper;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.neo4j.server.rest.domain.JsonParseException;

public class GetNodePropertiesFunctionalTest {
    
    private NeoServer server;

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
    public void shouldGet204ForNoProperties() {
        Client client = Client.create();
        WebResource createResource = client.resource(server.restApiUri() + "node/");
        ClientResponse createResponse = createResource.accept(MediaType.APPLICATION_JSON).entity("").post(ClientResponse.class);
        WebResource resource = client.resource(createResponse.getLocation().toString() + "/properties");
        ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
        assertEquals(204, response.getStatus());
    }

    @Test
    public void shouldGet200ForProperties() throws JsonParseException
    {
        Client client = Client.create();
        WebResource createResource = client.resource(server.restApiUri() + "node/");
        String entity = JsonHelper.createJsonFrom(Collections.singletonMap("foo", "bar"));
        ClientResponse createResponse = createResource.type(MediaType.APPLICATION_JSON).entity(entity).accept(MediaType.APPLICATION_JSON).post(
                ClientResponse.class);
        WebResource resource = client.resource(createResponse.getLocation().toString() + "/properties");
        ClientResponse response = resource.type(MediaType.APPLICATION_FORM_URLENCODED).accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
        assertEquals(200, response.getStatus());
    }

    @Test
    public void shouldGetContentLengthHeaderForRetrievingProperties() throws JsonParseException
    {
        Client client = Client.create();
        WebResource createResource = client.resource(server.restApiUri() + "node/");
        String entity = JsonHelper.createJsonFrom(Collections.singletonMap("foo", "bar"));
        ClientResponse createResponse = createResource.type(MediaType.APPLICATION_JSON).entity(entity).accept(MediaType.APPLICATION_JSON).post(
                ClientResponse.class);
        WebResource resource = client.resource(createResponse.getLocation().toString() + "/properties");
        ClientResponse response = resource.type(MediaType.APPLICATION_FORM_URLENCODED).accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
        assertNotNull(response.getHeaders().get("Content-Length"));
    }

    @Test
    public void shouldGet404ForPropertiesOnNonExistentNode() {
        Client client = Client.create();
        WebResource resource = client.resource(server.restApiUri() + "node/999999/properties");
        ClientResponse response = resource.type(MediaType.APPLICATION_FORM_URLENCODED).accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldBeJSONContentTypeOnPropertiesResponse() throws JsonParseException
    {
        Client client = Client.create();
        WebResource createResource = client.resource(server.restApiUri() + "node/");
        String entity = JsonHelper.createJsonFrom(Collections.singletonMap("foo", "bar"));
        ClientResponse createResponse = createResource.type(MediaType.APPLICATION_JSON).entity(entity).accept(MediaType.APPLICATION_JSON).post(
                ClientResponse.class);
        WebResource resource = client.resource(createResponse.getLocation().toString() + "/properties");
        ClientResponse response = resource.type(MediaType.APPLICATION_FORM_URLENCODED).accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
        assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    }

    @Test
    public void shouldGet404ForNoProperty() {
        Client client = Client.create();
        WebResource createResource = client.resource(server.restApiUri() + "node/");
        ClientResponse createResponse = createResource.type(MediaType.APPLICATION_FORM_URLENCODED).accept(MediaType.APPLICATION_JSON)
                .post(ClientResponse.class);
        WebResource resource = client.resource(getPropertyUri(createResponse.getLocation().toString(), "foo"));
        ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldGet200ForProperty() throws JsonParseException
    {
        Client client = Client.create();
        WebResource createResource = client.resource(server.restApiUri() + "node/");
        String entity = JsonHelper.createJsonFrom(Collections.singletonMap("foo", "bar"));
        ClientResponse createResponse = createResource.type(MediaType.APPLICATION_JSON).entity(entity).accept(MediaType.APPLICATION_JSON).post(
                ClientResponse.class);
        WebResource resource = client.resource(getPropertyUri(createResponse.getLocation().toString(), "foo"));
        ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
        assertEquals(200, response.getStatus());
    }

    @Test
    public void shouldGet404ForPropertyOnNonExistentNode() {
        Client client = Client.create();
        WebResource resource = client.resource(getPropertyUri(server.restApiUri() + "node/" + "999999", "foo"));
        ClientResponse response = resource.type(MediaType.APPLICATION_FORM_URLENCODED).accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
        assertEquals(404, response.getStatus());

    }

    @Test
    public void shouldBeJSONContentTypeOnPropertyResponse() throws JsonParseException
    {
        Client client = Client.create();
        WebResource createResource = client.resource(server.restApiUri() + "node/");
        String entity = JsonHelper.createJsonFrom(Collections.singletonMap("foo", "bar"));
        ClientResponse createResponse = createResource.type(MediaType.APPLICATION_JSON).entity(entity).accept(MediaType.APPLICATION_JSON).post(
                ClientResponse.class);
        WebResource resource = client.resource(getPropertyUri(createResponse.getLocation().toString(), "foo"));
        ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
        assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    }

    private String getPropertyUri(String baseUri, String key) {
        return baseUri.toString() + "/properties/" + key;
    }
}
