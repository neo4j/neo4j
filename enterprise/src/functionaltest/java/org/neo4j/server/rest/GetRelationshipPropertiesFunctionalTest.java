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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.server.NeoServer;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.server.database.DatabaseBlockedException;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class GetRelationshipPropertiesFunctionalTest {
    private static String baseUri;
    private static GraphDbHelper helper;

    @BeforeClass
    public static void startServer() throws DatabaseBlockedException {
        ServerTestUtils.initializeServerWithRandomTemporaryDatabaseDirectory();
        helper = new GraphDbHelper(NeoServer.server().database());
        long relationship = helper.createRelationship("LIKES");
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("foo", "bar");
        helper.setRelationshipProperties(relationship, map);
        baseUri = NeoServer.server().restApiUri() + "relationship/" + relationship + "/properties/";
    }

    @AfterClass
    public static void stopServer() throws Exception {
        ServerTestUtils.nukeServer();
    }

    @Test
    public void shouldGet204ForNoProperties() throws DatabaseBlockedException {
        long relId = helper.createRelationship("LIKES");
        Client client = Client.create();
        WebResource resource = client.resource(NeoServer.server().restApiUri() + "relationship/" + relId + "/properties");
        ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
        assertEquals(204, response.getStatus());
    }

    @Test
    public void shouldGet200AndContentLengthForProperties() throws DatabaseBlockedException {
        long relId = helper.createRelationship("LIKES");
        helper.setRelationshipProperties(relId, Collections.<String, Object> singletonMap("foo", "bar"));
        Client client = Client.create();
        WebResource resource = client.resource(NeoServer.server().restApiUri() + "relationship/" + relId + "/properties");
        ClientResponse response = resource.type(MediaType.APPLICATION_FORM_URLENCODED).accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
        assertEquals(200, response.getStatus());
        assertNotNull(response.getHeaders().get("Content-Length"));
    }

    @Test
    public void shouldGet404ForPropertiesOnNonExistentRelationship() {
        Client client = Client.create();
        WebResource resource = client.resource(NeoServer.server().restApiUri() + "relationship/999999/properties");
        ClientResponse response = resource.type(MediaType.APPLICATION_FORM_URLENCODED).accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldBeJSONContentTypeOnPropertiesResponse() throws DatabaseBlockedException {
        long relId = helper.createRelationship("LIKES");
        helper.setRelationshipProperties(relId, Collections.<String, Object> singletonMap("foo", "bar"));
        Client client = Client.create();
        WebResource resource = client.resource(NeoServer.server().restApiUri() + "relationship/" + relId + "/properties");
        ClientResponse response = resource.type(MediaType.APPLICATION_FORM_URLENCODED).accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
        assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    }

    private String getPropertyUri(String key) {
        return baseUri + key;
    }

    @Test
    public void shouldGet404ForNoProperty() {
        WebResource resource = Client.create().resource(getPropertyUri("baz"));
        ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldGet200ForProperty() {
        String propertyUri = getPropertyUri("foo");
        WebResource resource = Client.create().resource(propertyUri);
        ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
        assertEquals(200, response.getStatus());
    }

    @Test
    public void shouldGet404ForNonExistingRelationship() {
        String uri = NeoServer.server().restApiUri() + "relationship/999999/properties/foo";
        WebResource resource = Client.create().resource(uri);
        ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldBeValidJSONOnResponse() {
        WebResource resource = Client.create().resource(getPropertyUri("foo"));
        ClientResponse response = resource.accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
        assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
        assertNotNull(JsonHelper.createJsonFrom(response.getEntity(String.class)));
    }
}
