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

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.server.NeoServer;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;

public class SetRelationshipPropertiesFunctionalTest {

    private static URI propertiesUri;
    private static URI badUri;

    @BeforeClass
    public static void startWebServer() throws Exception {
        ServerTestUtils.initializeServerWithRandomTemporaryDatabaseDirectory();
        long relationshipId = new GraphDbHelper(NeoServer.server().database()).createRelationship("KNOWS");
        propertiesUri = new URI(NeoServer.server().restApiUri() + "relationship/" + relationshipId + "/properties");
        badUri = new URI(NeoServer.server().restApiUri() + "relationship/" + (relationshipId + 1 * 99999) + "/properties");
    }

    @AfterClass
    public static void stopServer() throws Exception {
        ServerTestUtils.nukeServer();
    }

    @Test
    public void shouldReturn204WhenPropertiesAreUpdated() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("jim", "tobias");
        ClientResponse response = updatePropertiesOnServer(map);
        assertEquals(204, response.getStatus());
    }

    @Test
    public void shouldReturn400WhenSendinIncompatibleJsonProperties() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("jim", new HashMap<String, Object>());
        ClientResponse response = updatePropertiesOnServer(map);
        assertEquals(400, response.getStatus());
    }

    @Test
    public void shouldReturn400WhenSendingCorruptJsonProperties() {
        ClientResponse response = Client.create().resource(propertiesUri).type(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON).entity(
                "this:::Is::notJSON}").put(ClientResponse.class);
        assertEquals(400, response.getStatus());
    }

    @Test
    public void shouldReturn404WhenPropertiesSentToANodeWhichDoesNotExist() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("jim", "tobias");
        
        ClientResponse response = Client.create().resource(badUri).type(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON).entity(
                JsonHelper.createJsonFrom(map)).put(ClientResponse.class);
        assertEquals(404, response.getStatus());
    }

    private ClientResponse updatePropertiesOnServer(Map<String, Object> map) {
        return Client.create().resource(propertiesUri).type(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON).entity(
                JsonHelper.createJsonFrom(map)).put(ClientResponse.class);
    }

    private static URI getPropertyUri(String key) throws Exception {
        return new URI(propertiesUri.toString() + "/" + key);
    }

    @Test
    public void shouldReturn204WhenPropertyIsSet() throws Exception {
        ClientResponse response = setPropertyOnServer("foo", "bar");
        assertEquals(204, response.getStatus());
    }

    @Test
    public void shouldReturn400WhenSendinIncompatibleJsonProperty() throws Exception {
        ClientResponse response = setPropertyOnServer("jim", new HashMap<String, Object>());
        assertEquals(400, response.getStatus());
    }

    @Test
    public void shouldReturn400WhenSendingCorruptJsonProperty() throws Exception {
        ClientResponse response = Client.create().resource(getPropertyUri("foo")).type(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON).entity(
                "this:::Is::notJSON}").put(ClientResponse.class);
        assertEquals(400, response.getStatus());
    }

    @Test
    public void shouldReturn404WhenPropertySentToANodeWhichDoesNotExist() throws Exception {
        ClientResponse response = Client.create().resource(badUri.toString() + "/foo").type(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON)
                .entity(JsonHelper.createJsonFrom("bar")).put(ClientResponse.class);
        assertEquals(404, response.getStatus());
    }

    private ClientResponse setPropertyOnServer(String key, Object value) throws Exception {
        return Client.create().resource(getPropertyUri(key)).type(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON).entity(
                JsonHelper.createJsonFrom(value)).put(ClientResponse.class);
    }
}
