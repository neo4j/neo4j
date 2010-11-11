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

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.server.NeoServer;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;

import javax.ws.rs.core.MediaType;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SetNodePropertiesFunctionalTest {

    private static URI propertiesUri;
    private static URI badUri;
    public static NeoServer server;

    @BeforeClass
    public static void startServer() throws Exception {
        server = ServerTestUtils.initializeServerWithRandomTemporaryDatabaseDirectory();
        long nodeId = new GraphDbHelper(server.database()).createNode();
        propertiesUri = new URI(server.restApiUri() + "node/" + nodeId + "/properties");
        badUri = new URI(server.restApiUri() + "node/" + (nodeId * 999) + "/properties");
    }

    @AfterClass
    public static void stopServer() throws Exception {
        ServerTestUtils.nukeServer();
    }

    @Test
    public void shouldReturn204WhenPropertiesAreUpdated() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("jim", "tobias");
        ClientResponse response = updateNodePropertiesOnServer(map);
        assertEquals(204, response.getStatus());
    }

    @Test
    public void shouldReturn400WhenSendinIncompatibleJsonProperties() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("jim", new HashMap<String, Object>());
        ClientResponse response = updateNodePropertiesOnServer(map);
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

    private ClientResponse updateNodePropertiesOnServer(Map<String, Object> map) {
        return Client.create().resource(propertiesUri).type(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON).entity(
                JsonHelper.createJsonFrom(map)).put(ClientResponse.class);
    }

    private static URI getPropertyUri(String key) throws Exception {
        return new URI(propertiesUri.toString() + "/" + key);
    }

    @Test
    public void shouldReturn204WhenPropertyIsSet() throws Exception {
        ClientResponse response = setNodePropertyOnServer("foo", "bar");
        assertEquals(204, response.getStatus());
    }

    @Test
    public void shouldReturn400WhenSendinIncompatibleJsonProperty() throws Exception {
        ClientResponse response = setNodePropertyOnServer("jim", new HashMap<String, Object>());
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

    private ClientResponse setNodePropertyOnServer(String key, Object value) throws Exception {
        return Client.create().resource(getPropertyUri(key)).type(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON).entity(
                JsonHelper.createJsonFrom(value)).put(ClientResponse.class);
    }
}
