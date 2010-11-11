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
import com.sun.jersey.api.client.WebResource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.server.NeoServer;
import org.neo4j.server.ServerTestUtils;

import javax.ws.rs.core.MediaType;

import static org.junit.Assert.assertEquals;

public class InteractWith3rdPartyResourceTest {
    public static NeoServer server;

    @BeforeClass
    public static void startServer() {
        server = ServerTestUtils.initializeServerWithRandomTemporaryDatabaseDirectory();
    }

    @AfterClass
    public static void stopServer() throws Exception {
        ServerTestUtils.nukeServer();
    }

    @Test
    public void whenCreatingAResourceOnValid3rdPartyResourceResponseShouldBeCreated() {

        Client client = Client.create();
        WebResource ordersResource = client.resource(ensureEndsWithSlash(server.restApiUri().toString()) + "restbucks/order");
        ClientResponse response = ordersResource.type(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON).entity("{ \"coffee\" : \"latte\" }").post(
                ClientResponse.class);
        assertEquals(201, response.getStatus());

        WebResource order = client.resource(response.getLocation());
        ClientResponse getResponse = order.accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);

        assertEquals(200, getResponse.getStatus());
    }

    private String ensureEndsWithSlash(String baseUri) {
        if (baseUri.endsWith("/")) {
            return baseUri;
        }
        return baseUri + "/";
    }
}
