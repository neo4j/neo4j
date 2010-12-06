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
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.server.NeoServer;
import org.neo4j.server.ServerBuilder;
import org.neo4j.server.rest.domain.JsonHelper;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;

public class GetOnRootFunctionalTest {

    private NeoServer server;
    private FunctionalTestHelper functionalTestHelper;

    @Before
    public void setupServer() throws IOException {
        server = ServerBuilder.server().withRandomDatabaseDir().withPassingStartupHealthcheck().build();
        server.start();
        functionalTestHelper = new FunctionalTestHelper(server);
    }

    @After
    public void stopServer() {
        server.stop();
        server = null;
    }

    @Test
    public void assert200OkFromGet() throws Exception {
        ClientResponse response = Client.create().resource(functionalTestHelper.dataUri()).get(ClientResponse.class);
        assertEquals(200, response.getStatus());
    }

    @Test
    public void assertResponseHaveCorrectContentFromGet() throws Exception {
        ClientResponse response = Client.create().resource(functionalTestHelper.dataUri()).accept(MediaType.APPLICATION_JSON_TYPE).get(ClientResponse.class);
        String body = response.getEntity(String.class);
        Map<String, Object> map = JsonHelper.jsonToMap(body);
        assertEquals(functionalTestHelper.nodeUri(), map.get("node"));
        assertNotNull(map.get("reference_node"));
        assertNotNull(map.get("index"));

        String referenceNodeUri = (String) map.get("reference_node");
        response = Client.create().resource(referenceNodeUri).accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
        assertEquals(200, response.getStatus());
    }
}
