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
import static org.junit.Assert.assertTrue;

import java.util.Collection;
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

public class PathsFunctionalTest {
    private static long[] nodes;
    private static GraphDbHelper helper;

    @BeforeClass
    public static void startServer() throws DatabaseBlockedException {
        try {
            ServerTestUtils.initializeServerWithRandomTemporaryDatabaseDirectory();
            helper = new GraphDbHelper(NeoServer.server().database());
            nodes = createMoreComplexGraph();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static long[] createMoreComplexGraph() throws DatabaseBlockedException {
        // (a)
        // / \
        // v v
        // (b)<---(c) (d)-->(e)
        // \ / \ / /
        // v v v v /
        // (f)--->(g)<----

        long a = helper.createNode();
        long b = helper.createNode();
        long c = helper.createNode();
        long d = helper.createNode();
        long e = helper.createNode();
        long f = helper.createNode();
        long g = helper.createNode();
        helper.createRelationship("to", a, c);
        helper.createRelationship("to", a, d);
        helper.createRelationship("to", c, b);
        helper.createRelationship("to", d, e);
        helper.createRelationship("to", b, f);
        helper.createRelationship("to", c, f);
        helper.createRelationship("to", f, g);
        helper.createRelationship("to", d, g);
        helper.createRelationship("to", e, g);
        helper.createRelationship("to", c, g);
        return new long[] { a, g };
    }

    @AfterClass
    public static void stopServer() throws Exception {
        ServerTestUtils.nukeServer();
    }

    @Test
    public void shouldBeAbleToFindShortestPaths() {
        Client client = Client.create();

        // Get all shortest paths
        String json = "{\"to\":\"" + NeoServer.server().restApiUri() + "node/" + nodes[1]
                + "\", \"max depth\":3, \"relationships\":{\"type\":\"to\", \"direction\":\"out\"}, \"algorithm\":\"shortestPath\"}";

        WebResource resource = client.resource(NeoServer.server().restApiUri() + "node/" + nodes[0] + "/paths");
        ClientResponse response = resource.type(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON).entity(json).post(ClientResponse.class);
        assertEquals(200, response.getStatus());
        assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
        String entity = response.getEntity(String.class);
        Collection<?> result = (Collection<?>) JsonHelper.jsonToSingleValue(entity);
        assertEquals(2, result.size());
        for (Object representation : result) {
            Map<?, ?> path = (Map<?, ?>) representation;
            assertTrue(path.get("start").toString().endsWith("/node/" + nodes[0]));
            assertTrue(path.get("end").toString().endsWith("/node/" + nodes[1]));
            assertEquals(2, path.get("length"));
        }

        // Get single shortest path
        resource = client.resource(NeoServer.server().restApiUri() + "node/" + nodes[0] + "/path");
        response = resource.type(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON).entity(json).post(ClientResponse.class);
        assertEquals(200, response.getStatus());
        Map<?, ?> path = (Map<?, ?>) JsonHelper.jsonToMap(response.getEntity(String.class));
        assertTrue(path.get("start").toString().endsWith("/node/" + nodes[0]));
        assertTrue(path.get("end").toString().endsWith("/node/" + nodes[1]));
        assertEquals(2, path.get("length"));

        // Get single shortest path and expect no answer (404)
        String noHitsJson = "{\"to\":\"" + NeoServer.server().restApiUri() + "node/" + nodes[1]
                + "\", \"max depth\":3, \"relationships\":{\"type\":\"to\", \"direction\":\"in\"}, \"algorithm\":\"shortestPath\"}";
        resource = client.resource(NeoServer.server().restApiUri() + "node/" + nodes[0] + "/path");
        response = resource.type(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON).entity(noHitsJson).post(ClientResponse.class);
        assertEquals(404, response.getStatus());

        // Get single shortest paths and expect no content (since using /paths
        // {single:true} instead of /path)
        String noHitsSingleJson = "{\"to\":\"" + NeoServer.server().restApiUri() + "node/" + nodes[1]
                + "\", \"max depth\":3, \"relationships\":{\"type\":\"to\", \"direction\":\"in\"}, \"algorithm\":\"shortestPath\", \"single\":true}";
        resource = client.resource(NeoServer.server().restApiUri() + "node/" + nodes[0] + "/paths");
        response = resource.type(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON).entity(noHitsSingleJson).post(ClientResponse.class);
        assertEquals(204, response.getStatus());
    }
}
