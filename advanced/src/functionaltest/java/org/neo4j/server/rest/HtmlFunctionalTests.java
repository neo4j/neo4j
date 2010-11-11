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
import org.neo4j.server.database.DatabaseBlockedException;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.RelationshipDirection;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HtmlFunctionalTests {
    private static long thomasAnderson;
    private static long trinity;
    private static long thomasAndersonLovesTrinity;
    private static GraphDbHelper helper;
    public static NeoServer server;

    @BeforeClass
    public static void startServer() throws DatabaseBlockedException {
        server = ServerTestUtils.initializeServerWithRandomTemporaryDatabaseDirectory();
        helper = new GraphDbHelper(server.database());

        // Create the matrix example
        thomasAnderson = createAndIndexNode("Thomas Anderson");
        trinity = createAndIndexNode("Trinity");
        long tank = createAndIndexNode("Tank");

        helper.createRelationship("KNOWS", thomasAnderson, trinity);
        thomasAndersonLovesTrinity = helper.createRelationship("LOVES", thomasAnderson, trinity);
        helper.setRelationshipProperties(thomasAndersonLovesTrinity, Collections.singletonMap("strength", (Object) 100));
        helper.createRelationship("KNOWS", thomasAnderson, tank);
        helper.createRelationship("KNOWS", trinity, tank);
    }

    private static long createAndIndexNode(String name) throws DatabaseBlockedException {
        long id = helper.createNode();
        helper.setNodeProperties(id, Collections.singletonMap("name", (Object) name));
        helper.addNodeToIndex("node", "name", name, id);
        return id;
    }

    @AfterClass
    public static void stopServer() throws Exception {
        ServerTestUtils.nukeServer();
    }

    @Test
    public void shouldGetRoot() {
        ClientResponse response = Client.create().resource(server.restApiUri()).accept(MediaType.TEXT_HTML_TYPE).get(ClientResponse.class);
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        assertValidHtml(response.getEntity(String.class));
    }

    @Test
    public void shouldGetIndexRoot() {
        ClientResponse response = Client.create().resource(FunctionalTestUtil.indexUri()).accept(MediaType.TEXT_HTML_TYPE).get(ClientResponse.class);
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        assertValidHtml(response.getEntity(String.class));
    }

    @Test
    public void shouldGetTrinityWhenSearchingForHer() {
        ClientResponse response = Client.create().resource(FunctionalTestUtil.indexUri("node", "name", "Trinity")).accept(MediaType.TEXT_HTML_TYPE).get(
                ClientResponse.class);
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        String entity = response.getEntity(String.class);
        assertTrue(entity.contains("Trinity"));
        assertValidHtml(entity);
    }

    @Test
    public void shouldGetThomasAndersonDirectly() {
        ClientResponse response = Client.create().resource(FunctionalTestUtil.nodeUri(thomasAnderson)).accept(MediaType.TEXT_HTML_TYPE).get(
                ClientResponse.class);
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        String entity = response.getEntity(String.class);
        assertTrue(entity.contains("Thomas Anderson"));
        assertValidHtml(entity);
    }

    @Test
    public void shouldGetSomeRelationships() {
        ClientResponse response = Client.create().resource(FunctionalTestUtil.relationshipsUri(thomasAnderson, RelationshipDirection.all.name(), "KNOWS"))
                .accept(MediaType.TEXT_HTML_TYPE).get(ClientResponse.class);
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        String entity = response.getEntity(String.class);
        assertTrue(entity.contains("KNOWS"));
        assertFalse(entity.contains("LOVES"));
        assertValidHtml(entity);

        response = Client.create().resource(FunctionalTestUtil.relationshipsUri(thomasAnderson, RelationshipDirection.all.name(), "LOVES")).accept(
                MediaType.TEXT_HTML_TYPE).get(ClientResponse.class);
        entity = response.getEntity(String.class);
        assertFalse(entity.contains("KNOWS"));
        assertTrue(entity.contains("LOVES"));
        assertValidHtml(entity);

        response = Client.create().resource(FunctionalTestUtil.relationshipsUri(thomasAnderson, RelationshipDirection.all.name(), "LOVES", "KNOWS")).accept(
                MediaType.TEXT_HTML_TYPE).get(ClientResponse.class);
        entity = response.getEntity(String.class);
        assertTrue(entity.contains("KNOWS"));
        assertTrue(entity.contains("LOVES"));
        assertValidHtml(entity);
    }

    @Test
    public void shouldGetThomasAndersonLovesTrinityRelationship() {
        ClientResponse response = Client.create().resource(FunctionalTestUtil.relationshipUri(thomasAndersonLovesTrinity)).accept(MediaType.TEXT_HTML_TYPE)
                .get(ClientResponse.class);
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        String entity = response.getEntity(String.class);
        assertTrue(entity.contains("strength"));
        assertTrue(entity.contains("100"));
        assertTrue(entity.contains("LOVES"));
        assertValidHtml(entity);
    }

    private void assertValidHtml(String entity) {
        assertTrue(entity.contains("<html>"));
        assertTrue(entity.contains("</html>"));
    }
}
