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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.database.DatabaseBlockedException;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.RelationshipDirection;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;

public class HtmlFunctionalTest {
    private long thomasAnderson;
    private long trinity;
    private long thomasAndersonLovesTrinity;

    private static NeoServerWithEmbeddedWebServer server;
    private static FunctionalTestHelper functionalTestHelper;
    private static GraphDbHelper helper;

    @BeforeClass
    public static void setupServer() throws IOException
    {
        server = ServerHelper.createServer();
        functionalTestHelper = new FunctionalTestHelper( server );
        helper = functionalTestHelper.getGraphDbHelper();
    }

    @Before
    public void setupTheDatabase()
    {
        ServerHelper.cleanTheDatabase( server );
        createTheMatrix();
    }

    private void createTheMatrix()
    {
        // Create the matrix example
        thomasAnderson = createAndIndexNode("Thomas Anderson");
        trinity = createAndIndexNode("Trinity");
        long tank = createAndIndexNode("Tank");

        long knowsRelationshipId = helper.createRelationship( "KNOWS", thomasAnderson, trinity );
        thomasAndersonLovesTrinity = helper.createRelationship("LOVES", thomasAnderson, trinity);
        helper.setRelationshipProperties( thomasAndersonLovesTrinity, Collections.singletonMap( "strength", (Object) 100 ) );
        helper.createRelationship("KNOWS", thomasAnderson, tank);
        helper.createRelationship("KNOWS", trinity, tank);

        // index a relationship
        helper.createRelationshipIndex( "relationships" );
        helper.addRelationshipToIndex( "relationships", "key", "value", knowsRelationshipId );

        // index a relationship
        helper.createRelationshipIndex( "relationships2" );
        helper.addRelationshipToIndex( "relationships2", "key2", "value2", knowsRelationshipId );
    }

    @AfterClass
    public static void stopServer()
    {
        server.stop();
    }


    private long createAndIndexNode(String name) throws DatabaseBlockedException {
        long id = helper.createNode();
        helper.setNodeProperties(id, Collections.singletonMap("name", (Object) name));
        helper.addNodeToIndex("node", "name", name, id);
        return id;
    }
    @Ignore
    @Test
    public void shouldGetRoot() {
        ClientResponse response = Client.create().resource(functionalTestHelper.dataUri()).accept(MediaType.TEXT_HTML_TYPE).get(ClientResponse.class);
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        assertValidHtml(response.getEntity(String.class));
        response.close();
    }

    @Test
    @Ignore
    public void shouldGetNodeIndexRoot() {
        ClientResponse response = Client.create().resource(functionalTestHelper.nodeIndexUri()).accept(MediaType.TEXT_HTML_TYPE).get(ClientResponse.class);
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        assertValidHtml(response.getEntity(String.class));
        response.close();
    }

    @Test
    @Ignore
    public void shouldGetRelationshipIndexRoot() {
        ClientResponse response = Client.create().resource(functionalTestHelper.relationshipIndexUri()).accept(MediaType.TEXT_HTML_TYPE).get(ClientResponse.class);
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        assertValidHtml(response.getEntity(String.class));
        response.close();
    }

    @Test
    @Ignore
    public void shouldGetTrinityWhenSearchingForHer() {
        ClientResponse response = Client.create().resource(functionalTestHelper.indexNodeUri("node", "name", "Trinity" )).accept(MediaType.TEXT_HTML_TYPE).get(ClientResponse.class);
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        String entity = response.getEntity(String.class);
        assertTrue(entity.contains("Trinity"));
        assertValidHtml(entity);
        response.close();
    }

    @Test
    public void shouldGetThomasAndersonDirectly() {
        ClientResponse response = Client.create().resource(functionalTestHelper.nodeUri(thomasAnderson)).accept(MediaType.TEXT_HTML_TYPE).get(ClientResponse.class);
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        String entity = response.getEntity(String.class);
        assertTrue(entity.contains("Thomas Anderson"));
        assertValidHtml(entity);
        response.close();
    }

    @Test
    @Ignore
    public void shouldGetSomeRelationships() {
        ClientResponse response = Client.create().resource(functionalTestHelper.relationshipsUri(thomasAnderson, RelationshipDirection.all.name(), "KNOWS")).accept(
                MediaType.TEXT_HTML_TYPE).get(ClientResponse.class);
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        String entity = response.getEntity(String.class);
        assertTrue(entity.contains("KNOWS"));
        assertFalse(entity.contains("LOVES"));
        assertValidHtml(entity);
        response.close();

        response = Client.create().resource(functionalTestHelper.relationshipsUri(thomasAnderson, RelationshipDirection.all.name(), "LOVES")).accept(MediaType.TEXT_HTML_TYPE).get(
                ClientResponse.class);
        entity = response.getEntity(String.class);
        assertFalse(entity.contains("KNOWS"));
        assertTrue(entity.contains("LOVES"));
        assertValidHtml(entity);
        response.close();

        response = Client.create().resource(functionalTestHelper.relationshipsUri(thomasAnderson, RelationshipDirection.all.name(), "LOVES", "KNOWS")).accept(
                MediaType.TEXT_HTML_TYPE).get(ClientResponse.class);
        entity = response.getEntity(String.class);
        assertTrue(entity.contains("KNOWS"));
        assertTrue(entity.contains("LOVES"));
        assertValidHtml(entity);
        response.close();
    }

    @Test
    @Ignore
    public void shouldGetThomasAndersonLovesTrinityRelationship() {
        ClientResponse response = Client.create().resource(functionalTestHelper.relationshipUri(thomasAndersonLovesTrinity)).accept(MediaType.TEXT_HTML_TYPE).get(
                ClientResponse.class);
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        String entity = response.getEntity(String.class);
        assertTrue(entity.contains("strength"));
        assertTrue(entity.contains("100"));
        assertTrue(entity.contains("LOVES"));
        assertValidHtml(entity);
        response.close();
    }

    private void assertValidHtml(String entity) {
        assertTrue(entity.contains("<html>"));
        assertTrue(entity.contains("</html>"));
    }
}
