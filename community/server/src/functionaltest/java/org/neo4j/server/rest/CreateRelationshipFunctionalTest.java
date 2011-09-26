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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.database.DatabaseBlockedException;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.repr.RelationshipRepresentationTest;
import org.neo4j.test.TestData;

public class CreateRelationshipFunctionalTest
{
    private static String RELATIONSHIP_URI_PATTERN;

    public @Rule
    TestData<RESTDocsGenerator> gen = TestData.producedThrough( RESTDocsGenerator.PRODUCER );

    private static NeoServerWithEmbeddedWebServer server;
    private static FunctionalTestHelper functionalTestHelper;
    private static GraphDbHelper helper;

    @BeforeClass
    public static void setupServer() throws IOException
    {
        server = ServerHelper.createServer();
        functionalTestHelper = new FunctionalTestHelper( server );
        helper = functionalTestHelper.getGraphDbHelper();

        RELATIONSHIP_URI_PATTERN = functionalTestHelper.dataUri() + "relationship/[0-9]+";
    }

    @Before
    public void cleanTheDatabase()
    {
        ServerHelper.cleanTheDatabase( server );
    }

    @AfterClass
    public static void stopServer()
    {
        server.stop();
    }

    @Test
    public void shouldRespondWith201WhenSuccessfullyCreatedRelationshipWithProperties() throws Exception {
        long startNode = helper.createNode();
        long endNode = helper.createNode();
        String jsonString = "{\"to\" : \"" + functionalTestHelper.dataUri() + "node/" + endNode
                + "\", \"type\" : \"LOVES\", \"data\" : {\"foo\" : \"bar\"}}";
        JaxRsResponse response = RestRequest.req().post(functionalTestHelper.dataUri() + "node/" + startNode + "/relationships", jsonString);
        assertEquals(201, response.getStatus());
        assertTrue(response.getLocation()
                .toString()
                .matches(RELATIONSHIP_URI_PATTERN));
        assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
        String relationshipUri = response.getLocation()
                .toString();
        long relationshipId = Long.parseLong(relationshipUri.substring(relationshipUri.lastIndexOf('/') + 1));
        Map<String, Object> properties = helper.getRelationshipProperties(relationshipId);
        assertEquals(MapUtil.map("foo", "bar"), properties);
        assertProperRelationshipRepresentation(JsonHelper.jsonToMap(response.getEntity(String.class)));
        response.close();
    }

    /**
     * Create relationship.
     */
    @Documented
    @Test
    public void shouldRespondWith201WhenSuccessfullyCreatedRelationship() throws Exception {
        long startNode = helper.createNode();
        long endNode = helper.createNode();
        String jsonString = "{\"to\" : \"" + functionalTestHelper.dataUri() + "node/" + endNode
                + "\", \"type\" : \"LOVES\"}";
        String uri = functionalTestHelper.dataUri() + "node/" + startNode + "/relationships";
        JaxRsResponse response = RestRequest.req().post(uri, jsonString);
        assertEquals(201, response.getStatus());
        assertTrue(response.getLocation()
                .toString()
                .matches(RELATIONSHIP_URI_PATTERN));
        assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
        assertProperRelationshipRepresentation(JsonHelper.jsonToMap(response.getEntity(String.class)));
        gen.get()
                .payload(jsonString)
                .expectedStatus(201)
                .post(uri);
        response.close();
    }

    @Test
    public void shouldRespondWith404WhenStartNodeDoesNotExist() throws DatabaseBlockedException {
        long endNode = helper.createNode();
        String jsonString = "{\"to\" : \"" + functionalTestHelper.dataUri() + "node/" + endNode
                + "\", \"type\" : \"LOVES\", \"data\" : {\"foo\" : \"bar\"}}";
        JaxRsResponse response = RestRequest.req().post(functionalTestHelper.dataUri() + "node/999999/relationships", jsonString);
        assertEquals(404, response.getStatus());
        response.close();
    }

    @Test
    public void shouldRespondWith400WhenEndNodeDoesNotExist() throws DatabaseBlockedException {
        long startNode = helper.createNode();
        String jsonString = "{\"to\" : \"" + functionalTestHelper.dataUri() + "node/"
                + "999999\", \"type\" : \"LOVES\", \"data\" : {\"foo\" : \"bar\"}}";
        JaxRsResponse response = RestRequest.req().post(functionalTestHelper.dataUri() + "node/" + startNode + "/relationships", jsonString);
        assertEquals(400, response.getStatus());
        response.close();
    }

    @Test
    public void shouldRespondWith201WhenCreatingALoopRelationship() throws Exception {
        long theOnlyNode = helper.createNode();

        String jsonString = "{\"to\" : \"" + functionalTestHelper.dataUri() + "node/" + theOnlyNode
                + "\", \"type\" : \"LOVES\"}";
        String uri = functionalTestHelper.dataUri() + "node/" + theOnlyNode + "/relationships";
        JaxRsResponse response = RestRequest.req().post(uri, jsonString);
        assertEquals(201, response.getStatus());
        assertTrue(response.getLocation()
                .toString()
                .matches(RELATIONSHIP_URI_PATTERN));
        assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
        assertProperRelationshipRepresentation(JsonHelper.jsonToMap(response.getEntity(String.class)));
        gen.get()
                .payload(jsonString)
                .expectedStatus(201)
                .post(uri);
        response.close();
    }

    @Test
    public void shouldRespondWith400WhenBadJsonProvided() throws DatabaseBlockedException {
        long startNode = helper.createNode();
        long endNode = helper.createNode();
        String jsonString = "{\"to\" : \"" + functionalTestHelper.dataUri() + "node/" + endNode
                + "\", \"type\" : \"LOVES\", \"data\" : {\"foo\" : **BAD JSON HERE*** \"bar\"}}";
        JaxRsResponse response = RestRequest.req().post(functionalTestHelper.dataUri() + "node/" + startNode + "/relationships", jsonString);

        assertEquals(400, response.getStatus());
        response.close();
    }

    private void assertProperRelationshipRepresentation( Map<String, Object> relrep )
    {
        RelationshipRepresentationTest.verifySerialisation( relrep );
    }
}
