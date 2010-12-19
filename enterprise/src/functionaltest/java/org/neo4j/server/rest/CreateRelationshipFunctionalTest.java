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

import java.io.IOException;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.server.NeoServer;
import org.neo4j.server.ServerBuilder;
import org.neo4j.server.database.DatabaseBlockedException;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.repr.RelationshipRepresentationTest;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;

public class CreateRelationshipFunctionalTest
{
    private String RELATIONSHIP_URI_PATTERN;

    private NeoServer server;
    private FunctionalTestHelper functionalTestHelper;
    private GraphDbHelper helper;

    @Before
    public void setupServer() throws IOException {
        server = ServerBuilder.server().withRandomDatabaseDir().withPassingStartupHealthcheck().build();
        server.start();
        functionalTestHelper = new FunctionalTestHelper(server);
        helper = functionalTestHelper.getGraphDbHelper();

        RELATIONSHIP_URI_PATTERN = server.restApiUri() + "relationship/[0-9]+";
    }

    @After
    public void stopServer() {
        server.stop();
        server = null;
    }

    @Test
    public void shouldRespondWith201WhenSuccessfullyCreatedRelationshipWithProperties() throws Exception
    {
        long startNode = helper.createNode();
        long endNode = helper.createNode();
        String jsonString = "{\"to\" : \"" + server.restApiUri() + "node/" + endNode
                + "\", \"type\" : \"LOVES\", \"data\" : {\"foo\" : \"bar\"}}";
        ClientResponse response = Client.create().resource( server.restApiUri() + "node/" + startNode + "/relationships" ).type(
                MediaType.APPLICATION_JSON ).accept( MediaType.APPLICATION_JSON ).entity( jsonString ).post( ClientResponse.class );
        assertEquals( 201, response.getStatus() );
        assertTrue( response.getLocation().toString().matches( RELATIONSHIP_URI_PATTERN ) );
        assertEquals( MediaType.APPLICATION_JSON_TYPE, response.getType() );
        String relationshipUri = response.getLocation().toString();
        long relationshipId = Long.parseLong( relationshipUri.substring( relationshipUri.lastIndexOf( '/' ) + 1 ) );
        Map<String, Object> properties = helper.getRelationshipProperties( relationshipId );
        assertEquals( MapUtil.map( "foo", "bar" ), properties );
        assertProperRelationshipRepresentation( JsonHelper.jsonToMap( response.getEntity( String.class ) ) );
    }

    @Test
    public void shouldRespondWith201WhenSuccessfullyCreatedRelationship() throws Exception
    {
        long startNode = helper.createNode();
        long endNode = helper.createNode();
        String jsonString = "{\"to\" : \"" + server.restApiUri() + "node/" + endNode + "\", \"type\" : \"LOVES\"}";
        ClientResponse response = Client.create().resource( server.restApiUri() + "node/" + startNode + "/relationships" ).type(
                MediaType.APPLICATION_JSON ).accept( MediaType.APPLICATION_JSON ).entity( jsonString ).post( ClientResponse.class );
        assertEquals( 201, response.getStatus() );
        assertTrue( response.getLocation().toString().matches( RELATIONSHIP_URI_PATTERN ) );
        assertEquals( MediaType.APPLICATION_JSON_TYPE, response.getType() );
        assertProperRelationshipRepresentation( JsonHelper.jsonToMap( response.getEntity( String.class ) ) );
    }

    @Test
    public void shouldRespondWith404WhenStartNodeDoesNotExist() throws DatabaseBlockedException
    {
        long endNode = helper.createNode();
        String jsonString = "{\"to\" : \"" + server.restApiUri() + "node/" + endNode
                + "\", \"type\" : \"LOVES\", \"data\" : {\"foo\" : \"bar\"}}";
        ClientResponse response = Client.create().resource( server.restApiUri() + "node/999999/relationships" ).type(
                MediaType.APPLICATION_JSON ).accept( MediaType.APPLICATION_JSON ).entity( jsonString ).post( ClientResponse.class );
        assertEquals( 404, response.getStatus() );
    }

    @Test
    public void shouldRespondWith400WhenEndNodeDoesNotExist() throws DatabaseBlockedException
    {
        long startNode = helper.createNode();
        String jsonString = "{\"to\" : \"" + server.restApiUri() + "node/"
                + "999999\", \"type\" : \"LOVES\", \"data\" : {\"foo\" : \"bar\"}}";
        ClientResponse response = Client.create().resource( server.restApiUri() + "node/" + startNode + "/relationships" ).type(
                MediaType.APPLICATION_JSON ).accept( MediaType.APPLICATION_JSON ).entity( jsonString ).post( ClientResponse.class );
        assertEquals( 400, response.getStatus() );
    }

    @Test
    public void shouldRespondWith400WhenBadJsonProvided() throws DatabaseBlockedException
    {
        long startNode = helper.createNode();
        long endNode = helper.createNode();
        String jsonString = "{\"to\" : \"" + server.restApiUri() + "node/" + endNode
                + "\", \"type\" : \"LOVES\", \"data\" : {\"foo\" : **BAD JSON HERE*** \"bar\"}}";
        ClientResponse response = Client.create().resource( server.restApiUri() + "node/" + startNode + "/relationships" ).type(
                MediaType.APPLICATION_JSON ).accept( MediaType.APPLICATION_JSON ).entity( jsonString ).post( ClientResponse.class );

        assertEquals( 400, response.getStatus() );
    }

    private void assertProperRelationshipRepresentation(
            Map<String, Object> relrep )
    {
        RelationshipRepresentationTest.verifySerialisation( relrep );
    }
}
