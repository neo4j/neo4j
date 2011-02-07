/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.server.NeoServer;
import org.neo4j.server.ServerBuilder;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.repr.NodeRepresentationTest;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class ManageNodeFunctionalTest
{
    private static final long NON_EXISTENT_NODE_ID = 999999;
    private static String NODE_URI_PATTERN = "^.*/node/[0-9]+$";

    private NeoServer server;
    private FunctionalTestHelper functionalTestHelper;
    private GraphDbHelper helper;

    @Before
    public void setupServer() throws IOException {
        server = ServerBuilder.server().withRandomDatabaseDir().withPassingStartupHealthcheck().build();
        server.start();
        functionalTestHelper = new FunctionalTestHelper(server);
        helper = functionalTestHelper.getGraphDbHelper();
    }

    @After
    public void stopServer() {
        server.stop();
    }

    @Test
    public void shouldGet201WhenCreatingNode() throws Exception
    {
        ClientResponse response = sendCreateRequestToServer();
        assertEquals( 201, response.getStatus() );
        assertEquals( 201, response.getStatus() );
        assertTrue( response.getLocation().toString().matches( NODE_URI_PATTERN ) );
        assertEquals( MediaType.APPLICATION_JSON_TYPE, response.getType() );
        assertProperNodeRepresentation( JsonHelper.jsonToMap( response.getEntity( String.class ) ) );
    }

    @Test
    public void shouldGet201WhenCreatingNodeWithProperties() throws Exception
    {
        ClientResponse response = sendCreateRequestToServer( "{\"foo\" : \"bar\"}" );
        assertEquals( 201, response.getStatus() );
        assertNotNull( response.getHeaders().get( "Content-Length" ) );
        assertEquals( 201, response.getStatus() );
        assertTrue( response.getLocation().toString().matches( NODE_URI_PATTERN ) );
        assertEquals( MediaType.APPLICATION_JSON_TYPE, response.getType() );
        assertProperNodeRepresentation( JsonHelper.jsonToMap( response.getEntity( String.class ) ) );
    }

    @Test
    public void shouldGet400WhenSupplyingNullValueForAProperty() throws Exception
    {
        ClientResponse response = sendCreateRequestToServer( "{\"foo\":null}" );
        assertEquals( 400, response.getStatus() );
    }

    @Test
    public void shouldGet400WhenCreatingNodeMalformedProperties() throws Exception
    {
        ClientResponse response = sendCreateRequestToServer( "this:::isNot::JSON}" );
        assertEquals( 400, response.getStatus() );
    }

    @Test
    public void shouldGet400WhenCreatingNodeUnsupportedPropertyValues() throws Exception
    {
        ClientResponse response = sendCreateRequestToServer( "{\"foo\" : {\"bar\" : \"baz\"}}" );
        assertEquals( 400, response.getStatus() );
    }

    private ClientResponse sendCreateRequestToServer( String json )
    {
        Client client = Client.create();
        WebResource resource = client.resource( server.restApiUri() + "node/" );
        ClientResponse response = resource.type( MediaType.APPLICATION_JSON ).accept( MediaType.APPLICATION_JSON ).entity( json ).post( ClientResponse.class );
        return response;
    }

    private ClientResponse sendCreateRequestToServer()
    {
        Client client = Client.create();
        WebResource resource = client.resource( server.restApiUri() + "node/" );
        ClientResponse response = resource.type( MediaType.APPLICATION_FORM_URLENCODED ).accept( MediaType.APPLICATION_JSON ).post( ClientResponse.class );
        return response;
    }

    @Test
    public void shouldGetValidLocationHeaderWhenCreatingNode() throws Exception
    {
        ClientResponse response = sendCreateRequestToServer();
        assertNotNull( response.getLocation() );
        assertTrue( response.getLocation().toString().startsWith( server.restApiUri() + "node/" ) );
    }

    @Test
    public void shouldGetASingleContentLengthHeaderWhenCreatingANode()
    {
        ClientResponse response = sendCreateRequestToServer();
        List<String> contentLentgthHeaders = response.getHeaders().get( "Content-Length" );
        assertNotNull( contentLentgthHeaders );
        assertEquals( 1, contentLentgthHeaders.size() );
    }

    @Test
    public void shouldBeJSONContentTypeOnResponse()
    {
        ClientResponse response = sendCreateRequestToServer();
        assertEquals( MediaType.APPLICATION_JSON_TYPE, response.getType() );
    }

    @Test
    public void shouldGetValidNodeRepresentationWhenCreatingNode() throws Exception
    {
        ClientResponse response = sendCreateRequestToServer();
        String entity = response.getEntity( String.class );

        Map<String, Object> map = JsonHelper.jsonToMap( entity );

        assertNotNull( map );
        assertTrue( map.containsKey( "self" ) );

    }

    private void assertProperNodeRepresentation( Map<String, Object> noderep )
    {
        NodeRepresentationTest.verifySerialisation( noderep );
    }

    @Test
    public void shouldRespondWith204WhenNodeDeleted() throws Exception
    {
        ClientResponse response = sendDeleteRequestToServer( helper.createNode() );
        assertEquals( 204, response.getStatus() );
    }

    @Test
    public void shouldRespondWith404WhenNodeToBeDeletedCannotBeFound() throws Exception
    {
        ClientResponse response = sendDeleteRequestToServer( NON_EXISTENT_NODE_ID );
        assertEquals( 404, response.getStatus() );
    }

    @Test
    public void shouldRespondWith409WhenNodeCannotBeDeleted() throws Exception
    {
        long id = helper.createNode();
        helper.createRelationship( "LOVES", id, helper.createNode() );
        ClientResponse response = sendDeleteRequestToServer( id );
        assertEquals( 409, response.getStatus() );
    }
    
    @Test
    public void shouldRespondWith400IfInvalidJsonSentAsNodeProperties() throws URISyntaxException {
        String mangledJsonArray = "{\"myprop\":[1,2,\"three\"]}";
        ClientResponse response = sendCreateRequestToServer( mangledJsonArray);
        assertEquals(400, response.getStatus());
        assertEquals("text/plain", response.getType().toString());
        assertThat(response.getEntity(String.class), containsString(mangledJsonArray));
    }

    private ClientResponse sendDeleteRequestToServer( long id ) throws Exception
    {
        return Client.
                create().
                resource( new URI( server.restApiUri() + "node/" + id ) ).
                accept( MediaType.APPLICATION_JSON_TYPE ).
                delete( ClientResponse.class );
    }
}
