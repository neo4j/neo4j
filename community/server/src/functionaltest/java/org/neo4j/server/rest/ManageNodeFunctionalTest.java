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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.server.WebTestUtils.CLIENT;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.ServerBuilder;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class ManageNodeFunctionalTest
{
    private static final long NON_EXISTENT_NODE_ID = 999999;
    private static String NODE_URI_PATTERN = "^.*/node/[0-9]+$";

    private NeoServerWithEmbeddedWebServer server;
    private FunctionalTestHelper functionalTestHelper;
    private GraphDbHelper helper;

    public @Rule
    DocumentationGenerator gen = new DocumentationGenerator();

    @Before
    public void setupServer() throws IOException
    {
        server = ServerBuilder.server()
                .withRandomDatabaseDir()
                .withPassingStartupHealthcheck()
                .build();
        server.start();
        functionalTestHelper = new FunctionalTestHelper( server );
        helper = functionalTestHelper.getGraphDbHelper();
    }

    @After
    public void stopServer()
    {
        server.stop();
    }

    /**
     * Create node.
     */
    @Documented
    @Test
    public void shouldGet201WhenCreatingNode() throws Exception
    {
        ClientResponse response = gen.create()
                .expectedStatus( 201 )
                .expectedHeader( "Location" )
                .post( functionalTestHelper.nodeUri() )
                .response();
        assertTrue( response.getLocation()
                .toString()
                .matches( NODE_URI_PATTERN ) );
    }

    /**
     * Create node with properties.
     */
    @Documented
    @Test
    public void shouldGet201WhenCreatingNodeWithProperties() throws Exception
    {
        ClientResponse response = gen.create()
                .payload( "{\"foo\" : \"bar\"}" )
                .expectedStatus( 201 )
                .expectedHeader( "Location" )
                .expectedHeader( "Content-Length" )
                .post( functionalTestHelper.nodeUri() )
                .response();
        assertTrue( response.getLocation()
                .toString()
                .matches( NODE_URI_PATTERN ) );
    }

    /**
     * Property values can not be null.
     * 
     * This example shows the response you get when trying to set a property to null.
     */
    @Documented
    @Test
    public void shouldGet400WhenSupplyingNullValueForAProperty() throws Exception
    {
        gen.create()
                .payload( "{\"foo\":null}" )
                .expectedStatus( 400 )
                .post( functionalTestHelper.nodeUri() );
    }

    @Test
    public void shouldGet400WhenCreatingNodeMalformedProperties() throws Exception
    {
        ClientResponse response = sendCreateRequestToServer( "this:::isNot::JSON}" );
        assertEquals( 400, response.getStatus() );
    }

    @Test
    public void shouldGet400WhenCreatingNodeUnsupportedNestedPropertyValues() throws Exception
    {
        ClientResponse response = sendCreateRequestToServer( "{\"foo\" : {\"bar\" : \"baz\"}}" );
        assertEquals( 400, response.getStatus() );
    }

    private ClientResponse sendCreateRequestToServer( final String json )
    {
        WebResource resource = CLIENT.resource( functionalTestHelper.dataUri() + "node/" );
        ClientResponse response = resource.type( MediaType.APPLICATION_JSON )
                .accept( MediaType.APPLICATION_JSON )
                .entity( json )
                .post( ClientResponse.class );
        return response;
    }

    private ClientResponse sendCreateRequestToServer()
    {
        WebResource resource = CLIENT.resource( functionalTestHelper.dataUri() + "node/" );
        ClientResponse response = resource.type( MediaType.APPLICATION_FORM_URLENCODED )
                .accept( MediaType.APPLICATION_JSON )
                .post( ClientResponse.class );
        return response;
    }

    @Test
    public void shouldGetValidLocationHeaderWhenCreatingNode() throws Exception
    {
        ClientResponse response = sendCreateRequestToServer();
        assertNotNull( response.getLocation() );
        assertTrue( response.getLocation()
                .toString()
                .startsWith( functionalTestHelper.dataUri() + "node/" ) );
    }

    @Test
    public void shouldGetASingleContentLengthHeaderWhenCreatingANode()
    {
        ClientResponse response = sendCreateRequestToServer();
        List<String> contentLentgthHeaders = response.getHeaders()
                .get( "Content-Length" );
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

    /**
     * Delete node.
     */
    @Documented
    @Test
    public void shouldRespondWith204WhenNodeDeleted() throws Exception
    {
        gen.create()
                .expectedStatus( 204 )
                .delete( functionalTestHelper.dataUri() + "node/" + helper.createNode() );
    }

    @Test
    public void shouldRespondWith404AndSensibleEntityBodyWhenNodeToBeDeletedCannotBeFound() throws Exception
    {
        ClientResponse response = sendDeleteRequestToServer( NON_EXISTENT_NODE_ID );
        assertEquals( 404, response.getStatus() );

        Map<String, Object> jsonMap = JsonHelper.jsonToMap( response.getEntity( String.class ) );
        assertThat( jsonMap, hasKey( "message" ) );
        assertNotNull( jsonMap.get( "message" ) );
    }

    /**
     * Nodes with relationships can not be deleted.
     * 
     * The relationships on a node has to be deleted before the node can be
     * deleted.
     */
    @Documented
    @Test
    public void shouldRespondWith409AndSensibleEntityBodyWhenNodeCannotBeDeleted() throws Exception
    {
        long id = helper.createNode();
        helper.createRelationship( "LOVES", id, helper.createNode() );
        ClientResponse response = sendDeleteRequestToServer( id );
        assertEquals( 409, response.getStatus() );
        Map<String, Object> jsonMap = JsonHelper.jsonToMap( response.getEntity( String.class ) );
        assertThat( jsonMap, hasKey( "message" ) );
        assertNotNull( jsonMap.get( "message" ) );

        gen.create()
                .expectedStatus( 409 )
                .delete( functionalTestHelper.dataUri() + "node/" + id );
    }

    @Test
    public void shouldRespondWith400IfInvalidJsonSentAsNodePropertiesDuringNodeCreation() throws URISyntaxException
    {
        String mangledJsonArray = "{\"myprop\":[1,2,\"three\"]}";
        ClientResponse response = sendCreateRequestToServer( mangledJsonArray );
        assertEquals( 400, response.getStatus() );
        assertEquals( "text/plain", response.getType()
                .toString() );
        assertThat( response.getEntity( String.class ), containsString( mangledJsonArray ) );
    }

    @Test
    public void shouldRespondWith400IfInvalidJsonSentAsNodeProperty() throws URISyntaxException
    {
        URI nodeLocation = sendCreateRequestToServer().getLocation();

        String mangledJsonArray = "[1,2,\"three\"]";
        ClientResponse response = CLIENT.resource( new URI( nodeLocation.toString() + "/properties/myprop" ) )
                .type( MediaType.APPLICATION_JSON )
                .accept( MediaType.APPLICATION_JSON )
                .entity( mangledJsonArray )
                .put( ClientResponse.class );
        assertEquals( 400, response.getStatus() );
        assertEquals( "text/plain", response.getType()
                .toString() );
        assertThat( response.getEntity( String.class ), containsString( mangledJsonArray ) );
        response.close();
    }

    @Test
    public void shouldRespondWith400IfInvalidJsonSentAsNodeProperties() throws URISyntaxException
    {
        URI nodeLocation = sendCreateRequestToServer().getLocation();

        String mangledJsonProperties = "{\"a\":\"b\", \"c\":[1,2,\"three\"]}";
        ClientResponse response = CLIENT.resource( new URI( nodeLocation.toString() + "/properties" ) )
                .type( MediaType.APPLICATION_JSON )
                .accept( MediaType.APPLICATION_JSON )
                .entity( mangledJsonProperties )
                .put( ClientResponse.class );
        assertEquals( 400, response.getStatus() );
        assertEquals( "text/plain", response.getType()
                .toString() );
        assertThat( response.getEntity( String.class ), containsString( mangledJsonProperties ) );
        response.close();
    }

    private ClientResponse sendDeleteRequestToServer( final long id ) throws Exception
    {
        return Client.create()
                .resource( new URI( functionalTestHelper.dataUri() + "node/" + id ) )
                .accept( MediaType.APPLICATION_JSON_TYPE )
                .delete( ClientResponse.class );
    }
}
