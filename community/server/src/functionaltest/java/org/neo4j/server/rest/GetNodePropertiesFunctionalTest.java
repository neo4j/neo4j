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
import static org.junit.Assert.assertNotNull;
import static org.neo4j.server.WebTestUtils.CLIENT;

import java.io.IOException;
import java.util.Collections;

import javax.ws.rs.core.MediaType;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.TestData;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class GetNodePropertiesFunctionalTest
{
    private static NeoServerWithEmbeddedWebServer server;
    private static FunctionalTestHelper functionalTestHelper;

    @BeforeClass
    public static void setupServer() throws IOException
    {
        server = ServerHelper.createServer();
        functionalTestHelper = new FunctionalTestHelper( server );
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

    public @Rule
    TestData<DocsGenerator> gen = TestData.producedThrough( DocsGenerator.PRODUCER );

    /**
     * Get properties for node (empty result).
     * 
     * If there are no properties, there will be an HTTP 204 response.
     */
    @Documented
    @Test
    public void shouldGet204ForNoProperties()
    {
        Client client = CLIENT;
        WebResource createResource = client.resource( functionalTestHelper.dataUri() + "node/" );
        ClientResponse createResponse = createResource.accept( MediaType.APPLICATION_JSON )
                .entity( "" )
                .post( ClientResponse.class );
        gen.get()
                .expectedStatus( 204 )
                .get( createResponse.getLocation()
                        .toString() + "/properties" );
    }

    /**
     * Get properties for node.
     */
    @Documented
    @Test
    public void shouldGet200ForProperties() throws JsonParseException
    {
        Client client = CLIENT;
        WebResource createResource = client.resource( functionalTestHelper.dataUri() + "node/" );
        String entity = JsonHelper.createJsonFrom( Collections.singletonMap( "foo", "bar" ) );
        ClientResponse createResponse = createResource.type( MediaType.APPLICATION_JSON )
                .entity( entity )
                .accept( MediaType.APPLICATION_JSON )
                .post( ClientResponse.class );
        gen.get()
                .expectedStatus( 200 )
                .get( createResponse.getLocation()
                        .toString() + "/properties" );
    }

    @Test
    public void shouldGetContentLengthHeaderForRetrievingProperties() throws JsonParseException
    {
        Client client = CLIENT;
        WebResource createResource = client.resource( functionalTestHelper.dataUri() + "node/" );
        String entity = JsonHelper.createJsonFrom( Collections.singletonMap( "foo", "bar" ) );
        ClientResponse createResponse = createResource.type( MediaType.APPLICATION_JSON )
                .entity( entity )
                .accept( MediaType.APPLICATION_JSON )
                .post( ClientResponse.class );
        WebResource resource = client.resource( createResponse.getLocation()
                .toString() + "/properties" );
        ClientResponse response = resource.type( MediaType.APPLICATION_FORM_URLENCODED )
                .accept( MediaType.APPLICATION_JSON )
                .get( ClientResponse.class );
        assertNotNull( response.getHeaders()
                .get( "Content-Length" ) );
    }

    @Test
    public void shouldGet404ForPropertiesOnNonExistentNode()
    {
        WebResource resource = CLIENT.resource( functionalTestHelper.dataUri() + "node/999999/properties" );
        ClientResponse response = resource.type( MediaType.APPLICATION_FORM_URLENCODED )
                .accept( MediaType.APPLICATION_JSON )
                .get( ClientResponse.class );
        assertEquals( 404, response.getStatus() );
    }

    @Test
    public void shouldBeJSONContentTypeOnPropertiesResponse() throws JsonParseException
    {
        Client client = CLIENT;
        WebResource createResource = client.resource( functionalTestHelper.dataUri() + "node/" );
        String entity = JsonHelper.createJsonFrom( Collections.singletonMap( "foo", "bar" ) );
        ClientResponse createResponse = createResource.type( MediaType.APPLICATION_JSON )
                .entity( entity )
                .accept( MediaType.APPLICATION_JSON )
                .post( ClientResponse.class );
        WebResource resource = client.resource( createResponse.getLocation()
                .toString() + "/properties" );
        ClientResponse response = resource.type( MediaType.APPLICATION_FORM_URLENCODED )
                .accept( MediaType.APPLICATION_JSON )
                .get( ClientResponse.class );
        assertEquals( MediaType.APPLICATION_JSON_TYPE, response.getType() );

        createResponse.close();
        response.close();
    }

    @Test
    public void shouldGet404ForNoProperty()
    {
        Client client = CLIENT;
        WebResource createResource = client.resource( functionalTestHelper.dataUri() + "node/" );
        ClientResponse createResponse = createResource.type( MediaType.APPLICATION_FORM_URLENCODED )
                .accept( MediaType.APPLICATION_JSON )
                .post( ClientResponse.class );
        WebResource resource = client.resource( getPropertyUri( createResponse.getLocation()
                .toString(), "foo" ) );
        ClientResponse response = resource.accept( MediaType.APPLICATION_JSON )
                .get( ClientResponse.class );
        assertEquals( 404, response.getStatus() );

        createResponse.close();
        response.close();
    }

    /**
     * Get property for node.
     * 
     * Get a single node property from a node.
     */
    @Documented
    @Test
    public void shouldGet200ForProperty() throws JsonParseException
    {
        Client client = CLIENT;
        WebResource createResource = client.resource( functionalTestHelper.dataUri() + "node/" );
        String entity = JsonHelper.createJsonFrom( Collections.singletonMap( "foo", "bar" ) );
        ClientResponse createResponse = createResource.type( MediaType.APPLICATION_JSON )
                .entity( entity )
                .accept( MediaType.APPLICATION_JSON )
                .post( ClientResponse.class );
        WebResource resource = client.resource( getPropertyUri( createResponse.getLocation()
                .toString(), "foo" ) );
        ClientResponse response = resource.accept( MediaType.APPLICATION_JSON )
                .get( ClientResponse.class );
        assertEquals( 200, response.getStatus() );

        createResponse.close();
        response.close();

        gen.get()
                .expectedStatus( 200 )
                .get( getPropertyUri( createResponse.getLocation()
                        .toString(), "foo" ).toString() );
    }

    @Test
    public void shouldGet404ForPropertyOnNonExistentNode()
    {
        Client client = CLIENT;
        WebResource resource = client.resource( getPropertyUri( functionalTestHelper.dataUri() + "node/" + "999999",
                "foo" ) );
        ClientResponse response = resource.type( MediaType.APPLICATION_FORM_URLENCODED )
                .accept( MediaType.APPLICATION_JSON )
                .get( ClientResponse.class );
        assertEquals( 404, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldBeJSONContentTypeOnPropertyResponse() throws JsonParseException
    {
        Client client = CLIENT;
        WebResource createResource = client.resource( functionalTestHelper.dataUri() + "node/" );
        String entity = JsonHelper.createJsonFrom( Collections.singletonMap( "foo", "bar" ) );
        ClientResponse createResponse = createResource.type( MediaType.APPLICATION_JSON )
                .entity( entity )
                .accept( MediaType.APPLICATION_JSON )
                .post( ClientResponse.class );
        WebResource resource = client.resource( getPropertyUri( createResponse.getLocation()
                .toString(), "foo" ) );
        ClientResponse response = resource.accept( MediaType.APPLICATION_JSON )
                .get( ClientResponse.class );
        assertEquals( MediaType.APPLICATION_JSON_TYPE, response.getType() );

        createResponse.close();
        response.close();
    }

    private String getPropertyUri( final String baseUri, final String key )
    {
        return baseUri.toString() + "/properties/" + key;
    }
}
