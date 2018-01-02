/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MediaType;

import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.metatest.TestJavaTestDocsGenerator;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ManageNodeDocIT extends AbstractRestFunctionalDocTestBase
{
    private static final long NON_EXISTENT_NODE_ID = 999999;
    private static String NODE_URI_PATTERN = "^.*/node/[0-9]+$";

    private static FunctionalTestHelper functionalTestHelper;
    private static GraphDbHelper helper;

    @BeforeClass
    public static void setupServer() throws IOException
    {
        functionalTestHelper = new FunctionalTestHelper( server() );
        helper = functionalTestHelper.getGraphDbHelper();
    }

    @Test
    public void create_node() throws Exception
    {
        JaxRsResponse response = gen.get()
                .expectedStatus( 201 )
                .expectedHeader( "Location" )
                .post( functionalTestHelper.nodeUri() )
                .response();
        assertTrue( response.getLocation()
                .toString()
                .matches( NODE_URI_PATTERN ) );
    }

    @Test
    public void create_node_with_properties() throws Exception
    {
        JaxRsResponse response = gen.get()
                .payload( "{\"foo\" : \"bar\"}" )
                .expectedStatus( 201 )
                .expectedHeader( "Location" )
                .expectedHeader( "Content-Length" )
                .post( functionalTestHelper.nodeUri() )
                .response();
        assertTrue( response.getLocation()
                .toString()
                .matches( NODE_URI_PATTERN ) );
        checkGeneratedFiles();
    }

    private void checkGeneratedFiles()
    {
        String requestDocs, responseDocs, graphDocs;
        try
        {
            requestDocs = TestJavaTestDocsGenerator.readFileAsString( new File(
                    "target/docs/dev/rest-api/includes/create-node-with-properties.request.asciidoc" ) );
            responseDocs = TestJavaTestDocsGenerator.readFileAsString( new File(
                    "target/docs/dev/rest-api/includes/create-node-with-properties.response.asciidoc" ) );
            graphDocs = TestJavaTestDocsGenerator.readFileAsString( new File(
                    "target/docs/dev/rest-api/includes/create-node-with-properties.graph.asciidoc" ) );
        }
        catch ( IOException ioe )
        {
            throw new RuntimeException(
                    "Error reading generated documentation file: ", ioe );
        }
        for ( String s : new String[] { "POST", "Accept", "application/json",
                "Content-Type", "{", "foo", "bar", "}" } )
        {
            assertThat( requestDocs, containsString( s ) );
        }
        for ( String s : new String[] { "201", "Created", "Content-Length",
                "Content-Type", "Location", "{", "foo", "bar", "}" } )
        {
            assertThat( responseDocs, containsString( s ) );
        }
        for ( String s : new String[] { "foo", "bar" } )
        {
            assertThat( graphDocs, containsString( s ) );
        }
    }

    @Test
    public void create_node_with_array_properties() throws Exception
    {
        String response = gen.get()
                .payload( "{\"foo\" : [1,2,3]}" )
                .expectedStatus( 201 )
                .expectedHeader( "Location" )
                .expectedHeader( "Content-Length" )
                .post( functionalTestHelper.nodeUri() )
                .response().getEntity();
        assertThat( response, containsString( "[ 1, 2, 3 ]" ) );
    }

    @Documented( "Property values can not be null.\n" +
                 "\n" +
                 "This example shows the response you get when trying to set a property to +null+." )
    @Test
    public void shouldGet400WhenSupplyingNullValueForAProperty() throws Exception
    {
        gen.get()
                .noGraph()
                .payload( "{\"foo\":null}" )
                .expectedStatus( 400 )
                .post( functionalTestHelper.nodeUri() );
    }

    @Test
    public void shouldGet400WhenCreatingNodeMalformedProperties() throws Exception
    {
        JaxRsResponse response = sendCreateRequestToServer("this:::isNot::JSON}");
        assertEquals( 400, response.getStatus() );
    }

    @Test
    public void shouldGet400WhenCreatingNodeUnsupportedNestedPropertyValues() throws Exception
    {
        JaxRsResponse response = sendCreateRequestToServer("{\"foo\" : {\"bar\" : \"baz\"}}");
        assertEquals( 400, response.getStatus() );
    }

    private JaxRsResponse sendCreateRequestToServer(final String json)
    {
        return RestRequest.req().post( functionalTestHelper.dataUri() + "node/" , json );
    }

    private JaxRsResponse sendCreateRequestToServer()
    {
        return RestRequest.req().post( functionalTestHelper.dataUri() + "node/" , null, MediaType.APPLICATION_JSON_TYPE );
    }

    @Test
    public void shouldGetValidLocationHeaderWhenCreatingNode() throws Exception
    {
        JaxRsResponse response = sendCreateRequestToServer();
        assertNotNull( response.getLocation() );
        assertTrue( response.getLocation()
                .toString()
                .startsWith( functionalTestHelper.dataUri() + "node/" ) );
    }

    @Test
    public void shouldGetASingleContentLengthHeaderWhenCreatingANode()
    {
        JaxRsResponse response = sendCreateRequestToServer();
        List<String> contentLentgthHeaders = response.getHeaders()
                .get( "Content-Length" );
        assertNotNull( contentLentgthHeaders );
        assertEquals( 1, contentLentgthHeaders.size() );
    }

    @Test
    public void shouldBeJSONContentTypeOnResponse()
    {
        JaxRsResponse response = sendCreateRequestToServer();
        assertThat( response.getType().toString(), containsString( MediaType.APPLICATION_JSON ) );
    }

    @Test
    public void shouldGetValidNodeRepresentationWhenCreatingNode() throws Exception
    {
        JaxRsResponse response = sendCreateRequestToServer();
        String entity = response.getEntity();

        Map<String, Object> map = JsonHelper.jsonToMap( entity );

        assertNotNull( map );
        assertTrue( map.containsKey( "self" ) );

    }

    @Documented( "Delete node." )
    @Test
    public void shouldRespondWith204WhenNodeDeleted() throws Exception
    {
        long node = helper.createNode();
        gen.get().description( startGraph( "delete node" ) )
                .expectedStatus( 204 )
                .delete( functionalTestHelper.dataUri() + "node/" + node );
    }

    @Test
    public void shouldRespondWith404AndSensibleEntityBodyWhenNodeToBeDeletedCannotBeFound() throws Exception
    {
        JaxRsResponse response = sendDeleteRequestToServer(NON_EXISTENT_NODE_ID);
        assertEquals( 404, response.getStatus() );

        Map<String, Object> jsonMap = JsonHelper.jsonToMap( response.getEntity() );
        assertThat( jsonMap, hasKey( "message" ) );
        assertNotNull( jsonMap.get( "message" ) );
    }

    @Documented( "Nodes with relationships cannot be deleted.\n" +
                 "\n" +
                 "The relationships on a node has to be deleted before the node can be\n" +
                 "deleted.\n" +
                 " \n" +
                 "TIP: You can use `DETACH DELETE` in Cypher to delete nodes and their relationships in one go." )
    @Test
    public void shouldRespondWith409AndSensibleEntityBodyWhenNodeCannotBeDeleted() throws Exception
    {
        long id = helper.createNode();
        helper.createRelationship( "LOVES", id, helper.createNode() );
        JaxRsResponse response = sendDeleteRequestToServer(id);
        assertEquals( 409, response.getStatus() );
        Map<String, Object> jsonMap = JsonHelper.jsonToMap( response.getEntity() );
        assertThat( jsonMap, hasKey( "message" ) );
        assertNotNull( jsonMap.get( "message" ) );

        gen.get().description( startGraph( "nodes with rels can not be deleted" ) ).noGraph()
                .expectedStatus( 409 )
                .delete( functionalTestHelper.dataUri() + "node/" + id );
    }

    @Test
    public void shouldRespondWith400IfInvalidJsonSentAsNodePropertiesDuringNodeCreation() throws URISyntaxException
    {
        String mangledJsonArray = "{\"myprop\":[1,2,\"three\"]}";
        JaxRsResponse response = sendCreateRequestToServer(mangledJsonArray);
        assertEquals( 400, response.getStatus() );
        assertEquals( "text/plain", response.getType()
                .toString() );
        assertThat( response.getEntity(), containsString( mangledJsonArray ) );
    }

    @Test
    public void shouldRespondWith400IfInvalidJsonSentAsNodeProperty() throws URISyntaxException {
        URI nodeLocation = sendCreateRequestToServer().getLocation();

        String mangledJsonArray = "[1,2,\"three\"]";
        JaxRsResponse response = RestRequest.req().put(nodeLocation.toString() + "/properties/myprop", mangledJsonArray);
        assertEquals(400, response.getStatus());
        assertEquals("text/plain", response.getType()
                .toString());
        assertThat( response.getEntity(), containsString(mangledJsonArray));
        response.close();
    }

    @Test
    public void shouldRespondWith400IfInvalidJsonSentAsNodeProperties() throws URISyntaxException {
        URI nodeLocation = sendCreateRequestToServer().getLocation();

        String mangledJsonProperties = "{\"a\":\"b\", \"c\":[1,2,\"three\"]}";
        JaxRsResponse response = RestRequest.req().put(nodeLocation.toString() + "/properties", mangledJsonProperties);
        assertEquals(400, response.getStatus());
        assertEquals("text/plain", response.getType()
                .toString());
        assertThat( response.getEntity(), containsString(mangledJsonProperties));
        response.close();
    }

    private JaxRsResponse sendDeleteRequestToServer(final long id) throws Exception
    {
        return RestRequest.req().delete(functionalTestHelper.dataUri() + "node/" + id);
    }
}
