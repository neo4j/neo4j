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

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.database.DatabaseBlockedException;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.domain.URIHelper;
import org.neo4j.server.rest.web.PropertyValueException;
import org.neo4j.test.TestData;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;

public class IndexNodeFunctionalTest
{
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
     * List node indexes (empty result). This is an example covering the case
     * where no node index exists.
     * 
     * ...
     * 
     * GET ${org.neo4j.server.rest.web}/index/node/
     */
    @Documented
    @Test
    public void shouldGetEmptyListOfNodeIndexesWhenNoneExist()
    {
        gen.get()
                .expectedStatus( 204 )
                .get( functionalTestHelper.nodeIndexUri() );
    }

    /**
     * List node indexes.
     * 
     * ...
     * 
     * GET ${org.neo4j.server.rest.web}/index/node/
     * 
     * @throws PropertyValueException
     */
    @Documented
    @Test
    public void shouldGetListOfNodeIndexesWhenOneExist() throws PropertyValueException
    {
        String indexName = "favorites";
        helper.createNodeIndex( indexName );
        String entity = gen.get()
                .expectedStatus( 200 )
                .get( functionalTestHelper.nodeIndexUri() )
                .entity();
        Map<String, Object> map = JsonHelper.jsonToMap( entity );
        assertNotNull( map.get( indexName ) );
        assertEquals( 1, map.size() );
    }

    /**
     * POST ${org.neo4j.server.rest.web}/index/node { "name":"index-name" }
     */
    @TestData.Title( "Create node index" )
    @Documented( "NOTE: Instead of creating the index this way, " + "you can simply start to use it, "
                 + "and it will be created automatically." )
    @Test
    public void shouldCreateANamedNodeIndex() throws JsonParseException
    {
        String indexName = "favorites";
        Map<String, String> indexSpecification = new HashMap<String, String>();
        indexSpecification.put( "name", indexName );

        gen.get()
                .payload( JsonHelper.createJsonFrom( indexSpecification ) )
                .expectedStatus( 201 )
                .expectedHeader( "Location" )
                .post( functionalTestHelper.nodeIndexUri() );

        assertEquals( 1, helper.getNodeIndexes().length );
        assertEquals( indexName, helper.getNodeIndexes()[0] );
    }

    /**
     * Create node index with configuration. This request is only necessary if
     * you want to customize the index settings. If you are happy with the
     * defaults, you can just start indexing nodes/relationships, as
     * non-existent indexes will automatically be created as you do. See
     * <<indexing-create-advanced>> for more information on index configuration.
     * 
     * ...
     * 
     * POST ${org.neo4j.server.rest.web}/index/node { "name":"index-name",
     * "config":{"type":"fulltext","provider":"lucene"} }
     * 
     * @throws Exception
     * @throws ClientHandlerException
     * @throws PropertyValueException
     */
    @Documented
    @Test
    public void shouldCreateANamedNodeIndexWithConfiguration() throws Exception
    {
        gen.get()
                .payload( "{\"name\":\"fulltext\", \"config\":{\"type\":\"fulltext\",\"provider\":\"lucene\"}}" )
                .expectedStatus( 201 )
                .expectedHeader( "Location" )
                .post( functionalTestHelper.nodeIndexUri() );

        assertEquals( 1, helper.getNodeIndexes().length );
        assertEquals( "fulltext", helper.getNodeIndexes()[0] );
    }

    /**
     * Add node to index.
     * 
     * Associates a node with the given key/value pair in the given index.
     * 
     * NOTE: Spaces in the URI have to be escaped.
     * 
     * [CAUTION] This does *not* overwrite previous entries. If you index the
     * same key/value/item combination twice,two index entries are created. To
     * do update-type operations,you need to delete the old entry before adding
     * a new one.
     * 
     * ...
     * 
     * POST ${org.neo4j.server.rest.web}/index/node/{indexName}/{key}/{value}
     */
    @Documented
    @Test
    public void shouldAddToIndex() throws Exception
    {
        String indexName = "favorites";
        String key = "key";
        String value = "the value";
        value = URIHelper.encode( value );
        int nodeId = 0;
        // implicitly create the index
        gen.get()
                .expectedStatus( 201 )
                .payload( JsonHelper.createJsonFrom( functionalTestHelper.nodeUri( nodeId ) ) )
                .post( functionalTestHelper.indexNodeUri( indexName, key, value ) );
        // look if we get one entry back
        ClientResponse response = Client.create().resource( functionalTestHelper.indexNodeUri( indexName, key, value ) )
                .accept( MediaType.APPLICATION_JSON_TYPE )
                .get( ClientResponse.class );
        String entity = response.getEntity( String.class );
        Collection<?> hits = (Collection<?>) JsonHelper.jsonToSingleValue( entity );
        assertEquals( 1, hits.size() );
    }

    /**
     * Find node by exact match.
     * 
     * NOTE: Spaces in the URI have to be escaped.
     */
    @Documented
    @Test
    public void shouldAddToIndexAndRetrieveItByExactMatch() throws Exception
    {
        String indexName = "favorites";
        String key = "key";
        String value = "the value";
        value = URIHelper.encode( value );
        // implicitly create the index
        ClientResponse response = Client.create().resource( functionalTestHelper.indexNodeUri( indexName, key, value ) )
                .entity( JsonHelper.createJsonFrom( functionalTestHelper.nodeUri( 0 ) ),
                        MediaType.APPLICATION_JSON_TYPE )
                .post( ClientResponse.class );
        assertEquals( response.getStatus(), 201 );

        // search it exact
        String entity = gen.get()
                .expectedStatus( 200 )
                .get( functionalTestHelper.indexNodeUri( indexName, key, value ) )
                .entity();
        Collection<?> hits = (Collection<?>) JsonHelper.jsonToSingleValue( entity );
        assertEquals( 1, hits.size() );
    }

    /**
     * POST ${org.neo4j.server.rest.web}/index/node/{indexName}/{key}/{value}
     * "http://uri.for.node.to.index"
     */
    @Test
    public void shouldRespondWith201CreatedWhenIndexingJsonNodeUri() throws DatabaseBlockedException,
            JsonParseException
    {
        long nodeId = helper.createNode();
        String key = "key";
        String value = "value";
        String indexName = "testy";
        helper.createNodeIndex( indexName );
        String entity = JsonHelper.createJsonFrom( functionalTestHelper.nodeUri( nodeId ) );
        ClientResponse response = Client.create().resource( functionalTestHelper.indexNodeUri( indexName, key, value ) )
                .type( MediaType.APPLICATION_JSON )
                .accept( MediaType.APPLICATION_JSON )
                .entity( entity )
                .post( ClientResponse.class );
        assertEquals( 201, response.getStatus() );
        assertNotNull( response.getHeaders()
                .getFirst( "Location" ) );
        assertEquals( Arrays.asList( (Long) nodeId ), helper.getIndexedNodes( indexName, key, value ) );
    }

    @Test
    public void shouldGetNodeRepresentationFromIndexUri() throws DatabaseBlockedException, JsonParseException
    {
        long nodeId = helper.createNode();
        String key = "key2";
        String value = "value";

        String indexName = "mindex";
        helper.createNodeIndex( indexName );
        ClientResponse response = Client.create().resource( functionalTestHelper.indexNodeUri( indexName, key, value ) )
                .type( MediaType.APPLICATION_JSON )
                .accept( MediaType.APPLICATION_JSON )
                .entity( JsonHelper.createJsonFrom( functionalTestHelper.nodeUri( nodeId ) ) )
                .post( ClientResponse.class );

        assertEquals( Status.CREATED.getStatusCode(), response.getStatus() );
        String indexUri = response.getHeaders()
                .getFirst( "Location" );

        response = Client.create().resource( indexUri )
                .accept( MediaType.APPLICATION_JSON )
                .get( ClientResponse.class );
        assertEquals( 200, response.getStatus() );

        String entity = response.getEntity( String.class );

        Map<String, Object> map = JsonHelper.jsonToMap( entity );
        assertNotNull( map.get( "self" ) );
    }

    @Test
    public void shouldGet404WhenRequestingIndexUriWhichDoesntExist() throws DatabaseBlockedException
    {
        String key = "key3";
        String value = "value";
        String indexName = "nosuchindex";
        String indexUri = functionalTestHelper.nodeIndexUri() + indexName + "/" + key + "/" + value;
        ClientResponse response = Client.create().resource( indexUri )
                .accept( MediaType.APPLICATION_JSON )
                .get( ClientResponse.class );
        assertEquals( Status.NOT_FOUND.getStatusCode(), response.getStatus() );
    }

    @Test
    public void shouldGet200AndArrayOfNodeRepsWhenGettingFromIndex() throws PropertyValueException
    {
        String key = "myKey";
        String value = "myValue";

        String name1 = "Thomas Anderson";
        String name2 = "Agent Smith";

        String indexName = "matrix";
        ClientResponse responseToPost = Client.create().resource( functionalTestHelper.nodeUri() )
                .accept( MediaType.APPLICATION_JSON )
                .entity( "{\"name\":\"" + name1 + "\"}", MediaType.APPLICATION_JSON )
                .post( ClientResponse.class );
        assertEquals( 201, responseToPost.getStatus() );
        String location1 = responseToPost.getHeaders()
                .getFirst( HttpHeaders.LOCATION );
        responseToPost.close();
        responseToPost = Client.create().resource( functionalTestHelper.nodeUri() )
                .accept( MediaType.APPLICATION_JSON )
                .entity( "{\"name\":\"" + name2 + "\"}", MediaType.APPLICATION_JSON )
                .post( ClientResponse.class );
        assertEquals( 201, responseToPost.getStatus() );
        String location2 = responseToPost.getHeaders()
                .getFirst( HttpHeaders.LOCATION );
        responseToPost.close();
        responseToPost = Client.create().resource( functionalTestHelper.indexNodeUri( indexName, key, value ) )
                .entity( JsonHelper.createJsonFrom( location1 ), MediaType.APPLICATION_JSON )
                .accept( MediaType.APPLICATION_JSON )
                .post( ClientResponse.class );
        assertEquals( 201, responseToPost.getStatus() );
        String indexLocation1 = responseToPost.getHeaders()
                .getFirst( HttpHeaders.LOCATION );
        responseToPost.close();
        responseToPost = Client.create().resource( functionalTestHelper.indexNodeUri( indexName, key, value ) )
                .entity( JsonHelper.createJsonFrom( location2 ), MediaType.APPLICATION_JSON )
                .accept( MediaType.APPLICATION_JSON )
                .post( ClientResponse.class );
        assertEquals( 201, responseToPost.getStatus() );
        String indexLocation2 = responseToPost.getHeaders()
                .getFirst( HttpHeaders.LOCATION );
        Map<String, String> uriToName = new HashMap<String, String>();
        uriToName.put( indexLocation1.toString(), name1 );
        uriToName.put( indexLocation2.toString(), name2 );
        responseToPost.close();

        ClientResponse response = Client.create().resource( functionalTestHelper.indexNodeUri( indexName, key, value ) )
                .accept( MediaType.APPLICATION_JSON )
                .get( ClientResponse.class );
        assertEquals( 200, response.getStatus() );
        Collection<?> items = (Collection<?>) JsonHelper.jsonToSingleValue( response.getEntity( String.class ) );
        int counter = 0;
        for ( Object item : items )
        {
            Map<?, ?> map = (Map<?, ?>) item;
            Map<?, ?> properties = (Map<?, ?>) map.get( "data" );
            assertNotNull( map.get( "self" ) );
            String indexedUri = (String) map.get( "indexed" );
            assertEquals( uriToName.get( indexedUri ), properties.get( "name" ) );
            counter++;
        }
        assertEquals( 2, counter );
        response.close();
    }

    @Test
    public void shouldGet200WhenQueryingIndex() throws PropertyValueException
    {
        String indexName = "bobTheIndex";
        String key = "bobsKey";
        String value = "bobsValue";
        long node = helper.createNode();
        helper.addNodeToIndex( indexName, key, value, node );

        ClientResponse response = Client.create().resource(
                functionalTestHelper.indexNodeUri( indexName ) + "?query=" + key + ":" + value )
                .accept( MediaType.APPLICATION_JSON )
                .get( ClientResponse.class );

        assertEquals( 200, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldGet200WhenGettingNodesFromIndexWithNoHits()
    {
        String indexName = "empty-index";
        helper.createNodeIndex( indexName );
        ClientResponse response = Client.create().resource(
                functionalTestHelper.indexNodeUri( indexName, "non-existent-key", "non-existent-value" ) )
                .accept( MediaType.APPLICATION_JSON )
                .get( ClientResponse.class );
        assertEquals( 200, response.getStatus() );
        response.close();
    }

    /**
     * Delete node index.
     */
    @Documented
    @Test
    public void shouldReturn204WhenRemovingNodeIndexes() throws DatabaseBlockedException, JsonParseException
    {
        String indexName = "kvnode";
        helper.createNodeIndex( indexName );

        gen.get()
                .expectedStatus( 204 )
                .delete( functionalTestHelper.indexNodeUri( indexName ) );
    }

    @Test
    public void shouldReturn204WhenRemovingRelationshipIndexes() throws DatabaseBlockedException, JsonParseException
    {

        String indexName = "blah";
        helper.createRelationshipIndex( indexName );

        // Remove the index
        ClientResponse response = Client.create().resource( functionalTestHelper.indexRelationshipUri( indexName ) )
                .accept( MediaType.APPLICATION_JSON )
                .delete( ClientResponse.class );

        assertEquals( 204, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldBeAbleToRemoveIndexing() throws DatabaseBlockedException, JsonParseException
    {
        String key1 = "kvkey1";
        String key2 = "kvkey2";
        String value1 = "value1";
        String value2 = "value2";
        String indexName = "kvnode";
        long node = helper.createNode( MapUtil.map( key1, value1, key1, value2, key2, value1, key2, value2 ) );
        helper.addNodeToIndex( indexName, key1, value1, node );
        helper.addNodeToIndex( indexName, key1, value2, node );
        helper.addNodeToIndex( indexName, key2, value1, node );
        helper.addNodeToIndex( indexName, key2, value2, node );
        assertEquals( 1, helper.getIndexedNodes( indexName, key1, value1 )
                .size() );
        assertEquals( 1, helper.getIndexedNodes( indexName, key1, value2 )
                .size() );
        assertEquals( 1, helper.getIndexedNodes( indexName, key2, value1 )
                .size() );
        assertEquals( 1, helper.getIndexedNodes( indexName, key2, value2 )
                .size() );
        Client.create().resource( functionalTestHelper.nodeIndexUri() + indexName + "/" + key1 + "/" + value1 + "/" + node )
                .delete( ClientResponse.class );
        assertEquals( 0, helper.getIndexedNodes( indexName, key1, value1 )
                .size() );
        assertEquals( 1, helper.getIndexedNodes( indexName, key1, value2 )
                .size() );
        assertEquals( 1, helper.getIndexedNodes( indexName, key2, value1 )
                .size() );
        assertEquals( 1, helper.getIndexedNodes( indexName, key2, value2 )
                .size() );
        Client.create().resource( functionalTestHelper.nodeIndexUri() + indexName + "/" + key2 + "/" + node )
                .delete( ClientResponse.class );
        assertEquals( 0, helper.getIndexedNodes( indexName, key1, value1 )
                .size() );
        assertEquals( 1, helper.getIndexedNodes( indexName, key1, value2 )
                .size() );
        assertEquals( 0, helper.getIndexedNodes( indexName, key2, value1 )
                .size() );
        assertEquals( 0, helper.getIndexedNodes( indexName, key2, value2 )
                .size() );
        Client.create().resource( functionalTestHelper.nodeIndexUri() + indexName + "/" + node )
                .delete( ClientResponse.class );
        assertEquals( 0, helper.getIndexedNodes( indexName, key1, value1 )
                .size() );
        assertEquals( 0, helper.getIndexedNodes( indexName, key1, value2 )
                .size() );
        assertEquals( 0, helper.getIndexedNodes( indexName, key2, value1 )
                .size() );
        assertEquals( 0, helper.getIndexedNodes( indexName, key2, value2 )
                .size() );
    }

    @Test
    public void shouldBeAbleToIndexValuesContainingSpaces() throws Exception
    {
        long nodeId = helper.createNode();
        String key = "key";
        String value = "value with   spaces  in it";
        value = URIHelper.encode( value );
        String indexName = "spacey-values";
        helper.createNodeIndex( indexName );
        ClientResponse response = Client.create().resource( functionalTestHelper.indexNodeUri( indexName, key, value ) )
                .entity( JsonHelper.createJsonFrom( functionalTestHelper.nodeUri( nodeId ) ),
                        MediaType.APPLICATION_JSON )
                .accept( MediaType.APPLICATION_JSON )
                .post( ClientResponse.class );
        assertEquals( Status.CREATED.getStatusCode(), response.getStatus() );
        URI location = response.getLocation();
        response.close();
        response = Client.create().resource( functionalTestHelper.indexNodeUri( indexName, key, value ) )
                .accept( MediaType.APPLICATION_JSON_TYPE )
                .get( ClientResponse.class );
        assertEquals( Status.OK.getStatusCode(), response.getStatus() );
        Collection<?> hits = (Collection<?>) JsonHelper.jsonToSingleValue( response.getEntity( String.class ) );
        assertEquals( 1, hits.size() );
        response.close();

        Client.create().resource( location )
                .delete();
        response = Client.create().resource( functionalTestHelper.indexNodeUri( indexName, key, value ) )
                .accept( MediaType.APPLICATION_JSON_TYPE )
                .get( ClientResponse.class );
        hits = (Collection<?>) JsonHelper.jsonToSingleValue( response.getEntity( String.class ) );
        assertEquals( 0, hits.size() );
        response.close();
    }

    @Test
    public void shouldRespondWith400WhenSendingCorruptJson() throws Exception
    {
        long nodeId = helper.createNode();
        String key = "key";
        String value = "value";
        String indexName = "botherable-index";
        helper.createNodeIndex( indexName );
        ClientResponse response = Client.create().resource( functionalTestHelper.indexNodeUri( indexName, key, value ) )
                .type( MediaType.APPLICATION_JSON )
                .accept( MediaType.APPLICATION_JSON )
                .entity( functionalTestHelper.nodeUri( nodeId ) )
                .post( ClientResponse.class );
        assertEquals( 400, response.getStatus() );
        response.close();
    }
}
