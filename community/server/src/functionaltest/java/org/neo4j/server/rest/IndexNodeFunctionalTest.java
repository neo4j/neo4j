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

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.ServerBuilder;
import org.neo4j.server.database.DatabaseBlockedException;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.domain.URIHelper;
import org.neo4j.server.rest.web.PropertyValueException;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;

public class IndexNodeFunctionalTest
{
    private NeoServerWithEmbeddedWebServer server;
    private FunctionalTestHelper functionalTestHelper;
    private GraphDbHelper helper;

    @Before
    public void setupServer() throws IOException
    {
        server = ServerBuilder.server().withRandomDatabaseDir().withPassingStartupHealthcheck().build();
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
     * GET ${org.neo4j.server.rest.web}/index/node/
     */
    @Test
    public void shouldGetEmptyListOfNodeIndexesWhenNoneExist()
    {
        ClientResponse response = Client.create().resource( functionalTestHelper.nodeIndexUri() )
                .accept( MediaType.APPLICATION_JSON ).get( ClientResponse.class );

        Assert.assertEquals( 204, response.getStatus() );
    }

    /**
     * POST ${org.neo4j.server.rest.web}/index/node
     * {
     * "name":"index-name"
     * }
     */
    @Test
    public void shouldCreateANamedNodeIndex() throws JsonParseException
    {
        String indexName = "favorites";
        Map<String, String> indexSpecification = new HashMap<String, String>();
        indexSpecification.put( "name", indexName );
        ClientResponse response = Client.create().resource( functionalTestHelper.nodeIndexUri() )
                .type( MediaType.APPLICATION_JSON )
                .accept( MediaType.APPLICATION_JSON )
                .entity( JsonHelper.createJsonFrom( indexSpecification ) ).post( ClientResponse.class );
        assertEquals( 201, response.getStatus() );
        assertNotNull( response.getHeaders().getFirst( "Location" ) );
        assertEquals( 1, helper.getNodeIndexes().length );
        assertNotNull( helper.getNodeIndex( indexName ) );
    }

    /**
     * POST ${org.neo4j.server.rest.web}/index/node
     * {
     * "name":"index-name",
     * "config":{"type":"fulltext","provider":"lucene"}
     * }
     * @throws Exception 
     * @throws ClientHandlerException 
     * @throws PropertyValueException 
     */
    @Test
    public void shouldCreateANamedNodeIndexWithConfigurationAndRetrieveItByExactMatch() throws Exception
    {
        String indexName = "favorites";
        ClientResponse response = Client.create().resource( functionalTestHelper.nodeIndexUri() )
                .type( MediaType.APPLICATION_JSON )
                .accept( MediaType.APPLICATION_JSON )
                .entity( "{\"name\":\"fulltext\", \"config\":{\"type\":\"fulltext\",\"provider\":\"lucene\"}}" ).post( ClientResponse.class );
        assertEquals( 201, response.getStatus() );
        assertNotNull( response.getHeaders().getFirst( "Location" ) );
        assertEquals( 1, helper.getNodeIndexes().length );
        assertNotNull( helper.getNodeIndex( indexName ) );
        //index a node
        String key = "key";
        String value = "value with   spaces  in it";
        value = URIHelper.encode( value );
        helper.createNodeIndex( indexName );
        response = Client.create().resource( functionalTestHelper.indexNodeUri( indexName, key, value ) )
                .entity( JsonHelper.createJsonFrom( functionalTestHelper.nodeUri( 0 ) ), MediaType.APPLICATION_JSON )
                .accept( MediaType.APPLICATION_JSON )
                .post( ClientResponse.class );
        assertEquals( Status.CREATED.getStatusCode(), response.getStatus() );
        //search it exact
        response = Client.create().resource( functionalTestHelper.indexNodeUri( indexName, key, value ) ).accept( MediaType.APPLICATION_JSON )
        .get( ClientResponse.class );
        assertEquals( 200, response.getStatus() );
        Collection<?> hits = (Collection<?>) JsonHelper.jsonToSingleValue( response.getEntity( String.class ) );
        assertEquals( 1, hits.size() );

    }
    
    /**
     * POST ${org.neo4j.server.rest.web}/index/node/{indexName}/{key}/{value}
     * "http://uri.for.node.to.index"
     */
    @Test
    public void shouldRespondWith201CreatedWhenIndexingJsonNodeUri() throws DatabaseBlockedException, JsonParseException
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
                .entity( entity ).post( ClientResponse.class );
        assertEquals( 201, response.getStatus() );
        assertNotNull( response.getHeaders().getFirst( "Location" ) );
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
                .accept( MediaType.APPLICATION_JSON ).entity( JsonHelper.createJsonFrom( functionalTestHelper.nodeUri( nodeId ) ) ).post( ClientResponse.class );

        assertEquals( Status.CREATED.getStatusCode(), response.getStatus() );
        String indexUri = response.getHeaders().getFirst( "Location" );
        
        System.out.println(indexUri);

        response = Client.create().resource( indexUri ).accept( MediaType.APPLICATION_JSON ).get( ClientResponse.class );
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
        ClientResponse response = Client.create().resource( indexUri ).accept( MediaType.APPLICATION_JSON ).get( ClientResponse.class );
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
        String location1 = responseToPost.getHeaders().getFirst( HttpHeaders.LOCATION );
        responseToPost = Client.create().resource( functionalTestHelper.nodeUri() ).accept( MediaType.APPLICATION_JSON ).entity( "{\"name\":\"" + name2 + "\"}",
                MediaType.APPLICATION_JSON ).post( ClientResponse.class );
        assertEquals( 201, responseToPost.getStatus() );
        String location2 = responseToPost.getHeaders().getFirst( HttpHeaders.LOCATION );
        responseToPost = Client.create().resource( functionalTestHelper.indexNodeUri( indexName, key, value ) )
                .entity( JsonHelper.createJsonFrom( location1 ), MediaType.APPLICATION_JSON )
                .accept( MediaType.APPLICATION_JSON )
                .post( ClientResponse.class );
        assertEquals( 201, responseToPost.getStatus() );
        String indexLocation1 = responseToPost.getHeaders().getFirst( HttpHeaders.LOCATION );
        responseToPost = Client.create().resource( functionalTestHelper.indexNodeUri( indexName, key, value ) )
                .entity( JsonHelper.createJsonFrom( location2 ), MediaType.APPLICATION_JSON )
                .accept( MediaType.APPLICATION_JSON )
                .post( ClientResponse.class );
        assertEquals( 201, responseToPost.getStatus() );
        String indexLocation2 = responseToPost.getHeaders().getFirst( HttpHeaders.LOCATION );
        Map<String, String> uriToName = new HashMap<String, String>();
        uriToName.put( indexLocation1.toString(), name1 );
        uriToName.put( indexLocation2.toString(), name2 );

        ClientResponse response = Client.create().resource( functionalTestHelper.indexNodeUri( indexName, key, value ) ).accept( MediaType.APPLICATION_JSON )
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
    }

    @Test
    public void shouldGet200WhenGettingNodesFromIndexWithNoHits()
    {
        String indexName = "empty-index";
        helper.createNodeIndex( indexName );
        ClientResponse response = Client.create().resource( functionalTestHelper.indexNodeUri( indexName, "non-existent-key", "non-existent-value" ) ).accept(
                MediaType.APPLICATION_JSON ).get( ClientResponse.class );
        assertEquals( 200, response.getStatus() );
    }

    @Test
    @Ignore("Unclear contract: remove the index itself? That is unsupported in the new index api")
    public void shouldGet200AndBeAbleToRemoveIndexing() throws DatabaseBlockedException, JsonParseException
    {
        ClientResponse response = Client.create().resource( functionalTestHelper.nodeUri() ).type( MediaType.APPLICATION_FORM_URLENCODED ).accept(
                MediaType.APPLICATION_JSON ).post( ClientResponse.class );
        String nodeUri = response.getHeaders().getFirst( HttpHeaders.LOCATION );
        String key = "key_remove";
        String value = "value";
        String indexUri = Client.create().resource( functionalTestHelper.indexUri() + "/node/" + key + "/" + value ).entity( JsonHelper.createJsonFrom( nodeUri ),
                MediaType.APPLICATION_JSON ).post( ClientResponse.class ).getHeaders().getFirst( HttpHeaders.LOCATION );
        assertEquals( 1, helper.getIndexedNodes( "node", key, value ).size() );
        response = Client.create().resource( indexUri ).delete( ClientResponse.class );
        assertEquals( Status.NO_CONTENT.getStatusCode(), response.getStatus() );
        assertEquals( 0, helper.getIndexedNodes( "node", key, value ).size() );

        response = Client.create().resource( indexUri ).delete( ClientResponse.class );
        assertEquals( Status.NOT_FOUND.getStatusCode(), response.getStatus() );
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
        assertEquals( 1, helper.getIndexedNodes( indexName, key1, value1 ).size() );
        assertEquals( 1, helper.getIndexedNodes( indexName, key1, value2 ).size() );
        assertEquals( 1, helper.getIndexedNodes( indexName, key2, value1 ).size() );
        assertEquals( 1, helper.getIndexedNodes( indexName, key2, value2 ).size() );
        Client.create().resource( functionalTestHelper.nodeIndexUri() + indexName + "/" + key1 + "/" + value1 + "/" + node ).delete( ClientResponse.class );
        assertEquals( 0, helper.getIndexedNodes( indexName, key1, value1 ).size() );
        assertEquals( 1, helper.getIndexedNodes( indexName, key1, value2 ).size() );
        assertEquals( 1, helper.getIndexedNodes( indexName, key2, value1 ).size() );
        assertEquals( 1, helper.getIndexedNodes( indexName, key2, value2 ).size() );
        Client.create().resource( functionalTestHelper.nodeIndexUri() + indexName + "/" + key2 + "/" + node ).delete( ClientResponse.class );
        assertEquals( 0, helper.getIndexedNodes( indexName, key1, value1 ).size() );
        assertEquals( 1, helper.getIndexedNodes( indexName, key1, value2 ).size() );
        assertEquals( 0, helper.getIndexedNodes( indexName, key2, value1 ).size() );
        assertEquals( 0, helper.getIndexedNodes( indexName, key2, value2 ).size() );
        Client.create().resource( functionalTestHelper.nodeIndexUri() + indexName + "/" + node ).delete( ClientResponse.class );
        assertEquals( 0, helper.getIndexedNodes( indexName, key1, value1 ).size() );
        assertEquals( 0, helper.getIndexedNodes( indexName, key1, value2 ).size() );
        assertEquals( 0, helper.getIndexedNodes( indexName, key2, value1 ).size() );
        assertEquals( 0, helper.getIndexedNodes( indexName, key2, value2 ).size() );
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
                .entity( JsonHelper.createJsonFrom( functionalTestHelper.nodeUri( nodeId ) ), MediaType.APPLICATION_JSON )
                .accept( MediaType.APPLICATION_JSON )
                .post( ClientResponse.class );
        assertEquals( Status.CREATED.getStatusCode(), response.getStatus() );
        URI location = response.getLocation();
        response = Client.create().resource( functionalTestHelper.indexNodeUri( indexName, key, value ) ).accept( MediaType.APPLICATION_JSON_TYPE )
                .get( ClientResponse.class );
        assertEquals( Status.OK.getStatusCode(), response.getStatus() );
        Collection<?> hits = (Collection<?>) JsonHelper.jsonToSingleValue( response.getEntity( String.class ) );
        assertEquals( 1, hits.size() );

        Client.create().resource( location ).delete();
        response = Client.create().resource( functionalTestHelper.indexNodeUri( indexName, key, value ) ).accept( MediaType.APPLICATION_JSON_TYPE )
                .get( ClientResponse.class );
        hits = (Collection<?>) JsonHelper.jsonToSingleValue( response.getEntity( String.class ) );
        assertEquals( 0, hits.size() );
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
                .entity( functionalTestHelper.nodeUri( nodeId ) ).post( ClientResponse.class );
        assertEquals( 400, response.getStatus() );
    }
}
