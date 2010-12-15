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
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.server.NeoServer;
import org.neo4j.server.ServerBuilder;
import org.neo4j.server.database.DatabaseBlockedException;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.URIHelper;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class IndexNodeFunctionalityTest
{
    private NeoServer server;
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
        ClientResponse response = Client.create().resource( functionalTestHelper.indexNodeUri() )
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
    public void shouldCreateANamedNodeIndex()
    {
        String indexName = "favorites";
        Map<String, String> indexSpecification = new HashMap<String, String>();
        indexSpecification.put( "name", indexName );
        ClientResponse response = Client.create().resource( functionalTestHelper.indexNodeUri() ).type( MediaType.APPLICATION_JSON )
                .entity( JsonHelper.createJsonFrom( indexSpecification ) ).post( ClientResponse.class );
        assertEquals( 201, response.getStatus() );
        assertNotNull( response.getHeaders().getFirst( "Location" ) );
        assertEquals( helper.getNodeIndexes().length, 1 );
        assertNotNull( helper.getNodeIndex( indexName ) );
    }

    /**
     * POST ${org.neo4j.server.rest.web}/index/node/{indexName}/{key}/{value}
     * "http://uri.for.node.to.index"
     */
    @Test
    public void shouldRespondWith201CreatedWhenIndexingNode() throws DatabaseBlockedException
    {
        long nodeId = helper.createNode();
        String key = "key";
        String value = "value";
        String indexName = "testy";
        helper.createNodeIndex( indexName );
        String entity = functionalTestHelper.nodeUri( nodeId );
        System.out.println( "posting: " + entity );
        ClientResponse response = Client.create().resource( functionalTestHelper.indexUri() + "node/" + indexName + "/" + key + "/" + value ).type( MediaType.TEXT_PLAIN )
                .entity( entity ).post( ClientResponse.class );
        assertEquals( 201, response.getStatus() );
        assertNotNull( response.getHeaders().getFirst( "Location" ) );
        assertEquals( Arrays.asList( (Long)nodeId ), helper.getIndexedNodes( indexName, key, value ) );
    }

    @Test
    public void shouldGetNodeRepresentationFromIndexUri() throws DatabaseBlockedException
    {
        long nodeId = helper.createNode();
        String key = "key2";
        String value = "value";

        String indexName = "mindex";
        helper.createNodeIndex( indexName );
        ClientResponse response = Client.create().resource( functionalTestHelper.indexNodeUri() + indexName + "/" + key + "/" + value )
                .type( MediaType.APPLICATION_JSON )
                .accept( MediaType.APPLICATION_JSON ).entity( JsonHelper.createJsonFrom( functionalTestHelper.nodeUri( nodeId ) ) ).post( ClientResponse.class );

        assertEquals( Status.CREATED.getStatusCode(), response.getStatus() );
        String indexUri = response.getHeaders().getFirst( "Location" );

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
        String indexUri = functionalTestHelper.indexNodeUri() + indexName + "/" + key + "/" + value;
        ClientResponse response = Client.create().resource( indexUri ).accept( MediaType.APPLICATION_JSON ).get( ClientResponse.class );
        assertEquals( Status.NOT_FOUND.getStatusCode(), response.getStatus() );
    }

    @Test
    public void shouldGet200AndArrayOfNodeRepsWhenGettingFromIndex()
    {
        String key = "key_get";
        String value = "value";

        String name1 = "Thomas Anderson";
        String name2 = "Agent Smith";

        String indexName = "matrix";
        String location1 = Client.create().resource( functionalTestHelper.nodeUri() ).accept( MediaType.APPLICATION_JSON ).entity( "{\"name\":\"" + name1 + "\"}",
                MediaType.APPLICATION_JSON ).post( ClientResponse.class ).getHeaders().getFirst( HttpHeaders.LOCATION );
        String location2 = Client.create().resource( functionalTestHelper.nodeUri() ).accept( MediaType.APPLICATION_JSON ).entity( "{\"name\":\"" + name2 + "\"}",
                MediaType.APPLICATION_JSON ).post( ClientResponse.class ).getHeaders().getFirst( HttpHeaders.LOCATION );
        String indexLocation1 = Client.create().resource( functionalTestHelper.indexNodeUri() + indexName + "/" + key + "/" + value ).entity(
                JsonHelper.createJsonFrom( location1 ), MediaType.APPLICATION_JSON ).post( ClientResponse.class ).getHeaders().getFirst( HttpHeaders.LOCATION );
        String indexLocation2 = Client.create().resource( functionalTestHelper.indexNodeUri() + indexName + "/" + key + "/" + value ).entity(
                JsonHelper.createJsonFrom( location2 ), MediaType.APPLICATION_JSON ).post( ClientResponse.class ).getHeaders().getFirst( HttpHeaders.LOCATION );
        Map<String, String> uriToName = new HashMap<String, String>();
        uriToName.put( indexLocation1.toString(), name1 );
        uriToName.put( indexLocation2.toString(), name2 );

        ClientResponse response = Client.create().resource( functionalTestHelper.indexNodeUri() + indexName + "/" + key + "/" + value ).accept( MediaType.APPLICATION_JSON )
                .get( ClientResponse.class );
        assertEquals( 200, response.getStatus() );
        Collection<?> items = (Collection<?>)JsonHelper.jsonToSingleValue( response.getEntity( String.class ) );
        int counter = 0;
        for ( Object item : items )
        {
            Map<?, ?> map = (Map<?, ?>)item;
            Map<?, ?> properties = (Map<?, ?>)map.get( "data" );
            assertNotNull( map.get( "self" ) );
            String indexedUri = (String)map.get( "indexed" );
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
        ClientResponse response = Client.create().resource( functionalTestHelper.indexNodeUri() + indexName + "/non-existent-key/non-existent-value" ).accept(
                MediaType.APPLICATION_JSON ).get( ClientResponse.class );
        assertEquals( 200, response.getStatus() );
    }

    @Test
    @Ignore( "Unclear contract: remove the index itself? That is unsupported in the new index api" )
    public void shouldGet200AndBeAbleToRemoveIndexing() throws DatabaseBlockedException
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
    public void shouldBeAbleToIndexValuesContainingSpaces() throws Exception
    {
        long nodeId = helper.createNode();
        String key = "key";
        String value = "value with   spaces  in it";
        value = URIHelper.encode( value );
        String indexName = "spacey-values";
        helper.createNodeIndex( indexName );
        ClientResponse response = Client.create().resource( functionalTestHelper.indexNodeUri( indexName, key, value ) ).entity(
                JsonHelper.createJsonFrom( functionalTestHelper.nodeUri( nodeId ) ), MediaType.APPLICATION_JSON ).post( ClientResponse.class );
        assertEquals( Status.CREATED.getStatusCode(), response.getStatus() );
        URI location = response.getLocation();
        response = Client.create().resource( functionalTestHelper.indexNodeUri( indexName, key, value ) ).accept( MediaType.APPLICATION_JSON_TYPE )
                .get( ClientResponse.class );
        assertEquals( Status.OK.getStatusCode(), response.getStatus() );
        Collection<?> hits = (Collection<?>)JsonHelper.jsonToSingleValue( response.getEntity( String.class ) );
        assertEquals( 1, hits.size() );

        Client.create().resource( location ).delete();
        response = Client.create().resource( functionalTestHelper.indexNodeUri( indexName, key, value ) ).accept( MediaType.APPLICATION_JSON_TYPE )
                .get( ClientResponse.class );
        hits = (Collection<?>)JsonHelper.jsonToSingleValue( response.getEntity( String.class ) );
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
        ClientResponse response = Client.create().resource( functionalTestHelper.indexNodeUri() + indexName + "/" + key + "/" + value ).type( MediaType.APPLICATION_JSON )
                .entity( functionalTestHelper.nodeUri( nodeId ) ).post( ClientResponse.class );
        assertEquals( 400, response.getStatus() );
    }
}
