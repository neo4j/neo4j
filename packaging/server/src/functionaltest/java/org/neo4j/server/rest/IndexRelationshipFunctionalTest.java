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

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.server.NeoEmbeddedJettyServer;
import org.neo4j.server.ServerBuilder;
import org.neo4j.server.database.DatabaseBlockedException;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.domain.URIHelper;
import org.neo4j.server.rest.web.PropertyValueException;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;

public class IndexRelationshipFunctionalTest
{
    private NeoEmbeddedJettyServer server;
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
     * GET ${org.neo4j.server.rest.web}/index/relationship/
     */
    @Test
    public void shouldGetEmptyListOfRelationshipIndexesWhenNoneExist()
    {
        JaxRsResponse response = httpGetIndexRelationshipRoot();
        assertEquals( 204, response.getStatus() );
    }

    /**
     * GET ${org.neo4j.server.rest.web}/index/relationship/
     * <p/>
     * TODO: could be abstract
     *
     * @return the Reponse
     */
    public JaxRsResponse httpGetIndexRelationshipRoot()
    {
        return httpGet( functionalTestHelper.relationshipIndexUri(), MediaType.APPLICATION_JSON_TYPE );
    }

    /**
     * POST ${org.neo4j.server.rest.web}/index/relationship
     * {
     * "name":"index-name"
     * "config":{ // optional map of index configuration params
     * "key1":"value1",
     * "key2":"value2"
     * }
     * }
     */
    @Test
    public void shouldCreateANamedRelationshipIndex() throws JsonParseException
    {
        String indexName = "favorites";
        Map<String, String> indexSpecification = new HashMap<String, String>();
        indexSpecification.put( "name", indexName );
        JaxRsResponse response = httpPostIndexRelationshipRoot( JsonHelper.createJsonFrom( indexSpecification ) );
        assertEquals( 201, response.getStatus() );
        assertNotNull( response.getHeaders().get( "Location" ).get( 0 ) );
        assertEquals( helper.getRelationshipIndexes().length, 1 );
        assertNotNull( helper.getRelationshipIndex( indexName ) );
    }

    private JaxRsResponse httpPostIndexRelationshipRoot( String jsonIndexSpecification )
    {
        return new JaxRsResponse( Client.create().resource( functionalTestHelper.relationshipIndexUri() )
                .type( MediaType.APPLICATION_JSON )
                .accept( MediaType.APPLICATION_JSON )
                .entity( jsonIndexSpecification ).post( ClientResponse.class ) );
    }


    /**
     * POST ${org.neo4j.server.rest.web}/index/relationship/{indexName}/{key}/{value}
     * "http://uri.for.node.to.index"
     */
    @Test
    public void shouldRespondWith201CreatedWhenIndexingRelationship() throws DatabaseBlockedException, JsonParseException
    {
        String key = "key";
        String value = "value";
        String indexName = "testy";
        helper.createRelationshipIndex( indexName );
        String relationshipType = "related-to";
        long relationshipId = helper.createRelationship( relationshipType );
        String entity = JsonHelper.createJsonFrom( functionalTestHelper.relationshipUri( relationshipId ) );
        JaxRsResponse response = httpPostIndexRelationshipNameKeyValue( indexName, key, value, entity,
                MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_JSON_TYPE );
        assertEquals( 201, response.getStatus() );
        assertNotNull( response.getHeaders().get( "Location" ).get( 0 ) );
        assertEquals( Arrays.asList( (Long) relationshipId ), helper.getIndexedRelationships( indexName, key, value ) );
    }

    private JaxRsResponse httpGetIndexRelationshipNameKeyValue( String indexName, String key, String value, MediaType acceptType )
    {
        return new JaxRsResponse( Client.create().resource( functionalTestHelper.indexRelationshipUri( indexName, key, value ) )
                .accept( acceptType )
                .get( ClientResponse.class ) );
    }

    private JaxRsResponse httpPostIndexRelationshipNameKeyValue( String indexName, String key, String value, String entity, MediaType postType, MediaType acceptType )
    {
        return new JaxRsResponse(
                Client.create().resource(
                        functionalTestHelper.indexRelationshipUri( indexName, key, value ) )
                .type( postType )
                .accept( acceptType )
                .entity( entity ).post( ClientResponse.class ) );
    }

    @Test
    public void shouldGetRelationshipRepresentationFromIndexUri() throws DatabaseBlockedException, JsonParseException
    {
        String key = "key2";
        String value = "value";

        String indexName = "mindex";
        helper.createRelationshipIndex( indexName );
        String relationshipType = "related-to";
        long relationshipId = helper.createRelationship( relationshipType );
        String createdEntity = JsonHelper.createJsonFrom( functionalTestHelper.relationshipUri( relationshipId ) );
        JaxRsResponse response = httpPostIndexRelationshipNameKeyValue( indexName, key, value, createdEntity, MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_JSON_TYPE );

        assertEquals( Status.CREATED.getStatusCode(), response.getStatus() );
        String indexUri = response.getHeaders().get( "Location" ).get( 0 );

        response = httpGet( indexUri, MediaType.APPLICATION_JSON_TYPE );
        assertEquals( 200, response.getStatus() );

        String discovredEntity = response.getEntity( String.class );

        Map<String, Object> map = JsonHelper.jsonToMap( discovredEntity );
        assertNotNull( map.get( "self" ) );
    }

    private JaxRsResponse httpGet( String indexUri, MediaType acceptType )
    {
        return new JaxRsResponse( Client.create().resource( indexUri ).accept( acceptType ).get( ClientResponse.class ) );
    }

    @Test
    public void shouldGet404WhenRequestingIndexUriWhichDoesntExist() throws DatabaseBlockedException
    {
        String key = "key3";
        String value = "value";
        String indexName = "nosuchindex";
        String indexUri = functionalTestHelper.relationshipIndexUri() + indexName + "/" + key + "/" + value;
        JaxRsResponse response = httpGet( indexUri, MediaType.APPLICATION_JSON_TYPE );
        assertEquals( Status.NOT_FOUND.getStatusCode(), response.getStatus() );
    }

    @Test
    public void shouldGet200AndArrayOfRelationshipRepsWhenGettingFromIndex() throws PropertyValueException
    {
        long startNode = helper.createNode();
        long endNode = helper.createNode();

        String key = "key_get";
        String value = "value";

        String relationshipName1 = "related-to";
        String relationshipName2 = "dislikes";

        String jsonString = jsonRelationshipCreationSpecification( relationshipName1, endNode, key, value );
        ClientResponse createRelationshipResponse = httpPostCreateRelationship( startNode, jsonString );
        assertEquals( 201, createRelationshipResponse.getStatus() );
        String relationshipLocation1 = createRelationshipResponse.getLocation().toString(); // Headers().get(HttpHeaders.LOCATION).get(0);

        jsonString = jsonRelationshipCreationSpecification( relationshipName2, endNode, key, value );
        createRelationshipResponse = httpPostCreateRelationship( startNode, jsonString );
        assertEquals( 201, createRelationshipResponse.getStatus() );
        String relationshipLocation2 = createRelationshipResponse.getHeaders().get( HttpHeaders.LOCATION ).get( 0 );

        String indexName = "matrix";
        JaxRsResponse indexCreationResponse = httpPostIndexRelationshipRoot( "{\"name\":\"" + indexName + "\"}" );
        assertEquals( 201, indexCreationResponse.getStatus() );

        JaxRsResponse indexedRelationshipResponse = httpPostIndexRelationshipNameKeyValue( indexName, key, value,
                JsonHelper.createJsonFrom( relationshipLocation1 ),
                MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_JSON_TYPE );
        String indexLocation1 = indexedRelationshipResponse
                .getHeaders().get( HttpHeaders.LOCATION ).get( 0 );
        indexedRelationshipResponse = httpPostIndexRelationshipNameKeyValue( indexName, key, value,
                JsonHelper.createJsonFrom( relationshipLocation2 ),
                MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_JSON_TYPE );
        String indexLocation2 = indexedRelationshipResponse
                .getHeaders().get( HttpHeaders.LOCATION ).get( 0 );

        Map<String, String> uriToName = new HashMap<String, String>();
        uriToName.put( indexLocation1.toString(), relationshipName1 );
        uriToName.put( indexLocation2.toString(), relationshipName2 );

        ClientResponse response = Client.create().resource( functionalTestHelper.indexRelationshipUri( indexName, key, value ) ).accept( MediaType.APPLICATION_JSON )
                .get( ClientResponse.class );
        assertEquals( 200, response.getStatus() );
        Collection<?> items = (Collection<?>) JsonHelper.jsonToSingleValue( response.getEntity( String.class ) );
        int counter = 0;
        for ( Object item : items )
        {
            Map<?, ?> map = (Map<?, ?>) item;
            assertNotNull( map.get( "self" ) );
            String indexedUri = (String) map.get( "indexed" );
            assertEquals( uriToName.get( indexedUri ), map.get( "type" ) );
            counter++;
        }
        assertEquals( 2, counter );
    }

    private ClientResponse httpPostCreateRelationship( long startNode, String jsonString )
    {
        return Client.create().resource( functionalTestHelper.dataUri() + "node/" + startNode + "/relationships" )
                .type( MediaType.APPLICATION_JSON ).accept( MediaType.APPLICATION_JSON )
                .entity( jsonString )
                .post( ClientResponse.class );
    }

    private String jsonRelationshipCreationSpecification( String relationshipName, long endNode, String key, String value )
    {
        return "{\"to\" : \"" + functionalTestHelper.dataUri() + "node/" + endNode + "\"," +
                "\"type\" : \"" + relationshipName + "\", " +
                "\"data\" : {\"" + key + "\" : \"" + value + "\"}}";
    }

    @Test
    public void shouldGet200WhenGettingRelationshipFromIndexWithNoHits()
    {
        String indexName = "empty-index";
        helper.createRelationshipIndex( indexName );
        ClientResponse response = Client.create().resource( functionalTestHelper.indexRelationshipUri( indexName, "non-existent-key", "non-existent-value" ) ).accept(
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
    public void shouldBeAbleToIndexValuesContainingSpaces() throws Exception
    {
        long startNodeId = helper.createNode();
        long endNodeId = helper.createNode();
        String relationshiptype = "tested-together";
        long relationshipId = helper.createRelationship( relationshiptype, startNodeId, endNodeId );

        String key = "key";
        String value = "value with   spaces  in it";
        value = URIHelper.encode( value );
        String indexName = "spacey-values";
        helper.createRelationshipIndex( indexName );
        String entity = JsonHelper.createJsonFrom( functionalTestHelper.relationshipUri( relationshipId ));
        JaxRsResponse response = httpPostIndexRelationshipNameKeyValue( indexName, key, value, entity,
                MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_JSON_TYPE );
        assertEquals( Status.CREATED.getStatusCode(), response.getStatus() );
        URI location = response.getLocation();
        response = httpGetIndexRelationshipNameKeyValue( indexName, key, value, MediaType.APPLICATION_JSON_TYPE );
        assertEquals( Status.OK.getStatusCode(), response.getStatus() );
        String responseEntity = response.getEntity( String.class );
        Collection<?> hits = (Collection<?>) JsonHelper.jsonToSingleValue( responseEntity );
        assertEquals( 1, hits.size() );

        Client.create().resource( location ).delete();
        response = httpGetIndexRelationshipNameKeyValue( indexName, key, value, MediaType.APPLICATION_JSON_TYPE );
        assertEquals( 200, response.getStatus() );
        responseEntity = response.getEntity( String.class );
        hits = (Collection<?>) JsonHelper.jsonToSingleValue( responseEntity );
        assertEquals( 0, hits.size() );
    }

    @Test
    public void shouldRespondWith400WhenSendingCorruptJson() throws Exception
    {
        long startNodeId = helper.createNode();
        long endNodeId = helper.createNode();
        String relationshipType = "corrupt-me";
        long relationshipId = helper.createRelationship( relationshipType, startNodeId, endNodeId );
        String key = "key";
        String value = "value";
        String indexName = "botherable-index";
        helper.createRelationshipIndex( indexName );
        JaxRsResponse response = httpPostIndexRelationshipNameKeyValue( indexName, key, value,
                functionalTestHelper.relationshipUri( relationshipId ), MediaType.APPLICATION_JSON_TYPE, MediaType.APPLICATION_JSON_TYPE );
        assertEquals( 400, response.getStatus() );
    }
}
