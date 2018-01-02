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

import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response.Status;

import org.neo4j.function.Factory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.RESTDocsGenerator.ResponseEntity;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.domain.URIHelper;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.neo4j.server.helpers.FunctionalTestHelper.CLIENT;

public class IndexRelationshipDocIT extends AbstractRestFunctionalTestBase
{
    private static FunctionalTestHelper functionalTestHelper;
    private static GraphDbHelper helper;
    private static RestRequest request;

    private enum MyRelationshipTypes implements RelationshipType
    {
        KNOWS
    }

    @BeforeClass
    public static void setupServer()
    {
        functionalTestHelper = new FunctionalTestHelper( server() );
        helper = functionalTestHelper.getGraphDbHelper();
        request = RestRequest.req();
    }

    /**
     * POST ${org.neo4j.server.rest.web}/index/relationship {
     * "name":"index-name" "config":{ // optional map of index configuration
     * params "key1":"value1", "key2":"value2" } }
     *
     * POST ${org.neo4j.server.rest.web}/index/relationship/{indexName}/{key}/{
     * value} "http://uri.for.node.to.index"
     */
    @Test
    public void shouldCreateANamedRelationshipIndexAndAddToIt() throws JsonParseException
    {
        String indexName = indexes.newInstance();
        int expectedIndexes = helper.getRelationshipIndexes().length + 1;
        Map<String, String> indexSpecification = new HashMap<>();
        indexSpecification.put( "name", indexName );
        JaxRsResponse response = httpPostIndexRelationshipRoot( JsonHelper.createJsonFrom( indexSpecification ) );
        assertEquals( 201, response.getStatus() );
        assertNotNull( response.getHeaders().get( "Location" ).get( 0 ) );
        assertEquals( expectedIndexes, helper.getRelationshipIndexes().length );
        assertNotNull( helper.createRelationshipIndex( indexName ) );
        // Add a relationship to the index
        String key = "key";
        String value = "value";
        String relationshipType = "related-to";
        long relationshipId = helper.createRelationship( relationshipType );
        response = httpPostIndexRelationshipNameKeyValue( indexName, relationshipId, key, value );
        assertEquals( Status.CREATED.getStatusCode(), response.getStatus() );
        String indexUri = response.getHeaders().get( "Location" ).get( 0 );
        assertNotNull( indexUri );
        assertEquals( Arrays.asList( (Long) relationshipId ), helper.getIndexedRelationships( indexName, key, value ) );
        // Get the relationship from the indexed URI (Location in header)
        response = httpGet( indexUri );
        assertEquals( 200, response.getStatus() );
        String discovredEntity = response.getEntity();
        Map<String, Object> map = JsonHelper.jsonToMap( discovredEntity );
        assertNotNull( map.get( "self" ) );
    }

    @Test
    public void shouldGet404WhenRequestingIndexUriWhichDoesntExist()
    {
        String key = "key3";
        String value = "value";
        String indexName = indexes.newInstance();
        String indexUri = functionalTestHelper.relationshipIndexUri() + indexName + "/" + key + "/" + value;
        JaxRsResponse response = httpGet( indexUri );
        assertEquals( Status.NOT_FOUND.getStatusCode(), response.getStatus() );
    }

    @Test
    public void shouldGet404WhenDeletingNonExtistentIndex()
    {
        String indexName = indexes.newInstance();
        String indexUri = functionalTestHelper.relationshipIndexUri() + indexName;
        JaxRsResponse response = request.delete( indexUri );
        assertEquals( Status.NOT_FOUND.getStatusCode(), response.getStatus() );
    }

    @Test
    public void shouldGet200AndArrayOfRelationshipRepsWhenGettingFromIndex() throws JsonParseException
    {
        final long startNode = helper.createNode();
        final long endNode = helper.createNode();
        final String key = "key_get";
        final String value = "value";
        final String relationshipName1 = "related-to";
        final String relationshipName2 = "dislikes";
        String jsonString = jsonRelationshipCreationSpecification( relationshipName1, endNode, key, value );
        JaxRsResponse createRelationshipResponse = httpPostCreateRelationship( startNode, jsonString );
        assertEquals( 201, createRelationshipResponse.getStatus() );
        String relationshipLocation1 = createRelationshipResponse.getLocation().toString();
        jsonString = jsonRelationshipCreationSpecification( relationshipName2, endNode, key, value );
        createRelationshipResponse = httpPostCreateRelationship( startNode, jsonString );
        assertEquals( 201, createRelationshipResponse.getStatus() );
        String relationshipLocation2 = createRelationshipResponse.getHeaders().get( HttpHeaders.LOCATION ).get( 0 );
        String indexName = indexes.newInstance();
        JaxRsResponse indexCreationResponse = httpPostIndexRelationshipRoot( "{\"name\":\"" + indexName + "\"}" );
        assertEquals( 201, indexCreationResponse.getStatus() );
        JaxRsResponse indexedRelationshipResponse = httpPostIndexRelationshipNameKeyValue( indexName,
                functionalTestHelper.getRelationshipIdFromUri( relationshipLocation1 ), key, value );
        String indexLocation1 = indexedRelationshipResponse.getHeaders().get( HttpHeaders.LOCATION ).get( 0 );
        indexedRelationshipResponse = httpPostIndexRelationshipNameKeyValue( indexName,
                functionalTestHelper.getRelationshipIdFromUri( relationshipLocation2 ), key, value );
        String indexLocation2 = indexedRelationshipResponse.getHeaders().get( HttpHeaders.LOCATION ).get( 0 );
        Map<String, String> uriToName = new HashMap<>();
        uriToName.put( indexLocation1.toString(), relationshipName1 );
        uriToName.put( indexLocation2.toString(), relationshipName2 );
        JaxRsResponse response = RestRequest.req().get(
                functionalTestHelper.indexRelationshipUri( indexName, key, value ) );
        assertEquals( 200, response.getStatus() );
        Collection<?> items = (Collection<?>) JsonHelper.readJson( response.getEntity() );
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
        response.close();
    }

    @Test
    public void shouldGet200WhenGettingRelationshipFromIndexWithNoHits()
    {
        String indexName = indexes.newInstance();
        helper.createRelationshipIndex( indexName );
        JaxRsResponse response = RestRequest.req().get(
                functionalTestHelper.indexRelationshipUri( indexName, "non-existent-key", "non-existent-value" ) );
        assertEquals( 200, response.getStatus() );
    }

    @Test
    public void shouldGet200WhenQueryingIndex()
    {
        String indexName = indexes.newInstance();
        String key = "bobsKey";
        String value = "bobsValue";
        long relationship = helper.createRelationship( "TYPE" );
        helper.addRelationshipToIndex( indexName, key, value, relationship );
        JaxRsResponse response = RestRequest.req().get(
                functionalTestHelper.indexRelationshipUri( indexName ) + "?query=" + key + ":" + value );
        assertEquals( 200, response.getStatus() );
    }

    @Test
    public void shouldBeAbleToRemoveIndexing()
    {
        String key1 = "kvkey1";
        String key2 = "kvkey2";
        String value1 = "value1";
        String value2 = "value2";
        String indexName = indexes.newInstance();
        long relationship = helper.createRelationship( "some type" );
        helper.setRelationshipProperties( relationship,
                MapUtil.map( key1, value1, key1, value2, key2, value1, key2, value2 ) );
        helper.addRelationshipToIndex( indexName, key1, value1, relationship );
        helper.addRelationshipToIndex( indexName, key1, value2, relationship );
        helper.addRelationshipToIndex( indexName, key2, value1, relationship );
        helper.addRelationshipToIndex( indexName, key2, value2, relationship );
        assertEquals( 1, helper.getIndexedRelationships( indexName, key1, value1 ).size() );
        assertEquals( 1, helper.getIndexedRelationships( indexName, key1, value2 ).size() );
        assertEquals( 1, helper.getIndexedRelationships( indexName, key2, value1 ).size() );
        assertEquals( 1, helper.getIndexedRelationships( indexName, key2, value2 ).size() );
        JaxRsResponse response = RestRequest.req().delete(
                functionalTestHelper.relationshipIndexUri() + indexName + "/" + key1 + "/" + value1 + "/"
                        + relationship );
        assertEquals( 204, response.getStatus() );
        assertEquals( 0, helper.getIndexedRelationships( indexName, key1, value1 ).size() );
        assertEquals( 1, helper.getIndexedRelationships( indexName, key1, value2 ).size() );
        assertEquals( 1, helper.getIndexedRelationships( indexName, key2, value1 ).size() );
        assertEquals( 1, helper.getIndexedRelationships( indexName, key2, value2 ).size() );
        response = RestRequest.req().delete(
                functionalTestHelper.relationshipIndexUri() + indexName + "/" + key2 + "/" + relationship );
        assertEquals( 204, response.getStatus() );
        assertEquals( 0, helper.getIndexedRelationships( indexName, key1, value1 ).size() );
        assertEquals( 1, helper.getIndexedRelationships( indexName, key1, value2 ).size() );
        assertEquals( 0, helper.getIndexedRelationships( indexName, key2, value1 ).size() );
        assertEquals( 0, helper.getIndexedRelationships( indexName, key2, value2 ).size() );
        response = RestRequest.req().delete(
                functionalTestHelper.relationshipIndexUri() + indexName + "/" + relationship );
        assertEquals( 204, response.getStatus() );
        assertEquals( 0, helper.getIndexedRelationships( indexName, key1, value1 ).size() );
        assertEquals( 0, helper.getIndexedRelationships( indexName, key1, value2 ).size() );
        assertEquals( 0, helper.getIndexedRelationships( indexName, key2, value1 ).size() );
        assertEquals( 0, helper.getIndexedRelationships( indexName, key2, value2 ).size() );
        // Delete the index
        response = RestRequest.req().delete( functionalTestHelper.indexRelationshipUri( indexName ) );
        assertEquals( 204, response.getStatus() );
        assertFalse( asList( helper.getRelationshipIndexes() ).contains( indexName ) );
    }

    @Test
    public void shouldBeAbleToIndexValuesContainingSpaces() throws Exception
    {
        final long startNodeId = helper.createNode();
        final long endNodeId = helper.createNode();
        final String relationshiptype = "tested-together";
        final long relationshipId = helper.createRelationship( relationshiptype, startNodeId, endNodeId );
        final String key = "key";
        final String value = "value with   spaces  in it";
        final String indexName = indexes.newInstance();
        helper.createRelationshipIndex( indexName );
        JaxRsResponse response = httpPostIndexRelationshipNameKeyValue( indexName, relationshipId, key, value );
        assertEquals( Status.CREATED.getStatusCode(), response.getStatus() );
        URI location = response.getLocation();
        response.close();
        response = httpGetIndexRelationshipNameKeyValue( indexName, key, URIHelper.encode( value ) );
        assertEquals( Status.OK.getStatusCode(), response.getStatus() );
        String responseEntity = response.getEntity();
        Collection<?> hits = (Collection<?>) JsonHelper.readJson( responseEntity );
        assertEquals( 1, hits.size() );
        response.close();
        CLIENT.resource( location ).delete();
        response = httpGetIndexRelationshipNameKeyValue( indexName, key, URIHelper.encode( value ) );
        assertEquals( 200, response.getStatus() );
        responseEntity = response.getEntity();
        hits = (Collection<?>) JsonHelper.readJson( responseEntity );
        assertEquals( 0, hits.size() );
        response.close();
    }

    @Test
    public void shouldRespondWith400WhenSendingCorruptJson() throws Exception
    {
        final String indexName = indexes.newInstance();
        helper.createRelationshipIndex( indexName );
        final String corruptJson = "{[}";
        JaxRsResponse response = RestRequest.req().post( functionalTestHelper.indexRelationshipUri( indexName ),
                corruptJson );
        assertEquals( 400, response.getStatus() );
    }

    @Documented( "Get or create unique relationship (create).\n" +
                 "\n" +
                 "Create a unique relationship in an index.\n" +
                 "If a relationship matching the given key and value already exists in the index, it will be returned.\n" +
                 "If not, a new relationship will be created.\n" +
                 "\n" +
                 "NOTE: The type and direction of the relationship is not regarded when determining uniqueness." )
    @Test
    public void get_or_create_relationship() throws Exception
    {
        final String index = indexes.newInstance(), type="knowledge", key = "name", value = "Tobias";
        helper.createRelationshipIndex( index );
        long start = helper.createNode();
        long end = helper.createNode();
        gen.get()
                .noGraph()
                .expectedStatus( 201 /* created */)
                .payloadType( MediaType.APPLICATION_JSON_TYPE )
                .payload( "{\"key\": \"" + key + "\", \"value\":\"" + value +
                          "\", \"start\": \"" + functionalTestHelper.nodeUri( start ) +
                          "\", \"end\": \"" + functionalTestHelper.nodeUri( end ) +
                          "\", \"type\": \"" + type + "\"}" )
                .post( functionalTestHelper.relationshipIndexUri() + index + "/?uniqueness=get_or_create" );
    }

    @Documented( "Get or create unique relationship (existing).\n" +
                 "\n" +
                 "Here, in case\n" +
                 "of an already existing relationship, the sent data is ignored and the\n" +
                 "existing relationship returned." )
    @Test
    public void get_or_create_unique_relationship_existing() throws Exception
    {
        final String index = indexes.newInstance(), key = "name", value = "Peter";
        GraphDatabaseService graphdb = graphdb();
        helper.createRelationshipIndex( index );
        try ( Transaction tx = graphdb.beginTx() )
        {
            Node node1 = graphdb.createNode();
            Node node2 = graphdb.createNode();
            Relationship rel = node1.createRelationshipTo( node2, MyRelationshipTypes.KNOWS );
            graphdb.index().forRelationships( index ).add( rel, key, value );
            tx.success();
        }
        gen.get()
                .noGraph()
                .expectedStatus( 200 /* existing */)
                .payloadType( MediaType.APPLICATION_JSON_TYPE )
                .payload(
                        "{\"key\": \"" + key + "\", \"value\": \"" + value + "\", \"start\": \""
                                + functionalTestHelper.nodeUri( helper.createNode() ) + "\", \"end\": \""
                                + functionalTestHelper.nodeUri( helper.createNode() ) + "\", \"type\":\""
                                + MyRelationshipTypes.KNOWS + "\"}" )
                .post( functionalTestHelper.relationshipIndexUri() + index + "?uniqueness=get_or_create" );
    }

    @Documented( "Create a unique relationship or return fail (create).\n" +
                 "\n" +
                 "Here, in case\n" +
                 "of an already existing relationship, an error should be returned. In this\n" +
                 "example, no existing relationship is found and a new relationship is created." )
    @Test
    public void create_a_unique_relationship_or_return_fail___create() throws Exception
    {
        final String index = indexes.newInstance(), key = "name", value = "Tobias";
        helper.createRelationshipIndex( index );
        ResponseEntity response = gen
                .get()
                .noGraph()
                .expectedStatus( 201 /* created */)
                .payloadType( MediaType.APPLICATION_JSON_TYPE )
                .payload(
                        "{\"key\": \"" + key + "\", \"value\": \"" + value + "\", \"start\": \""
                                + functionalTestHelper.nodeUri( helper.createNode() ) + "\", \"end\": \""
                                + functionalTestHelper.nodeUri( helper.createNode() ) + "\", \"type\":\""
                                + MyRelationshipTypes.KNOWS + "\"}" )
                .post( functionalTestHelper.relationshipIndexUri() + index + "?uniqueness=create_or_fail" );
        MultivaluedMap<String, String> headers = response.response().getHeaders();
        Map<String, Object> result = JsonHelper.jsonToMap( response.entity() );
        assertEquals( result.get( "indexed" ), headers.getFirst( "Location" ) );
    }

    @Documented( "Create a unique relationship or return fail (fail).\n" +
                 "\n" +
                 "Here, in case\n" +
                 "of an already existing relationship, an error should be returned. In this\n" +
                 "example, an existing relationship is found and an error is returned." )
    @Test
    public void create_a_unique_relationship_or_return_fail___fail() throws Exception
    {
        final String index = indexes.newInstance(), key = "name", value = "Peter";
        GraphDatabaseService graphdb = graphdb();
        helper.createRelationshipIndex( index );
        try ( Transaction tx = graphdb.beginTx() )
        {
            Node node1 = graphdb.createNode();
            Node node2 = graphdb.createNode();
            Relationship rel = node1.createRelationshipTo( node2, MyRelationshipTypes.KNOWS );
            graphdb.index().forRelationships( index ).add( rel, key, value );
            tx.success();
        }
        gen.get()
                .noGraph()
                .expectedStatus( 409 /* conflict */)
                .payloadType( MediaType.APPLICATION_JSON_TYPE )
                .payload(
                        "{\"key\": \"" + key + "\", \"value\": \"" + value + "\", \"start\": \""
                                + functionalTestHelper.nodeUri( helper.createNode() ) + "\", \"end\": \""
                                + functionalTestHelper.nodeUri( helper.createNode() ) + "\", \"type\":\""
                                + MyRelationshipTypes.KNOWS + "\"}" )
                .post( functionalTestHelper.relationshipIndexUri() + index + "?uniqueness=create_or_fail" );
    }

    @Documented( "Add an existing relationship to a unique index (not indexed).\n" +
                 "\n" +
                 "If a relationship matching the given key and value already exists in the index, it will be returned.\n" +
                 "If not, an `HTTP 409` (conflict) status will be returned in this case, as we are using `create_or_fail`.\n" +
                 "\n" +
                 "It's possible to use `get_or_create` uniqueness as well.\n" +
                 "\n" +
                 "NOTE: The type and direction of the relationship is not regarded when determining uniqueness." )
    @Test
    public void put_relationship_or_fail_if_absent() throws Exception
    {
        final String index = indexes.newInstance(), key = "name", value = "Peter";
        helper.createRelationshipIndex( index );
        gen.get()
                .noGraph()
                .expectedStatus( 201 /* created */)
                .payloadType( MediaType.APPLICATION_JSON_TYPE )
                .payload(
                        "{\"key\": \""
                                + key
                                + "\", \"value\": \""
                                + value
                                + "\", \"uri\":\""
                                + functionalTestHelper.relationshipUri( helper.createRelationship( "KNOWS",
                                helper.createNode(), helper.createNode() ) ) + "\"}" )
                .post( functionalTestHelper.relationshipIndexUri() + index + "?uniqueness=create_or_fail" );
    }

    @Documented( "Add an existing relationship to a unique index (already indexed)." )
    @Test
    public void put_relationship_if_absent_only_fail() throws Exception
    {
        // Given
        final String index = indexes.newInstance(), key = "name", value = "Peter";
        GraphDatabaseService graphdb = graphdb();
        helper.createRelationshipIndex( index );
        try ( Transaction tx = graphdb.beginTx() )
        {
            Node node1 = graphdb.createNode();
            Node node2 = graphdb.createNode();
            Relationship rel = node1.createRelationshipTo( node2, MyRelationshipTypes.KNOWS );
            graphdb.index().forRelationships( index ).add( rel, key, value );
            tx.success();
        }

        Relationship rel;
        try ( Transaction tx = graphdb.beginTx() )
        {
            Node node1 = graphdb.createNode();
            Node node2 = graphdb.createNode();
            rel = node1.createRelationshipTo( node2, MyRelationshipTypes.KNOWS );
            tx.success();
        }

        // When & Then
        gen.get()
                .noGraph()
                .expectedStatus( 409 /* conflict */)
                .payloadType( MediaType.APPLICATION_JSON_TYPE )
                .payload(
                        "{\"key\": \"" + key + "\", \"value\": \"" + value + "\", \"uri\":\""
                                + functionalTestHelper.relationshipUri( rel.getId() ) + "\"}" )
                .post( functionalTestHelper.relationshipIndexUri() + index + "?uniqueness=create_or_fail" );
    }

    @Test
    public void already_indexed_relationship_should_not_fail_on_create_or_fail() throws Exception
    {
        // Given
        final String index = indexes.newInstance(), key = "name", value = "Peter";
        GraphDatabaseService graphdb = graphdb();
        helper.createRelationshipIndex( index );
        Relationship rel;
        try ( Transaction tx = graphdb.beginTx() )
        {
            Node node1 = graphdb.createNode();
            Node node2 = graphdb.createNode();
            rel = node1.createRelationshipTo( node2, MyRelationshipTypes.KNOWS );
            graphdb.index().forRelationships( index ).add( rel, key, value );
            tx.success();
        }

        // When & Then
        gen.get()
                .noGraph()
                .expectedStatus( 201 )
                .payloadType( MediaType.APPLICATION_JSON_TYPE )
                .payload(
                        "{\"key\": \"" + key + "\", \"value\": \"" + value + "\", \"uri\":\""
                                + functionalTestHelper.relationshipUri( rel.getId() ) + "\"}" )
                .post( functionalTestHelper.relationshipIndexUri() + index + "?uniqueness=create_or_fail" );
    }

    /**
     * This can be safely removed in version 1.11 an onwards.
     */
    @Test
    public void createUniqueShouldBeBackwardsCompatibleWith1_8() throws Exception
    {
        final String index = indexes.newInstance(), key = "name", value = "Peter";
        GraphDatabaseService graphdb = graphdb();
        helper.createRelationshipIndex( index );
        try ( Transaction tx = graphdb.beginTx() )
        {
            Node node1 = graphdb.createNode();
            Node node2 = graphdb.createNode();
            Relationship rel = node1.createRelationshipTo( node2, MyRelationshipTypes.KNOWS );
            graphdb.index().forRelationships( index ).add( rel, key, value );
            tx.success();
        }
        gen.get()
                .noGraph()
                .expectedStatus( 200 /* conflict */)
                .payloadType( MediaType.APPLICATION_JSON_TYPE )
                .payload(
                        "{\"key\": \"" + key + "\", \"value\": \"" + value + "\", \"start\": \""
                                + functionalTestHelper.nodeUri( helper.createNode() ) + "\", \"end\": \""
                                + functionalTestHelper.nodeUri( helper.createNode() ) + "\", \"type\":\""
                                + MyRelationshipTypes.KNOWS + "\"}" )
                .post( functionalTestHelper.relationshipIndexUri() + index + "?unique" );
    }

    private JaxRsResponse httpPostIndexRelationshipRoot( String jsonIndexSpecification )
    {
        return RestRequest.req().post( functionalTestHelper.relationshipIndexUri(), jsonIndexSpecification );
    }

    private JaxRsResponse httpGetIndexRelationshipNameKeyValue( String indexName, String key, String value )
    {
        return RestRequest.req().get( functionalTestHelper.indexRelationshipUri( indexName, key, value ) );
    }

    private JaxRsResponse httpPostIndexRelationshipNameKeyValue( String indexName, long relationshipId, String key,
            String value )
    {
        return RestRequest.req().post( functionalTestHelper.indexRelationshipUri( indexName ),
                createJsonStringFor( relationshipId, key, value ) );
    }

    private String createJsonStringFor( final long relationshipId, final String key, final String value )
    {
        return "{\"key\": \"" + key + "\", \"value\": \"" + value + "\", \"uri\": \""
               + functionalTestHelper.relationshipUri( relationshipId ) + "\"}";
    }

    private JaxRsResponse httpGet( String indexUri )
    {
        return request.get( indexUri );
    }

    private JaxRsResponse httpPostCreateRelationship( long startNode, String jsonString )
    {
        return RestRequest.req().post( functionalTestHelper.dataUri() + "node/" + startNode + "/relationships",
                jsonString );
    }

    private String jsonRelationshipCreationSpecification( String relationshipName, long endNode, String key,
            String value )
    {
        return "{\"to\" : \"" + functionalTestHelper.dataUri() + "node/" + endNode + "\"," + "\"type\" : \""
               + relationshipName + "\", " + "\"data\" : {\"" + key + "\" : \"" + value + "\"}}";
    }

    private final Factory<String> indexes = UniqueStrings.withPrefix( "index" );
}
