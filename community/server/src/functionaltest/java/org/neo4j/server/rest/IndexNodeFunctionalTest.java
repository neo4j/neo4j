/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.server.helpers.FunctionalTestHelper.CLIENT;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response.Status;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.RESTDocsGenerator.ResponseEntity;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.domain.URIHelper;
import org.neo4j.server.rest.web.PropertyValueException;

public class IndexNodeFunctionalTest extends AbstractRestFunctionalTestBase
{
    private static FunctionalTestHelper functionalTestHelper;
    private static GraphDbHelper helper;

    @BeforeClass
    public static void setupServer() throws IOException
    {
        functionalTestHelper = new FunctionalTestHelper( server() );
        helper = functionalTestHelper.getGraphDbHelper();
    }

    @Before
    public void cleanTheDatabase()
    {
        cleanDatabase();
        gen().setGraph( server().getDatabase().getGraph() );
    }

    long createNode()
    {
        GraphDatabaseAPI graphdb = server().getDatabase().getGraph();
        Transaction tx = graphdb.beginTx();
        Node node;
        try {
            node = graphdb.createNode();

            tx.success();
        } finally {
            tx.finish();
        }
        return node.getId();
    }

    /**
     * List node indexes.
     */
    @Documented
    @Test
    public void shouldGetListOfNodeIndexesWhenOneExist() throws PropertyValueException
    {
        String indexName = "favorites";
        helper.createNodeIndex( indexName );
        String entity = gen()
                .expectedStatus( 200 )
                .get( functionalTestHelper.nodeIndexUri() )
                .entity();
        Map<String, Object> map = JsonHelper.jsonToMap( entity );
        assertNotNull( map.get( indexName ) );

        assertEquals( "Was: " + map + ", no-auto-index:" + functionalTestHelper.removeAnyAutoIndex( map ), 1, functionalTestHelper.removeAnyAutoIndex( map ).size() );
    }

    /**
     * Create node index
     *
     * NOTE: Instead of creating the index this way, you can simply start to use
     * it, and it will be created automatically with default configuration.
     */
    @Documented
    @Test
    public void shouldCreateANamedNodeIndex() throws JsonParseException
    {
        String indexName = "favorites";
        int expectedIndexes = helper.getNodeIndexes().length + 1;
        Map<String, String> indexSpecification = new HashMap<String, String>();
        indexSpecification.put( "name", indexName );

        gen()
                .payload( JsonHelper.createJsonFrom( indexSpecification ) )
                .expectedStatus( 201 )
                .expectedHeader( "Location" )
                .post( functionalTestHelper.nodeIndexUri() );

        assertEquals( expectedIndexes, helper.getNodeIndexes().length );
        assertThat( helper.getNodeIndexes(), FunctionalTestHelper.arrayContains( indexName ) );
    }

    @Test
    public void shouldCreateANamedNodeIndexWithSpaces() throws JsonParseException
    {
        String indexName = "favorites with spaces";
        int expectedIndexes = helper.getNodeIndexes().length + 1;
        Map<String, String> indexSpecification = new HashMap<String, String>();
        indexSpecification.put( "name", indexName );

        gen()
                .payload( JsonHelper.createJsonFrom( indexSpecification ) )
                .expectedStatus( 201 )
                .expectedHeader( "Location" )
                .post( functionalTestHelper.nodeIndexUri() );

        assertEquals( expectedIndexes, helper.getNodeIndexes().length );
        assertThat( helper.getNodeIndexes(), FunctionalTestHelper.arrayContains( indexName ) );
    }

    /**
     * Create node index with configuration. This request is only necessary if
     * you want to customize the index settings. If you are happy with the
     * defaults, you can just start indexing nodes/relationships, as
     * non-existent indexes will automatically be created as you do. See
     * <<indexing-create-advanced>> for more information on index configuration.
     */
    @Documented
    @Test
    public void shouldCreateANamedNodeIndexWithConfiguration() throws Exception
    {
        int expectedIndexes = helper.getNodeIndexes().length+1;
        gen()
                .payload( "{\"name\":\"fulltext\", \"config\":{\"type\":\"fulltext\",\"provider\":\"lucene\"}}" )
                .expectedStatus( 201 )
                .expectedHeader( "Location" )
                .post( functionalTestHelper.nodeIndexUri() );

        assertEquals( expectedIndexes, helper.getNodeIndexes().length );
        assertThat( helper.getNodeIndexes(), FunctionalTestHelper.arrayContains( "fulltext" ) );
    }

    /**
     * Add node to index.
     *
     * Associates a node with the given key/value pair in the given index.
     *
     * NOTE: Spaces in the URI have to be escaped.
     *
     * CAUTION: This does *not* overwrite previous entries. If you index the
     * same key/value/item combination twice, two index entries are created. To
     * do update-type operations, you need to delete the old entry before adding
     * a new one.
     */
    @Documented
    @Test
    public void shouldAddToIndex() throws Exception
    {
        final String indexName = "favorites";
        final String key = "some-key";
        final String value = "some value";
        long nodeId = createNode();
        // implicitly create the index
        gen()
                .expectedStatus( 201 )
                .payload(
                        JsonHelper.createJsonFrom( generateNodeIndexCreationPayload( key, value,
                                functionalTestHelper.nodeUri( nodeId ) ) ) )
                .post( functionalTestHelper.indexNodeUri( indexName ) );
        // look if we get one entry back
        JaxRsResponse response = RestRequest.req().get(
                functionalTestHelper.indexNodeUri( indexName, key,
                        URIHelper.encode( value ) ) );
        String entity = response.getEntity( String.class );
        Collection<?> hits = (Collection<?>) JsonHelper.jsonToSingleValue( entity );
        assertEquals( 1, hits.size() );
    }

    private Object generateNodeIndexCreationPayload( String key, String value, String nodeUri )
    {
        Map<String, String> results = new HashMap<String, String>();
        results.put( "key", key );
        results.put( "value", value );
        results.put( "uri", nodeUri );
        return results;
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
        long nodeId = createNode();
        value = URIHelper.encode( value );
        // implicitly create the index
        JaxRsResponse response = RestRequest.req()
                .post( functionalTestHelper.indexNodeUri( indexName ), createJsonStringFor( nodeId, key, value ) );
        assertEquals( 201, response.getStatus() );

        // search it exact
        String entity = gen()
                .expectedStatus( 200 )
                .get( functionalTestHelper.indexNodeUri( indexName, key, URIHelper.encode( value ) ) )
                .entity();
        Collection<?> hits = (Collection<?>) JsonHelper.jsonToSingleValue( entity );
        assertEquals( 1, hits.size() );
    }

    /**
     * Find node by query.
     *
     * The query language used here depends on what type of index you are
     * querying. The default index type is Lucene, in which case you should use
     * the Lucene query language here. Below an example of a fuzzy search over
     * multiple keys.
     *
     * See: http://lucene.apache.org/java/{lucene-version}/queryparsersyntax.html
     *
     * Getting the results with a predefined ordering requires adding the
     * parameter
     *
     * order=ordering
     *
     * where ordering is one of index, relevance or score. In this case an
     * additional field will be added to each result, named score, that holds
     * the float value that is the score reported by the query result.
     */
    @Documented
    @Test
    public void shouldAddToIndexAndRetrieveItByQuery() throws PropertyValueException
    {
        String indexName = "bobTheIndex";
        String key = "Name";
        String value = "Builder";
        long node = helper.createNode( MapUtil.map( key, value ) );
        helper.addNodeToIndex( indexName, key, value, node );
        helper.addNodeToIndex( indexName, "Gender", "Male", node );

        String entity = gen()
                .expectedStatus( 200 )
                .get( functionalTestHelper.indexNodeUri( indexName ) + "?query=Name:Build~0.1%20AND%20Gender:Male" )
                .entity();

        Collection<?> hits = (Collection<?>) JsonHelper.jsonToSingleValue( entity );
        assertEquals( 1, hits.size() );
        LinkedHashMap<String, String> nodeMap = (LinkedHashMap) hits.iterator().next();
        assertNull( "score should not be present when not explicitly ordering",
                nodeMap.get( "score" ) );
    }

    @Test
    public void orderedResultsAreSupersetOfUnordered() throws Exception
    {
        String indexName = "bobTheIndex";
        String key = "Name";
        String value = "Builder";
        long node = helper.createNode( MapUtil.map( key, value ) );
        helper.addNodeToIndex( indexName, key, value, node );
        helper.addNodeToIndex( indexName, "Gender", "Male", node );

        String entity = gen().expectedStatus( 200 ).get(
                functionalTestHelper.indexNodeUri( indexName )
                        + "?query=Name:Build~0.1%20AND%20Gender:Male" ).entity();

        Collection<?> hits = (Collection<?>) JsonHelper.jsonToSingleValue( entity );
        LinkedHashMap<String, String> nodeMapUnordered = (LinkedHashMap) hits.iterator().next();

        entity = gen().expectedStatus( 200 ).get(
                functionalTestHelper.indexNodeUri( indexName )
                        + "?query=Name:Build~0.1%20AND%20Gender:Male&order=score" ).entity();

        hits = (Collection<?>) JsonHelper.jsonToSingleValue( entity );
        LinkedHashMap<String, String> nodeMapOrdered = (LinkedHashMap) hits.iterator().next();

        for ( Map.Entry<String, String> unorderedEntry : nodeMapUnordered.entrySet() )
        {
            assertEquals( "wrong entry for key: " + unorderedEntry.getKey(),
                    unorderedEntry.getValue(),
                    nodeMapOrdered.get( unorderedEntry.getKey() ) );
        }
        assertTrue( "There should be only one extra value for the ordered map",
                nodeMapOrdered.size() == nodeMapUnordered.size() + 1 );
    }

    @Test
    public void shouldAddToIndexAndRetrieveItByQuerySorted()
            throws PropertyValueException
    {
        String indexName = "bobTheIndex";
        String key = "Name";
        long node1 = helper.createNode();
        long node2 = helper.createNode();
        helper.addNodeToIndex( indexName, key, "Builder2", node1 );
        helper.addNodeToIndex( indexName, "Gender", "Male", node1 );
        helper.addNodeToIndex( indexName, key, "Builder", node2 );
        helper.addNodeToIndex( indexName, "Gender", "Male", node2 );

        String entity = gen().expectedStatus( 200 ).get(
                functionalTestHelper.indexNodeUri( indexName )
                        + "?query=Name:Build~0.1%20AND%20Gender:Male&order=relevance" ).entity();

        Collection<?> hits = (Collection<?>) JsonHelper.jsonToSingleValue( entity );
        assertEquals( 2, hits.size() );
        Iterator<LinkedHashMap<String, Object>> it = (Iterator<LinkedHashMap<String, Object>>) hits.iterator();

        LinkedHashMap<String, Object> node2Map = it.next();
        LinkedHashMap<String, Object> node1Map = it.next();
        float score2 = ( (Double) node2Map.get( "score" ) ).floatValue();
        float score1 = ( (Double) node1Map.get( "score" ) ).floatValue();
        assertTrue(
                "results returned in wrong order for relevance ordering",
                ( (String) node2Map.get( "self" ) ).endsWith( Long.toString( node2 ) ) );
        assertTrue(
                "results returned in wrong order for relevance ordering",
                ( (String) node1Map.get( "self" ) ).endsWith( Long.toString( node1 ) ) );
        /*
         * scores are always the same, just the ordering changes. So all subsequent tests will
         * check the same condition.
         */
        assertTrue( "scores are reversed", score2 > score1 );

        entity = gen().expectedStatus( 200 ).get(
                functionalTestHelper.indexNodeUri( indexName )
                        + "?query=Name:Build~0.1%20AND%20Gender:Male&order=index" ).entity();

        hits = (Collection<?>) JsonHelper.jsonToSingleValue( entity );
        assertEquals( 2, hits.size() );
        it = (Iterator<LinkedHashMap<String, Object>>) hits.iterator();

        /*
         * index order, so as they were added
         */
        node1Map = it.next();
        node2Map = it.next();
        score1 = ( (Double) node1Map.get( "score" ) ).floatValue();
        score2 = ( (Double) node2Map.get( "score" ) ).floatValue();
        assertTrue(
                "results returned in wrong order for index ordering",
                ( (String) node1Map.get( "self" ) ).endsWith( Long.toString( node1 ) ) );
        assertTrue(
                "results returned in wrong order for index ordering",
                ( (String) node2Map.get( "self" ) ).endsWith( Long.toString( node2 ) ) );
        assertTrue( "scores are reversed", score2 > score1 );

        entity = gen().expectedStatus( 200 ).get(
                functionalTestHelper.indexNodeUri( indexName )
                        + "?query=Name:Build~0.1%20AND%20Gender:Male&order=score" ).entity();

        hits = (Collection<?>) JsonHelper.jsonToSingleValue( entity );
        assertEquals( 2, hits.size() );
        it = (Iterator<LinkedHashMap<String, Object>>) hits.iterator();

        node2Map = it.next();
        node1Map = it.next();
        score2 = ( (Double) node2Map.get( "score" ) ).floatValue();
        score1 = ( (Double) node1Map.get( "score" ) ).floatValue();
        assertTrue(
                "results returned in wrong order for score ordering",
                ( (String) node2Map.get( "self" ) ).endsWith( Long.toString( node2 ) ) );
        assertTrue(
                "results returned in wrong order for score ordering",
                ( (String) node1Map.get( "self" ) ).endsWith( Long.toString( node1 ) ) );
        assertTrue( "scores are reversed", score2 > score1 );
    }

    /**
     * POST ${org.neo4j.server.rest.web}/index/node/{indexName}/{key}/{value}
     * "http://uri.for.node.to.index"
     */
    @Test
    public void shouldRespondWith201CreatedWhenIndexingJsonNodeUri() throws 
            JsonParseException
    {
        final long nodeId = helper.createNode();
        final String key = "key";
        final String value = "value";
        final String indexName = "testy";
        helper.createNodeIndex( indexName );

        JaxRsResponse response = RestRequest.req()
                .post( functionalTestHelper.indexNodeUri( indexName ), createJsonStringFor( nodeId, key, value ) );
        assertEquals( 201, response.getStatus() );
        assertNotNull( response.getHeaders()
                .getFirst( "Location" ) );
        assertEquals( Arrays.asList( (Long) nodeId ), helper.getIndexedNodes( indexName, key, value ) );
    }

    @Test
    public void shouldGetNodeRepresentationFromIndexUri() throws  JsonParseException
    {
        long nodeId = helper.createNode();
        String key = "key2";
        String value = "value";

        String indexName = "mindex";
        helper.createNodeIndex( indexName );
        JaxRsResponse response = RestRequest.req()
                .post( functionalTestHelper.indexNodeUri( indexName ),
                        createJsonStringFor( nodeId, key, value ));

        assertEquals( Status.CREATED.getStatusCode(), response.getStatus() );
        String indexUri = response.getHeaders()
                .getFirst( "Location" );

        response = RestRequest.req()
                .get( indexUri );
        assertEquals( 200, response.getStatus() );

        String entity = response.getEntity( String.class );

        Map<String, Object> map = JsonHelper.jsonToMap( entity );
        assertNotNull( map.get( "self" ) );
    }

    @Test
    public void shouldGet404WhenRequestingIndexUriWhichDoesntExist()
    {
        String key = "key3";
        String value = "value";
        String indexName = "nosuchindex";
        String indexUri = functionalTestHelper.nodeIndexUri() + indexName + "/" + key + "/" + value;
        JaxRsResponse response = RestRequest.req()
                .get( indexUri );
        assertEquals( Status.NOT_FOUND.getStatusCode(), response.getStatus() );
    }

    @Test
    public void shouldGet404WhenDeletingNonExtistentIndex()
    {
        String indexName = "nosuchindex";
        String indexUri = functionalTestHelper.nodeIndexUri() + indexName;
        JaxRsResponse response = RestRequest.req().delete(indexUri);
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
        final RestRequest request = RestRequest.req();
        JaxRsResponse responseToPost = request.post( functionalTestHelper.nodeUri(), "{\"name\":\"" + name1 + "\"}" );
        assertEquals( 201, responseToPost.getStatus() );
        String location1 = responseToPost.getHeaders()
                .getFirst( HttpHeaders.LOCATION );
        responseToPost.close();
        responseToPost = request.post( functionalTestHelper.nodeUri(), "{\"name\":\"" + name2 + "\"}" );
        assertEquals( 201, responseToPost.getStatus() );
        String location2 = responseToPost.getHeaders()
                .getFirst( HttpHeaders.LOCATION );
        responseToPost.close();
        responseToPost = request.post( functionalTestHelper.indexNodeUri( indexName ),
                createJsonStringFor( functionalTestHelper.getNodeIdFromUri( location1 ), key, value ) );
        assertEquals( 201, responseToPost.getStatus() );
        String indexLocation1 = responseToPost.getHeaders()
                .getFirst( HttpHeaders.LOCATION );
        responseToPost.close();
        responseToPost = request.post( functionalTestHelper.indexNodeUri( indexName ),
                createJsonStringFor( functionalTestHelper.getNodeIdFromUri( location2 ), key, value ) );
        assertEquals( 201, responseToPost.getStatus() );
        String indexLocation2 = responseToPost.getHeaders()
                .getFirst( HttpHeaders.LOCATION );
        Map<String, String> uriToName = new HashMap<String, String>();
        uriToName.put( indexLocation1, name1 );
        uriToName.put( indexLocation2, name2 );
        responseToPost.close();

        JaxRsResponse response = RestRequest.req()
                .get( functionalTestHelper.indexNodeUri( indexName, key, value ) );
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
    public void shouldGet200WhenGettingNodesFromIndexWithNoHits()
    {
        String indexName = "empty-index";
        helper.createNodeIndex( indexName );
        JaxRsResponse response = RestRequest.req()
                .get( functionalTestHelper.indexNodeUri( indexName, "non-existent-key", "non-existent-value" ) );
        assertEquals( 200, response.getStatus() );
        response.close();
    }

    /**
     * Delete node index.
     */
    @Documented
    @Test
    public void shouldReturn204WhenRemovingNodeIndexes() throws  JsonParseException
    {
        String indexName = "kvnode";
        helper.createNodeIndex( indexName );

        gen()
                .expectedStatus( 204 )
                .delete( functionalTestHelper.indexNodeUri( indexName ) );
    }

    //
    // REMOVING ENTRIES
    //

    /**
     * Remove all entries with a given node from an index.
     */
    @Documented
    @Test
    public void shouldBeAbleToRemoveIndexingById() throws  JsonParseException
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

        gen()
                .expectedStatus( 204 )
                .delete( functionalTestHelper.indexNodeUri( indexName ) + "/" + node );

        assertEquals( 0, helper.getIndexedNodes( indexName, key1, value1 )
                .size() );
        assertEquals( 0, helper.getIndexedNodes( indexName, key1, value2 )
                .size() );
        assertEquals( 0, helper.getIndexedNodes( indexName, key2, value1 )
                .size() );
        assertEquals( 0, helper.getIndexedNodes( indexName, key2, value2 )
                .size() );
    }

    /**
     * Remove all entries with a given node and key from an index.
     */
    @Documented
    @Test
    public void shouldBeAbleToRemoveIndexingByIdAndKey() throws  JsonParseException
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

        gen()
                .expectedStatus( 204 )
                .delete( functionalTestHelper.nodeIndexUri() + indexName + "/" + key2 + "/" + node );

        assertEquals( 1, helper.getIndexedNodes( indexName, key1, value1 )
                .size() );
        assertEquals( 1, helper.getIndexedNodes( indexName, key1, value2 )
                .size() );
        assertEquals( 0, helper.getIndexedNodes( indexName, key2, value1 )
                .size() );
        assertEquals( 0, helper.getIndexedNodes( indexName, key2, value2 )
                .size() );
    }

    /**
     * Remove all entries with a given node, key and value from an index.
     */
    @Documented
    @Test
    public void shouldBeAbleToRemoveIndexingByIdAndKeyAndValue() throws JsonParseException
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

        gen()
                .expectedStatus( 204 )
                .delete( functionalTestHelper.nodeIndexUri() + indexName + "/" + key1 + "/" + value1 + "/" + node );

        assertEquals( 0, helper.getIndexedNodes( indexName, key1, value1 )
                .size() );
        assertEquals( 1, helper.getIndexedNodes( indexName, key1, value2 )
                .size() );
        assertEquals( 1, helper.getIndexedNodes( indexName, key2, value1 )
                .size() );
        assertEquals( 1, helper.getIndexedNodes( indexName, key2, value2 )
                .size() );

    }

    @Test
    public void shouldBeAbleToIndexValuesContainingSpaces() throws Exception
    {
        final long nodeId = helper.createNode();
        final String key = "key";
        final String value = "value with   spaces  in it";

        String indexName = "spacey-values";
        helper.createNodeIndex( indexName );
        final RestRequest request = RestRequest.req();
        JaxRsResponse response = request.post( functionalTestHelper.indexNodeUri( indexName ),
                createJsonStringFor( nodeId, key, value ) );

        assertEquals( Status.CREATED.getStatusCode(), response.getStatus() );
        URI location = response.getLocation();
        response.close();
        response = request.get( functionalTestHelper.indexNodeUri( indexName, key, URIHelper.encode( value ) ) );
        assertEquals( Status.OK.getStatusCode(), response.getStatus() );
        Collection<?> hits = (Collection<?>) JsonHelper.jsonToSingleValue( response.getEntity( String.class ) );
        assertEquals( 1, hits.size() );
        response.close();

        CLIENT.resource( location )
                .delete();
        response = request.get( functionalTestHelper.indexNodeUri( indexName, key, URIHelper.encode( value ) ) );
        hits = (Collection<?>) JsonHelper.jsonToSingleValue( response.getEntity( String.class ) );
        assertEquals( 0, hits.size() );
    }

    private String createJsonStringFor( final long nodeId, final String key, final String value )
    {
        return "{\"key\": \"" + key + "\", \"value\": \"" + value + "\", \"uri\": \""
               + functionalTestHelper.nodeUri( nodeId ) + "\"}";
    }

    @Test
    public void shouldRespondWith400WhenSendingCorruptJson() throws Exception
    {
        final String indexName = "botherable-index";
        helper.createNodeIndex( indexName );
        final String corruptJson = "{\"key\" \"myKey\"}";
        JaxRsResponse response = RestRequest.req()
                .post( functionalTestHelper.indexNodeUri( indexName ),
                        corruptJson );
        assertEquals( 400, response.getStatus() );
        response.close();
    }

    /**
     * Create a unique node in an index.
     */
    @Documented
    @Test
    public void get_or_create_node() throws Exception
    {
        final String index = "people", key = "name", value = "Tobias";
        helper.createNodeIndex( index );
        ResponseEntity response = gen()
                                     .expectedStatus( 201 /* created */)
                                     .payloadType( MediaType.APPLICATION_JSON_TYPE )
                                     .payload( "{\"key\": \"" + key + "\", \"value\": \"" + value
                                                       + "\", \"properties\": {\"" + key + "\": \"" + value
                                                       + "\", \"sequence\": 1}}" )
                                     .post( functionalTestHelper.nodeIndexUri() + index + "?unique" );

        MultivaluedMap<String, String> headers = response.response().getHeaders();
        Map<String, Object> result = JsonHelper.jsonToMap( response.entity() );
        assertEquals( result.get( "indexed" ), headers.getFirst( "Location" ) );
        Map<String, Object> data = assertCast( Map.class, result.get( "data" ) );
        assertEquals( value, data.get( key ) );
        assertEquals( 1, data.get( "sequence" ) );
    }

    /**
     * Create a unique node in an index (the case where it exists).
     */
    @Documented
    @Test
    public void get_or_create_node_if_existing() throws Exception
    {
        final String index = "people", key = "name", value = "Peter";

        GraphDatabaseService graphdb = graphdb();
        Transaction tx = graphdb.beginTx();
        try
        {
            Node peter = graphdb.createNode();
            peter.setProperty( key, value );
            peter.setProperty( "sequence", 1 );
            graphdb.index().forNodes( index ).add( peter, key, value );

            tx.success();
        }
        finally
        {
            tx.finish();
        }

        helper.createNodeIndex( index );
        ResponseEntity response = gen()
                                     .expectedStatus( 200 /* ok */)
                                     .payloadType( MediaType.APPLICATION_JSON_TYPE )
                                     .payload( "{\"key\": \"" + key + "\", \"value\": \"" + value
                                                       + "\", \"properties\": {\"" + key + "\": \"" + value
                                                       + "\", \"sequence\": 2}}" )
                                     .post( functionalTestHelper.nodeIndexUri() + index + "?unique" );

        Map<String, Object> result = JsonHelper.jsonToMap( response.entity() );
        Map<String, Object> data = assertCast( Map.class, result.get( "data" ) );
        assertEquals( value, data.get( key ) );
        assertEquals( 1, data.get( "sequence" ) );
    }

    /**
     * Add a node to an index unless a node already exists for the given mapping.
     */
    @Documented
    @Test
    public void put_node_if_absent() throws Exception
    {
        final String index = "people", key = "name", value = "Mattias";
        helper.createNodeIndex( index );
        gen().expectedStatus( 201 /* created */ )
                 .payloadType( MediaType.APPLICATION_JSON_TYPE )
                 .payload( "{\"key\": \"" + key + "\", \"value\": \"" + value + "\", \"uri\":\"" + functionalTestHelper.nodeUri( helper.createNode() ) + "\"}" )
                 .post( functionalTestHelper.nodeIndexUri() + index + "?unique" );
    }

    private static <T> T assertCast( Class<T> type, Object object )
    {
        assertTrue( type.isInstance( object ) );
        return type.cast( object );
    }
}
