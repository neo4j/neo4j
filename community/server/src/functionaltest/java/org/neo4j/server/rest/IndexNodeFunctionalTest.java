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

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response.Status;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
<<<<<<< HEAD
=======
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
>>>>>>> d3302c3... Improved the putIfAbsent/getOrCreate API and exposed it in the REST API.
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.database.DatabaseBlockedException;
import org.neo4j.server.helpers.FunctionalTestHelper;
<<<<<<< HEAD
import org.neo4j.server.helpers.ServerHelper;
=======
import org.neo4j.server.rest.RESTDocsGenerator.ResponseEntity;
>>>>>>> d3302c3... Improved the putIfAbsent/getOrCreate API and exposed it in the REST API.
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.domain.URIHelper;
import org.neo4j.server.rest.web.PropertyValueException;
import org.neo4j.test.TestData;

<<<<<<< HEAD
public class IndexNodeFunctionalTest
=======
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.server.helpers.FunctionalTestHelper.CLIENT;

public class IndexNodeFunctionalTest extends AbstractRestFunctionalTestBase
>>>>>>> d3302c3... Improved the putIfAbsent/getOrCreate API and exposed it in the REST API.
{
    private static NeoServerWithEmbeddedWebServer server;
    private static FunctionalTestHelper functionalTestHelper;
    private static GraphDbHelper helper;
    public @Rule
    TestData<RESTDocsGenerator> gen = TestData.producedThrough( RESTDocsGenerator.PRODUCER );

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
        gen.get()
                .setGraph( server.getDatabase().graph );
    }

<<<<<<< HEAD
    @AfterClass
    public static void stopServer()
    {
        server.stop();
    }

    /**
     * List node indexes (empty result). This is an example covering the case
     * where no node index exists.
     */
    @Documented
    @Test
    public void shouldGetEmptyListOfNodeIndexesWhenNoneExist() throws PropertyValueException
    {
        gen.get()
                .expectedStatus( 204 )
                .get( functionalTestHelper.nodeIndexUri() );
=======
    long createNode()
    {
        AbstractGraphDatabase graphdb = server().getDatabase().graph;
        Transaction tx = graphdb.beginTx();
        Node node;
        try {
            node = graphdb.createNode();

            tx.success();
        } finally {
            tx.finish();
        }
        return node.getId();
>>>>>>> d3302c3... Improved the putIfAbsent/getOrCreate API and exposed it in the REST API.
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
        String entity = gen.get()
                .expectedStatus( 200 )
                .get( functionalTestHelper.nodeIndexUri() )
                .entity();
        Map<String, Object> map = JsonHelper.jsonToMap( entity );
        assertNotNull( map.get( indexName ) );

        assertEquals( 1, map.size() );
    }

    /**
     * Create node index
     *
     * NOTE: Instead of creating the index this way, you can simply start to use
     * it, and it will be created automatically.
     */
    @Documented
    @Test
    public void shouldCreateANamedNodeIndex() throws JsonParseException
    {
        String indexName = "favorites";
        int expectedIndexes = helper.getNodeIndexes().length + 1;
        Map<String, String> indexSpecification = new HashMap<String, String>();
        indexSpecification.put( "name", indexName );

        gen.get()
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

        gen.get()
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
        gen.get()
                .payload( "{\"name\":\"fulltext\", \"config\":{\"type\":\"fulltext\",\"provider\":\"lucene\"}}" )
                .expectedStatus( 201 )
                .expectedHeader( "Location" )
                .post( functionalTestHelper.nodeIndexUri() );

        assertEquals( 1, helper.getNodeIndexes().length );
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
        int nodeId = 0;
        // implicitly create the index
        gen.get()
                .expectedStatus( 201 )
                .payload(
                        JsonHelper.createJsonFrom( generateNodeIndexCreationPayload( key, value,
                                functionalTestHelper.nodeUri( nodeId ) ) ) )
                .post( functionalTestHelper.indexNodeUri( indexName ) );
        // look if we get one entry back
        JaxRsResponse response = RestRequest.req()
                .get( functionalTestHelper.indexNodeUri( indexName, key, URIHelper.encode( value ) ) );
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
        value = URIHelper.encode( value );
        // implicitly create the index
        JaxRsResponse response = RestRequest.req()
                .post( functionalTestHelper.indexNodeUri( indexName ), createJsonStringFor( 0, key, value ) );
        assertEquals( 201, response.getStatus() );

        // search it exact
        String entity = gen.get()
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
     * the Lucene query language here. Below and Example of a fuzzy search over
     * multiple keys.
     *
     * See: http://lucene.apache.org/java/3_1_0/queryparsersyntax.html
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

        String entity = gen.get()
                .expectedStatus( 200 )
                .get( functionalTestHelper.indexNodeUri( indexName ) + "?query=Name:Build~0.1%20AND%20Gender:Male" )
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
    public void shouldGetNodeRepresentationFromIndexUri() throws DatabaseBlockedException, JsonParseException
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
    public void shouldGet404WhenRequestingIndexUriWhichDoesntExist() throws DatabaseBlockedException
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
    public void shouldReturn204WhenRemovingNodeIndexes() throws DatabaseBlockedException, JsonParseException
    {
        String indexName = "kvnode";
        helper.createNodeIndex( indexName );

        gen.get()
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
    public void shouldBeAbleToRemoveIndexingById() throws DatabaseBlockedException, JsonParseException
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

        gen.get()
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
    public void shouldBeAbleToRemoveIndexingByIdAndKey() throws DatabaseBlockedException, JsonParseException
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

        gen.get()
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
    public void shouldBeAbleToRemoveIndexingByIdAndKeyAndValue() throws DatabaseBlockedException, JsonParseException
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

        gen.get()
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
        ResponseEntity response = gen.get()
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
     * Create a unique node in an index.
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
        ResponseEntity response = gen.get()
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
        gen.get().expectedStatus( 201 /* created */ )
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
