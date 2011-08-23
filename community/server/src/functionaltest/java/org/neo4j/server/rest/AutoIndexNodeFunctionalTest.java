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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
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

public class AutoIndexNodeFunctionalTest
{
    private static NeoServerWithEmbeddedWebServer server;
    private static FunctionalTestHelper functionalTestHelper;
    private static GraphDbHelper helper;
    public @Rule
    TestData<DocsGenerator> gen = TestData.producedThrough( DocsGenerator.PRODUCER );

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

    /**
     * Find node by query from an automatic index.
     * 
     * See Find node by query for the actual query syntax.
     */
    @Documented
    @Test
    public void shouldRetrieveFromAutoIndexByQuery() throws PropertyValueException
    {
        String key = "bobsKey";
        String value = "bobsValue";
        Map<String, Object> props = new HashMap<String, Object>();
        props.put( key, value );

        helper.enableNodeAutoIndexingFor( key );
        helper.createNode( props );

        String entity = gen.get()
                .expectedStatus( 200 )
                .get( functionalTestHelper.nodeAutoIndexUri() + "?query=" + key + ":" + value )
                .entity();

        Collection<?> hits = (Collection<?>) JsonHelper.jsonToSingleValue( entity );
        assertEquals( 1, hits.size() );
    }

    /**
     * Find node by exact match from an automatic index.
     */
    @Documented
    @Test
    public void shouldRetrieveFromAutoIndexByExactMatch() throws PropertyValueException
    {
        String key = "bobsKey";
        String value = "bobsValue";
        Map<String, Object> props = new HashMap<String, Object>();
        props.put( key, value );

        helper.enableNodeAutoIndexingFor( key );
        helper.createNode( props );

        String entity = gen.get()
                .expectedStatus( 200 )
                .get( functionalTestHelper.nodeAutoIndexUri() + key + "/" + value )
                .entity();

        Collection<?> hits = (Collection<?>) JsonHelper.jsonToSingleValue( entity );
        assertEquals( 1, hits.size() );
    }

    @Test
    public void shouldNotBeAbleToRemoveAutoIndex() throws DatabaseBlockedException, JsonParseException
    {
        String indexName = server.getDatabase().graph.index()
                .getNodeAutoIndexer()
                .getAutoIndex()
                .getName();
        Response r = RestRequest.req()
                .delete( functionalTestHelper.nodeIndexUri() + indexName );
        assertEquals( 405, r.getStatus() );
    }

    @Test
    public void shouldNotAddToAutoIndex() throws Exception
    {
        String indexName = server.getDatabase().graph.index()
                .getNodeAutoIndexer()
                .getAutoIndex()
                .getName();
        String key = "key";
        String value = "the value";
        value = URIHelper.encode( value );
        int nodeId = 0;

        Response r = RestRequest.req()
                .post( functionalTestHelper.indexNodeUri( indexName, key, value ),
                        JsonHelper.createJsonFrom( functionalTestHelper.nodeUri( nodeId ) ) );
        assertEquals( 405, r.getStatus() );
    }

    @Test
    public void shouldNotBeAbleToRemoveAutoIndexedItems() throws DatabaseBlockedException, JsonParseException
    {
        final RestRequest request = RestRequest.req();
        String indexName = server.getDatabase().graph.index()
                .getNodeAutoIndexer()
                .getAutoIndex()
                .getName();

        Response r = request.delete( functionalTestHelper.nodeIndexUri() + indexName + "/key/value/0" );
        assertEquals( 405, r.getStatus() );

        r = request.delete( functionalTestHelper.nodeIndexUri() + indexName + "/key/0" );
        assertEquals( 405, r.getStatus() );

        r = request.delete( functionalTestHelper.nodeIndexUri() + indexName + "/0" );
        assertEquals( 405, r.getStatus() );
    }
}
