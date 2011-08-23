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

public class AutoIndexRelationshipFunctionalTest
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
     * Find relationship by query from an automatic index.
     */
    @Documented
    @Test
    public void shouldRetrieveFromAutoIndexByQuery() throws PropertyValueException
    {
        String key = "bobsKey";
        String value = "bobsValue";
        Map<String, Object> props = new HashMap<String, Object>();
        props.put(key, value);
        
        helper.enableRelationshipAutoIndexingFor(key);
        helper.setRelationshipProperties(helper.createRelationship("sometype"), props);

        String entity = gen.get()
                .expectedStatus(200)
                .get(functionalTestHelper.relationshipAutoIndexUri() + "?query=" + key + ":" + value)
                .entity();
        
        Collection<?> hits = (Collection<?>) JsonHelper.jsonToSingleValue(entity);
        assertEquals(1, hits.size());
    }

    /**
     * Find relationship by exact match from an automatic index.
     */
    @Documented
    @Test
    public void shouldRetrieveFromAutoIndexByExactMatch() throws PropertyValueException
    {
        String key = "bobsKey";
        String value = "bobsValue";
        Map<String, Object> props = new HashMap<String, Object>();
        props.put(key, value);
        
        helper.enableRelationshipAutoIndexingFor(key);
        helper.setRelationshipProperties(helper.createRelationship("sometype"), props);

        String entity = gen.get()
                .expectedStatus(200)
                .get(functionalTestHelper.relationshipAutoIndexUri() + key + "/" + value)
                .entity();
        
        Collection<?> hits = (Collection<?>) JsonHelper.jsonToSingleValue(entity);
        assertEquals(1, hits.size());
    }

    @Test
    public void shouldNotBeAbleToRemoveAutoIndex() throws DatabaseBlockedException, JsonParseException
    {
        String indexName = server.getDatabase().graph.index().getRelationshipAutoIndexer().getAutoIndex().getName();
        Response r = RestRequest.req().delete(functionalTestHelper.relationshipIndexUri() + indexName);
        assertEquals(405, r.getStatus());
    }

    @Test
    public void shouldNotAddToAutoIndex() throws Exception {
        String indexName = server.getDatabase().graph.index().getRelationshipAutoIndexer().getAutoIndex().getName();
        String key = "key";
        String value = "the value";
        value = URIHelper.encode(value);
        long relId = helper.createRelationship("taa");

        Response r = RestRequest.req().post(
                functionalTestHelper.indexRelationshipUri(indexName, key, value), 
                JsonHelper.createJsonFrom(functionalTestHelper.relationshipUri(relId)));
        assertEquals(405, r.getStatus());
    }

    @Test
    public void shouldNotBeAbleToRemoveAutoIndexedItems() throws DatabaseBlockedException, JsonParseException
    {
        final RestRequest request = RestRequest.req();
        String indexName = server.getDatabase().graph.index().getRelationshipAutoIndexer().getAutoIndex().getName();
        long relId = helper.createRelationship("sometype");
        
        Response r = request.delete( functionalTestHelper.relationshipIndexUri() + indexName + "/key/value/" + relId );
        assertEquals( 405, r.getStatus() );
       
        r = request.delete(functionalTestHelper.relationshipIndexUri() + indexName + "/key/" + relId);
        assertEquals( 405, r.getStatus() );
        
        r = request.delete(functionalTestHelper.relationshipIndexUri() + indexName + "/" + relId);
        assertEquals( 405, r.getStatus() );
    }
}
