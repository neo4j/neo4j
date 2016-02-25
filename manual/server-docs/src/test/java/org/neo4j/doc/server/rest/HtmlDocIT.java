/*
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.neo4j.doc.server.rest;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;

import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.RelationshipDirection;
import org.neo4j.test.server.HTTP;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HtmlDocIT extends org.neo4j.doc.server.rest.AbstractRestFunctionalTestBase
{
    private long thomasAnderson;
    private long trinity;
    private long thomasAndersonLovesTrinity;

    private static FunctionalTestHelper functionalTestHelper;
    private static GraphDbHelper helper;

    @BeforeClass
    public static void setupServer() throws IOException
    {
        functionalTestHelper = new FunctionalTestHelper( server() );
        helper = functionalTestHelper.getGraphDbHelper();
    }

    @Before
    public void setupTheDatabase()
    {
        // Create the matrix example
        thomasAnderson = createAndIndexNode( "Thomas Anderson" );
        trinity = createAndIndexNode( "Trinity" );
        long tank = createAndIndexNode( "Tank" );

        long knowsRelationshipId = helper.createRelationship( "KNOWS", thomasAnderson, trinity );
        thomasAndersonLovesTrinity = helper.createRelationship( "LOVES", thomasAnderson, trinity );
        helper.setRelationshipProperties( thomasAndersonLovesTrinity,
                Collections.singletonMap( "strength", (Object) 100 ) );
        helper.createRelationship( "KNOWS", thomasAnderson, tank );
        helper.createRelationship( "KNOWS", trinity, tank );

        // index a relationship
        helper.createRelationshipIndex( "relationships" );
        helper.addRelationshipToIndex( "relationships", "key", "value", knowsRelationshipId );

        // index a relationship
        helper.createRelationshipIndex( "relationships2" );
        helper.addRelationshipToIndex( "relationships2", "key2", "value2", knowsRelationshipId );
    }

    private long createAndIndexNode( String name )
    {
        long id = helper.createNode();
        helper.setNodeProperties( id, Collections.singletonMap( "name", (Object) name ) );
        helper.addNodeToIndex( "node", "name", name, id );
        return id;
    }

    @Test
    public void shouldGetRoot() {
        org.neo4j.doc.server.rest.JaxRsResponse
                response = RestRequest.req().get(functionalTestHelper.dataUri(), MediaType.TEXT_HTML_TYPE);
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        assertValidHtml( response.getEntity() );
        response.close();
    }

    @Test
    public void shouldGetRootWithHTTP() {
        HTTP.Response response = HTTP.withHeaders("Accept", MediaType.TEXT_HTML).GET(functionalTestHelper.dataUri());
        assertEquals(Status.OK.getStatusCode(), response.status());
        assertValidHtml( response.rawContent() );
    }

    @Test
    public void shouldGetNodeIndexRoot() {
        org.neo4j.doc.server.rest.JaxRsResponse
                response = RestRequest.req().get(functionalTestHelper.nodeIndexUri(), MediaType.TEXT_HTML_TYPE);
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        assertValidHtml( response.getEntity() );
        response.close();
    }

    @Test
    public void shouldGetRelationshipIndexRoot() {
        org.neo4j.doc.server.rest.JaxRsResponse
                response = RestRequest.req().get(functionalTestHelper.relationshipIndexUri(), MediaType.TEXT_HTML_TYPE);
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        assertValidHtml( response.getEntity() );
        response.close();
    }

    @Test
    public void shouldGetTrinityWhenSearchingForHer() {
        org.neo4j.doc.server.rest.JaxRsResponse
                response = RestRequest.req().get(functionalTestHelper.indexNodeUri("node", "name", "Trinity"), MediaType.TEXT_HTML_TYPE);
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        String entity = response.getEntity();
        assertTrue(entity.contains("Trinity"));
        assertValidHtml(entity);
        response.close();
    }

    @Test
    public void shouldGetThomasAndersonDirectly() {
        org.neo4j.doc.server.rest.JaxRsResponse
                response = RestRequest.req().get(functionalTestHelper.nodeUri(thomasAnderson), MediaType.TEXT_HTML_TYPE);
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        String entity = response.getEntity();
        assertTrue(entity.contains("Thomas Anderson"));
        assertValidHtml(entity);
        response.close();
    }

    @Test
    public void shouldGetSomeRelationships() {
        final RestRequest request = RestRequest.req();
        org.neo4j.doc.server.rest.JaxRsResponse
                response = request.get(functionalTestHelper.relationshipsUri(thomasAnderson, RelationshipDirection.all.name(), "KNOWS"), MediaType.TEXT_HTML_TYPE);
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        String entity = response.getEntity();
        assertTrue(entity.contains("KNOWS"));
        assertFalse(entity.contains("LOVES"));
        assertValidHtml(entity);
        response.close();

        response = request.get(functionalTestHelper.relationshipsUri(thomasAnderson, RelationshipDirection.all.name(), "LOVES"),
                MediaType.TEXT_HTML_TYPE);

        entity = response.getEntity();
        assertFalse(entity.contains("KNOWS"));
        assertTrue(entity.contains("LOVES"));
        assertValidHtml(entity);
        response.close();

        response = request.get(
                functionalTestHelper.relationshipsUri(thomasAnderson, RelationshipDirection.all.name(), "LOVES",
                        "KNOWS"),MediaType.TEXT_HTML_TYPE);
        entity = response.getEntity();
        assertTrue(entity.contains("KNOWS"));
        assertTrue(entity.contains("LOVES"));
        assertValidHtml(entity);
        response.close();
    }

    @Test
    public void shouldGetThomasAndersonLovesTrinityRelationship() {
        org.neo4j.doc.server.rest.JaxRsResponse response = RestRequest.req().get(functionalTestHelper.relationshipUri(thomasAndersonLovesTrinity), MediaType.TEXT_HTML_TYPE);
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        String entity = response.getEntity();
        assertTrue(entity.contains("strength"));
        assertTrue(entity.contains("100"));
        assertTrue(entity.contains("LOVES"));
        assertValidHtml(entity);
        response.close();
    }

    private void assertValidHtml( String entity )
    {
        assertTrue( entity.contains( "<html>" ) );
        assertTrue( entity.contains( "</html>" ) );
    }
}
