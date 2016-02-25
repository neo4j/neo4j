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
import javax.ws.rs.core.Response.Status;

import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.repr.formats.CompactJsonFormat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CompactJsonDocIT extends org.neo4j.doc.server.rest.AbstractRestFunctionalTestBase
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
        createTheMatrix();
    }

    private void createTheMatrix()
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
    public void shouldGetThomasAndersonDirectly() {
        org.neo4j.doc.server.rest.JaxRsResponse
                response = RestRequest.req().get(functionalTestHelper.nodeUri(thomasAnderson), CompactJsonFormat.MEDIA_TYPE);
        assertEquals(Status.OK.getStatusCode(), response.getStatus());
        String entity = response.getEntity();
        assertTrue(entity.contains("Thomas Anderson"));
        assertValidJson(entity);
        response.close();
    }

    private void assertValidJson( String entity )
    {
        try
        {
            assertTrue( JsonHelper.jsonToMap( entity )
                    .containsKey( "self" ) );
            assertFalse( JsonHelper.jsonToMap( entity )
                    .containsKey( "properties" ) );
        }
        catch ( JsonParseException e )
        {
            e.printStackTrace();
        }
    }
}
