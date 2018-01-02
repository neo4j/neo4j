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

public class CompactJsonDocIT extends AbstractRestFunctionalTestBase
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
        JaxRsResponse response = RestRequest.req().get(functionalTestHelper.nodeUri(thomasAnderson), CompactJsonFormat.MEDIA_TYPE);
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
