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
import static org.neo4j.server.rest.FunctionalTestHelper.CLIENT;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.database.DatabaseBlockedException;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.server.rest.domain.GraphDbHelper;

import com.sun.jersey.api.client.ClientResponse;

public class RemoveRelationshipPropertiesFunctionalTest
{
    private static NeoServerWithEmbeddedWebServer server;
    private static FunctionalTestHelper functionalTestHelper;
    private static GraphDbHelper helper;

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

    private String getPropertiesUri( long relationshipId )
    {
        return functionalTestHelper.relationshipUri() + "/" + relationshipId + "/properties";
    }

    @Test
    public void shouldReturn204WhenPropertiesAreRemovedFromRelationship() throws DatabaseBlockedException
    {
        long relationshipId = helper.createRelationship( "LOVES" );
        Map<String, Object> map = new HashMap<String, Object>();
        map.put( "jim", "tobias" );
        helper.setRelationshipProperties( relationshipId, map );
        ClientResponse response = removeRelationshipPropertiesOnServer( relationshipId );
        assertEquals( 204, response.getStatus() );
    }

    @Test
    public void shouldReturn404WhenPropertiesRemovedFromRelationshipWhichDoesNotExist()
    {
        ClientResponse response = CLIENT.resource( getPropertiesUri( 999999 ) )
                .type( MediaType.APPLICATION_JSON )
                .accept( MediaType.APPLICATION_JSON )
                .delete( ClientResponse.class );
        assertEquals( 404, response.getStatus() );
        response.close();
    }

    private ClientResponse removeRelationshipPropertiesOnServer( long relationshipId )
    {
        return CLIENT.resource( getPropertiesUri( relationshipId ) )
                .delete( ClientResponse.class );
    }

    private String getPropertyUri( long relationshipId, String key )
    {
        return functionalTestHelper.relationshipUri() + "/" + relationshipId + "/properties/" + key;
    }

    @Test
    public void shouldReturn204WhenRelationshipPropertyIsRemoved() throws DatabaseBlockedException
    {
        long relationshipId = helper.createRelationship( "LOVES" );
        Map<String, Object> map = new HashMap<String, Object>();
        map.put( "jim", "tobias" );
        helper.setRelationshipProperties( relationshipId, map );
        ClientResponse response = removeRelationshipPropertyOnServer( relationshipId, "jim" );
        assertEquals( 204, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldReturn404WhenRemovingNonExistentRelationshipProperty() throws DatabaseBlockedException
    {
        long relationshipId = helper.createRelationship( "KNOWS" );
        Map<String, Object> map = new HashMap<String, Object>();
        map.put( "jim", "tobias" );
        helper.setRelationshipProperties( relationshipId, map );
        ClientResponse response = removeRelationshipPropertyOnServer( relationshipId, "foo" );
        assertEquals( 404, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldReturn404WhenPropertyRemovedFromARelationshipWhichDoesNotExist()
    {
        ClientResponse response = CLIENT.resource( getPropertyUri( 999999, "foo" ) )
                .accept( MediaType.APPLICATION_JSON_TYPE )
                .delete( ClientResponse.class );
        assertEquals( 404, response.getStatus() );
        response.close();
    }

    private ClientResponse removeRelationshipPropertyOnServer( long nodeId, String key )
    {
        return CLIENT.resource( getPropertyUri( nodeId, key ) )
                .accept( MediaType.APPLICATION_JSON_TYPE )
                .delete( ClientResponse.class );
    }
}
