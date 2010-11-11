/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.server.NeoServer;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.server.database.DatabaseBlockedException;
import org.neo4j.server.rest.domain.GraphDbHelper;

import javax.ws.rs.core.MediaType;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class RemoveRelationshipPropertiesFunctionalTest
{

    private static GraphDbHelper helper;
    public static NeoServer server;

    @BeforeClass
    public static void startServer() throws Exception
    {
        server = ServerTestUtils.initializeServerWithRandomTemporaryDatabaseDirectory();
        helper = new GraphDbHelper( server.database() );
    }

    private String getPropertiesUri( long relationshipId )
    {
        return server.restApiUri() + "relationship/" + relationshipId + "/properties";
    }

    @AfterClass
    public static void stopServer() throws Exception
    {
        ServerTestUtils.nukeServer();
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
        ClientResponse response = Client.create().resource( getPropertiesUri( 999999 ) ).type( MediaType.APPLICATION_JSON ).accept( MediaType.APPLICATION_JSON )
                .delete( ClientResponse.class );
        assertEquals( 404, response.getStatus() );
    }

    private ClientResponse removeRelationshipPropertiesOnServer(
            long relationshipId )
    {
        return Client.create().resource( getPropertiesUri( relationshipId ) ).delete( ClientResponse.class );
    }

    private String getPropertyUri( long relationshipId, String key )
    {
        return server.restApiUri() + "relationship/" + relationshipId + "/properties/" + key;
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
    }

    @Test
    public void shouldReturn404WhenPropertyRemovedFromARelationshipWhichDoesNotExist()
    {
        ClientResponse response = Client.create().resource( getPropertyUri( 999999, "foo" ) ).delete( ClientResponse.class );
        assertEquals( 404, response.getStatus() );
    }

    private ClientResponse removeRelationshipPropertyOnServer( long nodeId,
                                                               String key )
    {
        return Client.create().resource( getPropertyUri( nodeId, key ) ).delete( ClientResponse.class );
    }
}
