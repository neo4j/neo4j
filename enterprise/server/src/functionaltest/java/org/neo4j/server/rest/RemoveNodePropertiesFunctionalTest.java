/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.server.NeoServer;
import org.neo4j.server.ServerBuilder;
import org.neo4j.server.database.DatabaseBlockedException;
import org.neo4j.server.rest.domain.GraphDbHelper;

import javax.ws.rs.core.MediaType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class RemoveNodePropertiesFunctionalTest
{
    private NeoServer server;
    private FunctionalTestHelper functionalTestHelper;
    private GraphDbHelper helper;

    @Before
    public void setupServer() throws IOException {
        server = ServerBuilder.server().withRandomDatabaseDir().withPassingStartupHealthcheck().build();
        server.start();
        functionalTestHelper = new FunctionalTestHelper(server);
        helper = functionalTestHelper.getGraphDbHelper();
    }

    @After
    public void stopServer() {
        server.stop();
        server = null;
    }
    
    private String getPropertiesUri( long nodeId )
    {
        return server.restApiUri() + "node/" + nodeId + "/properties";
    }
    
    @Test
    public void shouldReturn204WhenPropertiesAreRemoved() throws DatabaseBlockedException
    {
        long nodeId = helper.createNode();
        Map<String, Object> map = new HashMap<String, Object>();
        map.put( "jim", "tobias" );
        helper.setNodeProperties( nodeId, map );
        ClientResponse response = removeNodePropertiesOnServer( nodeId );
        assertEquals( 204, response.getStatus() );
    }

    @Test
    public void shouldReturn404WhenPropertiesSentToANodeWhichDoesNotExist()
    {
        ClientResponse response = Client.create().resource( getPropertiesUri( 999999 ) ).type( MediaType.APPLICATION_JSON ).accept( MediaType.APPLICATION_JSON )
                .delete( ClientResponse.class );
        assertEquals( 404, response.getStatus() );
    }

    private ClientResponse removeNodePropertiesOnServer( long nodeId )
    {
        return Client.create().resource( getPropertiesUri( nodeId ) ).delete( ClientResponse.class );
    }

    @Test
    public void shouldReturn204WhenPropertyIsRemoved() throws DatabaseBlockedException
    {
        long nodeId = helper.createNode();
        Map<String, Object> map = new HashMap<String, Object>();
        map.put( "jim", "tobias" );
        helper.setNodeProperties( nodeId, map );
        ClientResponse response = removeNodePropertyOnServer( nodeId, "jim" );
        assertEquals( 204, response.getStatus() );
    }

    @Test
    public void shouldReturn404WhenRemovingNonExistingNodeProperty() throws DatabaseBlockedException
    {
        long nodeId = helper.createNode();
        Map<String, Object> map = new HashMap<String, Object>();
        map.put( "jim", "tobias" );
        helper.setNodeProperties( nodeId, map );
        ClientResponse response = removeNodePropertyOnServer( nodeId, "foo" );
        assertEquals( 404, response.getStatus() );
    }

    @Test
    public void shouldReturn404WhenPropertySentToANodeWhichDoesNotExist()
    {
        ClientResponse response = Client.create().resource( getPropertyUri( 999999, "foo" ) ).type( MediaType.APPLICATION_JSON ).accept( MediaType.APPLICATION_JSON )
                .delete( ClientResponse.class );
        assertEquals( 404, response.getStatus() );
    }

    private String getPropertyUri( long nodeId, String key )
    {
        return server.restApiUri() + "node/" + nodeId + "/properties/" + key;
    }

    private ClientResponse removeNodePropertyOnServer( long nodeId, String key )
    {
        return Client.create().resource( getPropertyUri( nodeId, key ) ).delete( ClientResponse.class );
    }
}
