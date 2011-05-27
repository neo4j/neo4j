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
import static org.neo4j.server.WebTestUtils.CLIENT;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.ServerBuilder;
import org.neo4j.server.database.DatabaseBlockedException;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.test.TestData;

import com.sun.jersey.api.client.ClientResponse;

public class RemoveNodePropertiesFunctionalTest
{
    private NeoServerWithEmbeddedWebServer server;
    private FunctionalTestHelper functionalTestHelper;
    private GraphDbHelper helper;

    public @Rule
    TestData<DocsGenerator> gen = TestData.producedThrough( DocsGenerator.PRODUCER );

    @Before
    public void setupServer() throws IOException
    {
        server = ServerBuilder.server()
                .withRandomDatabaseDir()
                .withPassingStartupHealthcheck()
                .build();
        server.start();
        functionalTestHelper = new FunctionalTestHelper( server );
        helper = functionalTestHelper.getGraphDbHelper();
    }

    @After
    public void stopServer()
    {
        server.stop();
        server = null;
    }

    private String getPropertiesUri( final long nodeId )
    {
        return functionalTestHelper.nodePropertiesUri( nodeId );
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
        response.close();
    }

    /**
     * Delete all properties from node.
     */
    @Documented
    @Test
    public void shouldReturn204WhenAllPropertiesAreRemoved() throws DatabaseBlockedException
    {
        long nodeId = helper.createNode();
        Map<String, Object> map = new HashMap<String, Object>();
        map.put( "jim", "tobias" );
        helper.setNodeProperties( nodeId, map );
        gen.get()
                .expectedStatus( 204 )
                .delete( functionalTestHelper.nodePropertiesUri( nodeId ) );
    }

    @Test
    public void shouldReturn404WhenPropertiesSentToANodeWhichDoesNotExist()
    {
        ClientResponse response = CLIENT.resource( getPropertiesUri( 999999 ) )
                .type( MediaType.APPLICATION_JSON )
                .accept( MediaType.APPLICATION_JSON )
                .delete( ClientResponse.class );
        assertEquals( 404, response.getStatus() );
        response.close();
    }

    private ClientResponse removeNodePropertiesOnServer( final long nodeId )
    {
        return CLIENT.resource( getPropertiesUri( nodeId ) )
                .delete( ClientResponse.class );
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
        response.close();
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
        response.close();
    }

    @Test
    public void shouldReturn404WhenPropertySentToANodeWhichDoesNotExist()
    {
        ClientResponse response = CLIENT.resource( getPropertyUri( 999999, "foo" ) )
                .type( MediaType.APPLICATION_JSON )
                .accept( MediaType.APPLICATION_JSON )
                .delete( ClientResponse.class );
        assertEquals( 404, response.getStatus() );
        response.close();
    }

    private String getPropertyUri( final long nodeId, final String key )
    {
        return functionalTestHelper.nodePropertyUri( nodeId, key );
    }

    private ClientResponse removeNodePropertyOnServer( final long nodeId, final String key )
    {
        return CLIENT.resource( getPropertyUri( nodeId, key ) )
                .delete( ClientResponse.class );
    }
}
