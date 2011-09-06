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
import java.util.HashMap;
import java.util.Map;

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
import org.neo4j.test.TestData;

public class RemoveNodePropertiesFunctionalTest
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

    public @Rule
    TestData<RESTDocsGenerator> gen = TestData.producedThrough( RESTDocsGenerator.PRODUCER );

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
        JaxRsResponse response = removeNodePropertiesOnServer(nodeId);
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
    public void shouldReturn404WhenPropertiesSentToANodeWhichDoesNotExist() {
        JaxRsResponse response = RestRequest.req().delete(getPropertiesUri(999999));
        assertEquals(404, response.getStatus());
        response.close();
    }

    private JaxRsResponse removeNodePropertiesOnServer(final long nodeId)
    {
        return RestRequest.req().delete(getPropertiesUri(nodeId));
    }

    @Test
    public void shouldReturn204WhenPropertyIsRemoved() throws DatabaseBlockedException
    {
        long nodeId = helper.createNode();
        Map<String, Object> map = new HashMap<String, Object>();
        map.put( "jim", "tobias" );
        helper.setNodeProperties( nodeId, map );
        JaxRsResponse response = removeNodePropertyOnServer(nodeId, "jim");
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
        JaxRsResponse response = removeNodePropertyOnServer(nodeId, "foo");
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldReturn404WhenPropertySentToANodeWhichDoesNotExist() {
        JaxRsResponse response = RestRequest.req().delete(getPropertyUri(999999, "foo"));
        assertEquals(404, response.getStatus());
    }

    private String getPropertyUri( final long nodeId, final String key )
    {
        return functionalTestHelper.nodePropertyUri( nodeId, key );
    }

    private JaxRsResponse removeNodePropertyOnServer(final long nodeId, final String key)
    {
        return RestRequest.req().delete(getPropertyUri(nodeId, key));
    }
}
