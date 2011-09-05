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
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.TestData;

public class SetNodePropertiesFunctionalTest
{

    private URI propertiesUri;
    private URI badUri;

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
    public void setupTheDatabase() throws Exception
    {
        ServerHelper.cleanTheDatabase( server );
        long nodeId = helper.createNode();
        propertiesUri = new URI( functionalTestHelper.nodeUri( nodeId ) + "/properties" );
        badUri = new URI( functionalTestHelper.nodeUri( nodeId * 999 ) + "/properties" );
    }

    @AfterClass
    public static void stopServer()
    {
        server.stop();
    }

    public @Rule
    TestData<RESTDocsGenerator> gen = TestData.producedThrough( RESTDocsGenerator.PRODUCER );

    /**
     * Update node properties.
     */
    @Documented
    @Test
    public void shouldReturn204WhenPropertiesAreUpdated() throws JsonParseException
    {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put( "jim", "tobias" );
        gen.get()
                .payload( JsonHelper.createJsonFrom( map ) )
                .expectedStatus( 204 )
                .put( propertiesUri.toString() );
        JaxRsResponse response = updateNodePropertiesOnServer(map);
        assertEquals(204, response.getStatus());
    }

    @Test
    public void shouldReturn400WhenSendinIncompatibleJsonProperties() throws JsonParseException
    {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put( "jim", new HashMap<String, Object>() );
        JaxRsResponse response = updateNodePropertiesOnServer(map);
        assertEquals( 400, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldReturn400WhenSendingCorruptJsonProperties() {
        JaxRsResponse response = RestRequest.req().put(propertiesUri, "this:::Is::notJSON}");
        assertEquals(400, response.getStatus());
        response.close();
    }

    @Test
    public void shouldReturn404WhenPropertiesSentToANodeWhichDoesNotExist() throws JsonParseException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("jim", "tobias");
        JaxRsResponse response = RestRequest.req().put(badUri, JsonHelper.createJsonFrom(map));
        assertEquals(404, response.getStatus());
        response.close();
    }

    private JaxRsResponse updateNodePropertiesOnServer(Map<String, Object> map) throws JsonParseException {
        return RestRequest.req().put(propertiesUri, JsonHelper.createJsonFrom(map));
    }

    private URI getPropertyUri( String key ) throws Exception
    {
        return new URI( propertiesUri.toString() + "/" + key );
    }

    /**
     * Set property on node.
     */
    @Documented
    @Test
    public void shouldReturn204WhenPropertyIsSet() throws Exception
    {
        gen.get()
                .payload( JsonHelper.createJsonFrom( "bar" ) )
                .expectedStatus( 204 )
                .put( getPropertyUri( "foo" ).toString() );
    }

    /**
     * Property values can not be nested.
     * 
     * Nesting properties is not supported. You could for example store the
     * nested json as a string instead.
     */
    @Documented
    @Test
    public void shouldReturn400WhenSendinIncompatibleJsonProperty() throws Exception
    {
        gen.get()
                .payload( "{\"foo\" : {\"bar\" : \"baz\"}}" )
                .expectedStatus( 400 )
                .post( functionalTestHelper.nodeUri() );
        JaxRsResponse response = setNodePropertyOnServer("jim", new HashMap<String, Object>());
        assertEquals( 400, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldReturn400WhenSendingCorruptJsonProperty() throws Exception {
        JaxRsResponse response = RestRequest.req().put(getPropertyUri("foo"), "this:::Is::notJSON}");
        assertEquals(400, response.getStatus());
        response.close();
    }

    @Test
    public void shouldReturn404WhenPropertySentToANodeWhichDoesNotExist() throws Exception {
        JaxRsResponse response = RestRequest.req().put(badUri.toString() + "/foo", JsonHelper.createJsonFrom("bar"));
        assertEquals(404, response.getStatus());
        response.close();
    }

    private JaxRsResponse setNodePropertyOnServer(String key, Object value) throws Exception {
        return RestRequest.req().put(getPropertyUri(key), JsonHelper.createJsonFrom(value));
    }
}
