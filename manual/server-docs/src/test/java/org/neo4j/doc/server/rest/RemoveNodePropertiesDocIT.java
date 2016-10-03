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

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.domain.GraphDbHelper;

import static org.junit.Assert.assertEquals;

public class RemoveNodePropertiesDocIT extends org.neo4j.doc.server.rest.AbstractRestFunctionalDocTestBase
{
    private static FunctionalTestHelper functionalTestHelper;
    private static GraphDbHelper helper;

    @BeforeClass
    public static void setupServer() throws IOException
    {
        functionalTestHelper = new FunctionalTestHelper( server() );
        helper = functionalTestHelper.getGraphDbHelper();
    }

    private String getPropertiesUri( final long nodeId )
    {
        return functionalTestHelper.nodePropertiesUri( nodeId );
    }

    @Test
    public void shouldReturn204WhenPropertiesAreRemoved()
    {
        long nodeId = helper.createNode();
        Map<String, Object> map = new HashMap<String, Object>();
        map.put( "jim", "tobias" );
        helper.setNodeProperties( nodeId, map );
        org.neo4j.doc.server.rest.JaxRsResponse response = removeNodePropertiesOnServer(nodeId);
        assertEquals( 204, response.getStatus() );
        response.close();
    }

    @Documented( "Delete all properties from node." )
    @Test
    public void shouldReturn204WhenAllPropertiesAreRemoved()
    {
        long nodeId = helper.createNode();
        Map<String, Object> map = new HashMap<String, Object>();
        map.put( "jim", "tobias" );
        helper.setNodeProperties( nodeId, map );
        gen.get().description( startGraph( "delete all prps from node" ) )
                .expectedStatus( 204 )
                .delete( functionalTestHelper.nodePropertiesUri( nodeId ) );
    }

    @Test
    public void shouldReturn404WhenPropertiesSentToANodeWhichDoesNotExist() {
        org.neo4j.doc.server.rest.JaxRsResponse response = RestRequest.req().delete(getPropertiesUri(999999));
        assertEquals(404, response.getStatus());
        response.close();
    }

    private org.neo4j.doc.server.rest.JaxRsResponse removeNodePropertiesOnServer(final long nodeId)
    {
        return RestRequest.req().delete(getPropertiesUri(nodeId));
    }

    @Documented( "To delete a single property\n" +
                 "from a node, see the example below" )
    @Test
    public void delete_a_named_property_from_a_node()
    {
        long nodeId = helper.createNode();
        Map<String, Object> map = new HashMap<String, Object>();
        map.put( "name", "tobias" );
        helper.setNodeProperties( nodeId, map );
        gen.get()
        .expectedStatus( 204 )
        .description( startGraph( "delete named property start" ))
        .delete( functionalTestHelper.nodePropertyUri( nodeId, "name") );
    }

    @Test
    public void shouldReturn404WhenRemovingNonExistingNodeProperty()
    {
        long nodeId = helper.createNode();
        Map<String, Object> map = new HashMap<String, Object>();
        map.put( "jim", "tobias" );
        helper.setNodeProperties( nodeId, map );
        org.neo4j.doc.server.rest.JaxRsResponse response = removeNodePropertyOnServer(nodeId, "foo");
        assertEquals(404, response.getStatus());
    }

    @Test
    public void shouldReturn404WhenPropertySentToANodeWhichDoesNotExist() {
        org.neo4j.doc.server.rest.JaxRsResponse response = RestRequest.req().delete(getPropertyUri(999999, "foo"));
        assertEquals(404, response.getStatus());
    }

    private String getPropertyUri( final long nodeId, final String key )
    {
        return functionalTestHelper.nodePropertyUri( nodeId, key );
    }

    private org.neo4j.doc.server.rest.JaxRsResponse removeNodePropertyOnServer(final long nodeId, final String key)
    {
        return RestRequest.req().delete(getPropertyUri(nodeId, key));
    }
}
