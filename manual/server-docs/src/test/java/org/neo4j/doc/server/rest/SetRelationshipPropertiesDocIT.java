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
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.GraphDescription.Graph;

import static org.junit.Assert.assertEquals;

public class SetRelationshipPropertiesDocIT extends org.neo4j.doc.server.rest.AbstractRestFunctionalDocTestBase
{
    private URI propertiesUri;
    private URI badUri;

    private static FunctionalTestHelper functionalTestHelper;

    @BeforeClass
    public static void setupServer() throws IOException
    {
        functionalTestHelper = new FunctionalTestHelper( server() );
    }

    @Before
    public void setupTheDatabase() throws Exception
    {
        long relationshipId = new GraphDbHelper( server().getDatabase() ).createRelationship( "KNOWS" );
        propertiesUri = new URI( functionalTestHelper.relationshipPropertiesUri( relationshipId ) );
        badUri = new URI( functionalTestHelper.relationshipPropertiesUri( relationshipId + 1 * 99999 ) );
    }

    @Documented( "Update relationship properties." )
    @Test
    @Graph
    public void shouldReturn204WhenPropertiesAreUpdated() throws JsonParseException
    {
        data.get();
        Map<String, Object> map = new HashMap<String, Object>();
        map.put( "jim", "tobias" );
        gen.get().description( startGraph( "update relationship properties" ) )
                .payload( JsonHelper.createJsonFrom( map ) )
                .expectedStatus( 204 )
                .put( propertiesUri.toString() );
        org.neo4j.doc.server.rest.JaxRsResponse response = updatePropertiesOnServer(map);
        assertEquals( 204, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldReturn400WhenSendinIncompatibleJsonProperties() throws JsonParseException
    {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put( "jim", new HashMap<String, Object>() );
        org.neo4j.doc.server.rest.JaxRsResponse response = updatePropertiesOnServer(map);
        assertEquals( 400, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldReturn400WhenSendingCorruptJsonProperties() {
        org.neo4j.doc.server.rest.JaxRsResponse
                response = RestRequest.req().put(propertiesUri.toString(), "this:::Is::notJSON}");
        assertEquals(400, response.getStatus());
        response.close();
    }

    @Test
    public void shouldReturn404WhenPropertiesSentToANodeWhichDoesNotExist() throws JsonParseException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("jim", "tobias");

        org.neo4j.doc.server.rest.JaxRsResponse response = RestRequest.req().put(badUri.toString(), JsonHelper.createJsonFrom(map));
        assertEquals(404, response.getStatus());
        response.close();
    }

    private org.neo4j.doc.server.rest.JaxRsResponse updatePropertiesOnServer(final Map<String, Object> map) throws JsonParseException
    {
        return RestRequest.req().put(propertiesUri.toString(), JsonHelper.createJsonFrom(map));
    }

    private String getPropertyUri(final String key) throws Exception
    {
        return propertiesUri.toString() + "/" + key ;
    }

    @Test
    public void shouldReturn204WhenPropertyIsSet() throws Exception
    {
        org.neo4j.doc.server.rest.JaxRsResponse response = setPropertyOnServer("foo", "bar");
        assertEquals( 204, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldReturn400WhenSendinIncompatibleJsonProperty() throws Exception
    {
        org.neo4j.doc.server.rest.JaxRsResponse response = setPropertyOnServer("jim", new HashMap<String, Object>());
        assertEquals( 400, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldReturn400WhenSendingCorruptJsonProperty() throws Exception {
        org.neo4j.doc.server.rest.JaxRsResponse
                response = RestRequest.req().put(getPropertyUri("foo"), "this:::Is::notJSON}");
        assertEquals(400, response.getStatus());
        response.close();
    }

    @Test
    public void shouldReturn404WhenPropertySentToANodeWhichDoesNotExist() throws Exception {
        org.neo4j.doc.server.rest.JaxRsResponse response = RestRequest.req().put( badUri.toString() + "/foo", JsonHelper.createJsonFrom("bar"));
        assertEquals(404, response.getStatus());
        response.close();
    }

    private org.neo4j.doc.server.rest.JaxRsResponse setPropertyOnServer(final String key, final Object value) throws Exception {
        return RestRequest.req().put(getPropertyUri(key), JsonHelper.createJsonFrom(value));
    }
}
