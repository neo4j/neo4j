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

public class SetRelationshipPropertiesDocIT extends AbstractRestFunctionalDocTestBase
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
        JaxRsResponse response = updatePropertiesOnServer(map);
        assertEquals( 204, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldReturn400WhenSendinIncompatibleJsonProperties() throws JsonParseException
    {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put( "jim", new HashMap<String, Object>() );
        JaxRsResponse response = updatePropertiesOnServer(map);
        assertEquals( 400, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldReturn400WhenSendingCorruptJsonProperties() {
        JaxRsResponse response = RestRequest.req().put(propertiesUri.toString(), "this:::Is::notJSON}");
        assertEquals(400, response.getStatus());
        response.close();
    }

    @Test
    public void shouldReturn404WhenPropertiesSentToANodeWhichDoesNotExist() throws JsonParseException {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("jim", "tobias");

        JaxRsResponse response = RestRequest.req().put(badUri.toString(), JsonHelper.createJsonFrom(map));
        assertEquals(404, response.getStatus());
        response.close();
    }

    private JaxRsResponse updatePropertiesOnServer(final Map<String, Object> map) throws JsonParseException
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
        JaxRsResponse response = setPropertyOnServer("foo", "bar");
        assertEquals( 204, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldReturn400WhenSendinIncompatibleJsonProperty() throws Exception
    {
        JaxRsResponse response = setPropertyOnServer("jim", new HashMap<String, Object>());
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

    private JaxRsResponse setPropertyOnServer(final String key, final Object value) throws Exception {
        return RestRequest.req().put(getPropertyUri(key), JsonHelper.createJsonFrom(value));
    }
}
