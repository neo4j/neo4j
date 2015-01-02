/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.core.MediaType;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class GetRelationshipPropertiesDocIT extends AbstractRestFunctionalTestBase
{
    private static String baseRelationshipUri;

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
        cleanDatabase();

        long relationship = helper.createRelationship( "LIKES" );
        Map<String, Object> map = new HashMap<String, Object>();
        map.put( "foo", "bar" );
        helper.setRelationshipProperties( relationship, map );
        baseRelationshipUri = functionalTestHelper.dataUri() + "relationship/" + relationship + "/properties/";
    }

    @Test
    public void shouldGet204ForNoProperties()
    {
        long relId = helper.createRelationship( "LIKES" );
        JaxRsResponse response = RestRequest.req().get( functionalTestHelper.dataUri() + "relationship/" + relId
                + "/properties" );
        assertEquals( 204, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldGet200AndContentLengthForProperties()
    {
        long relId = helper.createRelationship( "LIKES" );
        helper.setRelationshipProperties( relId, Collections.<String, Object>singletonMap( "foo", "bar" ) );
        JaxRsResponse response = RestRequest.req().get( functionalTestHelper.dataUri() + "relationship/" + relId
                + "/properties" );
        assertEquals( 200, response.getStatus() );
        assertNotNull( response.getHeaders()
                .get( "Content-Length" ) );
        response.close();
    }

    @Test
    public void shouldGet404ForPropertiesOnNonExistentRelationship()
    {
        JaxRsResponse response = RestRequest.req().get( functionalTestHelper.dataUri() +
                "relationship/999999/properties" );
        assertEquals( 404, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldBeJSONContentTypeOnPropertiesResponse()
    {
        long relId = helper.createRelationship( "LIKES" );
        helper.setRelationshipProperties( relId, Collections.<String, Object>singletonMap( "foo", "bar" ) );
        JaxRsResponse response = RestRequest.req().get( functionalTestHelper.dataUri() + "relationship/" + relId
                + "/properties" );
        assertThat( response.getType().toString(), containsString( MediaType.APPLICATION_JSON ) );
        response.close();
    }

    private String getPropertyUri( String key )
    {
        return baseRelationshipUri + key;
    }

    @Test
    public void shouldGet404ForNoProperty()
    {
        JaxRsResponse response = RestRequest.req().get( getPropertyUri( "baz" ) );
        assertEquals( 404, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldGet404ForNonExistingRelationship()
    {
        String uri = functionalTestHelper.dataUri() + "relationship/999999/properties/foo";
        JaxRsResponse response = RestRequest.req().get( uri );
        assertEquals( 404, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldBeValidJSONOnResponse() throws JsonParseException
    {
        JaxRsResponse response = RestRequest.req().get( getPropertyUri( "foo" ) );
        assertThat( response.getType().toString(), containsString( MediaType.APPLICATION_JSON ) );
        assertNotNull( JsonHelper.createJsonFrom( response.getEntity() ) );
        response.close();
    }
}
