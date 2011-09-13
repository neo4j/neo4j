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
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.database.DatabaseBlockedException;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;

public class GetRelationshipPropertiesFunctionalTest
{
    private static String baseRelationshipUri;

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
    public void setupTheDatabase()
    {
        ServerHelper.cleanTheDatabase( server );

        long relationship = helper.createRelationship( "LIKES" );
        Map<String, Object> map = new HashMap<String, Object>();
        map.put( "foo", "bar" );
        helper.setRelationshipProperties( relationship, map );
        baseRelationshipUri = functionalTestHelper.dataUri() + "relationship/" + relationship + "/properties/";
    }

    @AfterClass
    public static void stopServer()
    {
        server.stop();
    }

    @Test
    public void shouldGet204ForNoProperties() throws DatabaseBlockedException {
        long relId = helper.createRelationship("LIKES");
        JaxRsResponse response = RestRequest.req().get(functionalTestHelper.dataUri() + "relationship/" + relId
                + "/properties");
        assertEquals(204, response.getStatus());
        response.close();
    }

    @Test
    public void shouldGet200AndContentLengthForProperties() throws DatabaseBlockedException {
        long relId = helper.createRelationship("LIKES");
        helper.setRelationshipProperties(relId, Collections.<String, Object>singletonMap("foo", "bar"));
        JaxRsResponse response = RestRequest.req().get(functionalTestHelper.dataUri() + "relationship/" + relId
                + "/properties");
        assertEquals(200, response.getStatus());
        assertNotNull(response.getHeaders()
                .get("Content-Length"));
        response.close();
    }

    @Test
    public void shouldGet404ForPropertiesOnNonExistentRelationship() {
        JaxRsResponse response = RestRequest.req().get(functionalTestHelper.dataUri() + "relationship/999999/properties");
        assertEquals(404, response.getStatus());
        response.close();
    }

    @Test
    public void shouldBeJSONContentTypeOnPropertiesResponse() throws DatabaseBlockedException {
        long relId = helper.createRelationship("LIKES");
        helper.setRelationshipProperties(relId, Collections.<String, Object>singletonMap("foo", "bar"));
        JaxRsResponse response = RestRequest.req().get(functionalTestHelper.dataUri() + "relationship/" + relId
                + "/properties");
        assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
        response.close();
    }

    private String getPropertyUri( String key )
    {
        return baseRelationshipUri + key;
    }

    @Test
    public void shouldGet404ForNoProperty() {
        JaxRsResponse response = RestRequest.req().get(getPropertyUri("baz"));
        assertEquals(404, response.getStatus());
        response.close();
    }

    @Test
    public void shouldGet200ForProperty() {
        String propertyUri = getPropertyUri("foo");
        JaxRsResponse response = RestRequest.req().get(propertyUri);
        assertEquals(200, response.getStatus());
        response.close();
    }

    @Test
    public void shouldGet404ForNonExistingRelationship() {
        String uri = functionalTestHelper.dataUri() + "relationship/999999/properties/foo";
        JaxRsResponse response = RestRequest.req().get(uri);
        assertEquals(404, response.getStatus());
        response.close();
    }

    @Test
    public void shouldBeValidJSONOnResponse() throws JsonParseException
    {
        JaxRsResponse response = RestRequest.req().get(getPropertyUri("foo"));
        assertEquals( MediaType.APPLICATION_JSON_TYPE, response.getType() );
        assertNotNull( JsonHelper.createJsonFrom( response.getEntity( String.class ) ) );
        response.close();
    }
}
