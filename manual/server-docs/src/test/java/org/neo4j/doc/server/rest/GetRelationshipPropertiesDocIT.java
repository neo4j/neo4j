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

import org.hamcrest.MatcherAssert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.core.MediaType;

import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

public class GetRelationshipPropertiesDocIT extends org.neo4j.doc.server.rest.AbstractRestFunctionalTestBase
{
    private static String baseRelationshipUri;

    private static FunctionalTestHelper functionalTestHelper;
    private static GraphDbHelper helper;

    @BeforeClass
    public static void setupServer() throws IOException
    {
        functionalTestHelper = new FunctionalTestHelper( server() );
        helper = functionalTestHelper.getGraphDbHelper();
        setupTheDatabase();
    }

    private static void setupTheDatabase()
    {
        long relationship = helper.createRelationship( "LIKES" );
        Map<String, Object> map = new HashMap<String, Object>();
        map.put( "foo", "bar" );
        helper.setRelationshipProperties( relationship, map );
        baseRelationshipUri = functionalTestHelper.dataUri() + "relationship/" + relationship + "/properties/";
    }

    @Test
    public void shouldGet200AndContentLengthForProperties()
    {
        long relId = helper.createRelationship( "LIKES" );
        helper.setRelationshipProperties( relId, Collections.<String, Object>singletonMap( "foo", "bar" ) );
        org.neo4j.doc.server.rest.JaxRsResponse
                response = RestRequest.req().get( functionalTestHelper.dataUri() + "relationship/" + relId
                                                  + "/properties" );
        assertEquals( 200, response.getStatus() );
        assertNotNull( response.getHeaders()
                .get( "Content-Length" ) );
        response.close();
    }

    @Test
    public void shouldGet404ForPropertiesOnNonExistentRelationship()
    {
        org.neo4j.doc.server.rest.JaxRsResponse response = RestRequest.req().get( functionalTestHelper.dataUri() +
                                                                                  "relationship/999999/properties" );
        assertEquals( 404, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldBeJSONContentTypeOnPropertiesResponse()
    {
        long relId = helper.createRelationship( "LIKES" );
        helper.setRelationshipProperties( relId, Collections.<String, Object>singletonMap( "foo", "bar" ) );
        org.neo4j.doc.server.rest.JaxRsResponse
                response = RestRequest.req().get( functionalTestHelper.dataUri() + "relationship/" + relId
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
        org.neo4j.doc.server.rest.JaxRsResponse response = RestRequest.req().get( getPropertyUri( "baz" ) );
        assertEquals( 404, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldGet404ForNonExistingRelationship()
    {
        String uri = functionalTestHelper.dataUri() + "relationship/999999/properties/foo";
        org.neo4j.doc.server.rest.JaxRsResponse response = RestRequest.req().get( uri );
        assertEquals( 404, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldBeValidJSONOnResponse() throws JsonParseException
    {
        org.neo4j.doc.server.rest.JaxRsResponse response = RestRequest.req().get( getPropertyUri( "foo" ) );
        assertThat( response.getType().toString(), containsString( MediaType.APPLICATION_JSON ) );
        assertNotNull( JsonHelper.createJsonFrom( response.getEntity() ) );
        response.close();
    }

    @Test
    public void shouldReturnEmptyMapForEmptyProperties() throws Exception
    {
        // Given
        String node = HTTP.POST( server().baseUri().resolve( "db/data/node" ).toString() ).location();
        String rel = HTTP.POST( node + "/relationships", quotedJson( "{'to':'" + node + "', " +
                "'type':'LOVES'}" ) ).location();

        // When
        HTTP.Response res = HTTP.GET( rel + "/properties" );

        // Then
        MatcherAssert.assertThat( res.rawContent(), equalTo( "{ }" ) );
    }
}
