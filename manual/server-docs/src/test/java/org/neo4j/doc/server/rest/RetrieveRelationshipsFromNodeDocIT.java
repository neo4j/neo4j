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

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MediaType;

import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.repr.RelationshipRepresentationTest;
import org.neo4j.server.rest.repr.formats.StreamingJsonFormat;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class RetrieveRelationshipsFromNodeDocIT extends org.neo4j.doc.server.rest.AbstractRestFunctionalDocTestBase
{
    private long nodeWithRelationships;
    private long nodeWithoutRelationships;
    private long nonExistingNode;

    private static FunctionalTestHelper functionalTestHelper;
    private static GraphDbHelper helper;
    private long likes;

    @BeforeClass
    public static void setupServer() throws IOException
    {
        functionalTestHelper = new FunctionalTestHelper( server() );
        helper = functionalTestHelper.getGraphDbHelper();
    }

    @Before
    public void setupTheDatabase()
    {
        nodeWithRelationships = helper.createNode();
        likes = helper.createRelationship( "LIKES", nodeWithRelationships, helper.createNode() );
        helper.createRelationship( "LIKES", helper.createNode(), nodeWithRelationships );
        helper.createRelationship( "HATES", nodeWithRelationships, helper.createNode() );
        nodeWithoutRelationships = helper.createNode();
        nonExistingNode = nodeWithoutRelationships * 100;
    }

    private org.neo4j.doc.server.rest.JaxRsResponse sendRetrieveRequestToServer( long nodeId, String path )
    {
        return RestRequest.req().get( functionalTestHelper.nodeUri() + "/" + nodeId + "/relationships" + path );
    }

    private void verifyRelReps( int expectedSize, String json ) throws JsonParseException
    {
        List<Map<String, Object>> relreps = JsonHelper.jsonToList( json );
        assertEquals( expectedSize, relreps.size() );
        for ( Map<String, Object> relrep : relreps )
        {
            RelationshipRepresentationTest.verifySerialisation( relrep );
        }
    }

    @Test
    public void shouldParameteriseUrisInRelationshipRepresentationWithHostHeaderValue() throws Exception
    {
        HttpClient httpclient = new DefaultHttpClient();
        try
        {
            HttpGet httpget = new HttpGet( "http://localhost:7474/db/data/relationship/" + likes );
            httpget.setHeader( "Accept", "application/json" );
            httpget.setHeader( "Host", "dummy.neo4j.org" );
            HttpResponse response = httpclient.execute( httpget );
            HttpEntity entity = response.getEntity();

            String entityBody = IOUtils.toString( entity.getContent(), StandardCharsets.UTF_8 );

            System.out.println( entityBody );

            assertThat( entityBody, containsString( "http://dummy.neo4j.org/db/data/relationship/" + likes ) );
            assertThat( entityBody, not( containsString( "localhost:7474" ) ) );
        }
        finally
        {
            httpclient.getConnectionManager().shutdown();
        }
    }

    @Test
    public void shouldParameteriseUrisInRelationshipRepresentationWithoutHostHeaderUsingRequestUri() throws Exception
    {
        HttpClient httpclient = new DefaultHttpClient();
        try
        {
            HttpGet httpget = new HttpGet( "http://localhost:7474/db/data/relationship/" + likes );

            httpget.setHeader( "Accept", "application/json" );
            HttpResponse response = httpclient.execute( httpget );
            HttpEntity entity = response.getEntity();

            String entityBody = IOUtils.toString( entity.getContent(), StandardCharsets.UTF_8 );

            assertThat( entityBody, containsString( "http://localhost:7474/db/data/relationship/" + likes ) );
        }
        finally
        {
            httpclient.getConnectionManager().shutdown();
        }
    }

    @Documented( "Get all relationships." )
    @Test
    public void shouldRespondWith200AndListOfRelationshipRepresentationsWhenGettingAllRelationshipsForANode()
            throws JsonParseException
    {
        String entity = gen.get()
                .expectedStatus( 200 )
                .get( functionalTestHelper.nodeUri() + "/" + nodeWithRelationships + "/relationships" + "/all" )
                .entity();
        verifyRelReps( 3, entity );
    }

    @Test
    public void shouldRespondWith200AndListOfRelationshipRepresentationsWhenGettingAllRelationshipsForANodeStreaming()
            throws JsonParseException
    {
        String entity = gen.get()
                .withHeader(StreamingJsonFormat.STREAM_HEADER,"true")
                .expectedStatus(200)
                .get( functionalTestHelper.nodeUri() + "/" + nodeWithRelationships + "/relationships" + "/all" )
                .entity();
        verifyRelReps( 3, entity );
    }

    @Documented( "Get incoming relationships." )
    @Test
    public void shouldRespondWith200AndListOfRelationshipRepresentationsWhenGettingIncomingRelationshipsForANode()
            throws JsonParseException
    {
        String entity = gen.get()
                .expectedStatus( 200 )
                .get( functionalTestHelper.nodeUri() + "/" + nodeWithRelationships + "/relationships" + "/in" )
                .entity();
        verifyRelReps( 1, entity );
    }

    @Documented( "Get outgoing relationships." )
    @Test
    public void shouldRespondWith200AndListOfRelationshipRepresentationsWhenGettingOutgoingRelationshipsForANode()
            throws JsonParseException
    {
        String entity = gen.get()
                .expectedStatus( 200 )
                .get( functionalTestHelper.nodeUri() + "/" + nodeWithRelationships + "/relationships" + "/out" )
                .entity();
        verifyRelReps( 2, entity );
    }

    @Documented( "Get typed relationships.\n" +
                 "\n" +
                 "Note that the \"+&+\" needs to be encoded like \"+%26+\" for example when\n" +
                 "using http://curl.haxx.se/[cURL] from the terminal." )
    @Test
    public void shouldRespondWith200AndListOfRelationshipRepresentationsWhenGettingAllTypedRelationshipsForANode()
            throws JsonParseException
    {
        String entity = gen.get()
                .expectedStatus( 200 )
                .get( functionalTestHelper.nodeUri() + "/" + nodeWithRelationships + "/relationships"
                        + "/all/LIKES&HATES" )
                .entity();
        verifyRelReps( 3, entity );
    }

    @Test
    public void shouldRespondWith200AndListOfRelationshipRepresentationsWhenGettingIncomingTypedRelationshipsForANode()
            throws JsonParseException
    {
        org.neo4j.doc.server.rest.JaxRsResponse
                response = sendRetrieveRequestToServer( nodeWithRelationships, "/in/LIKES" );
        assertEquals( 200, response.getStatus() );
        assertThat( response.getType().toString(), containsString( MediaType.APPLICATION_JSON ) );
        verifyRelReps( 1, response.getEntity() );
        response.close();
    }

    @Test
    public void shouldRespondWith200AndListOfRelationshipRepresentationsWhenGettingOutgoingTypedRelationshipsForANode()
            throws JsonParseException
    {
        org.neo4j.doc.server.rest.JaxRsResponse
                response = sendRetrieveRequestToServer( nodeWithRelationships, "/out/HATES" );
        assertEquals( 200, response.getStatus() );
        assertThat( response.getType().toString(), containsString( MediaType.APPLICATION_JSON ) );
        verifyRelReps( 1, response.getEntity() );
        response.close();
    }

    @Documented( "Get relationships on a node without relationships." )
    @Test
    public void shouldRespondWith200AndEmptyListOfRelationshipRepresentationsWhenGettingAllRelationshipsForANodeWithoutRelationships()
            throws JsonParseException
    {
        String entity = gen.get()
                .expectedStatus( 200 )
                .get( functionalTestHelper.nodeUri() + "/" + nodeWithoutRelationships + "/relationships" + "/all" )
                .entity();
        verifyRelReps( 0, entity );
    }

    @Test
    public void shouldRespondWith200AndEmptyListOfRelationshipRepresentationsWhenGettingIncomingRelationshipsForANodeWithoutRelationships()
            throws JsonParseException
    {
        org.neo4j.doc.server.rest.JaxRsResponse response = sendRetrieveRequestToServer( nodeWithoutRelationships, "/in" );
        assertEquals( 200, response.getStatus() );
        assertThat( response.getType().toString(), containsString( MediaType.APPLICATION_JSON ) );
        verifyRelReps( 0, response.getEntity() );
        response.close();
    }

    @Test
    public void shouldRespondWith200AndEmptyListOfRelationshipRepresentationsWhenGettingOutgoingRelationshipsForANodeWithoutRelationships()
            throws JsonParseException
    {
        org.neo4j.doc.server.rest.JaxRsResponse response = sendRetrieveRequestToServer( nodeWithoutRelationships, "/out" );
        assertEquals( 200, response.getStatus() );
        assertThat( response.getType().toString(), containsString( MediaType.APPLICATION_JSON ) );
        verifyRelReps( 0, response.getEntity() );
        response.close();
    }

    @Test
    public void shouldRespondWith404WhenGettingAllRelationshipsForNonExistingNode()
    {
        org.neo4j.doc.server.rest.JaxRsResponse response = sendRetrieveRequestToServer( nonExistingNode, "/all" );
        assertEquals( 404, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldRespondWith404WhenGettingIncomingRelationshipsForNonExistingNode()
    {
        org.neo4j.doc.server.rest.JaxRsResponse response = sendRetrieveRequestToServer( nonExistingNode, "/in" );
        assertEquals( 404, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldRespondWith404WhenGettingIncomingRelationshipsForNonExistingNodeStreaming()
    {
        org.neo4j.doc.server.rest.JaxRsResponse
                response = RestRequest
                .req().header(StreamingJsonFormat.STREAM_HEADER,"true").get( functionalTestHelper.nodeUri() + "/" + nonExistingNode + "/relationships" + "/in");
        assertEquals( 404, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldRespondWith404WhenGettingOutgoingRelationshipsForNonExistingNode()
    {
        org.neo4j.doc.server.rest.JaxRsResponse response = sendRetrieveRequestToServer( nonExistingNode, "/out" );
        assertEquals( 404, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldGet200WhenRetrievingValidRelationship()
    {
        long relationshipId = helper.createRelationship( "LIKES" );

        org.neo4j.doc.server.rest.JaxRsResponse response = RestRequest.req().get( functionalTestHelper.relationshipUri( relationshipId ) );

        assertEquals( 200, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldGetARelationshipRepresentationInJsonWhenRetrievingValidRelationship() throws Exception
    {
        long relationshipId = helper.createRelationship( "LIKES" );

        org.neo4j.doc.server.rest.JaxRsResponse response = RestRequest.req().get( functionalTestHelper.relationshipUri( relationshipId ) );

        String entity = response.getEntity();
        assertNotNull( entity );
        isLegalJson( entity );
        response.close();
    }

    private void isLegalJson( String entity ) throws IOException, JsonParseException
    {
        JsonHelper.jsonToMap( entity );
    }
}
