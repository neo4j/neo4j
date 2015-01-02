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
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.MediaType;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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

public class RetrieveRelationshipsFromNodeDocIT extends AbstractRestFunctionalTestBase
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
        cleanDatabase();
        createSimpleGraph();
    }

    private void createSimpleGraph()
    {
        nodeWithRelationships = helper.createNode();
        likes = helper.createRelationship( "LIKES", nodeWithRelationships, helper.createNode() );
        helper.createRelationship( "LIKES", helper.createNode(), nodeWithRelationships );
        helper.createRelationship( "HATES", nodeWithRelationships, helper.createNode() );
        nodeWithoutRelationships = helper.createNode();
        nonExistingNode = nodeWithoutRelationships * 100;
    }

    private JaxRsResponse sendRetrieveRequestToServer( long nodeId, String path )
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

            String entityBody = IOUtils.toString( entity.getContent(), "UTF-8" );

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

            String entityBody = IOUtils.toString( entity.getContent(), "UTF-8" );

            assertThat( entityBody, containsString( "http://localhost:7474/db/data/relationship/" + likes ) );
        }
        finally
        {
            httpclient.getConnectionManager().shutdown();
        }
    }

    /**
     * Get all relationships.
     */
    @Documented
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

    /**
     * Get incoming relationships.
     */
    @Documented
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

    /**
     * Get outgoing relationships.
     */
    @Documented
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

    /**
     * Get typed relationships.
     * 
     * Note that the "+&+" needs to be encoded like "+%26+" for example when
     * using http://curl.haxx.se/[cURL] from the terminal.
     */
    @Documented
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
        JaxRsResponse response = sendRetrieveRequestToServer( nodeWithRelationships, "/in/LIKES" );
        assertEquals( 200, response.getStatus() );
        assertThat( response.getType().toString(), containsString( MediaType.APPLICATION_JSON ) );
        verifyRelReps( 1, response.getEntity() );
        response.close();
    }

    @Test
    public void shouldRespondWith200AndListOfRelationshipRepresentationsWhenGettingOutgoingTypedRelationshipsForANode()
            throws JsonParseException
    {
        JaxRsResponse response = sendRetrieveRequestToServer( nodeWithRelationships, "/out/HATES" );
        assertEquals( 200, response.getStatus() );
        assertThat( response.getType().toString(), containsString( MediaType.APPLICATION_JSON ) );
        verifyRelReps( 1, response.getEntity() );
        response.close();
    }

    /**
     * Get relationships on a node without relationships.
     */
    @Documented
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
        JaxRsResponse response = sendRetrieveRequestToServer( nodeWithoutRelationships, "/in" );
        assertEquals( 200, response.getStatus() );
        assertThat( response.getType().toString(), containsString( MediaType.APPLICATION_JSON ) );
        verifyRelReps( 0, response.getEntity() );
        response.close();
    }

    @Test
    public void shouldRespondWith200AndEmptyListOfRelationshipRepresentationsWhenGettingOutgoingRelationshipsForANodeWithoutRelationships()
            throws JsonParseException
    {
        JaxRsResponse response = sendRetrieveRequestToServer( nodeWithoutRelationships, "/out" );
        assertEquals( 200, response.getStatus() );
        assertThat( response.getType().toString(), containsString( MediaType.APPLICATION_JSON ) );
        verifyRelReps( 0, response.getEntity() );
        response.close();
    }

    @Test
    public void shouldRespondWith404WhenGettingAllRelationshipsForNonExistingNode()
    {
        JaxRsResponse response = sendRetrieveRequestToServer( nonExistingNode, "/all" );
        assertEquals( 404, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldRespondWith404WhenGettingIncomingRelationshipsForNonExistingNode()
    {
        JaxRsResponse response = sendRetrieveRequestToServer( nonExistingNode, "/in" );
        assertEquals( 404, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldRespondWith404WhenGettingIncomingRelationshipsForNonExistingNodeStreaming()
    {
        JaxRsResponse response = RestRequest.req().header(StreamingJsonFormat.STREAM_HEADER,"true").get(functionalTestHelper.nodeUri() + "/" + nonExistingNode + "/relationships" + "/in");
        assertEquals( 404, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldRespondWith404WhenGettingOutgoingRelationshipsForNonExistingNode()
    {
        JaxRsResponse response = sendRetrieveRequestToServer( nonExistingNode, "/out" );
        assertEquals( 404, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldGet200WhenRetrievingValidRelationship()
    {
        long relationshipId = helper.createRelationship( "LIKES" );

        JaxRsResponse response = RestRequest.req().get( functionalTestHelper.relationshipUri( relationshipId ) );

        assertEquals( 200, response.getStatus() );
        response.close();
    }

    @Test
    public void shouldGetARelationshipRepresentationInJsonWhenRetrievingValidRelationship() throws Exception
    {
        long relationshipId = helper.createRelationship( "LIKES" );

        JaxRsResponse response = RestRequest.req().get( functionalTestHelper.relationshipUri( relationshipId ) );

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
