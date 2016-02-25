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
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import javax.ws.rs.core.MediaType;

import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.doc.server.rest.RESTDocsGenerator.ResponseEntity;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.repr.formats.CompactJsonFormat;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class RetrieveNodeDocIT extends org.neo4j.doc.server.rest.AbstractRestFunctionalDocTestBase
{
    private URI nodeUri;
    private static FunctionalTestHelper functionalTestHelper;

    @BeforeClass
    public static void setupServer() throws IOException
    {
        functionalTestHelper = new FunctionalTestHelper( server() );
    }

    @Before
    public void cleanTheDatabaseAndInitialiseTheNodeUri() throws Exception
    {
        nodeUri = new URI( functionalTestHelper.nodeUri() + "/"
                + new GraphDbHelper( server().getDatabase() ).createNode() );
    }

    @Test
    public void shouldParameteriseUrisInNodeRepresentationWithHostHeaderValue() throws Exception
    {
        HttpClient httpclient = new DefaultHttpClient();
        try
        {
            HttpGet httpget = new HttpGet( nodeUri );

            httpget.setHeader( "Accept", "application/json" );
            httpget.setHeader( "Host", "dummy.neo4j.org" );
            HttpResponse response = httpclient.execute( httpget );
            HttpEntity entity = response.getEntity();

            String entityBody = IOUtils.toString( entity.getContent(), StandardCharsets.UTF_8 );

            assertThat( entityBody, containsString( "http://dummy.neo4j.org/db/data/node/" ) );

        } finally
        {
            httpclient.getConnectionManager().shutdown();
        }
    }

    @Test
    public void shouldParameteriseUrisInNodeRepresentationWithoutHostHeaderUsingRequestUri() throws Exception
    {
        HttpClient httpclient = new DefaultHttpClient();
        try
        {
            HttpGet httpget = new HttpGet( nodeUri );

            httpget.setHeader( "Accept", "application/json" );
            HttpResponse response = httpclient.execute( httpget );
            HttpEntity entity = response.getEntity();

            String entityBody = IOUtils.toString( entity.getContent(), StandardCharsets.UTF_8 );

            assertThat( entityBody, containsString( nodeUri.toString() ) );
        } finally
        {
            httpclient.getConnectionManager().shutdown();
        }
    }

    @Documented( "Get node.\n" +
                 "\n" +
                 "Note that the response contains URI/templates for the available\n" +
                 "operations for getting properties and relationships." )
    @Test
    public void shouldGet200WhenRetrievingNode() throws Exception
    {
        String uri = nodeUri.toString();
        gen.get()
                .expectedStatus( 200 )
                .get( uri );
    }

    @Documented( "Get node -- compact.\n" +
                 "\n" +
                 "Specifying the subformat in the requests media type yields a more compact\n" +
                 "JSON response without metadata and templates." )
    @Test
    public void shouldGet200WhenRetrievingNodeCompact()
    {
        String uri = nodeUri.toString();
        ResponseEntity entity = gen.get()
                .expectedType( CompactJsonFormat.MEDIA_TYPE )
                .expectedStatus( 200 )
                .get( uri );
        assertTrue( entity.entity()
                .contains( "self" ) );
    }

    @Test
    public void shouldGetContentLengthHeaderWhenRetrievingNode() throws Exception
    {
        org.neo4j.doc.server.rest.JaxRsResponse response = retrieveNodeFromService( nodeUri.toString() );
        assertNotNull( response.getHeaders()
                .get( "Content-Length" ) );
        response.close();
    }

    @Test
    public void shouldHaveJsonMediaTypeOnResponse()
    {
        org.neo4j.doc.server.rest.JaxRsResponse response = retrieveNodeFromService( nodeUri.toString() );
        assertThat( response.getType().toString(), containsString( MediaType.APPLICATION_JSON ) );
        response.close();
    }

    @Test
    public void shouldHaveJsonDataInResponse() throws Exception
    {
        org.neo4j.doc.server.rest.JaxRsResponse response = retrieveNodeFromService( nodeUri.toString() );

        Map<String, Object> map = JsonHelper.jsonToMap( response.getEntity() );
        assertTrue( map.containsKey( "self" ) );
        response.close();
    }

    @Documented( "Get non-existent node." )
    @Test
    public void shouldGet404WhenRetrievingNonExistentNode() throws Exception
    {
        gen.get()
                .expectedStatus( 404 )
                .get( nodeUri + "00000" );
    }

    private org.neo4j.doc.server.rest.JaxRsResponse retrieveNodeFromService( final String uri )
    {
        return RestRequest.req().get( uri );
    }

}
