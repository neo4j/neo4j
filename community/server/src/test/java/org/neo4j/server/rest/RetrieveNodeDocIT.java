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
import java.util.Map;
import javax.ws.rs.core.MediaType;

import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.RESTDocsGenerator.ResponseEntity;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.repr.formats.CompactJsonFormat;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class RetrieveNodeDocIT extends AbstractRestFunctionalDocTestBase
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

            String entityBody = IOUtils.toString( entity.getContent(), "UTF-8" );

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

            String entityBody = IOUtils.toString( entity.getContent(), "UTF-8" );

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
        JaxRsResponse response = retrieveNodeFromService( nodeUri.toString() );
        assertNotNull( response.getHeaders()
                .get( "Content-Length" ) );
        response.close();
    }

    @Test
    public void shouldHaveJsonMediaTypeOnResponse()
    {
        JaxRsResponse response = retrieveNodeFromService( nodeUri.toString() );
        assertThat( response.getType().toString(), containsString( MediaType.APPLICATION_JSON ) );
        response.close();
    }

    @Test
    public void shouldHaveJsonDataInResponse() throws Exception
    {
        JaxRsResponse response = retrieveNodeFromService( nodeUri.toString() );

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

    private JaxRsResponse retrieveNodeFromService( final String uri )
    {
        return RestRequest.req().get( uri );
    }

}
