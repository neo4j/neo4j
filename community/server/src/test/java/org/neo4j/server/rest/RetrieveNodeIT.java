/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import javax.ws.rs.core.MediaType;

import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.rest.RESTRequestGenerator.ResponseEntity;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.repr.formats.CompactJsonFormat;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class RetrieveNodeIT extends AbstractRestFunctionalDocTestBase
{
    private URI nodeUri;
    private GraphDbHelper helper;
    private static FunctionalTestHelper functionalTestHelper;

    @BeforeClass
    public static void setupServer()
    {
        functionalTestHelper = new FunctionalTestHelper( server() );
    }

    @Before
    public void cleanTheDatabaseAndInitialiseTheNodeUri() throws Exception
    {
        helper = new GraphDbHelper( server().getDatabase() );
        nodeUri = new URI( functionalTestHelper.nodeUri() + "/" + helper.createNode() );
    }

    @Test
    public void shouldParameteriseUrisInNodeRepresentationWithHostHeaderValue() throws Exception
    {
        try ( CloseableHttpClient httpclient = HttpClientBuilder.create().build() )
        {
            HttpGet httpget = new HttpGet( nodeUri );

            httpget.setHeader( "Accept", "application/json" );
            httpget.setHeader( "Host", "dummy.neo4j.org" );
            HttpResponse response = httpclient.execute( httpget );
            HttpEntity entity = response.getEntity();

            String entityBody = IOUtils.toString( entity.getContent(), StandardCharsets.UTF_8 );

            assertThat( entityBody, containsString( "http://dummy.neo4j.org/db/data/node/" ) );

        }
    }

    @Test
    public void shouldParameteriseUrisInNodeRepresentationWithoutHostHeaderUsingRequestUri() throws Exception
    {
        try ( CloseableHttpClient httpclient = HttpClientBuilder.create().build() )
        {
            HttpGet httpget = new HttpGet( nodeUri );

            httpget.setHeader( "Accept", "application/json" );
            HttpResponse response = httpclient.execute( httpget );
            HttpEntity entity = response.getEntity();

            String entityBody = IOUtils.toString( entity.getContent(), StandardCharsets.UTF_8 );

            assertThat( entityBody, containsString( nodeUri.toString() ) );
        }
    }

    @Documented( "Get node.\n" +
                 "\n" +
                 "Note that the response contains URI/templates for the available\n" +
                 "operations for getting properties and relationships." )
    @Test
    public void shouldGet200WhenRetrievingNode()
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
    public void shouldGetContentLengthHeaderWhenRetrievingNode()
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
        long nonExistentNode = helper.createNode();
        helper.deleteNode( nonExistentNode );
        URI nonExistentNodeUri = new URI( functionalTestHelper.nodeUri() + "/" + nonExistentNode );

        gen.get()
                .expectedStatus( 404 )
                .get( nonExistentNodeUri.toString() );
    }

    private JaxRsResponse retrieveNodeFromService( final String uri )
    {
        return RestRequest.req().get( uri );
    }

}
