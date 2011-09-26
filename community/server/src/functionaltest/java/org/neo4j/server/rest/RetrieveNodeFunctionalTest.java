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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.helpers.FunctionalTestHelper;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.server.rest.RESTDocsGenerator.ResponseEntity;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.repr.formats.CompactJsonFormat;
import org.neo4j.test.TestData;

public class RetrieveNodeFunctionalTest
{
    private URI nodeUri;
    private static NeoServerWithEmbeddedWebServer server;
    private static FunctionalTestHelper functionalTestHelper;

    @BeforeClass
    public static void setupServer() throws IOException
    {
        server = ServerHelper.createServer();
        functionalTestHelper = new FunctionalTestHelper( server );
    }

    @Before
    public void cleanTheDatabaseAndInitialiseTheNodeUri() throws Exception
    {
        ServerHelper.cleanTheDatabase( server );
        nodeUri = new URI( functionalTestHelper.nodeUri() + "/"
                           + new GraphDbHelper( server.getDatabase() ).createNode() );
    }

    @AfterClass
    public static void stopServer()
    {
        server.stop();
    }

    public @Rule
    TestData<RESTDocsGenerator> gen = TestData.producedThrough( RESTDocsGenerator.PRODUCER );

    /**
     * Get node.
     * 
     * Note that the response contains URI/templates for the available
     * operations for getting properties and relationships.
     */
    @Documented
    @Test
    public void shouldGet200WhenRetrievingNode() throws Exception
    {
        String uri = nodeUri.toString();
        gen.get()
                .expectedStatus( 200 )
                .get( uri );
    }

    /**
     * Get node - compact.
     * 
     * Specifying the subformat in the requests media type yields a more compact
     * JSON response without metadata and templates.
     */
    @Documented
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
        JaxRsResponse response = retrieveNodeFromService(nodeUri.toString());
        assertNotNull( response.getHeaders()
                .get( "Content-Length" ) );
        response.close();
    }

    @Test
    public void shouldHaveJsonMediaTypeOnResponse()
    {
        JaxRsResponse response = retrieveNodeFromService(nodeUri.toString());
        assertEquals( MediaType.APPLICATION_JSON_TYPE, response.getType() );
        response.close();
    }

    @Test
    public void shouldHaveJsonDataInResponse() throws Exception
    {
        JaxRsResponse response = retrieveNodeFromService(nodeUri.toString());

        Map<String, Object> map = JsonHelper.jsonToMap( response.getEntity( String.class ) );
        assertTrue( map.containsKey( "self" ) );
        response.close();
    }

    /**
     * Get non-existent node.
     */
    @Documented
    @Test
    public void shouldGet404WhenRetrievingNonExistentNode() throws Exception
    {
        gen.get()
                .expectedStatus( 404 )
                .get( nodeUri + "00000" );
    }

    private JaxRsResponse retrieveNodeFromService(final String uri)
    {
        return RestRequest.req().get( uri );
    }

}
