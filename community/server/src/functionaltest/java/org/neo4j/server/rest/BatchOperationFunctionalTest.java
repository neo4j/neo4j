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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.helpers.ServerHelper;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;

public class BatchOperationFunctionalTest
{

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
    public void cleanTheDatabase()
    {
        ServerHelper.cleanTheDatabase( server );
    }

    @AfterClass
    public static void stopServer()
    {
        server.stop();
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldReturnCorrectFromAndIdValuesOnMixedRequest() throws JsonParseException, ClientHandlerException,
            UniformInterfaceException
    {

        String jsonString = "[" + "{ " + "\"method\":\"PUT\"," + "\"to\":\"/node/0/properties\", "
                            + "\"body\":{ \"age\":1 }," + "\"id\":0" + "}," + "{ " + "\"method\":\"GET\","
                            + "\"to\":\"/node/0\"," + "\"id\":1" + "}," + "{ " + "\"method\":\"POST\","
                            + "\"to\":\"/node\", " + "\"id\":2," + "\"body\":{ \"age\":1 }" + "}," + "{ "
                            + "\"method\":\"POST\"," + "\"to\":\"/node\", " + "\"id\":3," + "\"body\":{ \"age\":1 }"
                            + "}" + "]";

        ClientResponse response = Client.create()
                .resource( functionalTestHelper.dataUri() + "batch" )
                .type( MediaType.APPLICATION_JSON )
                .accept( MediaType.APPLICATION_JSON )
                .entity( jsonString )
                .post( ClientResponse.class );

        assertEquals( 200, response.getStatus() );

        List<Map<String, Object>> results = JsonHelper.jsonToList( response.getEntity( String.class ) );

        assertEquals( 4, results.size() );

        Map<String, Object> putResult = results.get( 0 );
        Map<String, Object> getResult = results.get( 1 );
        Map<String, Object> firstPostResult = results.get( 2 );
        Map<String, Object> secondPostResult = results.get( 3 );

        // Ids should be ok
        assertEquals( 0, putResult.get( "id" ) );
        assertEquals( 1, getResult.get( "id" ) );
        assertEquals( 2, firstPostResult.get( "id" ) );
        assertEquals( 3, secondPostResult.get( "id" ) );

        // Should contain "from"
        assertEquals( "/node/0/properties", putResult.get( "from" ) );
        assertEquals( "/node/0", getResult.get( "from" ) );
        assertEquals( "/node", firstPostResult.get( "from" ) );
        assertEquals( "/node", secondPostResult.get( "from" ) );

        // Should have created by the first PUT request
        Map<String, Object> body = JsonHelper.jsonToMap( (String) getResult.get( "body" ) );
        assertEquals( 1, ( (Map<String, Object>) body.get( "data" ) ).get( "age" ) );
    }

    @Test
    public void shouldGetLocationHeadersWhenCreatingThings() throws JsonParseException, ClientHandlerException,
            UniformInterfaceException
    {

        String jsonString = "[" + "{ " + "\"method\":\"POST\"," + "\"to\":\"/node\", " + "\"body\":{ \"age\":1 }" + "}"
                            + "]";

        int originalNodeCount = helper.getNumberOfNodes();

        ClientResponse response = Client.create()
                .resource( functionalTestHelper.dataUri() + "batch" )
                .type( MediaType.APPLICATION_JSON )
                .accept( MediaType.APPLICATION_JSON )
                .entity( jsonString )
                .post( ClientResponse.class );

        assertEquals( 200, response.getStatus() );
        assertEquals( originalNodeCount + 1, helper.getNumberOfNodes() );

        List<Map<String, Object>> results = JsonHelper.jsonToList( response.getEntity( String.class ) );

        assertEquals( 1, results.size() );

        Map<String, Object> result = results.get( 0 );
        assertTrue( ( (String) result.get( "location" ) ).length() > 0 );
    }

    @Test
    public void shouldRollbackAllWhenGivenIncorrectRequest() throws JsonParseException, ClientHandlerException,
            UniformInterfaceException
    {

        String jsonString = "[" + "{ " + "\"method\":\"POST\"," + "\"to\":\"/node\", " + "\"body\":{ \"age\":1 }"
                            + "}," + "{ " + "\"method\":\"POST\"," + "\"to\":\"/node\", "
                            + "\"body\":[\"a_list\",\"this_makes_no_sense\"]" + "}" + "]";

        int originalNodeCount = helper.getNumberOfNodes();

        ClientResponse response = Client.create()
                .resource( functionalTestHelper.dataUri() + "batch" )
                .type( MediaType.APPLICATION_JSON )
                .accept( MediaType.APPLICATION_JSON )
                .entity( jsonString )
                .post( ClientResponse.class );

        assertEquals( 400, response.getStatus() );
        assertEquals( originalNodeCount, helper.getNumberOfNodes() );

    }

    @Test
    public void shouldRollbackAllWhenInsertingIllegalData() throws JsonParseException, ClientHandlerException,
            UniformInterfaceException
    {

        String jsonString = "[" + "{ " + "\"method\":\"POST\"," + "\"to\":\"/node\", " + "\"body\":{ \"age\":1 }"
                            + "}," + "{ " + "\"method\":\"POST\"," + "\"to\":\"/node\", "
                            + "\"body\":{ \"age\":{ \"age\":{ \"age\":1 } } }" + "}" + "]";

        int originalNodeCount = helper.getNumberOfNodes();

        ClientResponse response = Client.create()
                .resource( functionalTestHelper.dataUri() + "batch" )
                .type( MediaType.APPLICATION_JSON )
                .accept( MediaType.APPLICATION_JSON )
                .entity( jsonString )
                .post( ClientResponse.class );

        assertEquals( 400, response.getStatus() );
        assertEquals( originalNodeCount, helper.getNumberOfNodes() );

    }

    @Test
    public void shouldRollbackAllOnSingle404() throws JsonParseException, ClientHandlerException,
            UniformInterfaceException
    {

        String jsonString = "[" + "{ " + "\"method\":\"POST\"," + "\"to\":\"/node\", " + "\"body\":{ \"age\":1 }"
                            + "}," + "{ " + "\"method\":\"POST\"," + "\"to\":\"www.google.com\"" + "}" + "]";

        int originalNodeCount = helper.getNumberOfNodes();

        ClientResponse response = Client.create()
                .resource( functionalTestHelper.dataUri() + "batch" )
                .type( MediaType.APPLICATION_JSON )
                .accept( MediaType.APPLICATION_JSON )
                .entity( jsonString )
                .post( ClientResponse.class );

        assertEquals( 400, response.getStatus() );
        assertEquals( originalNodeCount, helper.getNumberOfNodes() );

    }
}
