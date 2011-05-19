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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.ServerBuilder;
import org.neo4j.server.modules.RESTApiModule;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;

public class BatchOperationFunctionalTest
{
    private NeoServerWithEmbeddedWebServer server;
    private FunctionalTestHelper functionalTestHelper;
    private GraphDbHelper helper;
    
    @Before
    public void setupServer() throws IOException {
        server = ServerBuilder.server().withRandomDatabaseDir().withSpecificServerModules(RESTApiModule.class).withPassingStartupHealthcheck().build();
        server.start();
        functionalTestHelper = new FunctionalTestHelper(server);
        helper = functionalTestHelper.getGraphDbHelper();
    }

    @After
    public void stopServer() {
        server.stop();
        server = null;
    }
    
    @Test
    public void shouldReturn200OnGetRequest() throws JsonParseException, ClientHandlerException, UniformInterfaceException {
        
        String jsonString = "[" +
          "{ " +
            "\"method\":\"PUT\"," +
            "\"to\":\"/node/0/properties\", " +
            "\"body\":{ \"age\":1 }," +
            "\"id\":0"+
          "},"+
          "{ " +
            "\"method\":\"GET\"," +
            "\"to\":\"/node/0\"," +
            "\"id\":1"+
          "}"+
        "]";
        
        ClientResponse response = Client.create()
          .resource( functionalTestHelper.dataUri() + "batch")
          .type(MediaType.APPLICATION_JSON ).accept( MediaType.APPLICATION_JSON )
          .entity( jsonString ).post( ClientResponse.class );
        
        assertEquals(200, response.getStatus());
        
        List<Map<String, Object>> results = JsonHelper.jsonToList( response.getEntity( String.class ));
        
        assertEquals(2, results.size());
        
        int id=0;
        for(Map<String, Object> result : results) {
            assertEquals(id,result.get("id"));
            id++;
        }
    }
    
    @Test
    public void shouldGetLocationHeadersWhenCreatingThings() throws JsonParseException, ClientHandlerException, UniformInterfaceException {
        
        String jsonString = "[" +
          "{ " +
            "\"method\":\"POST\"," +
            "\"to\":\"/node\", " +
            "\"body\":{ \"age\":1 }" +
          "}"+
        "]";
        
        ClientResponse response = Client.create()
          .resource( functionalTestHelper.dataUri() + "batch")
          .type(MediaType.APPLICATION_JSON ).accept( MediaType.APPLICATION_JSON )
          .entity( jsonString ).post( ClientResponse.class );
        
        assertEquals(200, response.getStatus());
        
        List<Map<String, Object>> results = JsonHelper.jsonToList( response.getEntity( String.class ));
        
        assertEquals(1, results.size());
        
        
        Map<String, Object> result = results.get( 0 );
        
        assertTrue(result.containsKey( "headers" ));
        Map<String, String> headers = (Map<String, String>) result.get( "headers" );
        
        assertTrue(headers.containsKey( "Location" ));
        assertTrue(headers.get( "Location" ).length() > 0);
    }
}
