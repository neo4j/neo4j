/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.server.rest;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;

import javax.ws.rs.core.MediaType;
import java.net.URI;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RetrieveNodeFunctionalTest extends FunctionalTestBase
{
    private static URI nodeUri;

    @BeforeClass
    public static void startServer() throws Exception
    {
        nodeUri = new URI( server.restApiUri().toString() + "node/" + new GraphDbHelper( server.database() ).createNode() );
    }

    @Test
    public void shouldGet200WhenRetrievingNode() throws Exception
    {
        ClientResponse response = retrieveNodeFromService( nodeUri.toString() );
        assertEquals( 200, response.getStatus() );
    }

    @Test
    public void shouldGetContentLengthHeaderWhenRetrievingNode() throws Exception
    {
        ClientResponse response = retrieveNodeFromService( nodeUri.toString() );
        assertNotNull( response.getHeaders().get( "Content-Length" ) );
    }

    @Test
    public void shouldHaveJsonMediaTypeOnResponse()
    {
        ClientResponse response = retrieveNodeFromService( nodeUri.toString() );
        assertEquals( MediaType.APPLICATION_JSON_TYPE, response.getType() );
    }

    @Test
    public void shouldHaveJsonDataInResponse() throws Exception
    {
        ClientResponse response = retrieveNodeFromService( nodeUri.toString() );

        Map<String, Object> map = JsonHelper.jsonToMap( response.getEntity( String.class ) );
        assertTrue( map.containsKey( "self" ) );
    }

    @Test
    public void shouldGet404WhenRetrievingNonExistentNode() throws Exception
    {
        ClientResponse response = retrieveNodeFromService( nodeUri + "00000" );
        assertEquals( 404, response.getStatus() );
    }

    private ClientResponse retrieveNodeFromService( String uri )
    {
        WebResource resource = Client.create().resource( uri );
        ClientResponse response = resource.accept( MediaType.APPLICATION_JSON ).get( ClientResponse.class );
        return response;
    }
}
