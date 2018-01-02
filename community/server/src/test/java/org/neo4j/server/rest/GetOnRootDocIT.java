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

import org.junit.Test;

import java.util.Map;

import org.neo4j.kernel.Version;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.rest.RESTDocsGenerator.ResponseEntity;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.repr.StreamingFormat;
import org.neo4j.test.GraphDescription.Graph;
import org.neo4j.test.TestData.Title;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class GetOnRootDocIT extends AbstractRestFunctionalTestBase
{
    @Title("Get service root")
    @Documented( "The service root is your starting point to discover the REST API. It contains the basic starting " +
                 "points for the database, and some version and extension information." )
    @Test
    @Graph("I know you")
    public void assert200OkFromGet() throws Exception
    {
        String body = gen.get().expectedStatus( 200 ).get( getDataUri() ).entity();
        Map<String, Object> map = JsonHelper.jsonToMap( body );
        assertEquals( getDataUri() + "node", map.get( "node" ) );
        assertNotNull( map.get( "node_index" ) );
        assertNotNull( map.get( "relationship_index" ) );
        assertNotNull( map.get( "extensions_info" ) );
        assertNotNull( map.get( "batch" ) );
        assertNotNull( map.get( "cypher" ) );
        assertNotNull( map.get( "indexes" ) );
        assertNotNull( map.get( "constraints" ) );
        assertNotNull( map.get( "node_labels" ) );
        assertEquals( Version.getKernel().getReleaseVersion(), map.get( "neo4j_version" ) );

        // Make sure advertised urls work
        JaxRsResponse response;
        if ( map.get( "reference_node" ) != null )
        {
            response = RestRequest.req().get(
                    (String) map.get( "reference_node" ) );
            assertEquals( 200, response.getStatus() );
            response.close();
        }
        response = RestRequest.req().get( (String) map.get( "node_index" ) );
        assertTrue( response.getStatus() == 200 || response.getStatus() == 204 );
        response.close();

        response = RestRequest.req().get(
                (String) map.get( "relationship_index" ) );
        assertTrue( response.getStatus() == 200 || response.getStatus() == 204 );
        response.close();

        response = RestRequest.req().get( (String) map.get( "extensions_info" ) );
        assertEquals( 200, response.getStatus() );
        response.close();

        response = RestRequest.req().post( (String) map.get( "batch" ), "[]" );
        assertEquals( 200, response.getStatus() );
        response.close();

        response = RestRequest.req().post( (String) map.get( "cypher" ), "{\"query\":\"CREATE (n) RETURN n\"}" );
        assertEquals( 200, response.getStatus() );
        response.close();

        response = RestRequest.req().get( (String) map.get( "indexes" ) );
        assertEquals( 200, response.getStatus() );
        response.close();

        response = RestRequest.req().get( (String) map.get( "constraints" ) );
        assertEquals( 200, response.getStatus() );
        response.close();

        response = RestRequest.req().get( (String) map.get( "node_labels" ) );
        assertEquals( 200, response.getStatus() );
        response.close();
    }

    @Documented( "All responses from the REST API can be transmitted as JSON streams, resulting in\n" +
                 "better performance and lower memory overhead on the server side. To use\n" +
                 "streaming, supply the header `X-Stream: true` with each request." )
    @Test
    public void streaming() throws Exception
    {
        data.get();
        ResponseEntity responseEntity = gen().docHeadingLevel( 2 ).noGraph()
                .withHeader( StreamingFormat.STREAM_HEADER, "true" )
                .expectedType( APPLICATION_JSON_TYPE )
                .expectedStatus( 200 )
                .get( getDataUri() );
        JaxRsResponse response = responseEntity.response();
        // this gets the full media type, including things like
        // ; stream=true at the end
        String foundMediaType = response.getType()
                .toString();
        String expectedMediaType = StreamingFormat.MEDIA_TYPE.toString();
        assertEquals( expectedMediaType, foundMediaType );

        String body = responseEntity.entity();
        Map<String, Object> map = JsonHelper.jsonToMap( body );
        assertEquals( getDataUri() + "node", map.get( "node" ) );
    }
}
