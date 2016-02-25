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

import org.junit.Test;

import java.util.Map;

import org.neo4j.kernel.Version;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.doc.server.rest.RESTDocsGenerator.ResponseEntity;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.repr.StreamingFormat;
import org.neo4j.test.GraphDescription.Graph;
import org.neo4j.test.TestData.Title;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class GetOnRootDocIT extends org.neo4j.doc.server.rest.AbstractRestFunctionalTestBase
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
        org.neo4j.doc.server.rest.JaxRsResponse response;
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
        org.neo4j.doc.server.rest.JaxRsResponse response = responseEntity.response();
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
