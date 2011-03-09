/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.ServerBuilder;
import org.neo4j.server.database.DatabaseBlockedException;
import org.neo4j.server.rest.domain.GraphDbHelper;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.web.PropertyValueException;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.MapUtil.map;

public class PathsFunctionalTest
{
    private NeoServerWithEmbeddedWebServer server;
    private FunctionalTestHelper functionalTestHelper;
    private GraphDbHelper helper;

    @Before
    public void setupServer() throws IOException
    {
        server = ServerBuilder.server().withRandomDatabaseDir().withPassingStartupHealthcheck().build();
        server.start();
        functionalTestHelper = new FunctionalTestHelper( server );
        helper = functionalTestHelper.getGraphDbHelper();
    }

    @After
    public void stopServer()
    {
        server.stop();
    }

    @Test
    public void shouldBeAbleToFindAllShortestPaths() throws PropertyValueException
    {
        long[] nodes = createMoreComplexGraph();

        // Get all shortest paths
        ClientResponse response = postPathQuery( nodes, getAllShortestPathPayLoad( nodes[ 1 ] ), "/paths" );
        assertEquals( 200, response.getStatus() );

        String entity = response.getEntity( String.class );
        Collection<?> result = (Collection<?>)JsonHelper.jsonToSingleValue( entity );
        assertEquals( 2, result.size() );
        for ( Object representation : result )
        {
            Map<?, ?> path = (Map<?, ?>)representation;

            assertThatPathStartsWith( path, nodes[ 0 ] );
            assertThatPathEndsWith( path, nodes[ 1 ] );
            assertThatPathHasLength( path, 2 );
        }
    }

    @Test
    public void shouldBeAbleToFetchSingleShortestPath() throws JsonParseException
    {
        long[] nodes = createMoreComplexGraph();

        // Get single shortest path
        ClientResponse response = postPathQuery( nodes, getAllShortestPathPayLoad( nodes[ 1 ] ), "/path" );
        assertEquals( 200, response.getStatus() );

        Map<?, ?> path = JsonHelper.jsonToMap( response.getEntity( String.class ) );

        assertThatPathStartsWith( path, nodes[ 0 ] );
        assertThatPathEndsWith( path, nodes[ 1 ] );
        assertThatPathHasLength( path, 2 );
    }

    private void assertThatPathStartsWith( Map<?, ?> path, long start )
    {
        assertTrue( "Path should start with " + start + "\nBut it was " + path, path.get( "start" ).toString().endsWith( "/node/" + start ) );
    }

    private void assertThatPathEndsWith( Map<?, ?> path, long start )
    {
        assertTrue( "Path should end with " + start + "\nBut it was " + path, path.get( "end" ).toString().endsWith( "/node/" + start ) );
    }

    private void assertThatPathHasLength( Map<?, ?> path, int length )
    {
        Object actual = path.get( "length" );

        assertEquals( "Expected path to have a length of " + length + "\nBut it was " + actual, length, actual );
    }


    @Test
    public void shouldGetCorrectDijkstraPathsWithWeights() throws Exception
    {
        long[] nodes = createDijkstraGraph( true );


//        Get cheapest paths using Dijkstra
        ClientResponse response = postPathQuery( nodes, getAllPathsUsingDijkstraPayLoad( nodes[ 1 ], false ), "/path" );
        assertEquals( 200, response.getStatus() );


        Map<?, ?> path = JsonHelper.jsonToMap( response.getEntity( String.class ) );
        assertThatPathStartsWith( path, nodes[ 0 ] );
        assertThatPathEndsWith( path, nodes[ 1 ] );
        assertThatPathHasLength( path, 6 );
        assertEquals( 6.0, path.get( "weight" ) );
    }

    private ClientResponse postPathQuery( long[] nodes, String query, String functionToCall )
    {
        Client client = Client.create();
        WebResource resource = client.resource( functionalTestHelper.nodeUri( nodes[ 0 ] ) + functionToCall );
        ClientResponse response = resource.type( MediaType.APPLICATION_JSON ).accept( MediaType.APPLICATION_JSON ).entity( query ).post( ClientResponse.class );

        return response;
    }

    @Test
    public void shouldGetCorrectDijkstraPathsWithWeightsWithDefaultCost() throws Exception
    {
        long[] nodes = createDijkstraGraph( false );

        // Get cheapest paths using Dijkstra
        ClientResponse response = postPathQuery( nodes, getAllPathsUsingDijkstraPayLoad( nodes[ 1 ], true ), "/path" );
        assertEquals( 200, response.getStatus() );


        Map<?, ?> path = JsonHelper.jsonToMap( response.getEntity( String.class ) );
        assertThatPathStartsWith( path, nodes[ 0 ] );
        assertThatPathEndsWith( path, nodes[ 1 ] );
        assertThatPathHasLength( path, 6 );
        assertEquals( 6.0, path.get( "weight" ) );
    }

    @Test
    public void shouldReturn404WhenFailingToFindASinglePath() throws JsonParseException
    {
        long[] nodes = createMoreComplexGraph();

        // Get single shortest path and expect no answer (404)
        String noHitsJson = "{\"to\":\"" + functionalTestHelper.nodeUri( nodes[ 1 ] )
                + "\", \"max depth\":3, \"relationships\":{\"type\":\"to\", \"direction\":\"in\"}, \"algorithm\":\"shortestPath\"}";

        ClientResponse response = postPathQuery( nodes, noHitsJson, "/path" );
        assertEquals( 404, response.getStatus() );
    }

    private long[] createMoreComplexGraph() throws DatabaseBlockedException
    {
        //          (a)
        //          / \
        //         v   v
        // (b)<---(c)  (d)-->(e)
        //  \    /  \   /     /
        //   v  v    v v     /
        //    (f)--->(g)<----

        long a = helper.createNode();
        long b = helper.createNode();
        long c = helper.createNode();
        long d = helper.createNode();
        long e = helper.createNode();
        long f = helper.createNode();
        long g = helper.createNode();
        helper.createRelationship( "to", a, c );
        helper.createRelationship( "to", a, d );
        helper.createRelationship( "to", c, b );
        helper.createRelationship( "to", d, e );
        helper.createRelationship( "to", b, f );
        helper.createRelationship( "to", c, f );
        helper.createRelationship( "to", f, g );
        helper.createRelationship( "to", d, g );
        helper.createRelationship( "to", e, g );
        helper.createRelationship( "to", c, g );
        return new long[]{a, g};
    }

    private long[] createDijkstraGraph( boolean includeOnes ) throws DatabaseBlockedException
    {
        /* Layout:
         *                       (y)
         *                        ^
         *                        [2]  _____[1]___
         *                          \ v           |
         * (start)--[1]->(a)--[9]-->(x)<-        (e)--[2]->(f)
         *                |         ^ ^^  \       ^
         *               [1]  ---[7][5][4] -[3]  [1]
         *                v  /       | /      \  /
         *               (b)--[1]-->(c)--[1]->(d)
         */

        Map<String, Object> costOneProperties = includeOnes ? map( "cost", (double)1 ) : map();
        long start = helper.createNode();
        long a = helper.createNode();
        long b = helper.createNode();
        long c = helper.createNode();
        long d = helper.createNode();
        long e = helper.createNode();
        long f = helper.createNode();
        long x = helper.createNode();
        long y = helper.createNode();

        createRelationshipWithProperties( start, a, costOneProperties );
        createRelationshipWithProperties( a, x, map( "cost", (double)9 ) );
        createRelationshipWithProperties( a, b, costOneProperties );
        createRelationshipWithProperties( b, x, map( "cost", (double)7 ) );
        createRelationshipWithProperties( b, c, costOneProperties );
        createRelationshipWithProperties( c, x, map( "cost", (double)5 ) );
        createRelationshipWithProperties( c, x, map( "cost", (double)4 ) );
        createRelationshipWithProperties( c, d, costOneProperties );
        createRelationshipWithProperties( d, x, map( "cost", (double)3 ) );
        createRelationshipWithProperties( d, e, costOneProperties );
        createRelationshipWithProperties( e, x, costOneProperties );
        createRelationshipWithProperties( e, f, map( "cost", (double)2 ) );
        createRelationshipWithProperties( x, y, map( "cost", (double)2 ) );
        return new long[]{start, x};
    }

    private void createRelationshipWithProperties( long start, long end, Map<String, Object> properties )
    {
        long rel = helper.createRelationship( "to", start, end );
        helper.setRelationshipProperties( rel, properties );
    }

    private String getAllShortestPathPayLoad( long to )
    {
        return "{\"to\":\"" + functionalTestHelper.nodeUri( to )
                + "\", \"max depth\":3, \"relationships\":{\"type\":\"to\", \"direction\":\"out\"}, \"algorithm\":\"shortestPath\"}";
    }

    private String getAllPathsUsingDijkstraPayLoad( long to, boolean includeDefaultCost )
    {
        return "{\"to\":\"" + functionalTestHelper.nodeUri( to ) + "\"" +
                ", \"cost property\":\"cost\"" +
                ( includeDefaultCost ? ", \"default cost\":1" : "" ) +
                ", \"relationships\":{\"type\":\"to\", \"direction\":\"out\"}, \"algorithm\":\"dijkstra\"}";
    }


}
