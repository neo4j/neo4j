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

import java.util.Collection;
import java.util.Map;

import javax.ws.rs.core.Response.Status;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.rest.web.PropertyValueException;
import org.neo4j.test.GraphDescription.Graph;
import org.neo4j.test.TestData.Title;

public class PathsFunctionalTest extends AbstractRestFunctionalTestBase
{
    private static final String NODES = "http://localhost:7474/db/data/";

    
    /**
     * The +shortestPath+ algorithm can find multiple paths between
     * the same nodes, like in this example.
     */
    @Test
    @Graph( value = { "a to c", "a to d", "c to b", "d to e", "b to f",
            "c to f", "f to g", "d to g", "e to g", "c to g" } )
    @Documented
    @Title( "Find all shortest paths" )
    public void shouldBeAbleToFindAllShortestPaths()
            throws PropertyValueException
    {

        // Get all shortest paths
        long a = nodeId( data.get(), "a" );
        long g = nodeId( data.get(), "g" );
        String response = gen.get().expectedStatus( Status.OK.getStatusCode() ).payload(
                getAllShortestPathPayLoad( g ) ).post(
                "http://localhost:7474/db/data/node/" + a + "/paths" ).entity();
        Collection<?> result = (Collection<?>) JsonHelper.jsonToSingleValue( response );
        assertEquals( 2, result.size() );
        for ( Object representation : result )
        {
            Map<?, ?> path = (Map<?, ?>) representation;

            assertThatPathStartsWith( path, a );
            assertThatPathEndsWith( path, g );
            assertThatPathHasLength( path, 2 );
        }
    }

    /**
     * If no path algorithm is specified, a +ShortestPath+ algorithm with a max
     * depth of 1 will be chosen. In this example, the +max_depth+ is set to +3+
     * in order to find the shortest path between 3 linked nodes.
     */
    @Title( "Find one of the shortest paths between nodes" )
    @Test
    @Graph( value = { "a to c", "a to d", "c to b", "d to e", "b to f",
            "c to f", "f to g", "d to g", "e to g", "c to g" } )
    @Documented
    public void shouldBeAbleToFetchSingleShortestPath()
            throws JsonParseException
    {
        long a = nodeId( data.get(), "a" );
        long g = nodeId( data.get(), "g" );
        String response = gen.get().expectedStatus( Status.OK.getStatusCode() ).payload(
                getAllShortestPathPayLoad( g ) ).post(
                "http://localhost:7474/db/data/node/" + a + "/path" ).entity();
        // Get single shortest path

        Map<?, ?> path = JsonHelper.jsonToMap( response );

        assertThatPathStartsWith( path, a );
        assertThatPathEndsWith( path, g );
        assertThatPathHasLength( path, 2 );
    }

    private void assertThatPathStartsWith( Map<?, ?> path, long start )
    {
        assertTrue( "Path should start with " + start + "\nBut it was " + path,
                path.get( "start" ).toString().endsWith( "/node/" + start ) );
    }

    private void assertThatPathEndsWith( Map<?, ?> path, long start )
    {
        assertTrue( "Path should end with " + start + "\nBut it was " + path,
                path.get( "end" ).toString().endsWith( "/node/" + start ) );
    }

    private void assertThatPathHasLength( Map<?, ?> path, int length )
    {
        Object actual = path.get( "length" );

        assertEquals( "Expected path to have a length of " + length
                      + "\nBut it was " + actual, length, actual );
    }

    // @Test
    // public void shouldGetCorrectDijkstraPathsWithWeights() throws Exception
    // {
    // long[] nodes = createDijkstraGraph( true );
    //
    // // Get cheapest paths using Dijkstra
    // ClientResponse response = postPathQuery( nodes,
    // getAllPathsUsingDijkstraPayLoad( nodes[1], false ), "/path" );
    // assertEquals( 200, response.getStatus() );
    //
    // Map<?, ?> path = JsonHelper.jsonToMap( response.getEntity( String.class )
    // );
    // assertThatPathStartsWith( path, nodes[0] );
    // assertThatPathEndsWith( path, nodes[1] );
    // assertThatPathHasLength( path, 6 );
    // assertEquals( 6.0, path.get( "weight" ) );
    // response.close();
    // }
    //
    // private ClientResponse postPathQuery( long[] nodes, String query, String
    // functionToCall )
    // {
    // WebResource resource = Client.create().resource(
    // functionalTestHelper.nodeUri( nodes[0] ) + functionToCall );
    // ClientResponse response = resource.type( MediaType.APPLICATION_JSON )
    // .accept( MediaType.APPLICATION_JSON )
    // .entity( query )
    // .post( ClientResponse.class );
    //
    // return response;
    // }

    // @Test
    // @Graph(value = {
    // "a to c",
    // "a to d",
    // "c to b",
    // "d to e",
    // "b to f",
    // "c to f",
    // "f to g",
    // "d to g",
    // "e to g",
    // "c to g"} )
    // public void shouldGetCorrectDijkstraPathsWithWeightsWithDefaultCost()
    // throws Exception
    // {
    // long[] nodes = createDijkstraGraph( false );
    //
    // // Get cheapest paths using Dijkstra
    // ClientResponse response = postPathQuery( nodes,
    // getAllPathsUsingDijkstraPayLoad( nodes[1], true ), "/path" );
    // assertEquals( 200, response.getStatus() );
    //
    // Map<?, ?> path = JsonHelper.jsonToMap( response.getEntity( String.class )
    // );
    // assertThatPathStartsWith( path, nodes[0] );
    // assertThatPathEndsWith( path, nodes[1] );
    // assertThatPathHasLength( path, 6 );
    // assertEquals( 6.0, path.get( "weight" ) );
    // response.close();
    // }

    @Test
    @Graph( value = { "a to c", "a to d", "c to b", "d to e", "b to f",
            "c to f", "f to g", "d to g", "e to g", "c to g" } )
    public void shouldReturn404WhenFailingToFindASinglePath()
            throws JsonParseException
    {
        long a = nodeId( data.get(), "a" );
        long g = nodeId( data.get(), "g" );
        String noHitsJson = "{\"to\":\""
                            + nodeUri( g )
                            + "\", \"max_depth\":1, \"relationships\":{\"type\":\"dummy\", \"direction\":\"in\"}, \"algorithm\":\"shortestPath\"}";
        String entity = gen.get().expectedStatus(
                Status.NOT_FOUND.getStatusCode() ).payload( noHitsJson ).post(
                "http://localhost:7474/db/data/node/" + a + "/path" ).entity();
        System.out.println( entity );
    }

    private long nodeId( Map<String, Node> map, String string )
    {
        return map.get( string ).getId();
    }

    private String nodeUri( long l )
    {
        return NODES + l;
    }

    // private long[] createDijkstraGraph( boolean includeOnes ) throws
    // DatabaseBlockedException
    // {
    // /* Layout:
    // * (y)
    // * ^
    // * [2] _____[1]___
    // * \ v |
    // * (start)--[1]->(a)--[9]-->(x)<- (e)--[2]->(f)
    // * | ^ ^^ \ ^
    // * [1] ---[7][5][4] -[3] [1]
    // * v / | / \ /
    // * (b)--[1]-->(c)--[1]->(d)
    // */
    //
    // Map<String, Object> costOneProperties = includeOnes ? map( "cost",
    // (double) 1 ) : map();
    // long start = helper.createNode();
    // long a = helper.createNode();
    // long b = helper.createNode();
    // long c = helper.createNode();
    // long d = helper.createNode();
    // long e = helper.createNode();
    // long f = helper.createNode();
    // long x = helper.createNode();
    // long y = helper.createNode();
    //
    // createRelationshipWithProperties( start, a, costOneProperties );
    // createRelationshipWithProperties( a, x, map( "cost", (double) 9 ) );
    // createRelationshipWithProperties( a, b, costOneProperties );
    // createRelationshipWithProperties( b, x, map( "cost", (double) 7 ) );
    // createRelationshipWithProperties( b, c, costOneProperties );
    // createRelationshipWithProperties( c, x, map( "cost", (double) 5 ) );
    // createRelationshipWithProperties( c, x, map( "cost", (double) 4 ) );
    // createRelationshipWithProperties( c, d, costOneProperties );
    // createRelationshipWithProperties( d, x, map( "cost", (double) 3 ) );
    // createRelationshipWithProperties( d, e, costOneProperties );
    // createRelationshipWithProperties( e, x, costOneProperties );
    // createRelationshipWithProperties( e, f, map( "cost", (double) 2 ) );
    // createRelationshipWithProperties( x, y, map( "cost", (double) 2 ) );
    // return new long[] { start, x };
    // }
    //
    // private void createRelationshipWithProperties( long start, long end,
    // Map<String, Object> properties )
    // {
    // long rel = helper.createRelationship( "to", start, end );
    // helper.setRelationshipProperties( rel, properties );
    // }
    //
    private String getAllShortestPathPayLoad( long to )
    {
        return "{\"to\":\""
               + nodeUri( to )
               + "\", \"max_depth\":3, \"relationships\":{\"type\":\"to\", \"direction\":\"out\"}, \"algorithm\":\"shortestPath\"}";
    }
    //
    // private String getAllPathsUsingDijkstraPayLoad( long to, boolean
    // includeDefaultCost )
    // {
    // return "{\"to\":\"" + functionalTestHelper.nodeUri( to ) + "\"" +
    // ", \"cost_property\":\"cost\""
    // + ( includeDefaultCost ? ", \"default_cost\":1" : "" )
    // +
    // ", \"relationships\":{\"type\":\"to\", \"direction\":\"out\"}, \"algorithm\":\"dijkstra\"}";
    // }

}
