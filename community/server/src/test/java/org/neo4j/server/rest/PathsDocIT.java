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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.Response.Status;

import org.neo4j.graphdb.Node;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphDescription.Graph;
import org.neo4j.test.GraphDescription.NODE;
import org.neo4j.test.GraphDescription.PROP;
import org.neo4j.test.GraphDescription.REL;
import org.neo4j.test.TestData.Title;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PathsDocIT extends AbstractRestFunctionalTestBase
{
//     Layout
//
//     (e)----------------
//      |                 |
//     (d)-------------   |
//      |               \/
//     (a)-(c)-(b)-(f)-(g)
//          |\     /   /
//          | ----    /
//           --------
//
    @Test
    @Graph( value = { "a to c", "a to d", "c to b", "d to e", "b to f", "c to f", "f to g", "d to g", "e to g",
    "c to g" } )
    @Title( "Find all shortest paths" )
    @Documented( "The +shortestPath+ algorithm can find multiple paths between the same nodes, like in this example." )
    public void shouldBeAbleToFindAllShortestPaths() throws JsonParseException
    {

        // Get all shortest paths
        long a = nodeId( data.get(), "a" );
        long g = nodeId( data.get(), "g" );
        String response = gen()
        .expectedStatus( Status.OK.getStatusCode() )
        .payload( getAllShortestPathPayLoad( g ) )
        .post( "http://localhost:7474/db/data/node/" + a + "/paths" )
        .entity();
        Collection<?> result = (Collection<?>) JsonHelper.readJson( response );
        assertEquals( 2, result.size() );
        for ( Object representation : result )
        {
            Map<?, ?> path = (Map<?, ?>) representation;

            assertThatPathStartsWith( path, a );
            assertThatPathEndsWith( path, g );
            assertThatPathHasLength( path, 2 );
        }
    }

//      Layout
//
//      (e)----------------
//       |                 |
//      (d)-------------   |
//       |               \/
//      (a)-(c)-(b)-(f)-(g)
//           |\     /   /
//           | ----    /
//            --------
    @Title( "Find one of the shortest paths" )
    @Test
    @Graph( value = { "a to c", "a to d", "c to b", "d to e", "b to f", "c to f", "f to g", "d to g", "e to g",
    "c to g" } )
    @Documented( "If no path algorithm is specified, a +shortestPath+ algorithm with a max\n" +
                 "depth of 1 will be chosen. In this example, the +max_depth+ is set to +3+\n" +
                 "in order to find the shortest path between a maximum of 3 linked nodes." )
    public void shouldBeAbleToFetchSingleShortestPath() throws JsonParseException
    {
        long a = nodeId( data.get(), "a" );
        long g = nodeId( data.get(), "g" );
        String response = gen()
        .expectedStatus( Status.OK.getStatusCode() )
        .payload( getAllShortestPathPayLoad( g ) )
        .post( "http://localhost:7474/db/data/node/" + a + "/path" )
        .entity();
        // Get single shortest path

        Map<?, ?> path = JsonHelper.jsonToMap( response );

        assertThatPathStartsWith( path, a );
        assertThatPathEndsWith( path, g );
        assertThatPathHasLength( path, 2 );
    }

    private void assertThatPathStartsWith( final Map<?, ?> path, final long start )
    {
        assertTrue( "Path should start with " + start + "\nBut it was " + path, path.get( "start" )
                .toString()
                .endsWith( "/node/" + start ) );
    }

    private void assertThatPathEndsWith( final Map<?, ?> path, final long start )
    {
        assertTrue( "Path should end with " + start + "\nBut it was " + path, path.get( "end" )
                .toString()
                .endsWith( "/node/" + start ) );
    }

    private void assertThatPathHasLength( final Map<?, ?> path, final int length )
    {
        Object actual = path.get( "length" );

        assertEquals( "Expected path to have a length of " + length + "\nBut it was " + actual, length, actual );
    }

//      Layout
//
//         1.5------(b)--------0.5
//        /                      \
//      (a)-0.5-(c)-0.5-(d)-0.5-(e)
//        \                     /
//        0.5-------(f)------1.2
//
    @Test
    @Graph( nodes = { @NODE( name = "a", setNameProperty = true ), @NODE( name = "b", setNameProperty = true ),
            @NODE( name = "c", setNameProperty = true ), @NODE( name = "d", setNameProperty = true ),
            @NODE( name = "e", setNameProperty = true ), @NODE( name = "f", setNameProperty = true ) }, relationships = {
            @REL( start = "a", end = "b", type = "to", properties = { @PROP( key = "cost", value = "1.5", type = GraphDescription.PropType.DOUBLE ) } ),
            @REL( start = "a", end = "c", type = "to", properties = { @PROP( key = "cost", value = "0.5", type = GraphDescription.PropType.DOUBLE ) } ),
            @REL( start = "a", end = "f", type = "to", properties = { @PROP( key = "cost", value = "0.5", type = GraphDescription.PropType.DOUBLE ) } ),
            @REL( start = "c", end = "d", type = "to", properties = { @PROP( key = "cost", value = "0.5", type = GraphDescription.PropType.DOUBLE ) } ),
            @REL( start = "d", end = "e", type = "to", properties = { @PROP( key = "cost", value = "0.5", type = GraphDescription.PropType.DOUBLE ) } ),
            @REL( start = "b", end = "e", type = "to", properties = { @PROP( key = "cost", value = "0.5", type = GraphDescription.PropType.DOUBLE ) } ),
            @REL( start = "f", end = "e", type = "to", properties = { @PROP( key = "cost", value = "1.2", type = GraphDescription.PropType.DOUBLE ) } ) } )
    @Title( "Execute a Dijkstra algorithm and get a single path" )
    @Documented( "This example is running a Dijkstra algorithm over a graph with different\n" +
                 "cost properties on different relationships. Note that the request URI\n" +
                 "ends with +/path+ which means a single path is what we want here." )
    public void shouldGetCorrectDijkstraPathWithWeights() throws Exception
    {
        // Get cheapest paths using Dijkstra
        long a = nodeId( data.get(), "a" );
        long e = nodeId( data.get(), "e" );
        String response = gen().expectedStatus( Status.OK.getStatusCode() )
                .payload( getAllPathsUsingDijkstraPayLoad( e, false ) )
                .post( "http://localhost:7474/db/data/node/" + a + "/path" )
                .entity();
        //
        Map<?, ?> path = JsonHelper.jsonToMap( response );
        assertThatPathStartsWith( path, a );
        assertThatPathEndsWith( path, e );
        assertThatPathHasLength( path, 3 );
        assertEquals( 1.5, path.get( "weight" ) );
    }

//      Layout
//
//         1.5------(b)--------0.5
//        /                      \
//      (a)-0.5-(c)-0.5-(d)-0.5-(e)
//        \                     /
//        0.5-------(f)------1.0
//
    @Test
    @Graph( nodes = { @NODE( name = "a", setNameProperty = true ), @NODE( name = "b", setNameProperty = true ),
            @NODE( name = "c", setNameProperty = true ), @NODE( name = "d", setNameProperty = true ),
            @NODE( name = "e", setNameProperty = true ), @NODE( name = "f", setNameProperty = true ) }, relationships = {
            @REL( start = "a", end = "b", type = "to", properties = { @PROP( key = "cost", value = "1.5", type = GraphDescription.PropType.DOUBLE ) } ),
            @REL( start = "a", end = "c", type = "to", properties = { @PROP( key = "cost", value = "0.5", type = GraphDescription.PropType.DOUBLE ) } ),
            @REL( start = "a", end = "f", type = "to", properties = { @PROP( key = "cost", value = "0.5", type = GraphDescription.PropType.DOUBLE ) } ),
            @REL( start = "c", end = "d", type = "to", properties = { @PROP( key = "cost", value = "0.5", type = GraphDescription.PropType.DOUBLE ) } ),
            @REL( start = "d", end = "e", type = "to", properties = { @PROP( key = "cost", value = "0.5", type = GraphDescription.PropType.DOUBLE ) } ),
            @REL( start = "b", end = "e", type = "to", properties = { @PROP( key = "cost", value = "0.5", type = GraphDescription.PropType.DOUBLE ) } ),
            @REL( start = "f", end = "e", type = "to", properties = { @PROP( key = "cost", value = "1.0", type = GraphDescription.PropType.DOUBLE ) } ) } )
    @Title( "Execute a Dijkstra algorithm and get multiple paths" )
    @Documented( "This example is running a Dijkstra algorithm over a graph with different\n" +
                 "cost properties on different relationships. Note that the request URI\n" +
                 "ends with +/paths+ which means we want multiple paths returned, in case\n" +
                 "they exist." )
    public void shouldGetCorrectDijkstraPathsWithWeights() throws Exception
    {
        // Get cheapest paths using Dijkstra
        long a = nodeId( data.get(), "a" );
        long e = nodeId( data.get(), "e" );
        String response = gen().expectedStatus( Status.OK.getStatusCode() )
                .payload( getAllPathsUsingDijkstraPayLoad( e, false ) )
                .post( "http://localhost:7474/db/data/node/" + a + "/paths" )
                .entity();
        //
        List<Map<String, Object>> list = JsonHelper.jsonToList( response );
        assertEquals( 2, list.size() );
        Map<String, Object> firstPath = list.get( 0 );
        Map<String, Object> secondPath = list.get( 1 );
        System.out.println( firstPath );
        System.out.println( secondPath );
        assertThatPathStartsWith( firstPath, a );
        assertThatPathStartsWith( secondPath, a );
        assertThatPathEndsWith( firstPath, e );
        assertThatPathEndsWith( secondPath, e );
        assertEquals( 1.5, firstPath.get( "weight" ) );
        assertEquals( 1.5, secondPath.get( "weight" ) );
        // 5 = 3 + 2
        assertEquals( 5, (Integer) firstPath.get( "length" ) + (Integer) secondPath.get( "length" ) );
        assertEquals( 1, Math.abs( (Integer) firstPath.get( "length" ) - (Integer) secondPath.get( "length" ) ) );
    }

//      Layout
//
//         1------(b)-----1
//        /                \
//      (a)-1-(c)-1-(d)-1-(e)
//        \                /
//         1------(f)-----1
//
    @Test
    @Graph( nodes = { @NODE( name = "a", setNameProperty = true ),
            @NODE( name = "b", setNameProperty = true ), @NODE( name = "c", setNameProperty = true ),
            @NODE( name = "d", setNameProperty = true ), @NODE( name = "e", setNameProperty = true ),
            @NODE( name = "f", setNameProperty = true ) }, relationships = {
            @REL( start = "a", end = "b", type = "to", properties = { @PROP( key = "cost", value = "1", type = GraphDescription.PropType.INTEGER ) } ),
            @REL( start = "a", end = "c", type = "to", properties = { @PROP( key = "cost", value = "1", type = GraphDescription.PropType.INTEGER ) } ),
            @REL( start = "a", end = "f", type = "to", properties = { @PROP( key = "cost", value = "1", type = GraphDescription.PropType.INTEGER ) } ),
            @REL( start = "c", end = "d", type = "to", properties = { @PROP( key = "cost", value = "1", type = GraphDescription.PropType.INTEGER ) } ),
            @REL( start = "d", end = "e", type = "to", properties = { @PROP( key = "cost", value = "1", type = GraphDescription.PropType.INTEGER ) } ),
            @REL( start = "b", end = "e", type = "to", properties = { @PROP( key = "cost", value = "1", type = GraphDescription.PropType.INTEGER ) } ),
            @REL( start = "f", end = "e", type = "to", properties = { @PROP( key = "cost", value = "1", type = GraphDescription.PropType.INTEGER ) } ) } )
    @Title( "Execute a Dijkstra algorithm with equal weights on relationships" )
    @Documented( "The following is executing a Dijkstra search on a graph with equal\n" +
                 "weights on all relationships. This example is included to show the\n" +
                 "difference when the same graph structure is used, but the path weight is\n" +
                 "equal to the number of hops." )
    public void shouldGetCorrectDijkstraPathsWithEqualWeightsWithDefaultCost() throws Exception
    {
        // Get cheapest path using Dijkstra
        long a = nodeId( data.get(), "a" );
        long e = nodeId( data.get(), "e" );
        String response = gen()
                .expectedStatus( Status.OK.getStatusCode() )
                .payload( getAllPathsUsingDijkstraPayLoad( e, false ) )
                .post( "http://localhost:7474/db/data/node/" + a + "/path" )
                .entity();

        Map<?, ?> path = JsonHelper.jsonToMap( response );
        assertThatPathStartsWith( path, a );
        assertThatPathEndsWith( path, e );
        assertThatPathHasLength( path, 2 );
        assertEquals( 2.0, path.get( "weight" ) );
    }

//      Layout
//
//      (e)----------------
//       |                 |
//      (d)-------------   |
//       |               \/
//      (a)-(c)-(b)-(f)-(g)
//           |\     /   /
//           | ----    /
//            --------
    @Test
    @Graph( value = { "a to c", "a to d", "c to b", "d to e", "b to f", "c to f", "f to g", "d to g", "e to g",
    "c to g" } )
    public void shouldReturn404WhenFailingToFindASinglePath() throws JsonParseException
    {
        long a = nodeId( data.get(), "a" );
        long g = nodeId( data.get(), "g" );
        String noHitsJson = "{\"to\":\""
            + nodeUri( g )
            + "\", \"max_depth\":1, \"relationships\":{\"type\":\"dummy\", \"direction\":\"in\"}, \"algorithm\":\"shortestPath\"}";
        String entity = gen()
        .expectedStatus( Status.NOT_FOUND.getStatusCode() )
        .payload( noHitsJson )
        .post( "http://localhost:7474/db/data/node/" + a + "/path" )
        .entity();
        System.out.println( entity );
    }

    private long nodeId( final Map<String, Node> map, final String string )
    {
        return map.get( string )
        .getId();
    }

    private String nodeUri( final long l )
    {
        return NODES + l;
    }

    private String getAllShortestPathPayLoad( final long to )
    {
        String json = "{\"to\":\""
            + nodeUri( to )
            + "\", \"max_depth\":3, \"relationships\":{\"type\":\"to\", \"direction\":\"out\"}, \"algorithm\":\"shortestPath\"}";
        return json;
    }

    //
    private String getAllPathsUsingDijkstraPayLoad( final long to, final boolean includeDefaultCost )
    {
        String json = "{\"to\":\"" + nodeUri( to ) + "\"" + ", \"cost_property\":\"cost\""
        + ( includeDefaultCost ? ", \"default_cost\":1" : "" )
        + ", \"relationships\":{\"type\":\"to\", \"direction\":\"out\"}, \"algorithm\":\"dijkstra\"}";
        return json;
    }

}
