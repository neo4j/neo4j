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
package org.neo4j.server.rest.transactional.integration;

import org.codehaus.jackson.JsonNode;
import org.junit.After;
import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.spatial.CRS;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.store.GeometryType;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.server.HTTP;
import org.neo4j.values.storable.CoordinateReferenceSystem;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;
import static org.neo4j.values.storable.Values.pointValue;

public class PointTypeIT extends AbstractRestFunctionalTestBase
{
    @After
    public void tearDown()
    {
        // empty the database
        graphdb().execute( "MATCH (n) DETACH DELETE n" );
    }

    @Test
    public void shouldWorkWithPoint2DArrays() throws Exception
    {
        HTTP.Response response =
                runQuery( "create (:Node {points: [point({x:1, y:1}), point({x:2, y:2}), point({x: 3.0, y: 3.0})]})" );

        assertEquals( 200, response.status() );
        assertNoErrors( response );

        GraphDatabaseFacade db = server().getDatabase().getGraph();
        try ( Transaction tx = db.beginTx() )
        {
            for ( Node node : db.getAllNodes() )
            {
                if ( node.hasLabel( label( "Node" ) ) && node.hasProperty( "points" ) )
                {
                    Point[] points = (Point[]) node.getProperty( "points" );

                    verifyPoint( points[0], Cartesian, 1.0, 1.0 );
                    verifyPoint( points[1], Cartesian, 2.0, 2.0 );
                    verifyPoint( points[2], Cartesian, 3.0, 3.0 );
                }
            }
            tx.success();
        }
    }

    @Test
    public void shouldReturnPoint2DWithXAndY() throws Exception
    {
        testPoint( "RETURN point({x: 42.05, y: 90.99})", new double[]{42.05, 90.99}, Cartesian, "point" );
    }

    @Test
    public void shouldReturnPoint2DWithLatitudeAndLongitude() throws Exception
    {
        testPoint( "RETURN point({longitude: 56.7, latitude: 12.78})", new double[]{56.7, 12.78}, WGS84, "point" );
    }

    @Test
    public void shouldHandlePointArrays() throws Exception
    {
        //Given
        GraphDatabaseFacade db = server().getDatabase().getGraph();
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( label( "N" ) );
            node.setProperty( "coordinates", new Point[]{pointValue( WGS84, 30.655691, 104.081602 )} );
            node.setProperty( "location", "Shanghai" );
            node.setProperty( "type", "gps" );
            tx.success();
        }

        //When
        HTTP.Response response = runQuery( "MATCH (n:N) RETURN n" );

        assertEquals( 200, response.status() );
        assertNoErrors( response );

        //Then
        JsonNode row = response.get( "results" ).get( 0 ).get( "data" ).get( 0 ).get( "row" ).get( 0 )
                .get( "coordinates" ).get( 0 );
        assertGeometryTypeEqual( GeometryType.GEOMETRY_POINT, row );
        assertCoordinatesEqual( new double[]{30.655691, 104.081602}, row );
        assertCrsEqual( WGS84, row );
    }

    @Test
    public void shouldHandlePointsUsingRestResultDataContent() throws Exception
    {
        //Given
        GraphDatabaseFacade db = server().getDatabase().getGraph();
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( label( "N" ) );
            node.setProperty( "coordinates", pointValue( WGS84, 30.655691, 104.081602 ) );
            node.setProperty( "location", "Shanghai" );
            node.setProperty( "type", "gps" );
            tx.success();
        }

        //When
        HTTP.Response response = runQuery( "MATCH (n:N) RETURN n", "rest" );

        assertEquals( 200, response.status() );
        assertNoErrors( response );

        //Then
        JsonNode row = response.get( "results" ).get( 0 ).get( "data" ).get( 0 ).get( "rest" )
                .get( 0 ).get( "data" ).get( "coordinates" );
        assertGeometryTypeEqual( GeometryType.GEOMETRY_POINT, row );
        assertCoordinatesEqual( new double[]{30.655691, 104.081602}, row );
        assertCrsEqual( WGS84, row );
    }

    @Test
    public void shouldHandlePointsUsingGraphResultDataContent() throws Exception
    {
        //Given
        GraphDatabaseFacade db = server().getDatabase().getGraph();
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( label( "N" ) );
            node.setProperty( "coordinates", pointValue( WGS84, 30.655691, 104.081602 ) );
            tx.success();
        }

        //When
        HTTP.Response response = runQuery( "MATCH (n:N) RETURN n", "graph" );

        assertEquals( 200, response.status() );
        assertNoErrors( response );

        //Then
        JsonNode row = response.get( "results" ).get( 0 ).get( "data" ).get( 0 ).get( "graph" )
                .get("nodes").get( 0 ).get( "properties" ).get( "coordinates" );
        assertGeometryTypeEqual( GeometryType.GEOMETRY_POINT, row );
        assertCoordinatesEqual( new double[]{30.655691, 104.081602}, row );
        assertCrsEqual( WGS84, row );
    }

    @Test
    public void shouldHandleArrayOfPointsUsingRestResultDataContent() throws Exception
    {
        //Given
        GraphDatabaseFacade db = server().getDatabase().getGraph();
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( label( "N" ) );
            node.setProperty( "coordinates", new Point[]{pointValue( WGS84, 30.655691, 104.081602 )});
            tx.success();
        }

        //When
        HTTP.Response response = runQuery( "MATCH (n:N) RETURN n", "rest" );

        assertEquals( 200, response.status() );
        assertNoErrors( response );

        //Then
        JsonNode row = response.get( "results" ).get( 0 ).get( "data" ).get( 0 ).get( "rest" )
                .get( 0 ).get( "data" ).get( "coordinates" ).get( 0 );
        assertGeometryTypeEqual( GeometryType.GEOMETRY_POINT, row );
        assertCoordinatesEqual( new double[]{30.655691, 104.081602}, row );
        assertCrsEqual( WGS84, row );
    }

    @Test
    public void shouldHandleArrayOfPointsUsingGraphResultDataContent() throws Exception
    {
        //Given
        GraphDatabaseFacade db = server().getDatabase().getGraph();
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( label( "N" ) );
            node.setProperty( "coordinates", new Point[]{pointValue( WGS84, 30.655691, 104.081602 )});
            tx.success();
        }

        //When
        HTTP.Response response = runQuery( "MATCH (n:N) RETURN n", "graph" );

        assertEquals( 200, response.status() );
        assertNoErrors( response );

        //Then
        JsonNode row = response.get( "results" ).get( 0 ).get( "data" ).get( 0 ).get( "graph" )
                .get("nodes").get( 0 ).get( "properties" ).get( "coordinates" ).get( 0 );
        assertGeometryTypeEqual( GeometryType.GEOMETRY_POINT, row );
        assertCoordinatesEqual( new double[]{30.655691, 104.081602}, row );
        assertCrsEqual( WGS84, row );
    }

    private static void testPoint( String query, double[] expectedCoordinate, CoordinateReferenceSystem expectedCrs, String expectedType ) throws Exception
    {
        HTTP.Response response = runQuery( query );

        assertEquals( 200, response.status() );
        assertNoErrors( response );

        JsonNode element = extractSingleElement( response );
        assertGeometryTypeEqual( GeometryType.GEOMETRY_POINT, element );
        assertCoordinatesEqual( expectedCoordinate, element );
        assertCrsEqual( expectedCrs, element );

        assertTypeEqual( expectedType, response );
    }

    private static void assertTypeEqual( String expectedType, HTTP.Response response ) throws JsonParseException
    {
        JsonNode data = response.get( "results" ).get( 0 ).get( "data" );
        JsonNode meta = data.get( 0 ).get( "meta" );
        assertEquals( 1, meta.size() );
        assertEquals( expectedType, meta.get( 0 ).get( "type" ).asText() );
    }

    private static JsonNode extractSingleElement( HTTP.Response response ) throws JsonParseException
    {
        JsonNode data = response.get( "results" ).get( 0 ).get( "data" );
        assertEquals( 1, data.size() );
        JsonNode row = data.get( 0 ).get( "row" );
        assertEquals( 1, row.size() );
        return row.get( 0 );
    }

    private static void assertGeometryTypeEqual( GeometryType expected, JsonNode element )
    {
        assertEquals( expected.getName(), element.get( "type" ).asText() );
    }

    private static void assertCoordinatesEqual( double[] expected, JsonNode element )
    {
        assertArrayEquals( expected, coordinatesAsArray( element ), 0.000001 );
    }

    private static double[] coordinatesAsArray( JsonNode element )
    {
        return Iterables.stream( element.get( "coordinates" ) )
                .mapToDouble( JsonNode::asDouble )
                .toArray();
    }

    private static void assertCrsEqual( CoordinateReferenceSystem crs, JsonNode element )
    {
        assertEquals( crs.getName(), element.get( "crs" ).get( "name" ).asText() );
    }

    private static void verifyPoint( Point point, CRS expectedCRS, Double... expectedCoordinate )
    {
        assertEquals( expectedCRS.getCode(), point.getCRS().getCode() );
        assertEquals( asList( expectedCoordinate ), point.getCoordinate().getCoordinate() );
    }
}
