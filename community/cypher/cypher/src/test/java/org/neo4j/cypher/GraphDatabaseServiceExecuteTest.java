/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher;

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.spatial.CRS;
import org.neo4j.graphdb.spatial.Coordinate;
import org.neo4j.graphdb.spatial.Geometry;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.MapUtil.map;

public class GraphDatabaseServiceExecuteTest
{

    @Rule
    public final DatabaseRule graphDb = new ImpermanentDatabaseRule();

    @Test
    public void shouldExecuteCypher() throws Exception
    {
        // given
        final long before;
        final long after;
        try ( Transaction tx = graphDb.beginTx() )
        {
            before = Iterables.count( graphDb.getAllNodes() );
            tx.success();
        }

        // when
        graphDb.execute( "CREATE (n:Foo{bar:\"baz\"})" );

        // then
        try ( Transaction tx = graphDb.beginTx() )
        {
            after = Iterables.count( graphDb.getAllNodes() );
            tx.success();
        }
        assertEquals( before + 1, after );
    }

    @Test
    public void shouldNotReturnInternalGeographicPointType() throws Exception
    {
        // when
        Result execute = graphDb.execute( "RETURN point({longitude: 144.317718, latitude: -37.031738}) AS p" );

        // then
        Object obj = execute.next().get( "p" );
        assertThat( obj, Matchers.instanceOf(Point.class));

        Point point = (Point) obj;
        assertThat( point.getCoordinate().getCoordinate().get(0), equalTo( 144.317718 ));
        assertThat( point.getCoordinate().getCoordinate().get(1), equalTo( -37.031738  ));

        CRS crs = point.getCRS();
        assertThat( crs.getCode(), equalTo(4326));
        assertThat( crs.getType(), equalTo("WGS-84"));
        assertThat( crs.getHref(), equalTo("http://spatialreference.org/ref/epsg/4326/"));
    }

    @Test
    public void shouldNotReturnInternalCartesianPointType() throws Exception
    {
        // when
        Result execute = graphDb.execute( "RETURN point({x: 13.37, y: 13.37, crs:'cartesian'}) AS p" );

        // then
        Object obj = execute.next().get( "p" );
        assertThat( obj, Matchers.instanceOf(Point.class));

        Point point = (Point) obj;
        assertThat( point.getCoordinate(), equalTo(new Coordinate( 13.37, 13.37 )));

        CRS crs = point.getCRS();
        assertThat( crs.getCode(), equalTo(7203));
        assertThat( crs.getType(), equalTo("cartesian"));
        assertThat( crs.getHref(), equalTo("http://spatialreference.org/ref/sr-org/7203/"));
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldNotReturnInternalPointWhenInArray() throws Exception
    {
        // when
        Result execute = graphDb.execute( "RETURN [point({longitude: 144.317718, latitude: -37.031738})] AS ps" );

        // then
        List<Point> points = (List<Point>)execute.next().get( "ps" );
        assertThat( points.get(0), Matchers.instanceOf(Point.class));
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldNotReturnInternalPointWhenInMap() throws Exception
    {
        // when
        Result execute = graphDb.execute( "RETURN {p: point({longitude: 144.317718, latitude: -37.031738})} AS m" );

        // then
        Map<String,Object> points = (Map<String, Object>)execute.next().get( "m" );
        assertThat( points.get("p"), Matchers.instanceOf(Point.class));
    }

    @Test
    public void shouldBeAbleToUseResultingPointFromOneQueryAsParameterToNext() throws Exception
    {
        // given a point create by one cypher query
        Result execute = graphDb.execute( "RETURN point({longitude: 144.317718, latitude: -37.031738}) AS p" );
        Point point = (Point) execute.next().get( "p" );

        // when passing as params to a distance function
        Result result = graphDb.execute(
                "RETURN distance(point({longitude: 144.317718, latitude: -37.031738}),{previous}) AS dist",
                map( "previous", point ) );

        // then
        Double dist = (Double) result.next().get( "dist" );
        assertThat( dist, equalTo( 0.0 ) );
    }

    @Test
    public void shouldBeAbleToUseExternalPointAsParameterToQuery() throws Exception
    {
        // given a point created from public interface
        Point point = makeFakePoint( 144.317718, -37.031738, makeWGS84() );

        // when passing as params to a distance function
        Result result = graphDb.execute(
                "RETURN distance(point({longitude: 144.317718, latitude: -37.031738}),{previous}) AS dist",
                map( "previous", point ) );

        // then
        Double dist = (Double) result.next().get( "dist" );
        assertThat( dist, equalTo( 0.0 ) );
    }

    @Test
    public void shouldBeAbleToUseExternalGeometryAsParameterToQuery() throws Exception
    {
        // given a point created from public interface
        Geometry geometry = makeFakePointAsGeometry( 144.317718, -37.031738, makeWGS84() );

        // when passing as params to a distance function
        Result result = graphDb.execute(
                "RETURN distance(point({longitude: 144.317718, latitude: -37.031738}),{previous}) AS dist",
                map( "previous", geometry ) );

        // then
        Double dist = (Double) result.next().get( "dist" );
        assertThat( dist, equalTo( 0.0 ) );
    }

    @Test
    public void shouldBeAbleToUseExternalPointArrayAsParameterToQuery() throws Exception
    {
        // given a point created from public interface
        Point point = makeFakePoint( 144.317718, -37.031738, makeWGS84() );
        Point[] points = new Point[]{point, point};

        // when passing as params to a distance function
        Result result = graphDb.execute(
                "RETURN distance({points}[0],{points}[1]) AS dist",
                map( "points", points ) );

        // then
        Double dist = (Double) result.next().get( "dist" );
        assertThat( dist, equalTo( 0.0 ) );
    }

    @Test
    public void shouldBeAbleToUseResultsOfPointProcedureAsInputToDistanceFunction() throws Exception
    {
        // given procedure that produces a point
        Procedures procedures =
                graphDb.getDependencyResolver().resolveDependency( Procedures.class );
        procedures.registerProcedure( PointProcs.class );

        // when calling procedure that produces a point
        Result result = graphDb.execute(
                "CALL spatial.point(144.317718, -37.031738) YIELD point " +
                "RETURN distance(point({longitude: 144.317718, latitude: -37.031738}), point) AS dist" );

        // then
        Double dist = (Double) result.next().get( "dist" );
        assertThat( dist, equalTo( 0.0 ) );

    }

    @Test
    public void shouldBeAbleToUseResultsOfPointGeometryProcedureAsInputToDistanceFunction() throws Exception
    {
        // given procedure that produces a point
        Procedures procedures =
                graphDb.getDependencyResolver().resolveDependency( Procedures.class );
        procedures.registerProcedure( PointProcs.class );

        // when calling procedure that produces a point
        Result result = graphDb.execute(
                "CALL spatial.pointGeometry(144.317718, -37.031738) YIELD geometry " +
                "RETURN distance(point({longitude: 144.317718, latitude: -37.031738}), geometry) AS dist" );

        // then
        Object dist1 = result.next().get( "dist" );
        Double dist = (Double) dist1;
        assertThat( dist, equalTo( 0.0 ) );

    }

    private static Point makeFakePoint( double x, double y, final CRS crs )
    {
        final Coordinate coord = new Coordinate( x, y );
        return new Point()
        {

            @Override
            public String getGeometryType()
            {
                return "Point";
            }

            @Override
            public List<Coordinate> getCoordinates()
            {
                return Arrays.asList( coord );
            }

            @Override
            public CRS getCRS()
            {
                return crs;
            }
        };
    }

    private static Geometry makeFakePointAsGeometry( double x, double y, final CRS crs )
    {
        final Coordinate coord = new Coordinate( x, y );
        return new Geometry()
        {

            @Override
            public String getGeometryType()
            {
                return "Point";
            }

            @Override
            public List<Coordinate> getCoordinates()
            {
                return Arrays.asList( coord );
            }

            @Override
            public CRS getCRS()
            {
                return crs;
            }
        };
    }

    private static CRS makeWGS84()
    {
        // "WGS-84", 4326, "http://spatialreference.org/ref/epsg/4326/"
        return new CRS()
        {
            @Override
            public int getCode()
            {
                return 4326;
            }

            @Override
            public String getType()
            {
                return "WGS-84";
            }

            @Override
            public String getHref()
            {
                return "http://spatialreference.org/ref/epsg/4326/";
            }
        };
    }

    public static class PointProcs
    {
        @Procedure( "spatial.point" )
        public Stream<PointResult> spatialPoint( @Name( "longitude" ) double longitude, @Name( "latitude" ) double latitude )
        {
            Point point = makeFakePoint( longitude, latitude, makeWGS84() );
            return Stream.of( new PointResult( point ) );
        }
        @Procedure( "spatial.pointGeometry" )
        public Stream<GeometryResult> spatialPointGeometry( @Name( "longitude" ) double longitude, @Name( "latitude" ) double latitude )
        {
            Geometry geometry = makeFakePointAsGeometry( longitude, latitude, makeWGS84() );
            return Stream.of( new GeometryResult( geometry ) );
        }
    }

    public static class PointResult
    {
        public Point point;

        public PointResult( Point point )
        {
            this.point = point;
        }
    }

    public static class GeometryResult
    {
        public Geometry geometry;

        public GeometryResult( Geometry geometry )
        {
            this.geometry = geometry;
        }
    }
}
