/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.junit.Test;

import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.spatial.CRS;
import org.neo4j.graphdb.spatial.Coordinate;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

public class GraphDatabaseServiceExecuteTest
{
    @Test
    public void shouldExecuteCypher() throws Exception
    {
        // given
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        final long before, after;
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
        // given
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();

        // when
        Result execute = graphDb.execute( "RETURN point({longitude: 144.317718, latitude: -37.031738}) AS p" );

        // then
        Object obj = execute.next().get( "p" );
        assertThat( obj, Matchers.instanceOf(Point.class));

        Point point = (Point) obj;
        assertThat( point.getCoordinate(), equalTo(new Coordinate( 144.317718, -37.031738 )));

        CRS crs = point.getCRS();
        assertThat( crs.getCode(), equalTo(4326));
        assertThat( crs.getType(), equalTo("WGS-84"));
        assertThat( crs.getHref(), equalTo("http://spatialreference.org/ref/epsg/4326/"));
    }

    @Test
    public void shouldNotReturnInternalCartesianPointType() throws Exception
    {
        // given
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();

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
        // given
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();

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
        // given
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabase();

        // when
        Result execute = graphDb.execute( "RETURN {p: point({longitude: 144.317718, latitude: -37.031738})} AS m" );

        // then
        Map<String,Object> points = (Map<String, Object>)execute.next().get( "m" );
        assertThat( points.get("p"), Matchers.instanceOf(Point.class));
    }
}
