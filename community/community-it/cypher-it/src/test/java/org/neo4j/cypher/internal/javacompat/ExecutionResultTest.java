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
package org.neo4j.cypher.internal.javacompat;

import org.junit.jupiter.api.Test;

import org.neo4j.exceptions.ArithmeticException;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.IsNull.nullValue;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

@ImpermanentDbmsExtension
class ExecutionResultTest
{
    @Inject
    private GraphDatabaseAPI db;

    @Test
    void shouldThrowAppropriateException()
    {
        try
        {
            try ( Transaction transaction = db.beginTx() )
            {
                transaction.execute( "RETURN rand()/0" ).next();
            }
        }
        catch ( QueryExecutionException ex )
        {
            assertThat( ex.getCause(), instanceOf( QueryExecutionKernelException.class ) );
            assertThat( ex.getCause().getCause(), instanceOf( ArithmeticException.class ) );
        }
    }

    @Test
    void shouldThrowAppropriateExceptionAlsoWhenVisiting()
    {
        try
        {
            try ( Transaction transaction = db.beginTx() )
            {
                transaction.execute( "RETURN rand()/0" ).accept( row -> true );
            }
        }
        catch ( QueryExecutionException ex )
        {
            assertThat( ex.getCause(), instanceOf( QueryExecutionKernelException.class ) );
            assertThat( ex.getCause().getCause(), instanceOf( ArithmeticException.class ) );
        }
    }

    private void createNode()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode();
            tx.commit();
        }
    }

    @Test
    void shouldHandleListsOfPointsAsInput()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            // Given
            Point point1 = (Point) transaction.execute( "RETURN point({latitude: 12.78, longitude: 56.7}) as point" ).next().get( "point" );
            Point point2 = (Point) transaction.execute( "RETURN point({latitude: 12.18, longitude: 56.2}) as point" ).next().get( "point" );

            // When
            double distance = (double) transaction.execute( "RETURN distance($points[0], $points[1]) as dist",
                    map( "points", asList( point1, point2 ) ) ).next().get( "dist" );
            // Then
            assertThat( Math.round( distance ), equalTo( 86107L ) );
            transaction.commit();
        }
    }

    @Test
    void shouldHandleMapWithPointsAsInput()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            // Given
            Point point1 = (Point) transaction.execute( "RETURN point({latitude: 12.78, longitude: 56.7}) as point" ).next().get( "point" );
            Point point2 = (Point) transaction.execute( "RETURN point({latitude: 12.18, longitude: 56.2}) as point" ).next().get( "point" );

            // When
            double distance = (double) transaction.execute( "RETURN distance($points['p1'], $points['p2']) as dist",
                    map( "points", map( "p1", point1, "p2", point2 ) ) ).next().get( "dist" );
            // Then
            assertThat( Math.round( distance ), equalTo( 86107L ) );
            transaction.commit();
        }
    }

    @Test
    void shouldHandleColumnAsWithNull()
    {
        try ( Transaction transaction = db.beginTx() )
        {
            assertThat( transaction.execute( "RETURN toLower(null) AS lower" ).<String>columnAs( "lower" ).next(), nullValue() );
        }
    }
}
