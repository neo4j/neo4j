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
package org.neo4j.values.storable;

import org.junit.Test;

import org.neo4j.values.utils.InvalidValuesArgumentException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;

public class CoordinateReferenceSystemTest
{
    @Test
    public void shouldGetCrsByCode()
    {
        assertEquals( Cartesian, CoordinateReferenceSystem.get( Cartesian.getCode() ) );
        assertEquals( WGS84, CoordinateReferenceSystem.get( WGS84.getCode() ) );
    }

    @Test
    public void shouldFailToGetWithIncorrectCode()
    {
        try
        {
            CoordinateReferenceSystem.get( 42 );
            fail( "Exception expected" );
        }
        catch ( InvalidValuesArgumentException e )
        {
            assertEquals( "Unknown coordinate reference system code: 42", e.getMessage() );
        }
    }

    @Test
    public void shouldFindByTableAndCode()
    {
        assertThat( CoordinateReferenceSystem.get( 1, 4326 ), equalTo( CoordinateReferenceSystem.WGS84 ) );
        assertThat( CoordinateReferenceSystem.get( 1, 4979 ), equalTo( CoordinateReferenceSystem.WGS84_3D ) );
        assertThat( CoordinateReferenceSystem.get( 2, 7203 ), equalTo( CoordinateReferenceSystem.Cartesian ) );
        assertThat( CoordinateReferenceSystem.get( 2, 9157 ), equalTo( CoordinateReferenceSystem.Cartesian_3D ) );
    }

    @Test
    public void shouldCalculateCartesianDistance()
    {
        CoordinateReferenceSystem crs = CoordinateReferenceSystem.Cartesian;
        assertThat( "", crs.getCalculator().distance( cart( 0.0, 0.0 ), cart( 0.0, 1.0 ) ), equalTo( 1.0 ) );
        assertThat( "", crs.getCalculator().distance( cart( 0.0, 0.0 ), cart( 1.0, 0.0 ) ), equalTo( 1.0 ) );
        assertThat( "", crs.getCalculator().distance( cart( 0.0, 0.0 ), cart( 1.0, 1.0 ) ), closeTo( 1.4, 0.02 ) );
        assertThat( "", crs.getCalculator().distance( cart( 0.0, 0.0 ), cart( 0.0, -1.0 ) ), equalTo( 1.0 ) );
        assertThat( "", crs.getCalculator().distance( cart( 0.0, 0.0 ), cart( -1.0, 0.0 ) ), equalTo( 1.0 ) );
        assertThat( "", crs.getCalculator().distance( cart( 0.0, 0.0 ), cart( -1.0, -1.0 ) ), closeTo( 1.4, 0.02 ) );
        assertThat( "", crs.getCalculator().distance( cart( 1.0, 0.0 ), cart( 0.0, -1.0 ) ), closeTo( 1.4, 0.02 ) );
        assertThat( "", crs.getCalculator().distance( cart( 1.0, 0.0 ), cart( -1.0, 0.0 ) ), equalTo( 2.0 ) );
        assertThat( "", crs.getCalculator().distance( cart( 1.0, 0.0 ), cart( -1.0, -1.0 ) ), closeTo( 2.24, 0.01 ) );
        assertThat( "", crs.getCalculator().distance( cart( -1000000.0, -1000000.0 ), cart( 1000000.0, 1000000.0 ) ), closeTo( 2828427.0, 1.0 ) );
    }

    @Test
    public void shouldCalculateCartesianDistance3D()
    {
        CoordinateReferenceSystem crs = CoordinateReferenceSystem.Cartesian_3D;
        assertThat( "", crs.getCalculator().distance( cart( 0.0, 0.0, 0.0 ), cart( 1.0, 0.0, 0.0 ) ), equalTo( 1.0 ) );
        assertThat( "", crs.getCalculator().distance( cart( 0.0, 0.0, 0.0 ), cart( 0.0, 1.0, 0.0 ) ), equalTo( 1.0 ) );
        assertThat( "", crs.getCalculator().distance( cart( 0.0, 0.0, 0.0 ), cart( 0.0, 0.0, 1.0 ) ), equalTo( 1.0 ) );
        assertThat( "", crs.getCalculator().distance( cart( 0.0, 0.0, 0.0 ), cart( 0.0, 1.0, 1.0 ) ), closeTo( 1.41, 0.01 ) );
        assertThat( "", crs.getCalculator().distance( cart( 0.0, 0.0, 0.0 ), cart( 1.0, 1.0, 1.0 ) ), closeTo( 1.73, 0.01 ) );
        assertThat( "", crs.getCalculator().distance( cart( -1000000.0, -1000000.0, -1000000.0 ), cart( 1000000.0, 1000000.0, 1000000.0 ) ),
                closeTo( 3464102.0, 1.0 ) );
    }

    @Test
    public void shouldCalculateGeographicDistance()
    {
        CoordinateReferenceSystem crs = CoordinateReferenceSystem.WGS84;
        assertThat( "2D distance should match", crs.getCalculator().distance( geo( 0.0, 0.0 ), geo( 0.0, 90.0 ) ), closeTo( 10000000.0, 20000.0 ) );
        assertThat( "2D distance should match", crs.getCalculator().distance( geo( 0.0, 0.0 ), geo( 0.0, -90.0 ) ), closeTo( 10000000.0, 20000.0 ) );
        assertThat( "2D distance should match", crs.getCalculator().distance( geo( 0.0, -45.0 ), geo( 0.0, 45.0 ) ), closeTo( 10000000.0, 20000.0 ) );
        assertThat( "2D distance should match", crs.getCalculator().distance( geo( -45.0, 0.0 ), geo( 45.0, 0.0 ) ), closeTo( 10000000.0, 20000.0 ) );
        assertThat( "2D distance should match", crs.getCalculator().distance( geo( -45.0, 0.0 ), geo( 45.0, 0.0 ) ), closeTo( 10000000.0, 20000.0 ) );
        //"distance function should measure distance from Copenhagen train station to Neo4j in Malmö"
        PointValue cph = geo( 12.564590, 55.672874 );
        PointValue malmo = geo( 12.994341, 55.611784 );
        double expected = 27842.0;
        assertThat( "2D distance should match", crs.getCalculator().distance( cph, malmo ), closeTo( expected, 0.1 ) );
    }

    @Test
    public void shouldCalculateGeographicDistance3D()
    {
        CoordinateReferenceSystem crs = CoordinateReferenceSystem.WGS84_3D;
        //"distance function should measure distance from Copenhagen train station to Neo4j in Malmö"
        PointValue cph = geo( 12.564590, 55.672874, 0.0 );
        PointValue cphHigh = geo( 12.564590, 55.672874, 1000.0 );
        PointValue malmo = geo( 12.994341, 55.611784, 0.0 );
        PointValue malmoHigh = geo( 12.994341, 55.611784, 1000.0 );
        double expected = 27842.0;
        double expectedHigh = 27862.0;
        assertThat( "3D distance should match", crs.getCalculator().distance( cph, malmo ), closeTo( expected, 0.1 ) );
        assertThat( "3D distance should match", crs.getCalculator().distance( cph, malmoHigh ), closeTo( expectedHigh, 0.2 ) );
        assertThat( "3D distance should match", crs.getCalculator().distance( cphHigh, malmo ), closeTo( expectedHigh, 0.2 ) );
    }

    private PointValue cart( double... coords )
    {
        CoordinateReferenceSystem crs = coords.length == 3 ? CoordinateReferenceSystem.Cartesian_3D : CoordinateReferenceSystem.Cartesian;
        return Values.pointValue( crs, coords );
    }

    private PointValue geo( double... coords )
    {
        CoordinateReferenceSystem crs = coords.length == 3 ? CoordinateReferenceSystem.Cartesian_3D : CoordinateReferenceSystem.Cartesian;
        return Values.pointValue( crs, coords );
    }
}
