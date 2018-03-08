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
package org.neo4j.values.storable;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian_3D;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84_3D;
import static org.neo4j.values.storable.Values.pointValue;
import static org.neo4j.values.utils.AnyValueTestUtil.assertEqual;
import static org.neo4j.values.utils.AnyValueTestUtil.assertNotEqual;

public class PointTest
{
    @Test
    public void cartesianShouldEqualItself()
    {
        assertEqual( pointValue( Cartesian, 1.0, 2.0 ), pointValue( Cartesian, 1.0, 2.0 ) );
        assertEqual( pointValue( Cartesian, -1.0, 2.0 ), pointValue( Cartesian, -1.0, 2.0 ) );
        assertEqual( pointValue( Cartesian, -1.0, -2.0 ), pointValue( Cartesian, -1.0, -2.0 ) );
        assertEqual( pointValue( Cartesian, 0.0, 0.0 ), pointValue( Cartesian, 0.0, 0.0 ) );
    }

    @Test
    public void cartesianShouldNotEqualOtherPoint()
    {
        assertNotEqual( pointValue( Cartesian, 1.0, 2.0 ), pointValue( Cartesian, 3.0, 4.0 ) );
        assertNotEqual( pointValue( Cartesian, 1.0, 2.0 ), pointValue( Cartesian, -1.0, 2.0 ) );
    }

    @Test
    public void geographicShouldEqualItself()
    {
        assertEqual( pointValue( WGS84, 1.0, 2.0 ), pointValue( WGS84, 1.0, 2.0 ) );
        assertEqual( pointValue( WGS84, -1.0, 2.0 ), pointValue( WGS84, -1.0, 2.0 ) );
        assertEqual( pointValue( WGS84, -1.0, -2.0 ), pointValue( WGS84, -1.0, -2.0 ) );
        assertEqual( pointValue( WGS84, 0.0, 0.0 ), pointValue( WGS84, 0.0, 0.0 ) );
    }

    @Test
    public void geographicShouldNotEqualOtherPoint()
    {
        assertNotEqual( pointValue( WGS84, 1.0, 2.0 ), pointValue( WGS84, 3.0, 4.0 ) );
        assertNotEqual( pointValue( WGS84, 1.0, 2.0 ), pointValue( WGS84, -1.0, 2.0 ) );
    }

    @Test
    public void geographicShouldNotEqualCartesian()
    {
        assertNotEqual( pointValue( WGS84, 1.0, 2.0 ), pointValue( Cartesian, 1.0, 2.0 ) );
    }

    @Test
    public void shouldHaveValueGroup()
    {
        assertTrue( pointValue( Cartesian, 1, 2 ).valueGroup() != null );
        assertTrue( pointValue( WGS84, 1, 2 ).valueGroup() != null );
    }

    //-------------------------------------------------------------
    // Parser tests

    @Test
    public void shouldBeAbleToParsePoints()
    {
        assertEqual( pointValue( WGS84, 13.2, 56.7 ),
                PointValue.parse( "{latitude: 56.7, longitude: 13.2}" ) );
        assertEqual( pointValue( WGS84, -74.0060, 40.7128 ),
                PointValue.parse( "{latitude: 40.7128, longitude: -74.0060, crs: 'wgs-84'}" ) ); // - explicitly WGS84
        assertEqual( pointValue( Cartesian, -21, -45.3 ),
                PointValue.parse( "{x: -21, y: -45.3}" ) ); // - default to cartesian 2D
        assertEqual( pointValue( Cartesian, 17, -52.8 ),
                PointValue.parse( "{x: 17, y: -52.8, crs: 'cartesian'}" ) ); // - explicit cartesian 2D
        assertEqual( pointValue( WGS84_3D, 13.2, 56.7, 123.4 ),
                PointValue.parse( "{latitude: 56.7, longitude: 13.2, height: 123.4}" ) ); // - defaults to WGS84-3D
        assertEqual( pointValue( WGS84_3D, 13.2, 56.7, 123.4 ),
                PointValue.parse( "{latitude: 56.7, longitude: 13.2, z: 123.4}" ) ); // - defaults to WGS84-3D
        assertEqual( pointValue( WGS84_3D, -74.0060, 40.7128, 567.8 ),
                PointValue.parse( "{latitude: 40.7128, longitude: -74.0060, height: 567.8, crs: 'wgs-84-3D'}" ) ); // - explicitly WGS84-3D
        assertEqual( pointValue( Cartesian_3D, -21, -45.3, 7.2 ),
                PointValue.parse( "{x: -21, y: -45.3, z: 7.2}" ) ); // - default to cartesian 3D
        assertEqual( pointValue( Cartesian_3D, 17, -52.8, -83.1 ),
                PointValue.parse( "{x: 17, y: -52.8, z: -83.1, crs: 'cartesian-3D'}" ) ); // - explicit cartesian 3D
    }

    @Test
    public void shouldBeAbleToParsePointWithUnquotedCrs()
    {
        assertEqual( pointValue( WGS84_3D, -74.0060, 40.7128, 567.8 ),
                PointValue.parse( "{latitude: 40.7128, longitude: -74.0060, height: 567.8, crs:wgs-84-3D}" ) ); // - explicitly WGS84-3D, without quotes
    }

    @Test
    public void shouldNotBeAbleToParsePointWithConflictingCrsInformation()
    {
        try
        {
            PointValue.parse( "{latitude: 40.7128, longitude: -74.0060, height: 567.8, crs:wgs-84-3D}", "wgs-84" );
        }
        catch ( IllegalArgumentException e )
        {
            assertEquals( "Conflicting crs attributes for point found. Header crs states 'wgs-84' while column specifies 'wgs-84-3D'", e.getMessage() );
        }
    }

    @Test
    public void shouldBeAbleToParseWeirdlyFormattedPoints()
    {
        assertEqual( pointValue( WGS84, 1.0, 2.0 ), PointValue.parse( " \t\n { latitude : 2.0  ,longitude :1.0  } \t" ) );
        // TODO: Should some/all of these fail?
        assertEqual( pointValue( WGS84, 1.0, 2.0 ), PointValue.parse( " \t\n { latitude : 2.0  ,longitude :1.0 , } \t" ) );
        assertEqual( pointValue( Cartesian, 2.0E-8, -1.0E7 ), PointValue.parse( " \t\n { x :+.2e-7,y: -1.0E07 , } \t" ) );
        assertEqual( pointValue( Cartesian, 2.0E-8, -1.0E7 ), PointValue.parse( " \t\n { x :+.2e-7,y: -1.0E07 , garbage} \t" ) );
        assertEqual( pointValue( Cartesian, 2.0E-8, -1.0E7 ), PointValue.parse( " \t\n { gar ba ge,x :+.2e-7,y: -1.0E07} \t" ) );
    }

    @Test
    public void shouldBeAbleToParsePointAndIgnoreUnknownFields()
    {
        assertEqual( pointValue( WGS84, 1.0, 2.0 ), PointValue.parse( "{latitude:2.0,longitude:1.0,unknown_field:\"hello\"}" ) );
    }

    @Test
    public void shouldNotBeAbleToParsePointsWithConflictingDuplicateFields()
    {
        assertTrue( assertCannotParse( "{latitude: 2.0, longitude: 1.0, latitude: 3.0}" ).getMessage().contains( "Duplicate field" ) );
        assertTrue( assertCannotParse( "{crs: 'cartesian', x: 2.0, x: 1.0, y: 3}" ).getMessage().contains( "Duplicate field" ) );
        assertTrue( assertCannotParse( "{crs: 'invalid crs', x: 1.0, y: 3, crs: 'cartesian'}" ).getMessage().contains( "Duplicate field" ) );
    }

    @Test
    public void shouldNotBeAbleToParseIncompletePoints()
    {
        assertCannotParse( "{latitude: 56.7, longitude:}" );
        assertCannotParse( "{latitude: 56.7}" );
        assertCannotParse( "{}" );
        assertCannotParse( "{only_a_key}" );
        assertCannotParse( "{crs:'WGS-84'}" );
        assertCannotParse( "{a:a}" );
        assertCannotParse( "{ : 2.0, x : 1.0 }" );
        assertCannotParse( "x:1,y:2" );
        assertCannotParse( "{x:1,y:'2'}" );
        assertCannotParse( "{crs:WGS-84 , lat:1, y:2}" );
    }

    private IllegalArgumentException assertCannotParse( String text )
    {
        PointValue value;
        try
        {
            value = PointValue.parse( text );
        }
        catch ( IllegalArgumentException e )
        {
            return e;
        }
        throw new AssertionError( String.format( "'%s' parsed to %s", text, value ) );
    }
}
