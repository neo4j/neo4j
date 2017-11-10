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
package org.neo4j.values.storable;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;
import static org.neo4j.values.storable.Values.pointValue;

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

    private static void assertEqual( Value a, Value b )
    {
        assertTrue( String.format( "%s should be equal %s", a.getClass().getSimpleName(), b.getClass().getSimpleName() ), a.equals( b ) );
        assertTrue( String.format( "%s should be equal %s", a.getClass().getSimpleName(), b.getClass().getSimpleName() ), b.equals( a ) );
        assertTrue( String.format( "%s should have same hashcode as %s", a.getClass().getSimpleName(), b.getClass().getSimpleName() ),
                a.hashCode() == b.hashCode() );
    }

    private static void assertNotEqual( Value a, Value b )
    {
        assertFalse( a + " should not equal " + b, a.equals( b ) );
        assertFalse( b + "should not equal " + a, b.equals( a ) );
    }
}
