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
package org.neo4j.values.utils;

import org.neo4j.values.AnyValue;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AnyValueTestUtil
{
    public static void assertEqual( AnyValue a, AnyValue b )
    {
        assertTrue(
                String.format( "%s should be equivalent to %s", a.getClass().getSimpleName(), b.getClass().getSimpleName() ),
                a.equals( b ) );
        assertTrue(
                String.format( "%s should be equivalent to %s", a.getClass().getSimpleName(), b.getClass().getSimpleName() ),
                b.equals( a ) );
        assertTrue(
                String.format( "%s should be equal %s", a.getClass().getSimpleName(), b.getClass().getSimpleName() ),
                a.ternaryEquals( b ) );
        assertTrue(
                String.format( "%s should be equal %s", a.getClass().getSimpleName(), b.getClass().getSimpleName() ),
                b.ternaryEquals( a ) );
        assertTrue( String.format( "%s should have same hashcode as %s", a.getClass().getSimpleName(),
                b.getClass().getSimpleName() ), a.hashCode() == b.hashCode() );
    }

    public static void assertEqualValues( AnyValue a, AnyValue b )
    {
        assertTrue( a + " should be equivalent to " + b, a.equals( b ) );
        assertTrue( a + " should be equivalent to " + b, b.equals( a ) );
        assertTrue( a + " should be equal to " + b, a.ternaryEquals( b ) );
        assertTrue( a + " should be equal to " + b, b.ternaryEquals( a ) );
    }

    public static void assertNotEqual( AnyValue a, AnyValue b )
    {
        assertFalse( a + " should not be equivalent to " + b, a.equals( b ) );
        assertFalse( b + " should not be equivalent to " + a, b.equals( a ) );
        assertFalse( a + " should not equal " + b, a.ternaryEquals( b ) );
        assertFalse( b + " should not equal " + a, b.ternaryEquals( a ) );
    }

    public static void assertIncomparable( AnyValue a, AnyValue b )
    {
        assertFalse( a + " should not be equivalent to " + b, a.equals( b ) );
        assertFalse( b + " should not be equivalent to " + a, b.equals( a ) );
        assertNull( a + " should be incomparable to " + b, a.ternaryEquals( b ) );
        assertNull( b + " should be incomparable to " + a, b.ternaryEquals( a ) );
    }
}
