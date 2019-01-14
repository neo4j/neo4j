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
package org.neo4j.values.utils;

import java.util.function.Supplier;

import org.neo4j.values.AnyValue;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AnyValueTestUtil
{
    public static void assertEqual( AnyValue a, AnyValue b )
    {
        assertTrue( formatMessage( "should be equivalent to", a, b ),
                a.equals( b ) );
        assertTrue(
                formatMessage( "should be equivalent to", b, a ),
                b.equals( a ) );
        assertTrue( formatMessage( "should be equal to", a, b ),
                a.ternaryEquals( b ) );
        assertTrue( formatMessage( "should be equal to", b, a ),
                b.ternaryEquals( a ) );
        assertTrue( formatMessage( "should have same hashcode as", a, b ),
                a.hashCode() == b.hashCode() );
    }

    private static String formatMessage( String should, AnyValue a, AnyValue b )
    {
        return String.format( "%s(%s) %s %s(%s)", a.getClass().getSimpleName(), a.toString(), should, b.getClass().getSimpleName(), b.toString() );
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

    public static <X extends Exception, T> X assertThrows( Class<X> exception, Supplier<T> thunk )
    {
        T value;
        try
        {
            value = thunk.get();
        }
        catch ( Exception e )
        {
            if ( exception.isInstance( e ) )
            {
                return exception.cast( e );
            }
            else
            {
                throw new AssertionError( "Expected " + exception.getName(), e );
            }
        }
        throw new AssertionError( "Expected " + exception.getName() + " but returned: " + value );
    }
}
