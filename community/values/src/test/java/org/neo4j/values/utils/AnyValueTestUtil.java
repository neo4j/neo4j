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
import org.neo4j.values.Equality;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class AnyValueTestUtil
{
    public static void assertEqual( AnyValue a, AnyValue b )
    {
        assertEquals( a, b, formatMessage( "should be equivalent to", a, b ) );
        assertEquals( b, a, formatMessage( "should be equivalent to", b, a ) );
        assertEquals( a.ternaryEquals( b ), Equality.TRUE, formatMessage( "should be equal to", a, b ) );
        assertEquals( b.ternaryEquals( a ), Equality.TRUE, formatMessage( "should be equal to", b, a ) );
        assertEquals( a.hashCode(), b.hashCode(), formatMessage( "should have same hashcode as", a, b ) );
    }

    private static String formatMessage( String should, AnyValue a, AnyValue b )
    {
        return String.format( "%s(%s) %s %s(%s)", a.getClass().getSimpleName(), a.toString(), should, b.getClass().getSimpleName(), b.toString() );
    }

    public static void assertEqualValues( AnyValue a, AnyValue b )
    {
        assertEquals( a, b, a + " should be equivalent to " + b );
        assertEquals( b, a, a + " should be equivalent to " + b );
        assertEquals( a.ternaryEquals( b ), Equality.TRUE, a + " should be equal to " + b );
        assertEquals( b.ternaryEquals( a ), Equality.TRUE, a + " should be equal to " + b );
    }

    public static void assertNotEqual( AnyValue a, AnyValue b )
    {
        assertNotEquals( a, b, a + " should not be equivalent to " + b );
        assertNotEquals( b, a, b + " should not be equivalent to " + a );
        assertNotEquals( a.ternaryEquals( b ), Equality.TRUE, a + " should not equal " + b );
        assertNotEquals( b.ternaryEquals( a ), Equality.TRUE, b + " should not equal " + a );
    }

    public static void assertIncomparable( AnyValue a, AnyValue b )
    {
        assertNotEquals( a, b, a + " should not be equivalent to " + b );
        assertNotEquals( b, a, b + " should not be equivalent to " + a );
        assertEquals( a.ternaryEquals( b ), Equality.UNDEFINED, a + " should be incomparable to " + b );
        assertEquals( b.ternaryEquals( a ), Equality.UNDEFINED, b + " should be incomparable to " + a );
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
