/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class AnyValueTestUtil
{
    public static void assertEqual( AnyValue a, AnyValue b )
    {
        assertEquals( formatMessage( "should be equivalent to", a, b ), a, b );
        assertEquals( formatMessage( "should be equivalent to", b, a ), b, a );
        assertEquals( formatMessage( "should be equal to", a, b ),
                a.ternaryEquals( b ), Equality.TRUE );
        assertEquals( formatMessage( "should be equal to", b, a ),
                b.ternaryEquals( a ), Equality.TRUE );
        assertEquals( formatMessage( "should have same hashcode as", a, b ), a.hashCode(), b.hashCode() );
    }

    private static String formatMessage( String should, AnyValue a, AnyValue b )
    {
        return String.format( "%s(%s) %s %s(%s)", a.getClass().getSimpleName(), a.toString(), should, b.getClass().getSimpleName(), b.toString() );
    }

    public static void assertEqualValues( AnyValue a, AnyValue b )
    {
        assertEquals( a + " should be equivalent to " + b, a, b );
        assertEquals( a + " should be equivalent to " + b, b, a );
        assertEquals( a + " should be equal to " + b, a.ternaryEquals( b ), Equality.TRUE );
        assertEquals( a + " should be equal to " + b, b.ternaryEquals( a ), Equality.TRUE );
    }

    public static void assertNotEqual( AnyValue a, AnyValue b )
    {
        assertNotEquals( a + " should not be equivalent to " + b, a, b );
        assertNotEquals( b + " should not be equivalent to " + a, b, a );
        assertNotEquals( a + " should not equal " + b, a.ternaryEquals( b ), Equality.TRUE );
        assertNotEquals( b + " should not equal " + a, b.ternaryEquals( a ), Equality.TRUE );
    }

    public static void assertIncomparable( AnyValue a, AnyValue b )
    {
        assertNotEquals( a + " should not be equivalent to " + b, a, b );
        assertNotEquals( b + " should not be equivalent to " + a, b, a );
        assertEquals( a + " should be incomparable to " + b, a.ternaryEquals( b ), Equality.UNDEFINED );
        assertEquals( b + " should be incomparable to " + a, b.ternaryEquals( a ), Equality.UNDEFINED );
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
