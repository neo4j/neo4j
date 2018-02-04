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
package org.neo4j.values.utils;

import java.util.function.Supplier;

import org.neo4j.values.AnyValue;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AnyValueTestUtil
{
    public static void assertEqual( AnyValue a, AnyValue b )
    {
        assertTrue( a.equals( b ), format( "%s should be equivalent to %s", a.getClass().getSimpleName(),
                b.getClass().getSimpleName() ) );
        assertTrue( b.equals( a ), format( "%s should be equivalent to %s", a.getClass().getSimpleName(),
                b.getClass().getSimpleName() ) );
        assertTrue( a.ternaryEquals( b ),
                format( "%s should be equal %s", a.getClass().getSimpleName(), b.getClass().getSimpleName() ) );
        assertTrue( b.ternaryEquals( a ),
                format( "%s should be equal %s", a.getClass().getSimpleName(), b.getClass().getSimpleName() ) );
        assertTrue( a.hashCode() == b.hashCode(), format( "%s should have same hashcode as %s", a.getClass().getSimpleName(),
                b.getClass().getSimpleName() ) );
    }

    public static void assertEqualValues( AnyValue a, AnyValue b )
    {
        assertTrue( a.equals( b ), a + " should be equivalent to " + b );
        assertTrue( b.equals( a ), a + " should be equivalent to " + b );
        assertTrue( a.ternaryEquals( b ), a + " should be equal to " + b );
        assertTrue( b.ternaryEquals( a ), a + " should be equal to " + b );
    }

    public static void assertNotEqual( AnyValue a, AnyValue b )
    {
        assertFalse( a.equals( b ), a + " should not be equivalent to " + b );
        assertFalse( b.equals( a ), b + " should not be equivalent to " + a );
        assertFalse( a.ternaryEquals( b ), a + " should not equal " + b );
        assertFalse( b.ternaryEquals( a ), b + " should not equal " + a );
    }

    public static void assertIncomparable( AnyValue a, AnyValue b )
    {
        assertFalse( a.equals( b ), a + " should not be equivalent to " + b );
        assertFalse( b.equals( a ), b + " should not be equivalent to " + a );
        assertNull( a.ternaryEquals( b ), a + " should be incomparable to " + b );
        assertNull( b.ternaryEquals( a ), b + " should be incomparable to " + a );
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
