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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.neo4j.values.utils.AnyValueTestUtil.assertEqual;
import static org.neo4j.values.utils.AnyValueTestUtil.assertNotEqual;

/**
 * This test was faithfully converted (including personal remarks) from PropertyEqualityTest.
 */
public class ValueEqualityTest
{
    public static Stream<Arguments> parameters()
    {
        return Stream.of(
            Arguments.of( shouldMatch( true, true ) ),
            Arguments.of( shouldMatch( false, false ) ),
            Arguments.of( shouldNotMatch( true, false ) ),
            Arguments.of( shouldNotMatch( false, true ) ),
            Arguments.of( shouldNotMatch( true, 0 ) ),
            Arguments.of( shouldNotMatch( false, 0 ) ),
            Arguments.of( shouldNotMatch( true, 1 ) ),
            Arguments.of( shouldNotMatch( false, 1 ) ),
            Arguments.of( shouldNotMatch( false, "false" ) ),
            Arguments.of( shouldNotMatch( true, "true" ) ),

            Arguments.of( shouldMatch( (byte) 42, (byte) 42 ) ),
            Arguments.of( shouldMatch( (byte) 42, (short) 42 ) ),
            Arguments.of( shouldNotMatch( (byte) 42, 42 + 256 ) ),
            Arguments.of( shouldMatch( (byte) 43, 43 ) ),
            Arguments.of( shouldMatch( (byte) 43, 43L ) ),
            Arguments.of( shouldMatch( (byte) 23, 23.0d ) ),
            Arguments.of( shouldMatch( (byte) 23, 23.0f ) ),
            Arguments.of( shouldNotMatch( (byte) 23, 23.5 ) ),
            Arguments.of( shouldNotMatch( (byte) 23, 23.5f ) ),

            Arguments.of( shouldMatch( (short) 11, (byte) 11 ) ),
            Arguments.of( shouldMatch( (short) 42, (short) 42 ) ),
            Arguments.of( shouldNotMatch( (short) 42, 42 + 65536 ) ),
            Arguments.of( shouldMatch( (short) 43, 43 ) ),
            Arguments.of( shouldMatch( (short) 43, 43L ) ),
            Arguments.of( shouldMatch( (short) 23, 23.0f ) ),
            Arguments.of( shouldMatch( (short) 23, 23.0d ) ),
            Arguments.of( shouldNotMatch( (short) 23, 23.5 ) ),
            Arguments.of( shouldNotMatch( (short) 23, 23.5f ) ),

            Arguments.of( shouldMatch( 11, (byte) 11 ) ),
            Arguments.of( shouldMatch( 42, (short) 42 ) ),
            Arguments.of( shouldNotMatch( 42, 42 + 4294967296L ) ),
            Arguments.of( shouldMatch( 43, 43 ) ),
            Arguments.of( shouldMatch( Integer.MAX_VALUE, Integer.MAX_VALUE ) ),
            Arguments.of( shouldMatch( 43, (long) 43 ) ),
            Arguments.of( shouldMatch( 23, 23.0 ) ),
            Arguments.of( shouldNotMatch( 23, 23.5 ) ),
            Arguments.of( shouldNotMatch( 23, 23.5f ) ),

            Arguments.of( shouldMatch( 11L, (byte) 11 ) ),
            Arguments.of( shouldMatch( 42L, (short) 42 ) ),
            Arguments.of( shouldMatch( 43L, 43 ) ),
            Arguments.of( shouldMatch( 43L, 43L ) ),
            Arguments.of( shouldMatch( 87L, 87L ) ),
            Arguments.of( shouldMatch( Long.MAX_VALUE, Long.MAX_VALUE ) ),
            Arguments.of( shouldMatch( 23L, 23.0 ) ),
            Arguments.of( shouldNotMatch( 23L, 23.5 ) ),
            Arguments.of( shouldNotMatch( 23L, 23.5f ) ),
            Arguments.of( shouldMatch( 9007199254740992L, 9007199254740992D ) ),

            Arguments.of( shouldMatch( 11f, (byte) 11 ) ),
            Arguments.of( shouldMatch( 42f, (short) 42 ) ),
            Arguments.of( shouldMatch( 43f, 43 ) ),
            Arguments.of( shouldMatch( 43f, 43L ) ),
            Arguments.of( shouldMatch( 23f, 23.0 ) ),
            Arguments.of( shouldNotMatch( 23f, 23.5 ) ),
            Arguments.of( shouldNotMatch( 23f, 23.5f ) ),
            Arguments.of( shouldMatch( 3.14f, 3.14f ) ),
            Arguments.of( shouldNotMatch( 3.14f, 3.14d ) ),

            Arguments.of( shouldMatch( 11d, (byte) 11 ) ),
            Arguments.of( shouldMatch( 42d, (short) 42 ) ),
            Arguments.of( shouldMatch( 43d, 43 ) ),
            Arguments.of( shouldMatch( 43d, 43d ) ),
            Arguments.of( shouldMatch( 23d, 23.0 ) ),
            Arguments.of( shouldNotMatch( 23d, 23.5 ) ),
            Arguments.of( shouldNotMatch( 23d, 23.5f ) ),
            Arguments.of( shouldNotMatch( 3.14d, 3.14f ) ),
            Arguments.of( shouldMatch( 3.14d, 3.14d ) ),

            Arguments.of( shouldMatch( "A", "A" ) ),
            Arguments.of( shouldMatch( 'A', 'A' ) ),
            Arguments.of( shouldMatch( 'A', "A" ) ),
            Arguments.of( shouldMatch( "A", 'A' ) ),
            Arguments.of( shouldNotMatch( "AA", 'A' ) ),
            Arguments.of( shouldNotMatch( "a", "A" ) ),
            Arguments.of( shouldNotMatch( "A", "a" ) ),
            Arguments.of( shouldNotMatch( "0", 0 ) ),
            Arguments.of( shouldNotMatch( '0', 0 ) ),

            Arguments.of( shouldMatch( new int[]{1, 2, 3}, new int[]{1, 2, 3} ) ),
            Arguments.of( shouldMatch( new int[]{1, 2, 3}, new long[]{1, 2, 3} ) ),
            Arguments.of( shouldMatch( new int[]{1, 2, 3}, new double[]{1.0, 2.0, 3.0} ) ),
            Arguments.of( shouldMatch( new String[]{"A", "B", "C"}, new String[]{"A", "B", "C"} ) ),
            Arguments.of( shouldMatch( new String[]{"A", "B", "C"}, new char[]{'A', 'B', 'C'} ) ),
            Arguments.of( shouldMatch( new char[]{'A', 'B', 'C'}, new String[]{"A", "B", "C"} ) ),

            Arguments.of( shouldNotMatch( false, new boolean[]{false} ) ),
            Arguments.of( shouldNotMatch( 1, new int[]{1} ) ),
            Arguments.of( shouldNotMatch( "apa", new String[]{"apa"} ) )
        );
    }

    private Testcase currentTest;

    @ParameterizedTest
    @MethodSource( "parameters" )
    void runTest( Testcase testcase )
    {
        testcase.checkAssertion();
    }

    private static Testcase shouldMatch( boolean propertyValue, Object value )
    {
        return new Testcase( Values.booleanValue( propertyValue ), value, true );
    }

    private static Testcase shouldNotMatch( boolean propertyValue, Object value )
    {
        return new Testcase( Values.booleanValue( propertyValue ), value, false );
    }

    private static Testcase shouldMatch( byte propertyValue, Object value )
    {
        return new Testcase( Values.byteValue( propertyValue ), value, true );
    }

    private static Testcase shouldNotMatch( byte propertyValue, Object value )
    {
        return new Testcase( Values.byteValue( propertyValue ), value, false );
    }

    private static Testcase shouldMatch( short propertyValue, Object value )
    {
        return new Testcase( Values.shortValue( propertyValue ), value, true );
    }

    private static Testcase shouldNotMatch( short propertyValue, Object value )
    {
        return new Testcase( Values.shortValue( propertyValue ), value, false );
    }

    private static Testcase shouldMatch( float propertyValue, Object value )
    {
        return new Testcase( Values.floatValue( propertyValue ), value, true );
    }

    private static Testcase shouldNotMatch( float propertyValue, Object value )
    {
        return new Testcase( Values.floatValue( propertyValue ), value, false );
    }

    private static Testcase shouldMatch( long propertyValue, Object value )
    {
        return new Testcase( Values.longValue( propertyValue ), value, true );
    }

    private static Testcase shouldNotMatch( long propertyValue, Object value )
    {
        return new Testcase( Values.longValue( propertyValue ), value, false );
    }

    private static Testcase shouldMatch( double propertyValue, Object value )
    {
        return new Testcase( Values.doubleValue( propertyValue ), value, true );
    }

    private static Testcase shouldNotMatch( double propertyValue, Object value )
    {
        return new Testcase( Values.doubleValue( propertyValue ), value, false );
    }

    private static Testcase shouldMatch( String propertyValue, Object value )
    {
        return new Testcase( Values.stringValue( propertyValue ), value, true );
    }

    private static Testcase shouldNotMatch( String propertyValue, Object value )
    {
        return new Testcase( Values.stringValue( propertyValue ), value, false );
    }

    private static Testcase shouldMatch( char propertyValue, Object value )
    {
        return new Testcase( Values.charValue( propertyValue ), value, true );
    }

    private static Testcase shouldNotMatch( char propertyValue, Object value )
    {
        return new Testcase( Values.charValue( propertyValue ), value, false );
    }

    private static Testcase shouldMatch( int[] propertyValue, Object value )
    {
        return new Testcase( Values.intArray( propertyValue ), value, true );
    }

    public static Testcase shouldNotMatch( int[] propertyValue, Object value )
    {
        return new Testcase( Values.intArray( propertyValue ), value, false );
    }

    private static Testcase shouldMatch( char[] propertyValue, Object value )
    {
        return new Testcase( Values.charArray( propertyValue ), value, true );
    }

    public static Testcase shouldNotMatch( char[] propertyValue, Object value )
    {
        return new Testcase( Values.charArray( propertyValue ), value, false );
    }

    private static Testcase shouldMatch( String[] propertyValue, Object value )
    {
        return new Testcase( Values.stringArray( propertyValue ), value, true );
    }

    private static class Testcase
    {
        final Value a;
        final Value b;
        final boolean shouldMatch;

        private Testcase( Value a, Object b, boolean shouldMatch )
        {
            this.a = a;
            this.b = Values.of( b );
            this.shouldMatch = shouldMatch;
        }

        @Override
        public String toString()
        {
            return String.format( "%s %s %s", a, shouldMatch ? "==" : "!=", b );
        }

        void checkAssertion()
        {
            if ( shouldMatch )
            {
                assertEqual( a, b );
            }
            else
            {
                assertNotEqual( a, b );
            }
        }
    }
}
