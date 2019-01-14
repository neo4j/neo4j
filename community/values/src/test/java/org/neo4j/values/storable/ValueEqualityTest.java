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

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.neo4j.values.utils.AnyValueTestUtil.assertEqual;
import static org.neo4j.values.utils.AnyValueTestUtil.assertNotEqual;

/**
 * This test was faithfully converted (including personal remarks) from PropertyEqualityTest.
 */
@RunWith( value = Parameterized.class )
public class ValueEqualityTest
{
    @Parameterized.Parameters( name = "{0}" )
    public static Iterable<Test> data()
    {
        return Arrays.asList(
                // boolean properties
                shouldMatch( true, true ),
                shouldMatch( false, false ),
                shouldNotMatch( true, false ),
                shouldNotMatch( false, true ),
                shouldNotMatch( true, 0 ),
                shouldNotMatch( false, 0 ),
                shouldNotMatch( true, 1 ),
                shouldNotMatch( false, 1 ),
                shouldNotMatch( false, "false" ),
                shouldNotMatch( true, "true" ),

                //byte properties
                shouldMatch( (byte) 42, (byte) 42 ),
                shouldMatch( (byte) 42, (short) 42 ),
                shouldNotMatch( (byte) 42, 42 + 256 ),
                shouldMatch( (byte) 43, 43 ),
                shouldMatch( (byte) 43, 43L ),
                shouldMatch( (byte) 23, 23.0d ),
                shouldMatch( (byte) 23, 23.0f ),
                shouldNotMatch( (byte) 23, 23.5 ),
                shouldNotMatch( (byte) 23, 23.5f ),

                //short properties
                shouldMatch( (short) 11, (byte) 11 ),
                shouldMatch( (short) 42, (short) 42 ),
                shouldNotMatch( (short) 42, 42 + 65536 ),
                shouldMatch( (short) 43, 43 ),
                shouldMatch( (short) 43, 43L ),
                shouldMatch( (short) 23, 23.0f ),
                shouldMatch( (short) 23, 23.0d ),
                shouldNotMatch( (short) 23, 23.5 ),
                shouldNotMatch( (short) 23, 23.5f ),

                //int properties
                shouldMatch( 11, (byte) 11 ),
                shouldMatch( 42, (short) 42 ),
                shouldNotMatch( 42, 42 + 4294967296L ),
                shouldMatch( 43, 43 ),
                shouldMatch( Integer.MAX_VALUE, Integer.MAX_VALUE ),
                shouldMatch( 43, (long) 43 ),
                shouldMatch( 23, 23.0 ),
                shouldNotMatch( 23, 23.5 ),
                shouldNotMatch( 23, 23.5f ),

                //long properties
                shouldMatch( 11L, (byte) 11 ),
                shouldMatch( 42L, (short) 42 ),
                shouldMatch( 43L, 43 ),
                shouldMatch( 43L, 43L ),
                shouldMatch( 87L, 87L ),
                shouldMatch( Long.MAX_VALUE, Long.MAX_VALUE ),
                shouldMatch( 23L, 23.0 ),
                shouldNotMatch( 23L, 23.5 ),
                shouldNotMatch( 23L, 23.5f ),
                shouldMatch(9007199254740992L, 9007199254740992D),
                // shouldMatch(9007199254740993L, 9007199254740992D), // is stupid, m'kay?!

                // floats goddamnit
                shouldMatch( 11f, (byte) 11 ),
                shouldMatch( 42f, (short) 42 ),
                shouldMatch( 43f, 43 ),
                shouldMatch( 43f, 43L ),
                shouldMatch( 23f, 23.0 ),
                shouldNotMatch( 23f, 23.5 ),
                shouldNotMatch( 23f, 23.5f ),
                shouldMatch( 3.14f, 3.14f ),
                shouldNotMatch( 3.14f, 3.14d ),   // Would be nice if they matched, but they don't

                // doubles
                shouldMatch( 11d, (byte) 11 ),
                shouldMatch( 42d, (short) 42 ),
                shouldMatch( 43d, 43 ),
                shouldMatch( 43d, 43d ),
                shouldMatch( 23d, 23.0 ),
                shouldNotMatch( 23d, 23.5 ),
                shouldNotMatch( 23d, 23.5f ),
                shouldNotMatch( 3.14d, 3.14f ),   // this really is sheeeet
                shouldMatch( 3.14d, 3.14d ),

                // strings
                shouldMatch( "A", "A" ),
                shouldMatch( 'A', 'A' ),
                shouldMatch( 'A', "A" ),
                shouldMatch( "A", 'A' ),
                shouldNotMatch( "AA", 'A' ),
                shouldNotMatch( "a", "A" ),
                shouldNotMatch( "A", "a" ),
                shouldNotMatch( "0", 0 ),
                shouldNotMatch( '0', 0 ),

                // arrays
                shouldMatch( new int[]{1, 2, 3}, new int[]{1, 2, 3} ),
                shouldMatch( new int[]{1, 2, 3}, new long[]{1, 2, 3} ),
                shouldMatch( new int[]{1, 2, 3}, new double[]{1.0, 2.0, 3.0} ),
                shouldMatch( new String[]{"A", "B", "C"}, new String[]{"A", "B", "C"} ),
                shouldMatch( new String[]{"A", "B", "C"}, new char[]{'A', 'B', 'C'} ),
                shouldMatch( new char[]{'A', 'B', 'C'},  new String[]{"A", "B", "C"} ),

                shouldNotMatch( false, new boolean[]{false} ),
                shouldNotMatch( 1, new int[]{1} ),
                shouldNotMatch( "apa", new String[]{"apa"} )
        );
    }

    private Test currentTest;

    public ValueEqualityTest( Test currentTest )
    {
        this.currentTest = currentTest;
    }

    @org.junit.Test
    public void runTest()
    {
        currentTest.checkAssertion();
    }

    public static Test shouldMatch( boolean propertyValue, Object value )
    {
        return new Test( Values.booleanValue( propertyValue ), value, true );
    }

    public static Test shouldNotMatch( boolean propertyValue, Object value )
    {
        return new Test( Values.booleanValue( propertyValue ), value, false );
    }

    public static Test shouldMatch( byte propertyValue, Object value )
    {
        return new Test( Values.byteValue( propertyValue ), value, true );
    }

    public static Test shouldNotMatch( byte propertyValue, Object value )
    {
        return new Test( Values.byteValue( propertyValue ), value, false );
    }

    public static Test shouldMatch( short propertyValue, Object value )
    {
        return new Test( Values.shortValue( propertyValue ), value, true );
    }

    public static Test shouldNotMatch( short propertyValue, Object value )
    {
        return new Test( Values.shortValue( propertyValue ), value, false );
    }

    public static Test shouldMatch( float propertyValue, Object value )
    {
        return new Test( Values.floatValue( propertyValue ), value, true );
    }

    public static Test shouldNotMatch( float propertyValue, Object value )
    {
        return new Test( Values.floatValue( propertyValue ), value, false );
    }

    public static Test shouldMatch( long propertyValue, Object value )
    {
        return new Test( Values.longValue( propertyValue ), value, true );
    }

    public static Test shouldNotMatch( long propertyValue, Object value )
    {
        return new Test( Values.longValue( propertyValue ), value, false );
    }

    public static Test shouldMatch( double propertyValue, Object value )
    {
        return new Test( Values.doubleValue( propertyValue ), value, true );
    }

    public static Test shouldNotMatch( double propertyValue, Object value )
    {
        return new Test( Values.doubleValue( propertyValue ), value, false );
    }

    public static Test shouldMatch( String propertyValue, Object value )
    {
        return new Test( Values.stringValue( propertyValue ), value, true );
    }

    public static Test shouldNotMatch( String propertyValue, Object value )
    {
        return new Test( Values.stringValue( propertyValue ), value, false );
    }

    public static Test shouldMatch( char propertyValue, Object value )
    {
        return new Test( Values.charValue( propertyValue ), value, true );
    }

    public static Test shouldNotMatch( char propertyValue, Object value )
    {
        return new Test( Values.charValue( propertyValue ), value, false );
    }

    public static Test shouldMatch( int[] propertyValue, Object value )
    {
        return new Test( Values.intArray( propertyValue ), value, true );
    }

    public static Test shouldNotMatch( int[] propertyValue, Object value )
    {
        return new Test( Values.intArray( propertyValue ), value, false );
    }

    public static Test shouldMatch( char[] propertyValue, Object value )
    {
        return new Test( Values.charArray( propertyValue ), value, true );
    }

    public static Test shouldNotMatch( char[] propertyValue, Object value )
    {
        return new Test( Values.charArray( propertyValue ), value, false );
    }

    public static Test shouldMatch( String[] propertyValue, Object value )
    {
        return new Test( Values.stringArray( propertyValue ), value, true );
    }

    public static Test shouldNotMatch( String[] propertyValue, Object value )
    {
        return new Test( Values.stringArray( propertyValue ), value, false );
    }

    private static class Test
    {
        final Value a;
        final Value b;
        final boolean shouldMatch;

        private Test( Value a, Object b, boolean shouldMatch )
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
