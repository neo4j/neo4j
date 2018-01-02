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
package org.neo4j.kernel.api.properties;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.function.Function;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.helpers.collection.IteratorUtil.asIterable;

@RunWith(value = Parameterized.class)
public class PropertyEqualityTest
{
    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        Iterable<Test> testValues = asIterable(
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
                shouldMatch( (byte) 43, (int) 43 ),
                shouldMatch( (byte) 43, (long) 43 ),
                shouldMatch( (byte) 23, 23.0d ),
                shouldMatch( (byte) 23, 23.0f ),
                shouldNotMatch( (byte) 23, 23.5 ),
                shouldNotMatch( (byte) 23, 23.5f ),

                //short properties
                shouldMatch( (short) 11, (byte) 11 ),
                shouldMatch( (short) 42, (short) 42 ),
                shouldNotMatch( (short) 42, 42 + 65536 ),
                shouldMatch( (short) 43, (int) 43 ),
                shouldMatch( (short) 43, (long) 43 ),
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
                shouldMatch( 43L, (int) 43 ),
                shouldMatch( 43L, (long) 43 ),
                shouldMatch( 87L, (long) 87 ),
                shouldMatch( Long.MAX_VALUE, Long.MAX_VALUE ),
                shouldMatch( 23L, 23.0 ),
                shouldNotMatch( 23L, 23.5 ),
                shouldNotMatch( 23L, 23.5f ),
                shouldMatch(9007199254740992L, 9007199254740992D),
                // shouldMatch(9007199254740993L, 9007199254740992D), // is stupid, m'kay?!

                // floats goddamnit
                shouldMatch( 11f, (byte) 11 ),
                shouldMatch( 42f, (short) 42 ),
                shouldMatch( 43f, (int) 43 ),
                shouldMatch( 43f, (long) 43 ),
                shouldMatch( 23f, 23.0 ),
                shouldNotMatch( 23f, 23.5 ),
                shouldNotMatch( 23f, 23.5f ),
                shouldMatch( 3.14f, 3.14f ),
                shouldNotMatch( 3.14f, 3.14d ),   // Would be nice if they matched, but they don't

                // doubles
                shouldMatch( 11d, (byte) 11 ),
                shouldMatch( 42d, (short) 42 ),
                shouldMatch( 43d, (int) 43 ),
                shouldMatch( 43d, (long) 43 ),
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
                shouldMatch( new char[]{'A', 'B', 'C'},  new String[]{"A", "B", "C"} )
        );
        return asCollection( map( new Function<Test, Object[]>()
        {
            @Override
            public Object[] apply( Test testValue )
            {
                return new Object[]{testValue};
            }
        }, testValues ) );
    }

    private Test currentTest;

    public PropertyEqualityTest( Test currentTest )
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
        return new Test( new BooleanProperty( 0, propertyValue ), value, true );
    }

    public static Test shouldNotMatch( boolean propertyValue, Object value )
    {
        return new Test( new BooleanProperty( 0, propertyValue ), value, false );
    }

    public static Test shouldMatch( byte propertyValue, Object value )
    {
        return new Test( new ByteProperty( 0, propertyValue ), value, true );
    }

    public static Test shouldNotMatch( byte propertyValue, Object value )
    {
        return new Test( new ByteProperty( 0, propertyValue ), value, false );
    }

    public static Test shouldMatch( short propertyValue, Object value )
    {
        return new Test( new ShortProperty( 0, propertyValue ), value, true );
    }

    public static Test shouldNotMatch( short propertyValue, Object value )
    {
        return new Test( new ShortProperty( 0, propertyValue ), value, false );
    }

    public static Test shouldMatch( float propertyValue, Object value )
    {
        return new Test( new FloatProperty( 0, propertyValue ), value, true );
    }

    public static Test shouldNotMatch( float propertyValue, Object value )
    {
        return new Test( new FloatProperty( 0, propertyValue ), value, false );
    }

    public static Test shouldMatch( long propertyValue, Object value )
    {
        return new Test( new LongProperty( 0, propertyValue ), value, true );
    }

    public static Test shouldNotMatch( long propertyValue, Object value )
    {
        return new Test( new LongProperty( 0, propertyValue ), value, false );
    }

    public static Test shouldMatch( double propertyValue, Object value )
    {
        return new Test( new DoubleProperty( 0, propertyValue ), value, true );
    }

    public static Test shouldNotMatch( double propertyValue, Object value )
    {
        return new Test( new DoubleProperty( 0, propertyValue ), value, false );
    }

    public static Test shouldMatch( String propertyValue, Object value )
    {
        return new Test( new StringProperty( 0, propertyValue ), value, true );
    }

    public static Test shouldNotMatch( String propertyValue, Object value )
    {
        return new Test( new StringProperty( 0, propertyValue ), value, false );
    }

    public static Test shouldMatch( char propertyValue, Object value )
    {
        return new Test( new CharProperty( 0, propertyValue ), value, true );
    }

    public static Test shouldNotMatch( char propertyValue, Object value )
    {
        return new Test( new CharProperty( 0, propertyValue ), value, false );
    }

    public static Test shouldMatch( int[] propertyValue, Object value )
    {
        return new Test( new IntArrayProperty( 0, propertyValue ), value, true );
    }

    public static Test shouldNotMatch( int[] propertyValue, Object value )
    {
        return new Test( new IntArrayProperty( 0, propertyValue ), value, false );
    }

    public static Test shouldMatch( char[] propertyValue, Object value )
    {
        return new Test( new CharArrayProperty( 0, propertyValue ), value, true );
    }

    public static Test shouldNotMatch( char[] propertyValue, Object value )
    {
        return new Test( new CharArrayProperty( 0, propertyValue ), value, false );
    }

    public static Test shouldMatch( String[] propertyValue, Object value )
    {
        return new Test( new StringArrayProperty( 0, propertyValue ), value, true );
    }

    public static Test shouldNotMatch( String[] propertyValue, Object value )
    {
        return new Test( new StringArrayProperty( 0, propertyValue ), value, false );
    }

    private static class Test
    {
        final DefinedProperty property;
        final Object value;
        final boolean shouldMatch;

        private Test( DefinedProperty property, Object value, boolean shouldMatch )
        {
            this.property = property;
            this.value = value;
            this.shouldMatch = shouldMatch;
        }

        @Override
        public String toString()
        {
            return String.format( "%s (%s) %s %s (%s)",
                    property.value(), property.value().getClass().getSimpleName(), shouldMatch ? "==" : "!=", value,
                    value.getClass().getSimpleName() );
        }

        void checkAssertion()
        {
            if ( shouldMatch )
            {
                assertEquality( property, value );
            }
            else
            {
                assertNonEquality( property, value );
            }
        }

        void assertEquality( Property property, Object value )
        {
            assertTrue( String.format( "Expected the value %s to be equal to %s but it wasn't.",
                    getValueRepresentation( value ), property.toString() ), property.valueEquals( value ) );
        }

        private String getValueRepresentation( Object value )
        {
            String className = value.getClass().getSimpleName();

            String valueRepresentation;

            if ( value.getClass().isArray() )
            {
                if ( value instanceof Object[] )
                {
                    valueRepresentation = Arrays.toString( (Object[]) value );
                } else
                {
                    int length = Array.getLength( value );
                    Object[] objArr = new Object[length];
                    for ( int i = 0; i < length; i++ )
                    {
                        objArr[i] = Array.get( value, i );
                    }
                    valueRepresentation = Arrays.toString( objArr );
                }
            } else
            {
                valueRepresentation = value.toString();
            }
            return valueRepresentation + " of type " + className;
        }

        void assertNonEquality( Property property, Object value )
        {
            assertFalse( String.format( "Expected the value %s to not be equal to %s but it was.",
                    getValueRepresentation( value ), property.toString() ), property.valueEquals( value ) );
        }
    }
}
