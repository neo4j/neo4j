/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import org.neo4j.helpers.Function;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.helpers.collection.IteratorUtil.asIterable;

@RunWith(value = Parameterized.class)
public class PropertyEqualityTest
{
    @Parameterized.Parameters
    public static Collection<Object[]> data()
    {
        int integerValue = 87;
        double doubleValue = 42.5;
        int[] intArray = {11, 27, 43};
        long[] longArray = {11, 27, 43};
        short[] shortArray = {11, 27, 43};
        byte[] byteArray = {11, 27, 43};
        float[] floatArray = {11, 27, 43};
        double[] doubleArray = {11, 27, 43};
        double[] decimalDoubles = {11.5, 27.5, 43.5};
        float[] decimalFloats = {11.5f, 27.5f, 43.5f};

        Iterable<Test> testValues = asIterable(
                new Test( new IntProperty( 0, integerValue ), integerValue, true ),
                new Test( new DoubleProperty( 0, integerValue ), integerValue, true ),
                new Test( new FloatProperty( 0, integerValue ), integerValue, true ),
                new Test( new ShortProperty( 0, (short) integerValue ), integerValue, true ),
                new Test( new ByteProperty( 0, (byte) integerValue ), integerValue, true ),
                new Test( new BigLongProperty( 0, integerValue ), integerValue, true ),
                new Test( new SmallLongProperty( 0, integerValue ), integerValue, true ),

                new Test( new IntProperty( 0, (int) doubleValue ), doubleValue, false ),
                new Test( new DoubleProperty( 0, doubleValue ), doubleValue, true ),
                new Test( new FloatProperty( 0, (float) doubleValue ), doubleValue, true ),
                new Test( new ShortProperty( 0, (short) doubleValue ), doubleValue, false ),
                new Test( new ByteProperty( 0, (byte) doubleValue ), doubleValue, false ),
                new Test( new BigLongProperty( 0, (long) doubleValue ), doubleValue, false ),
                new Test( new SmallLongProperty( 0, (int) doubleValue ), doubleValue, false ),

                new Test( new IntArrayProperty( 0, intArray ), intArray, true ),
                new Test( new LongArrayProperty( 0, longArray ), intArray, true ),
                new Test( new ShortArrayProperty( 0, shortArray ), intArray, true ),
                new Test( new ByteArrayProperty( 0, byteArray ), intArray, true ),
                new Test( new FloatArrayProperty( 0, floatArray ), intArray, true ),
                new Test( new DoubleArrayProperty( 0, doubleArray ), intArray, true ),

                new Test( new IntArrayProperty( 0, intArray ), longArray, true ),
                new Test( new LongArrayProperty( 0, longArray ), longArray, true ),
                new Test( new ShortArrayProperty( 0, shortArray ), longArray, true ),
                new Test( new ByteArrayProperty( 0, byteArray ), longArray, true ),
                new Test( new FloatArrayProperty( 0, floatArray ), longArray, true ),
                new Test( new DoubleArrayProperty( 0, doubleArray ), longArray, true ),

                new Test( new IntArrayProperty( 0, intArray ), decimalDoubles, false ),
                new Test( new LongArrayProperty( 0, longArray ), decimalDoubles, false ),
                new Test( new ShortArrayProperty( 0, shortArray ), decimalDoubles, false ),
                new Test( new ByteArrayProperty( 0, byteArray ), decimalDoubles, false ),
                new Test( new FloatArrayProperty( 0, decimalFloats ), decimalDoubles, true ),
                new Test( new DoubleArrayProperty( 0, decimalDoubles ), decimalDoubles, true )

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

    private static class Test
    {
        final Property property;
        final Object value;
        final boolean shouldMatch;

        Test( Property property, Object value, boolean shouldMatch )
        {
            this.property = property;
            this.value = value;
            this.shouldMatch = shouldMatch;
        }

        void checkAssertion()
        {
            if ( shouldMatch )
            {
                assertEquality( property, value );
            } else
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
