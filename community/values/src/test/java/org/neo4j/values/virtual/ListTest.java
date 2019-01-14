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
package org.neo4j.values.virtual;

import org.junit.Test;

import java.util.Arrays;

import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.booleanArray;
import static org.neo4j.values.storable.Values.byteArray;
import static org.neo4j.values.storable.Values.charArray;
import static org.neo4j.values.storable.Values.doubleArray;
import static org.neo4j.values.storable.Values.floatArray;
import static org.neo4j.values.storable.Values.intArray;
import static org.neo4j.values.storable.Values.longArray;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.shortArray;
import static org.neo4j.values.storable.Values.stringArray;
import static org.neo4j.values.utils.AnyValueTestUtil.assertEqual;
import static org.neo4j.values.utils.AnyValueTestUtil.assertEqualValues;
import static org.neo4j.values.utils.AnyValueTestUtil.assertIncomparable;
import static org.neo4j.values.utils.AnyValueTestUtil.assertNotEqual;
import static org.neo4j.values.virtual.VirtualValueTestUtil.list;
import static org.neo4j.values.virtual.VirtualValues.range;

public class ListTest
{
    @Test
    public void shouldBeEqualToItself()
    {
        assertEqual(
                list( new String[]{"hi"}, 3.0 ),
                list( new String[]{"hi"}, 3.0 ) );

        assertEqual( list(), list() );
    }

    @Test
    public void shouldBeEqualToArrayIfValuesAreEqual()
    {
        // the empty list equals any array that is empty
        assertEqualValues( list(), booleanArray( new boolean[]{} ) );
        assertEqualValues( list(), byteArray( new byte[]{} ) );
        assertEqualValues( list(), charArray( new char[]{} ) );
        assertEqualValues( list(), doubleArray( new double[]{} ) );
        assertEqualValues( list(), floatArray( new float[]{} ) );
        assertEqualValues( list(), intArray( new int[]{} ) );
        assertEqualValues( list(), longArray( new long[]{} ) );
        assertEqualValues( list(), shortArray( new short[]{} ) );
        assertEqualValues( list(), stringArray() );

        //actual values to test the equality
        assertEqualValues( list( true ), booleanArray( new boolean[]{true} ) );
        assertEqualValues( list( true, false ), booleanArray( new boolean[]{true, false} ) );
        assertEqualValues( list( 84 ), byteArray( "T".getBytes() ) );
        assertEqualValues(
                list( 84, 104, 105, 115, 32, 105, 115, 32, 106, 117, 115, 116, 32, 97, 32, 116, 101, 115, 116 ),
                byteArray( "This is just a test".getBytes() ) );
        assertEqualValues( list( 'h' ), charArray( new char[]{'h'} ) );
        assertEqualValues( list( 'h', 'i' ), charArray( new char[]{'h', 'i'} ) );
        assertEqualValues( list( 1.0 ), doubleArray( new double[]{1.0} ) );
        assertEqualValues( list( 1.0, 2.0 ), doubleArray( new double[]{1.0, 2.0} ) );
        assertEqualValues( list( 1.5f ), floatArray( new float[]{1.5f} ) );
        assertEqualValues( list( 1.5f, -5f ), floatArray( new float[]{1.5f, -5f} ) );
        assertEqualValues( list( 1 ), intArray( new int[]{1} ) );
        assertEqualValues( list( 1, -3 ), intArray( new int[]{1, -3} ) );
        assertEqualValues( list( 2L ), longArray( new long[]{2L} ) );
        assertEqualValues( list( 2L, -3L ), longArray( new long[]{2L, -3L} ) );
        assertEqualValues( list( (short) 2 ), shortArray( new short[]{(short) 2} ) );
        assertEqualValues( list( (short) 2, (short) -3 ), shortArray( new short[]{(short) 2, (short) -3} ) );
        assertEqualValues( list( "hi" ), stringArray( "hi" ) );
        assertEqualValues( list( "hi", "ho" ), stringArray( "hi", "ho" ) );
    }

    @Test
    public void shouldNotEqual()
    {
        assertNotEqual( list(), list( 2 ) );
        assertNotEqual( list(), list( 1, 2 ) );
        assertNotEqual( list( 1 ), list( 2 ) );
        assertNotEqual( list( 1 ), list( 1, 2 ) );
        assertNotEqual( list( 1, 1 ), list( 1, 2 ) );
        assertNotEqual( list( 1, "d" ), list( 1, "f" ) );
        assertNotEqual( list( 1, "d" ), list( "d", 1 ) );
        assertNotEqual( list( "d" ), list( false ) );
        assertNotEqual(
                list( Values.stringArray( "d" ) ),
                list( "d" ) );

        assertNotEqual(
                list( longArray( new long[]{3, 4, 5} ) ),
                list( intArray( new int[]{3, 4, 50} ) ) );

        // different value types
        assertNotEqual( list( true, true ), intArray( new int[]{0, 0} ) );
        assertNotEqual( list( true, true ), longArray( new long[]{0L, 0L} ) );
        assertNotEqual( list( true, true ), shortArray( new short[]{(short) 0, (short) 0} ) );
        assertNotEqual( list( true, true ), floatArray( new float[]{0.0f, 0.0f} ) );
        assertNotEqual( list( true, true ), doubleArray( new double[]{0.0, 0.0} ) );
        assertNotEqual( list( true, true ), charArray( new char[]{'T', 'T'} ) );
        assertNotEqual( list( true, true ), stringArray( "True", "True" ) );
        assertNotEqual( list( true, true ), byteArray( new byte[]{(byte) 0, (byte) 0} ) );

        // wrong or missing items
        assertNotEqual( list( true ), booleanArray( new boolean[]{true, false} ) );
        assertNotEqual( list( true, true ), booleanArray( new boolean[]{true, false} ) );
        assertNotEqual( list( 84, 104, 32, 105, 115, 32, 106, 117, 115, 116, 32, 97, 32, 116, 101, 115, 116 ),
                byteArray( "This is just a test".getBytes() ) );
        assertNotEqual( list( 'h' ), charArray( new char[]{'h', 'i'} ) );
        assertNotEqual( list( 'h', 'o' ), charArray( new char[]{'h', 'i'} ) );
        assertNotEqual( list( 9.0, 2.0 ), doubleArray( new double[]{1.0, 2.0} ) );
        assertNotEqual( list( 1.0 ), doubleArray( new double[]{1.0, 2.0} ) );
        assertNotEqual( list( 1.5f ), floatArray( new float[]{1.5f, -5f} ) );
        assertNotEqual( list( 1.5f, 5f ), floatArray( new float[]{1.5f, -5f} ) );
        assertNotEqual( list( 1, 3 ), intArray( new int[]{1, -3} ) );
        assertNotEqual( list( -3 ), intArray( new int[]{1, -3} ) );
        assertNotEqual( list( 2L, 3L ), longArray( new long[]{2L, -3L} ) );
        assertNotEqual( list( 2L ), longArray( new long[]{2L, -3L} ) );
        assertNotEqual( list( (short) 2, (short) 3 ), shortArray( new short[]{(short) 2, (short) -3} ) );
        assertNotEqual( list( (short) 2 ), shortArray( new short[]{(short) 2, (short) -3} ) );
        assertNotEqual( list( "hi", "hello" ), stringArray( "hi" ) );
        assertNotEqual( list( "hello" ), stringArray( "hi" ) );

        assertNotEqual( list( 1, 'b' ), charArray( new char[]{'a', 'b'} ) );
    }

    @Test
    public void shouldHandleNullInList()
    {
        assertIncomparable( list( 1, null ), list( 1, 2 ) );
        assertNotEqual( list( 1, null ), list( 2, 3 ) );
    }

    @Test
    public void shouldCoerce()
    {
        assertEqual(
                list( new String[]{"h"}, 3.0 ),
                list( new char[]{'h'}, 3 ) );

        assertEqualValues( list( "a", 'b' ), charArray( new char[]{'a', 'b'} ) );
    }

    @Test
    public void shouldRecurse()
    {
        assertEqual(
                list( 'a', list( 'b', list( 'c' ) ) ),
                list( 'a', list( 'b', list( 'c' ) ) ) );
    }

    @Test
    public void shouldNestCorrectly()
    {
        assertEqual(
                list(
                        booleanArray( new boolean[]{true, false} ),
                        intArray( new int[]{1, 2} ),
                        stringArray( "Hello", "World" ) ),
                list(
                        booleanArray( new boolean[]{true, false} ),
                        intArray( new int[]{1, 2} ),
                        stringArray( "Hello", "World" ) ) );

        assertNotEqual(
                list(
                        booleanArray( new boolean[]{true, false} ),
                        intArray( new int[]{5, 2} ),
                        stringArray( "Hello", "World" ) ),
                list(
                        booleanArray( new boolean[]{true, false} ),
                        intArray( new int[]{1, 2} ),
                        stringArray( "Hello", "World" ) ) );

        assertNotEqual(
                list(
                        intArray( new int[]{1, 2} ),
                        booleanArray( new boolean[]{true, false} ),
                        stringArray( "Hello", "World" ) ),
                list(
                        booleanArray( new boolean[]{true, false} ),
                        intArray( new int[]{1, 2} ),
                        stringArray( "Hello", "World" ) ) );
    }

    @Test
    public void shouldRecurseAndCoerce()
    {
        assertEqual(
                list( "a", list( 'b', list( "c" ) ) ),
                list( 'a', list( "b", list( 'c' ) ) ) );
    }

    private ListValue[] equivalentLists =
            {VirtualValues.list( Values.longValue( 1L ), Values.longValue( 4L ), Values.longValue( 7L ) ),

                    range( 1L, 8L, 3L ),
                    VirtualValues.fromArray( Values.longArray( new long[]{1L, 4L, 7L} ) ),
                    VirtualValues.dropNoValues( VirtualValues.list( NO_VALUE,
                                                                    longValue( 1L ),
                                                                    NO_VALUE,
                                                                    longValue( 4L ),
                                                                    longValue( 7L ),
                                                                    NO_VALUE ) ),
                    VirtualValues.slice( list( -2L, 1L, 4L, 7L, 10L ), 1, 4 ),
                    VirtualValues.drop( list( -2L, 1L, 4L, 7L ), 1 ),
                    VirtualValues.take( list( 1L, 4L, 7L, 10L, 13L ), 3 ),
                    VirtualValues.reverse( list( 7L, 4L, 1L ) ),
                    VirtualValues.concat( list( 1L, 4L ), list( 7L ) )
            };

    private ListValue[] nonEquivalentLists =
            {VirtualValues.list( Values.longValue( 1L ), Values.longValue( 4L ), Values.longValue( 7L ) ),

                    range( 2L, 9L, 3L ),
                    VirtualValues.fromArray( Values.longArray( new long[]{3L, 6L, 9L} ) ),
                    VirtualValues.dropNoValues( VirtualValues.list( NO_VALUE,
                                                                    longValue( 1L ),
                                                                    NO_VALUE,
                                                                    longValue( 5L ),
                                                                    longValue( 7L ),
                                                                    NO_VALUE ) ),
                    VirtualValues.slice( list( -2L, 1L, 5L, 8L, 11L ), 1, 4 ),
                    VirtualValues.drop( list( -2L, 6L, 9L, 12L ), 1 ),
                    VirtualValues.take( list( 7L, 10L, 13L, 10L, 13L ), 3 ),
                    VirtualValues.reverse( list( 15L, 12L, 9L ) ),
                    VirtualValues.concat( list( 10L, 13L ), list( 16L ) )
            };

    @Test
    public void shouldTreatDifferentListImplementationSimilar()
    {
        for ( ListValue list1 : equivalentLists )
        {
            for ( ListValue list2 : equivalentLists )
            {
                assertEqual( list1, list2 );
                assertArrayEquals(
                        format( "%s.asArray != %s.toArray", list1.getClass().getSimpleName(),
                                list2.getClass().getSimpleName() ),
                        list1.asArray(), list2.asArray() );
            }
        }
    }

    @Test
    public void shouldNotTreatDifferentListImplementationSimilarOfNonEquivalentListsSimilar()
    {
        for ( ListValue list1 : nonEquivalentLists )
        {
            for ( ListValue list2 : nonEquivalentLists )
            {
                if ( list1 == list2 )
                {
                    continue;
                }
                assertNotEqual( list1, list2 );
                assertFalse(
                        format( "%s.asArray != %s.toArray", list1.getClass().getSimpleName(),
                                list2.getClass().getSimpleName() ),
                        Arrays.equals( list1.asArray(), list2.asArray() ) );
            }
        }
    }
}
