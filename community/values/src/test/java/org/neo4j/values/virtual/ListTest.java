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
package org.neo4j.values.virtual;

import org.junit.Test;

import org.neo4j.values.storable.Values;

import static org.neo4j.values.storable.Values.booleanArray;
import static org.neo4j.values.storable.Values.byteArray;
import static org.neo4j.values.storable.Values.charArray;
import static org.neo4j.values.storable.Values.doubleArray;
import static org.neo4j.values.storable.Values.floatArray;
import static org.neo4j.values.storable.Values.intArray;
import static org.neo4j.values.storable.Values.longArray;
import static org.neo4j.values.storable.Values.shortArray;
import static org.neo4j.values.storable.Values.stringArray;
import static org.neo4j.values.virtual.VirtualValueTestUtil.assertEqual;
import static org.neo4j.values.virtual.VirtualValueTestUtil.assertEqualValues;
import static org.neo4j.values.virtual.VirtualValueTestUtil.assertNotEqual;
import static org.neo4j.values.virtual.VirtualValueTestUtil.assertNotEqualValues;
import static org.neo4j.values.virtual.VirtualValueTestUtil.list;

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
        assertEqualValues( list(), stringArray( new String[]{} ) );

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
        assertEqualValues( list( 2l ), longArray( new long[]{2l} ) );
        assertEqualValues( list( 2l, -3l ), longArray( new long[]{2l, -3l} ) );
        assertEqualValues( list( (short) 2 ), shortArray( new short[]{(short) 2} ) );
        assertEqualValues( list( (short) 2l, (short) -3l ), shortArray( new short[]{(short) 2l, (short) -3l} ) );
        assertEqualValues( list( "hi" ), stringArray( new String[]{"hi"} ) );
        assertEqualValues( list( "hi", "ho" ), stringArray( new String[]{"hi", "ho"} ) );
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
                list( Values.stringArray( new String[]{"d"} ) ),
                list( "d" ) );

        assertNotEqual(
                list( longArray( new long[]{3, 4, 5} ) ),
                list( intArray( new int[]{3, 4, 50} ) ) );

        // different value types
        assertNotEqualValues( list( true, true ), intArray( new int[]{0, 0} ) );
        assertNotEqualValues( list( true, true ), longArray( new long[]{0L, 0L} ) );
        assertNotEqualValues( list( true, true ), shortArray( new short[]{(short)0,(short) 0} ) );
        assertNotEqualValues( list( true, true ), floatArray( new float[]{0.0f, 0.0f} ) );
        assertNotEqualValues( list( true, true ), doubleArray( new double[]{0.0, 0.0} ) );
        assertNotEqualValues( list( true, true ), charArray( new char[]{'T','T'} ) );
        assertNotEqualValues( list( true, true ), stringArray( new String[]{"True","True"} ) );
        assertNotEqualValues( list( true, true ), byteArray( new byte[]{(byte)0,(byte)0} ) );

        // wrong or missing items
        assertNotEqualValues( list( true ), booleanArray( new boolean[]{true, false} ) );
        assertNotEqualValues( list( true, true ), booleanArray( new boolean[]{true, false} ) );
        assertNotEqualValues( list( 84, 104, 32, 105, 115, 32, 106, 117, 115, 116, 32, 97, 32, 116, 101, 115, 116 ),
                byteArray( "This is just a test".getBytes() ) );
        assertNotEqualValues( list( 'h' ), charArray( new char[]{'h', 'i'} ) );
        assertNotEqualValues( list( 'h', 'o' ), charArray( new char[]{'h', 'i'} ) );
        assertNotEqualValues( list( 9.0, 2.0 ), doubleArray( new double[]{1.0, 2.0} ) );
        assertNotEqualValues( list( 1.0 ), doubleArray( new double[]{1.0, 2.0} ) );
        assertNotEqualValues( list( 1.5f ), floatArray( new float[]{1.5f, -5f} ) );
        assertNotEqualValues( list( 1.5f, 5f ), floatArray( new float[]{1.5f, -5f} ) );
        assertNotEqualValues( list( 1, 3 ), intArray( new int[]{1, -3} ) );
        assertNotEqualValues( list( -3 ), intArray( new int[]{1, -3} ) );
        assertNotEqualValues( list( 2l, 3l ), longArray( new long[]{2l, -3l} ) );
        assertNotEqualValues( list( 2l ), longArray( new long[]{2l, -3l} ) );
        assertNotEqualValues( list( (short) 2l, (short) 3l ), shortArray( new short[]{(short) 2l, (short) -3l} ) );
        assertNotEqualValues( list( (short) 2l ), shortArray( new short[]{(short) 2l, (short) -3l} ) );
        assertNotEqualValues( list( "hi", "hello" ), stringArray( new String[]{"hi"} ) );
        assertNotEqualValues( list( "hello" ), stringArray( new String[]{"hi"} ) );
    }

    @Test
    public void shouldCoerce()
    {
        assertEqual(
                list( new String[]{"h"}, 3.0 ),
                list( new char[]{'h'}, 3 ) );
    }

    @Test
    public void shouldRecurse()
    {
        assertEqual(
                list( 'a', list( 'b', list( 'c' ) ) ),
                list( 'a', list( 'b', list( 'c' ) ) ) );
    }

    @Test
    public void shouldRecurseAndCoerce()
    {
        assertEqual(
                list( "a", list( 'b', list( "c" ) ) ),
                list( 'a', list( "b", list( 'c' ) ) ) );
    }
}
