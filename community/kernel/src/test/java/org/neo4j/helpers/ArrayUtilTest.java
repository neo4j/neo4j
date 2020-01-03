/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.helpers;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.helpers.collection.Iterators.asSet;

class ArrayUtilTest
{
    @Test
    void shouldProduceUnionOfTwoArrays()
    {
        // GIVEN
        String[] first = {"one", "three"};
        String[] other = {"two", "four", "five"};

        // WHEN
        String[] union = ArrayUtil.union( first, other );

        // THEN
        assertEquals( asSet( "one", "two", "three", "four", "five" ),
                      asSet( union ) );
    }

    @Test
    void shouldProduceUnionWhereFirstIsNull()
    {
        // GIVEN
        String[] first = null;
        String[] other = {"one", "two"};

        // WHEN
        String[] union = ArrayUtil.union( first, other );

        // THEN
        assertEquals( asSet( "one", "two" ), asSet( union ) );
    }

    @Test
    void shouldProduceUnionWhereOtherIsNull()
    {
        // GIVEN
        String[] first = {"one", "two"};
        String[] other = null;

        // WHEN
        String[] union = ArrayUtil.union( first, other );

        // THEN
        assertEquals( asSet( "one", "two" ), asSet( union ) );
    }

    @Test
    void shouldCheckNullSafeEqual()
    {
        // WHEN/THEN
        assertTrue( ArrayUtil.nullSafeEquals( null, null ) );
        assertFalse( ArrayUtil.nullSafeEquals( "1", null ) );
        assertFalse( ArrayUtil.nullSafeEquals( null, "1" ) );
        assertTrue( ArrayUtil.nullSafeEquals( "1", "1" ) );
    }

    @Test
    void emptyArray()
    {
        assertTrue( ArrayUtil.isEmpty( null ) );
        assertTrue( ArrayUtil.isEmpty( new String[] {} ) );
        assertFalse( ArrayUtil.isEmpty( new Long[] { 1L } ) );
    }

    @Test
    void shouldConcatOneAndMany()
    {
        // WHEN
        Integer[] result = ArrayUtil.concat( 0, 1, 2, 3, 4 );

        // THEN
        for ( int i = 0; i < 5; i++ )
        {
            assertEquals( (Integer)i, result[i] );
        }
    }

    @Test
    void shouldConcatSeveralArrays()
    {
        // GIVEN
        Integer[] a = {0, 1, 2};
        Integer[] b = {3, 4};
        Integer[] c = {5, 6, 7, 8};

        // WHEN
        Integer[] result = ArrayUtil.concatArrays( a, b, c );

        // THEN
        assertEquals( a.length + b.length + c.length, result.length );

        for ( int i = 0; i < result.length; i++ )
        {
            assertEquals( (Integer) i, result[i] );
        }
    }

    @Test
    void shouldFindIndexOf()
    {
        // GIVEN
        Integer[] numbers = ArrayUtil.concat( 0, 1, 2, 3, 4, 5 );

        // WHEN/THEN
        for ( int i = 0; i < 6; i++ )
        {
            assertEquals( i, ArrayUtil.indexOf( numbers, i ) );
        }
    }

    @Test
    void shouldFindLastOf()
    {
        // GIVEN
        Integer[] numbers = new Integer[]{0, 100, 4, 5, 6, 3};

        // WHEN/THEN
        assertEquals( 3, (int) ArrayUtil.lastOf( numbers ) );
    }

    @Test
    void shouldRemoveItems()
    {
        // GIVEN
        Integer[] numbers = ArrayUtil.concat( 0, 1, 2, 3, 4, 5 );

        // WHEN
        Integer[] trimmed = ArrayUtil.without( numbers, 2 );
        trimmed = ArrayUtil.without( trimmed, 5 );
        trimmed = ArrayUtil.without( trimmed, 0 );

        // THEN
        assertEquals( 3, trimmed.length );
        assertFalse( ArrayUtil.contains( trimmed, 0 ) );
        assertTrue( ArrayUtil.contains( trimmed, 1 ) );
        assertFalse( ArrayUtil.contains( trimmed, 2 ) );
        assertTrue( ArrayUtil.contains( trimmed, 3 ) );
        assertTrue( ArrayUtil.contains( trimmed, 4 ) );
        assertFalse( ArrayUtil.contains( trimmed, 5 ) );
    }

    @Test
    void shouldConcatArrays()
    {
        // GIVEN
        Integer[] initial = new Integer[] {0, 1, 2};

        // WHEN
        Integer[] all = ArrayUtil.concat( initial, 3, 4, 5 );

        // THEN
        assertArrayEquals( new Integer[] {0, 1, 2, 3, 4, 5}, all );
    }

    @Test
    void shouldReverseEvenCount()
    {
        // given
        Integer[] array = new Integer[] {0, 1, 2, 3, 4, 5};

        // when
        ArrayUtil.reverse( array );

        // then
        assertArrayEquals( new Integer[] {5, 4, 3, 2, 1, 0}, array );
    }

    @Test
    void shouldReverseUnevenCount()
    {
        // given
        Integer[] array = new Integer[] {0, 1, 2, 3, 4};

        // when
        ArrayUtil.reverse( array );

        // then
        assertArrayEquals( new Integer[] {4, 3, 2, 1, 0}, array );
    }

    @Test
    void shouldReverseEmptyArray()
    {
        // given
        Integer[] array = new Integer[] {};

        // when
        ArrayUtil.reverse( array );

        // then
        assertArrayEquals( new Integer[] {}, array );
    }
}
