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
package org.neo4j.helpers;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.neo4j.helpers.collection.IteratorUtil.asSet;

public class ArrayUtilTest
{
    @Test
    public void shouldProduceUnionOfTwoArrays() throws Exception
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
    public void shouldProduceUnionWhereFirstIsNull() throws Exception
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
    public void shouldProduceUnionWhereOtherIsNull() throws Exception
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
    public void shouldCheckNullSafeEqual() throws Exception
    {
        // WHEN/THEN
        assertTrue( ArrayUtil.nullSafeEquals( null, null ) );
        assertFalse( ArrayUtil.nullSafeEquals( "1", null ) );
        assertFalse( ArrayUtil.nullSafeEquals( null, "1" ) );
        assertTrue( ArrayUtil.nullSafeEquals( "1", "1" ) );
    }

    @Test
    public void emptyArray()
    {
        assertTrue( ArrayUtil.isEmpty( null ) );
        assertTrue( ArrayUtil.isEmpty( new String[] {} ) );
        assertFalse( ArrayUtil.isEmpty( new Long[] { 1L } ) );
    }

    @Test
    public void shouldConcatOneAndMany() throws Exception
    {
        // WHEN
        Integer[] result = ArrayUtil.concat( 0, 1, 2, 3, 4 );

        // THEN
        for ( int i = 0; i < result.length; i++ )
        {
            assertEquals( (Integer)i, result[i] );
        }
    }

    @Test
    public void shouldFindIndexOf() throws Exception
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
    public void shouldRemoveItems() throws Exception
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
    public void shouldConcatArrays() throws Exception
    {
        // GIVEN
        Integer[] initial = new Integer[] {0, 1, 2};

        // WHEN
        Integer[] all = ArrayUtil.concat( initial, 3, 4, 5 );

        // THEN
        assertArrayEquals( new Integer[] {0, 1, 2, 3, 4, 5}, all );
    }
}
