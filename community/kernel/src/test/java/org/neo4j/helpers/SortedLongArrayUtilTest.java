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
package org.neo4j.helpers;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.neo4j.test.assertion.Assert.assertException;

public class SortedLongArrayUtilTest
{
    private static final long[] EMPTY = new long[0];
    private static final long[] ONE_VALUE = new long[]{1};

    // union()

    @Test
    public void union_shouldHandleNullInput()
    {
        assertThat( SortedLongArrayUtil.union( null, null ), nullValue() );
        assertThat( SortedLongArrayUtil.union( null, EMPTY ), equalTo( EMPTY ) );
        assertThat( SortedLongArrayUtil.union( EMPTY, null ), equalTo( EMPTY ) );
        assertThat( SortedLongArrayUtil.union( null, ONE_VALUE ), equalTo( ONE_VALUE ) );
        assertThat( SortedLongArrayUtil.union( ONE_VALUE, null ), equalTo( ONE_VALUE ) );
    }

    @Test
    public void union_shouldHandleNonIntersectingArrays()
    {
        assertThat( SortedLongArrayUtil.union( new long[]{1, 2, 3}, new long[]{4, 5, 6} ),
                isArray( 1, 2, 3, 4, 5, 6 ) );

        assertThat( SortedLongArrayUtil.union( new long[]{1, 2, 3}, new long[]{14, 15, 16} ),
                isArray( 1, 2, 3, 14, 15, 16 ) );

        assertThat( SortedLongArrayUtil.union( new long[]{14, 15, 16}, new long[]{1, 2, 3} ),
                isArray( 1, 2, 3, 14, 15, 16 ) );
    }

    @Test
    public void union_shouldHandleIntersectingArrays()
    {
        assertThat( SortedLongArrayUtil.union( new long[]{1, 2, 3}, new long[]{3, 4, 5} ),
                isArray( 1, 2, 3, 4, 5 ) );

        assertThat( SortedLongArrayUtil.union( new long[]{3, 4, 5}, new long[]{1, 2, 3} ),
                isArray( 1, 2, 3, 4, 5 ) );
    }

    @Test
    public void union_shouldHandleComplexIntersectingArraysWithGaps()
    {
        assertThat(
                SortedLongArrayUtil.union( new long[]{4, 6, 9, 11, 12, 15}, new long[]{2, 3, 4, 7, 8, 9, 12, 16, 19} ),
                isArray( 2, 3, 4, 6, 7, 8, 9, 11, 12, 15, 16, 19 ) );
        assertThat(
                SortedLongArrayUtil.union( new long[]{2, 3, 4, 7, 8, 9, 12, 16, 19}, new long[]{4, 6, 9, 11, 12, 15} ),
                isArray( 2, 3, 4, 6, 7, 8, 9, 11, 12, 15, 16, 19 ) );
    }

    // intersect()

    @Test
    public void intersect_shouldHandleNullInput()
    {
        assertThat( SortedLongArrayUtil.intersect( null, null ), equalTo( EMPTY ) );
        assertThat( SortedLongArrayUtil.intersect( null, EMPTY ), equalTo( EMPTY ) );
        assertThat( SortedLongArrayUtil.intersect( EMPTY, null ), equalTo( EMPTY ) );
        assertThat( SortedLongArrayUtil.intersect( null, ONE_VALUE ), equalTo( EMPTY ) );
        assertThat( SortedLongArrayUtil.intersect( ONE_VALUE, null ), equalTo( EMPTY ) );
    }

    @Test
    public void intersect_shouldHandleNonIntersectingArrays()
    {
        assertThat( SortedLongArrayUtil.intersect( new long[]{1, 2, 3}, new long[]{4, 5, 6} ),
                equalTo( EMPTY ) );

        assertThat( SortedLongArrayUtil.intersect( new long[]{14, 15, 16}, new long[]{1, 2, 3} ),
                equalTo( EMPTY ) );
    }

    @Test
    public void intersect_shouldHandleIntersectingArrays()
    {
        assertThat( SortedLongArrayUtil.intersect( new long[]{1, 2, 3}, new long[]{3, 4, 5} ),
                isArray( 3 ) );

        assertThat( SortedLongArrayUtil.intersect( new long[]{3, 4, 5}, new long[]{1, 2, 3, 4} ),
                isArray( 3, 4 ) );
    }

    @Test
    public void intersect_shouldHandleComplexIntersectingArraysWithGaps()
    {
        assertThat(
                SortedLongArrayUtil.intersect( new long[]{4, 6, 9, 11, 12, 15}, new long[]{2, 3, 4, 7, 8, 9, 12, 16, 19} ),
                isArray( 4, 9, 12 ) );
        assertThat(
                SortedLongArrayUtil.intersect( new long[]{2, 3, 4, 7, 8, 9, 12, 16, 19}, new long[]{4, 6, 9, 11, 12, 15} ),
                isArray( 4, 9, 12 ) );
    }

    // symmetricDifference()

    @Test
    public void symDiff_shouldHandleNullInput()
    {
        assertThat( SortedLongArrayUtil.symmetricDifference( null, null ), equalTo( null ) );
        assertThat( SortedLongArrayUtil.symmetricDifference( null, EMPTY ), equalTo( EMPTY ) );
        assertThat( SortedLongArrayUtil.symmetricDifference( EMPTY, null ), equalTo( EMPTY ) );
        assertThat( SortedLongArrayUtil.symmetricDifference( null, ONE_VALUE ), equalTo( ONE_VALUE ) );
        assertThat( SortedLongArrayUtil.symmetricDifference( ONE_VALUE, null ), equalTo( ONE_VALUE ) );
    }

    @Test
    public void symDiff_shouldHandleNonIntersectingArrays()
    {
        assertThat( SortedLongArrayUtil.symmetricDifference( new long[]{1, 2, 3}, new long[]{4, 5, 6} ),
                isArray( 1, 2, 3, 4, 5, 6 ) );

        assertThat( SortedLongArrayUtil.symmetricDifference( new long[]{14, 15, 16}, new long[]{1, 2, 3} ),
                isArray( 1, 2, 3, 14, 15, 16 ) );
    }

    @Test
    public void symDiff_shouldHandleIntersectingArrays()
    {
        assertThat( SortedLongArrayUtil.symmetricDifference( new long[]{1, 2, 3}, new long[]{3, 4, 5} ),
                isArray( 1, 2, 4, 5 ) );

        assertThat( SortedLongArrayUtil.symmetricDifference( new long[]{3, 4, 5}, new long[]{1, 2, 3, 4} ),
                isArray( 1, 2, 5 ) );
    }

    @Test
    public void symDiff_shouldHandleComplexIntersectingArraysWithGaps()
    {
        assertThat(
                SortedLongArrayUtil.symmetricDifference( new long[]{4, 6, 9, 11, 12, 15}, new long[]{2, 3, 4, 7, 8, 9, 12, 16, 19} ),
                isArray( 2, 3, 6, 7, 8, 11, 15, 16, 19 ) );
        assertThat(
                SortedLongArrayUtil.symmetricDifference( new long[]{2, 3, 4, 7, 8, 9, 12, 16, 19}, new long[]{4, 6, 9, 11, 12, 15} ),
                isArray( 2, 3, 6, 7, 8, 11, 15, 16, 19 ) );
    }

    // count unique

    @Test
    public void shouldCountUnique()
    {
        assertThat(
                SortedLongArrayUtil.countUnique( new long[]{1, 2, 3}, new long[]{4, 5, 6} ),
                isIntPair( 3, 3 ) );

        assertThat(
                SortedLongArrayUtil.countUnique( new long[]{1, 2, 3}, new long[]{3, 6} ),
                isIntPair( 2, 1 ) );

        assertThat(
                SortedLongArrayUtil.countUnique( new long[]{1, 2, 3}, new long[]{3} ),
                isIntPair( 2, 0 ) );

        assertThat(
                SortedLongArrayUtil.countUnique( new long[]{3}, new long[]{1, 2, 3} ),
                isIntPair( 0, 2 ) );

        assertThat(
                SortedLongArrayUtil.countUnique( new long[]{3}, new long[]{3} ),
                isIntPair( 0, 0 ) );

        assertThat(
                SortedLongArrayUtil.countUnique( new long[]{3, 6, 8}, new long[]{} ),
                isIntPair( 3, 0 ) );

        assertThat(
                SortedLongArrayUtil.countUnique( new long[]{}, new long[]{3, 6, 8} ),
                isIntPair( 0, 3 ) );

        assertThat(
                SortedLongArrayUtil.countUnique( new long[]{}, new long[]{} ),
                isIntPair( 0, 0 ) );

        assertThat(
                SortedLongArrayUtil.countUnique( new long[]{4, 6, 9, 11, 12, 15}, new long[]{2, 3, 4, 7, 8, 9, 12, 16, 19} ),
                isIntPair( 3, 6 ) );
    }

    // helpers

    private Matcher<Long> isIntPair( int left, int right )
    {
        return new BaseMatcher<Long>()
        {
            @Override
            public void describeTo( Description description )
            {
                description.appendValue( left );
                description.appendValue( right );
            }

            @Override
            public boolean matches( Object o )
            {
                return o instanceof Long && ((Long) o) == ((long) left << 32 | right);
            }
        };
    }

    private static Matcher<long[]> isArray( long... values )
    {
        return equalTo( values );
    }
}
