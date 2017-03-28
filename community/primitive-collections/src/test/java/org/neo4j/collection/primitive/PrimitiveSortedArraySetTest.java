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
package org.neo4j.collection.primitive;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class PrimitiveSortedArraySetTest
{
    private static final int[] NO_INTS = new int[0];
    private static final int[] ONE_INT = new int[]{1};
    private static final long[] NO_LONGS = new long[0];
    private static final long[] ONE_LONG = new long[]{1};

    // union() null checks. Actual behaviour is tested in PrimitiveSortedArraySetUnionTest

    @Test
    public void union_shouldHandleNullInput()
    {
        assertThat( PrimitiveSortedArraySet.union( null, null ), nullValue() );
        assertThat( PrimitiveSortedArraySet.union( null, NO_INTS ), equalTo( NO_INTS ) );
        assertThat( PrimitiveSortedArraySet.union( NO_INTS, null ), equalTo( NO_INTS ) );
        assertThat( PrimitiveSortedArraySet.union( null, ONE_INT ), equalTo( ONE_INT ) );
        assertThat( PrimitiveSortedArraySet.union( ONE_INT, null ), equalTo( ONE_INT ) );
    }

    // intersect()

    @Test
    public void intersect_shouldHandleNullInput()
    {
        assertThat( PrimitiveSortedArraySet.intersect( null, null ), equalTo( NO_LONGS ) );
        assertThat( PrimitiveSortedArraySet.intersect( null, NO_LONGS ), equalTo( NO_LONGS ) );
        assertThat( PrimitiveSortedArraySet.intersect( NO_LONGS, null ), equalTo( NO_LONGS ) );
        assertThat( PrimitiveSortedArraySet.intersect( null, ONE_LONG ), equalTo( NO_LONGS ) );
        assertThat( PrimitiveSortedArraySet.intersect( ONE_LONG, null ), equalTo( NO_LONGS ) );
    }

    @Test
    public void intersect_shouldHandleNonIntersectingArrays()
    {
        assertThat( PrimitiveSortedArraySet.intersect( new long[]{1, 2, 3}, new long[]{4, 5, 6} ),
                equalTo( NO_LONGS ) );

        assertThat( PrimitiveSortedArraySet.intersect( new long[]{14, 15, 16}, new long[]{1, 2, 3} ),
                equalTo( NO_LONGS ) );
    }

    @Test
    public void intersect_shouldHandleIntersectingArrays()
    {
        assertThat( PrimitiveSortedArraySet.intersect( new long[]{1, 2, 3}, new long[]{3, 4, 5} ),
                isArray( 3 ) );

        assertThat( PrimitiveSortedArraySet.intersect( new long[]{3, 4, 5}, new long[]{1, 2, 3, 4} ),
                isArray( 3, 4 ) );
    }

    @Test
    public void intersect_shouldHandleComplexIntersectingArraysWithGaps()
    {
        assertThat(
                PrimitiveSortedArraySet.intersect( new long[]{4, 6, 9, 11, 12, 15}, new long[]{2, 3, 4, 7, 8, 9, 12, 16, 19} ),
                isArray( 4, 9, 12 ) );
        assertThat(
                PrimitiveSortedArraySet.intersect( new long[]{2, 3, 4, 7, 8, 9, 12, 16, 19}, new long[]{4, 6, 9, 11, 12, 15} ),
                isArray( 4, 9, 12 ) );
    }

    // symmetricDifference()

    @Test
    public void symDiff_shouldHandleNullInput()
    {
        assertThat( PrimitiveSortedArraySet.symmetricDifference( null, null ), equalTo( null ) );
        assertThat( PrimitiveSortedArraySet.symmetricDifference( null, NO_LONGS ), equalTo( NO_LONGS ) );
        assertThat( PrimitiveSortedArraySet.symmetricDifference( NO_LONGS, null ), equalTo( NO_LONGS ) );
        assertThat( PrimitiveSortedArraySet.symmetricDifference( null, ONE_LONG ), equalTo( ONE_LONG ) );
        assertThat( PrimitiveSortedArraySet.symmetricDifference( ONE_LONG, null ), equalTo( ONE_LONG ) );
    }

    @Test
    public void symDiff_shouldHandleNonIntersectingArrays()
    {
        assertThat( PrimitiveSortedArraySet.symmetricDifference( new long[]{1, 2, 3}, new long[]{4, 5, 6} ),
                isArray( 1, 2, 3, 4, 5, 6 ) );

        assertThat( PrimitiveSortedArraySet.symmetricDifference( new long[]{14, 15, 16}, new long[]{1, 2, 3} ),
                isArray( 1, 2, 3, 14, 15, 16 ) );
    }

    @Test
    public void symDiff_shouldHandleIntersectingArrays()
    {
        assertThat( PrimitiveSortedArraySet.symmetricDifference( new long[]{1, 2, 3}, new long[]{3, 4, 5} ),
                isArray( 1, 2, 4, 5 ) );

        assertThat( PrimitiveSortedArraySet.symmetricDifference( new long[]{3, 4, 5}, new long[]{1, 2, 3, 4} ),
                isArray( 1, 2, 5 ) );
    }

    @Test
    public void symDiff_shouldHandleComplexIntersectingArraysWithGaps()
    {
        assertThat(
                PrimitiveSortedArraySet.symmetricDifference( new long[]{4, 6, 9, 11, 12, 15}, new long[]{2, 3, 4, 7, 8, 9, 12, 16, 19} ),
                isArray( 2, 3, 6, 7, 8, 11, 15, 16, 19 ) );
        assertThat(
                PrimitiveSortedArraySet.symmetricDifference( new long[]{2, 3, 4, 7, 8, 9, 12, 16, 19}, new long[]{4, 6, 9, 11, 12, 15} ),
                isArray( 2, 3, 6, 7, 8, 11, 15, 16, 19 ) );
    }

    // count unique

    @Test
    public void shouldCountUnique()
    {
        assertThat(
                PrimitiveSortedArraySet.countUnique( new long[]{1, 2, 3}, new long[]{4, 5, 6} ),
                isIntPair( 3, 3 ) );

        assertThat(
                PrimitiveSortedArraySet.countUnique( new long[]{1, 2, 3}, new long[]{3, 6} ),
                isIntPair( 2, 1 ) );

        assertThat(
                PrimitiveSortedArraySet.countUnique( new long[]{1, 2, 3}, new long[]{3} ),
                isIntPair( 2, 0 ) );

        assertThat(
                PrimitiveSortedArraySet.countUnique( new long[]{3}, new long[]{1, 2, 3} ),
                isIntPair( 0, 2 ) );

        assertThat(
                PrimitiveSortedArraySet.countUnique( new long[]{3}, new long[]{3} ),
                isIntPair( 0, 0 ) );

        assertThat(
                PrimitiveSortedArraySet.countUnique( new long[]{3, 6, 8}, new long[]{} ),
                isIntPair( 3, 0 ) );

        assertThat(
                PrimitiveSortedArraySet.countUnique( new long[]{}, new long[]{3, 6, 8} ),
                isIntPair( 0, 3 ) );

        assertThat(
                PrimitiveSortedArraySet.countUnique( new long[]{}, new long[]{} ),
                isIntPair( 0, 0 ) );

        assertThat(
                PrimitiveSortedArraySet.countUnique( new long[]{4, 6, 9, 11, 12, 15}, new long[]{2, 3, 4, 7, 8, 9, 12, 16, 19} ),
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
                return o instanceof Long && ((Long) o) == (((long) left << 32) | right);
            }
        };
    }

    private static Matcher<long[]> isArray( long... values )
    {
        return equalTo( values );
    }
}
