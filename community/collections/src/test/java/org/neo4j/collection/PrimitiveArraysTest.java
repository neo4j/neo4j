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
package org.neo4j.collection;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.internal.Longs;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.collection.PrimitiveArrays.countUnique;

class PrimitiveArraysTest
{
    private static final int[] NO_INTS = new int[0];
    private static final int[] ONE_INT = new int[]{1};
    private static final long[] NO_LONGS = new long[0];
    private static final long[] ONE_LONG = new long[]{1};

    // union() null checks. Actual behaviour is tested in PrimitiveSortedArraySetUnionTest

    @Test
    void union_shouldHandleNullInput()
    {
        assertThat( PrimitiveArrays.union( null, null ) ).isNull();
        assertThat( PrimitiveArrays.union( null, NO_INTS ) ).isEqualTo( NO_INTS );
        assertThat( PrimitiveArrays.union( NO_INTS, null ) ).isEqualTo( NO_INTS );
        assertThat( PrimitiveArrays.union( null, ONE_INT ) ).isEqualTo( ONE_INT );
        assertThat( PrimitiveArrays.union( ONE_INT, null ) ).isEqualTo( ONE_INT );
    }

    // intersect()

    @Test
    void intersect_shouldHandleNullInput()
    {
        assertThat( PrimitiveArrays.intersect( null, null ) ).isEqualTo( NO_LONGS );
        assertThat( PrimitiveArrays.intersect( null, NO_LONGS ) ).isEqualTo( NO_LONGS );
        assertThat( PrimitiveArrays.intersect( NO_LONGS, null ) ).isEqualTo( NO_LONGS );
        assertThat( PrimitiveArrays.intersect( null, ONE_LONG ) ).isEqualTo( NO_LONGS );
        assertThat( PrimitiveArrays.intersect( ONE_LONG, null ) ).isEqualTo( NO_LONGS );
    }

    @Test
    void intersect_shouldHandleNonIntersectingArrays()
    {
        assertThat( PrimitiveArrays.intersect( new long[]{1, 2, 3}, new long[]{4, 5, 6} ) ).isEqualTo( NO_LONGS );

        assertThat( PrimitiveArrays.intersect( new long[]{14, 15, 16}, new long[]{1, 2, 3} ) ).isEqualTo( NO_LONGS );
    }

    @Test
    void intersect_shouldHandleIntersectingArrays()
    {
        assertThat( PrimitiveArrays.intersect( new long[]{1, 2, 3}, new long[]{3, 4, 5} ) ).containsExactly( 3 );

        assertThat( PrimitiveArrays.intersect( new long[]{3, 4, 5}, new long[]{1, 2, 3, 4} ) ).containsExactly( 3, 4 );
    }

    @Test
    void intersect_shouldHandleComplexIntersectingArraysWithGaps()
    {
        assertThat( PrimitiveArrays.intersect( new long[]{4, 6, 9, 11, 12, 15}, new long[]{2, 3, 4, 7, 8, 9, 12, 16, 19} ) ).containsExactly( 4, 9, 12 );
        assertThat( PrimitiveArrays.intersect( new long[]{2, 3, 4, 7, 8, 9, 12, 16, 19}, new long[]{4, 6, 9, 11, 12, 15} ) ).containsExactly( 4, 9, 12 );
    }

    // symmetricDifference()

    @Test
    void symDiff_shouldHandleNullInput()
    {
        assertThat( PrimitiveArrays.symmetricDifference( null, null ) ).isEqualTo( null );
        assertThat( PrimitiveArrays.symmetricDifference( null, NO_LONGS ) ).isEqualTo( NO_LONGS );
        assertThat( PrimitiveArrays.symmetricDifference( NO_LONGS, null ) ).isEqualTo( NO_LONGS );
        assertThat( PrimitiveArrays.symmetricDifference( null, ONE_LONG ) ).isEqualTo( ONE_LONG );
        assertThat( PrimitiveArrays.symmetricDifference( ONE_LONG, null ) ).isEqualTo( ONE_LONG );
    }

    @Test
    void symDiff_shouldHandleNonIntersectingArrays()
    {
        assertThat( PrimitiveArrays.symmetricDifference( new long[]{1, 2, 3}, new long[]{4, 5, 6} ) ).containsExactly( 1, 2, 3, 4, 5, 6 );

        assertThat( PrimitiveArrays.symmetricDifference( new long[]{14, 15, 16}, new long[]{1, 2, 3} ) ).containsExactly( 1, 2, 3, 14, 15, 16 );
    }

    @Test
    void symDiff_shouldHandleIntersectingArrays()
    {
        assertThat( PrimitiveArrays.symmetricDifference( new long[]{1, 2, 3}, new long[]{3, 4, 5} ) ).containsExactly( 1, 2, 4, 5 );

        assertThat( PrimitiveArrays.symmetricDifference( new long[]{3, 4, 5}, new long[]{1, 2, 3, 4} ) ).containsExactly( 1, 2, 5 );
    }

    @Test
    void symDiff_shouldHandleComplexIntersectingArraysWithGaps()
    {
        assertThat( PrimitiveArrays.symmetricDifference( new long[]{4, 6, 9, 11, 12, 15}, new long[]{2, 3, 4, 7, 8, 9, 12, 16, 19} ) ).containsExactly( 2, 3, 6,
                7, 8, 11, 15, 16, 19 );
        assertThat( PrimitiveArrays.symmetricDifference( new long[]{2, 3, 4, 7, 8, 9, 12, 16, 19}, new long[]{4, 6, 9, 11, 12, 15} ) ).containsExactly( 2, 3, 6,
                7, 8, 11, 15, 16, 19 );
    }

    // count unique

    @Test
    void shouldCountUnique()
    {
        IntPairAssert.assertThat( countUnique( new long[]{1, 2, 3}, new long[]{4, 5, 6} ) ).hasCounts( 3, 3 );

        IntPairAssert.assertThat( countUnique( new long[]{1, 2, 3}, new long[]{3, 6} ) ).hasCounts( 2, 1 );

        IntPairAssert.assertThat( countUnique( new long[]{1, 2, 3}, new long[]{3} ) ).hasCounts( 2, 0 );

        IntPairAssert.assertThat( countUnique( new long[]{3}, new long[]{1, 2, 3} ) ).hasCounts( 0, 2 );

        IntPairAssert.assertThat( countUnique( new long[]{3}, new long[]{3} ) ).hasCounts( 0, 0 );

        IntPairAssert.assertThat( countUnique( new long[]{3, 6, 8}, new long[]{} ) ).hasCounts( 3, 0 );

        IntPairAssert.assertThat( countUnique( new long[]{}, new long[]{3, 6, 8} ) ).hasCounts( 0, 3 );

        IntPairAssert.assertThat( countUnique( new long[]{}, new long[]{} ) ).hasCounts( 0, 0 );

        IntPairAssert.assertThat( countUnique( new long[]{4, 6, 9, 11, 12, 15}, new long[]{2, 3, 4, 7, 8, 9, 12, 16, 19} ) ).hasCounts( 3, 6 );
    }

    private static class IntPairAssert extends AbstractAssert<IntPairAssert,Long>
    {
        private final Longs longs = Longs.instance();

        static IntPairAssert assertThat( long value )
        {
            return new IntPairAssert( value );
        }

        IntPairAssert( Long value )
        {
            super( value, IntPairAssert.class );
        }

        IntPairAssert hasCounts( int left, int right )
        {
            isNotNull();
            long expectedValue = (long) left << 32 | right;
            longs.assertEqual( info, actual, expectedValue );
            return myself;
        }
    }
}
