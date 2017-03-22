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

import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.neo4j.test.assertion.Assert.assertException;

public class SortedLongArrayUtilTest
{
    private static final long[] EMPTY = new long[0];
    private static final long[] ONE_VALUE = new long[]{1};

    // missing()

    @Test
    public void missingShouldNotHandleNulls() throws Exception
    {
        assertException( () -> SortedLongArrayUtil.missing( null, null ), NullPointerException.class );
        assertException( () -> SortedLongArrayUtil.missing( EMPTY, null ), NullPointerException.class );
        assertException( () -> SortedLongArrayUtil.missing( null, EMPTY ), NullPointerException.class );
    }

    @Test
    public void missingShouldWorkWithNonIntersectingArrays()
    {
        assertThat( SortedLongArrayUtil.missing( new long[]{1, 2, 3}, new long[]{4, 5, 6} ), equalTo( 3 ) );
        assertThat( SortedLongArrayUtil.missing( new long[]{1, 2, 3}, new long[]{14, 15, 16} ), equalTo( 3 ) );
        assertThat( SortedLongArrayUtil.missing( new long[]{4, 5, 6}, new long[]{1, 2, 3} ), equalTo( 3 ) );
    }

    @Test
    public void missingShouldWorkWithIntersectingArrays()
    {
        assertThat( SortedLongArrayUtil.missing( new long[]{1, 2, 3}, new long[]{3, 4, 5} ), equalTo( 2 ) );
        assertThat( SortedLongArrayUtil.missing( new long[]{3, 4, 5}, new long[]{1, 2, 3} ), equalTo( 2 ) );
    }

    @Test
    public void missingShouldWorkWithComplexIntersectingArraysWithGaps()
    {
        assertThat(
                SortedLongArrayUtil.missing( new long[]{4, 6, 9, 11, 12, 15}, new long[]{2, 3, 4, 7, 8, 9, 12, 16, 19} ),
                equalTo( 6 ) );
        assertThat(
                SortedLongArrayUtil.missing( new long[]{2, 3, 4, 7, 8, 9, 12, 16, 19}, new long[]{4, 6, 9, 11, 12, 15} ),
                equalTo( 3 ) );
    }

    // union()

    @Test
    public void shouldHandleNullInput()
    {
        assertThat( SortedLongArrayUtil.union( null, null ), nullValue() );
        assertThat( SortedLongArrayUtil.union( null, EMPTY ), equalTo( EMPTY ) );
        assertThat( SortedLongArrayUtil.union( EMPTY, null ), equalTo( EMPTY ) );
        assertThat( SortedLongArrayUtil.union( null, ONE_VALUE ), equalTo( ONE_VALUE ) );
        assertThat( SortedLongArrayUtil.union( ONE_VALUE, null ), equalTo( ONE_VALUE ) );
    }

    @Test
    public void shouldCreateUnionOfNonIntersectingArrays()
    {
        assertThat( SortedLongArrayUtil.union( new long[]{1, 2, 3}, new long[]{4, 5, 6} ),
                equalToIgnoringOrder( 1, 2, 3, 4, 5, 6 ) );

        assertThat( SortedLongArrayUtil.union( new long[]{1, 2, 3}, new long[]{14, 15, 16} ),
                equalToIgnoringOrder( 1, 2, 3, 14, 15, 16 ) );

        assertThat( SortedLongArrayUtil.union( new long[]{14, 15, 16}, new long[]{1, 2, 3} ),
                equalToIgnoringOrder( 1, 2, 3, 14, 15, 16 ) );
    }

    @Test
    public void shouldCreateUnionOfIntersectingArrays()
    {
        assertThat( SortedLongArrayUtil.union( new long[]{1, 2, 3}, new long[]{3, 4, 5} ),
                equalToIgnoringOrder( 1, 2, 3, 4, 5 ) );

        assertThat( SortedLongArrayUtil.union( new long[]{3, 4, 5}, new long[]{1, 2, 3} ),
                equalToIgnoringOrder( 1, 2, 3, 4, 5 ) );
    }

    @Test
    public void shouldCreateUnionOfComplexIntersectingArraysWithGaps()
    {
        assertThat(
                SortedLongArrayUtil.union( new long[]{4, 6, 9, 11, 12, 15}, new long[]{2, 3, 4, 7, 8, 9, 12, 16, 19} ),
                equalToIgnoringOrder( 2, 3, 4, 6, 7, 8, 9, 11, 12, 15, 16, 19 ) );
        assertThat(
                SortedLongArrayUtil.union( new long[]{2, 3, 4, 7, 8, 9, 12, 16, 19}, new long[]{4, 6, 9, 11, 12, 15} ),
                equalToIgnoringOrder( 2, 3, 4, 6, 7, 8, 9, 11, 12, 15, 16, 19 ) );
    }

    // helpers

    private static Matcher<long[]> equalToIgnoringOrder( long... operand )
    {
        return new IsEqualIgnoreOrder( operand );
    }

    private static class IsEqualIgnoreOrder extends BaseMatcher<long[]>
    {
        private final Set<Long> expectedValue = new HashSet<>();

        IsEqualIgnoreOrder( long[] equalArg )
        {
            for ( long val : equalArg )
            {
                this.expectedValue.add( val );
            }
        }

        @Override
        public boolean matches( Object o )
        {
            if ( o instanceof long[] )
            {
                Set<Long> values = new HashSet<>();
                for ( long val : (long[]) o )
                {
                    values.add( val );
                }
                return values.equals( expectedValue );
            }
            return false;
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendValue( this.expectedValue );
        }
    }
}
