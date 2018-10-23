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
package org.neo4j.consistency.newchecker;

import org.junit.jupiter.api.Test;

import org.neo4j.internal.helpers.collection.LongRange;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class NodeBasedMemoryLimiterTest
{
    @Test
    void shouldReturnTheWholeRangeIfItFits()
    {
        // given
        NodeBasedMemoryLimiter limiter = new NodeBasedMemoryLimiter( 100, 100, 250, 1, 40 );
        assertEquals( 1, limiter.numberOfRanges() );

        // when
        LongRange range = limiter.next();

        // then
        assertRange( range, 0, 40 );
        assertFalse( limiter.hasNext() );
    }

    @Test
    void shouldReturnMultipleRangesIfWholeRangeDontFit()
    {
        // given
        NodeBasedMemoryLimiter limiter = new NodeBasedMemoryLimiter( 100, 100, 1000, 10, 200 );
        assertEquals( 3, limiter.numberOfRanges() );

        // when/then
        assertRange( limiter.next(), 0, 80 );
        assertRange( limiter.next(), 80, 160 );
        assertRange( limiter.next(), 160, 200 );
        assertFalse( limiter.hasNext() );
    }

    @Test
    void shouldReturnCorrectNumberOfRangesOnExactMatch()
    {
        // given
        NodeBasedMemoryLimiter limiter = new NodeBasedMemoryLimiter( 10, 20, 40, 1, 100 );

        // then
        assertEquals( 10, limiter.numberOfRanges() );
    }

    private void assertRange( LongRange range, long from, long to )
    {
        assertEquals( from, range.from() );
        assertEquals( to, range.to() );
    }
}
