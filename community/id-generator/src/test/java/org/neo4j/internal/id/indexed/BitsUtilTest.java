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
package org.neo4j.internal.id.indexed;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static java.lang.Integer.max;
import static java.lang.Integer.min;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith( RandomExtension.class )
class BitsUtilTest
{
    @Inject
    private RandomRule random;

    @Test
    void shouldSetBitsInSingleLong()
    {
        // when
        long bits = BitsUtil.bitsInSingleLong( 3, 20 );

        // then
        assertBits( bits, 3, 20 );
    }

    @Test
    void shouldSetBitsCrossingMultipleLongs()
    {
        // given
        long[] bits = new long[2];

        // when
        BitsUtil.setBits( bits, 60, 8, 0 );

        // then
        assertBits( bits, 60, 8, 0 );
    }

    @Test
    void shouldSetBitsUpToTheLastBitInMultipleLongs()
    {
        // given
        long[] bits = new long[2];

        // when
        BitsUtil.setBits( bits, 94, 34, 0 );

        // then
        assertBits( bits, 94, 34, 0 );
    }

    @RepeatedTest( 100 )
    void shouldSetRandomBits()
    {
        // given
        long[] bits = new long[2];
        int start = random.nextInt( 128 );
        int slots = random.nextInt( 128 - start ) + 1;

        // when
        BitsUtil.setBits( bits, start, slots, 0 );

        // then
        assertBits( bits, start, slots, 0 );
    }

    private void assertBits( long bits, int start, int slots )
    {
        for ( int i = 0; i < Long.SIZE; i++ )
        {
            long mask = 1L << i;
            long expected = i >= start && i < start + slots ? mask : 0;
            assertEquals( expected, bits & mask );
        }
    }

    private void assertBits( long[] bits, int start, int slots, int startArrayIndex )
    {
        for ( int i = startArrayIndex; slots > 0; i++ )
        {
            if ( start < Long.SIZE )
            {
                int slotsInThisLong = min( slots, Long.SIZE - start );
                assertBits( bits[i], start, slotsInThisLong );
                slots -= slotsInThisLong;
            }
            start = max( 0, start - Long.SIZE );
        }
    }
}
