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
package org.neo4j.values.storable;

import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.values.storable.NumberValues.hash;
import static org.neo4j.values.utils.AnyValueTestUtil.assertNotEqual;
import static org.neo4j.values.virtual.VirtualValueTestUtil.toAnyValue;

class NumberValuesTest
{

    @Test
    void shouldHashNaN()
    {
        assertThat( hash( Double.NaN ) ).isEqualTo( hash( Float.NaN ) );
    }

    @Test
    void shouldHashInfinite()
    {
        assertThat( hash( Double.NEGATIVE_INFINITY ) ).isEqualTo( hash( Float.NEGATIVE_INFINITY ) );
        assertThat( hash( Double.POSITIVE_INFINITY ) ).isEqualTo( hash( Float.POSITIVE_INFINITY ) );
    }

    @Test
    void shouldHandleNaNCorrectly()
    {
        assertNotEqual( toAnyValue(Double.NaN), toAnyValue( Double.NaN ) );
        assertNotEqual( toAnyValue( 1 ), toAnyValue( Double.NaN ) );
        assertNotEqual( toAnyValue( Double.NaN ), toAnyValue( 1 ) );
    }

    @Test
    void shouldHashIntegralDoubleAsLong()
    {
        assertThat( hash( 1337d ) ).isEqualTo( hash( 1337L ) );
    }

    @Test
    void shouldGiveSameResultEvenWhenArraysContainDifferentTypes()
    {
        int[] ints = new int[32];
        long[] longs = new long[32];

        Random r = ThreadLocalRandom.current();
        for ( int i = 0; i < 32; i++ )
        {
            int nextInt = r.nextInt();
            ints[i] = nextInt;
            longs[i] = nextInt;
        }

        assertThat( hash( ints ) ).isEqualTo( hash( longs ) );
    }

    @Test
    void shouldGiveSameHashForLongsAndInts()
    {
        Random r = ThreadLocalRandom.current();
        for ( int i = 0; i < 1_000_000; i++ )
        {
            int anInt = r.nextInt();
            assertThat( anInt ).isEqualTo( hash( (long) anInt ) );
        }
    }

    @Test
    void shouldGiveSameResultEvenWhenArraysContainDifferentTypes2()
    {
        byte[] bytes = new byte[32];
        short[] shorts = new short[32];

        Random r = ThreadLocalRandom.current();
        for ( int i = 0; i < 32; i++ )
        {
            byte nextByte = ((Number) (r.nextInt())).byteValue();
            bytes[i] = nextByte;
            shorts[i] = nextByte;
        }

        assertThat( hash( bytes ) ).isEqualTo( hash( shorts ) );
    }
}
