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

import org.junit.Test;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.values.storable.NumberValues.hash;
import static org.neo4j.values.utils.AnyValueTestUtil.assertIncomparable;
import static org.neo4j.values.virtual.VirtualValueTestUtil.toAnyValue;

public class NumberValuesTest
{

    @Test
    public void shouldHashNaN()
    {
        assertThat( hash( Double.NaN ), equalTo( hash( Float.NaN ) ) );
    }

    @Test
    public void shouldHashInfinite()
    {
        assertThat( hash( Double.NEGATIVE_INFINITY ), equalTo( hash( Float.NEGATIVE_INFINITY ) ) );
        assertThat( hash( Double.POSITIVE_INFINITY ), equalTo( hash( Float.POSITIVE_INFINITY ) ) );
    }

    @Test
    public void shouldHandleNaNCorrectly()
    {
        assertIncomparable( toAnyValue(Double.NaN), toAnyValue( Double.NaN ) );
        assertIncomparable( toAnyValue( 1 ), toAnyValue( Double.NaN ) );
        assertIncomparable( toAnyValue( Double.NaN ), toAnyValue( 1 ) );
    }

    @Test
    public void shouldHashIntegralDoubleAsLong()
    {
        assertThat( hash( 1337d ), equalTo( hash( 1337L ) ) );
    }

    @Test
    public void shouldGiveSameResultEvenWhenArraysContainDifferentTypes()
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

        assertThat( hash( ints ), equalTo( hash( longs ) ) );
    }

    @Test
    public void shouldGiveSameHashForLongsAndInts()
    {
        Random r = ThreadLocalRandom.current();
        for ( int i = 0; i < 1_000_000; i++ )
        {
            int anInt = r.nextInt();
            assertThat( anInt, equalTo( hash( (long) anInt ) ) );
        }
    }

    @Test
    public void shouldGiveSameResultEvenWhenArraysContainDifferentTypes2()
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

        assertThat( hash( bytes ), equalTo( hash( shorts ) ) );
    }
}
