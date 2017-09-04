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
package org.neo4j.values.storable;

import org.junit.Test;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.values.storable.NumberValues.MAX_LENGTH;
import static org.neo4j.values.storable.NumberValues.hash;

public class NumberValuesTest
{

    @Test
    public void shouldHashNaN()
    {
        assertThat( hash( Double.NaN ), equalTo( Double.hashCode( Double.NaN ) ) );
    }

    @Test
    public void shouldHashInfinite()
    {
        assertThat( hash( Double.NEGATIVE_INFINITY ), equalTo( Double.hashCode( Double.NEGATIVE_INFINITY ) ) );
        assertThat( hash( Double.POSITIVE_INFINITY ), equalTo( Double.hashCode( Double.POSITIVE_INFINITY ) ) );
    }

    @Test
    public void shouldHashIntegralDoubleAsLong()
    {
        assertThat( hash( 1337d ), equalTo( hash( 1337L ) ) );
    }

    @Test
    public void shouldGiveSameResultsAsBuiltInHashForInts()
    {
        int[] ints = new int[32];
        Random r = ThreadLocalRandom.current();
        for ( int i = 0; i < 32; i++ )
        {
            ints[i] = r.nextInt();
        }

        assertThat(hash(ints), equalTo(Arrays.hashCode( ints )));
    }

    @Test
    public void shouldGiveSameResultsAsBuiltInHashForBytes()
    {
        byte[] bytes = new byte[32];
        Random r = ThreadLocalRandom.current();
        for ( int i = 0; i < 32; i++ )
        {
            bytes[i] = (byte)r.nextInt();
        }

        assertThat(hash(bytes), equalTo(Arrays.hashCode( bytes )));
    }

    @Test
    public void shouldGiveSameResultsAsBuiltInHashForShorts()
    {
        short[] shorts = new short[32];
        Random r = ThreadLocalRandom.current();
        for ( int i = 0; i < 32; i++ )
        {
            shorts[i] = (short)r.nextInt();
        }

        assertThat(hash(shorts), equalTo(Arrays.hashCode( shorts )));
    }

    @Test
    public void shouldGiveSameResultsAsBuiltInHashForLongs()
    {
        long[] longs = new long[32];
        Random r = ThreadLocalRandom.current();
        for ( int i = 0; i < 32; i++ )
        {
            longs[i] = (long)r.nextInt();
        }

        assertThat(hash(longs), equalTo(Arrays.hashCode( longs )));
    }

    @Test
    public void shouldHandleBigIntArrays()
    {
        int[] big = new int[NumberValues.MAX_LENGTH + 1];
        int[] slightlySmaller = new int[MAX_LENGTH];
        Arrays.fill( big, 1337 );
        Arrays.fill( slightlySmaller, 1337 );

        assertThat( hash( big ), equalTo( hash( slightlySmaller ) ) );
    }

    @Test
    public void shouldHandleBigByteArrays()
    {
        byte[] big = new byte[NumberValues.MAX_LENGTH + 1];
        byte[] slightlySmaller = new byte[MAX_LENGTH];
        Arrays.fill( big, (byte) 1337 );
        Arrays.fill( slightlySmaller, (byte) 1337 );

        assertThat( hash( big ), equalTo( hash( slightlySmaller ) ) );
    }

    @Test
    public void shouldHandleBigShortArrays()
    {
        short[] big = new short[NumberValues.MAX_LENGTH + 1];
        short[] slightlySmaller = new short[MAX_LENGTH];
        Arrays.fill( big, (short) 1337 );
        Arrays.fill( slightlySmaller, (short) 1337 );

        assertThat( hash( big ), equalTo( hash( slightlySmaller ) ) );
    }

    @Test
    public void shouldHandleBigLongArrays()
    {
        long[] big = new long[NumberValues.MAX_LENGTH + 1];
        long[] slightlySmaller = new long[MAX_LENGTH];
        Arrays.fill( big, 1337L );
        Arrays.fill( slightlySmaller, 1337L );

        assertThat( hash( big ), equalTo( hash( slightlySmaller ) ) );
    }
}
