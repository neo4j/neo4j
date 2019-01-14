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
package org.neo4j.unsafe.impl.batchimport;

import org.junit.Test;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UtilsTest
{

    @Test
    public void shouldDetectCollisions()
    {
        // GIVEN
        long[] first = new long[] {1, 4, 7, 10, 100, 101};
        long[] other = new long[] {2, 3, 34, 75, 101};

        // WHEN
        boolean collides = Utils.anyIdCollides( first, first.length, other, other.length );

        // THEN
        assertTrue( collides );
    }

    @Test
    public void shouldNotReportDisjointArraysAsCollision()
    {
        // GIVEN
        long[] first = new long[] {1, 4, 7, 10, 100, 101};
        long[] other = new long[] {2, 3, 34, 75, 102};

        // WHEN
        boolean collides = Utils.anyIdCollides( first, first.length, other, other.length );

        // THEN
        assertFalse( collides );
    }

    @Test
    public void shouldBeCorrectForSomeRandomBatches()
    {
        // GIVEN
        Random random = ThreadLocalRandom.current();
        long[][] batches = new long[20][];
        for ( int i = 0; i < batches.length; i++ )
        {
            batches[i] = randomBatch( 1_000, random, 5_000_000 );
        }

        // WHEN
        for ( long[] rBatch : batches )
        {
            for ( long[] lBatch : batches )
            {
                // THEN
                assertEquals( actuallyCollides( rBatch, lBatch ),
                        Utils.anyIdCollides( rBatch, rBatch.length, lBatch, lBatch.length ) );
            }
        }
    }

    @Test
    public void shouldMergeIdsInto()
    {
        // GIVEN
        long[] values = new long[]{2, 4, 10, 11, 14};
        long[] into = new long[]{1, 5, 6, 11, 25};
        int intoLengthBefore = into.length;
        into = Arrays.copyOf( into, into.length + values.length );

        // WHEN
        Utils.mergeSortedInto( values, into, intoLengthBefore );

        // THEN
        assertArrayEquals( new long[] {1, 2, 4, 5, 6, 10, 11, 11, 14, 25}, into );
    }

    @Test
    public void shouldMergeSomeRandomIdsInto()
    {
        // GIVEN
        Random random = ThreadLocalRandom.current();
        int batchSize = 10_000;

        // WHEN
        for ( int i = 0; i < 100; i++ )
        {
            long[] values = randomBatch( batchSize, random, 100_000_000 );
            long[] into = randomBatch( batchSize, random, 100_000_000 );
            long[] expectedMergedArray = manuallyMerge( values, into );
            into = Arrays.copyOf( into, batchSize * 2 );
            Utils.mergeSortedInto( values, into, batchSize );
            assertArrayEquals( expectedMergedArray, into );
        }
    }

    private long[] manuallyMerge( long[] values, long[] into )
    {
        long[] all = new long[values.length + into.length];
        System.arraycopy( values, 0, all, 0, values.length );
        System.arraycopy( into, 0, all, values.length, into.length );
        Arrays.sort( all );
        return all;
    }

    private boolean actuallyCollides( long[] b1, long[] b2 )
    {
        for ( int i = 0; i < b1.length; i++ )
        {
            for ( int j = 0; j < b2.length; j++ )
            {
                if ( b1[i] == b2[j] )
                {
                    return true;
                }
            }
        }
        return false;
    }

    private long[] randomBatch( int length, Random random, int max )
    {
        long[] result = new long[length];
        randomBatchInto( result, length, random, max );
        return result;
    }

    private void randomBatchInto( long[] into, int length, Random random, int max )
    {
        for ( int i = 0; i < length; i++ )
        {
            into[i] = random.nextInt( max );
        }
        Arrays.sort( into, 0, length );
    }
}
