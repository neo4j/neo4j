/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.store;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.kernel.impl.util.ArrayQueueOutOfOrderSequence;
import org.neo4j.kernel.impl.util.OutOfOrderSequence;

import static java.lang.Thread.sleep;
import static java.lang.Thread.yield;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ArrayQueueOutOfOrderSequenceTest
{
    @Test
    public void shouldExposeGapFreeSequenceSingleThreaded() throws Exception
    {
        // GIVEN
        OutOfOrderSequence sequence = new ArrayQueueOutOfOrderSequence( 0L, 10, new long[1] );

        // WHEN/THEN
        offer( sequence, 1, new long[]{1} );
        assertGet( sequence, 1, new long[]{1} );

        offer( sequence, 2, new long[]{2} );
        assertGet( sequence, 2, new long[]{2} );

        assertFalse( sequence.seen( 4, new long[]{3} ) );
        sequence.offer( 4, new long[]{3} );
        assertGet( sequence, 2, new long[]{2} );

        offer( sequence, 3, new long[]{4} );
        assertGet( sequence, 4, new long[]{3} );

        offer( sequence, 5, new long[]{5} );
        assertGet( sequence, 5, new long[]{5} );

        // AND WHEN/THEN
        offer( sequence, 10, new long[]{6} );
        offer( sequence, 11, new long[]{7} );
        offer( sequence, 8, new long[]{8} );
        offer( sequence, 9, new long[]{9} );
        offer( sequence, 7, new long[]{10} );
        assertGet( sequence, 5, new long[]{5} );
        offer( sequence, 6, new long[]{11} );
        assertGet( sequence, 11L, new long[]{7} );
    }

    @Test
    public void shouldExtendArrayIfNeedBe() throws Exception
    {
        // GIVEN
        OutOfOrderSequence sequence = new ArrayQueueOutOfOrderSequence( 0L, 5, new long[1] );

        offer( sequence, 3L, new long[]{0} );
        offer( sequence, 2L, new long[]{1} );
        offer( sequence, 5L, new long[]{2} );
        offer( sequence, 4L, new long[]{3} );

        // WHEN offering a number that should result in extending the array
        offer( sequence, 6L, new long[]{4} );
        // and WHEN offering the missing number to fill the gap
        offer( sequence, 1L, new long[]{5} );

        // THEN the high number should be visible
        assertGet( sequence, 6L, new long[]{4} );
    }

    @Test
    public void shouldDealWithThisScenario() throws Exception
    {
        // GIVEN
        OutOfOrderSequence sequence = new ArrayQueueOutOfOrderSequence( 0, 5, new long[1] );
        assertTrue( offer( sequence, 1, new long[]{0} ) );
        assertFalse( offer( sequence, 3, new long[]{0} ) );
        assertFalse( offer( sequence, 4, new long[]{0} ) );
        assertTrue( offer( sequence, 2, new long[]{0} ) );
        assertFalse( offer( sequence, 6, new long[]{0} ) );
        assertTrue( offer( sequence, 5, new long[]{0} ) );
        // leave out 7
        assertFalse( offer( sequence, 8, new long[]{0} ) );
        assertFalse( offer( sequence, 9, new long[]{0} ) );
        assertFalse( offer( sequence, 10, new long[]{0} ) );
        assertFalse( offer( sequence, 11, new long[]{0} ) );
        // putting 12 should need extending the backing queue array
        assertFalse( offer( sequence, 12, new long[]{0} ) );
        assertFalse( offer( sequence, 13, new long[]{0} ) );
        assertFalse( offer( sequence, 14, new long[]{0} ) );

        // WHEN finally offering nr 7
        assertTrue( offer( sequence, 7, new long[]{0} ) );

        // THEN the number should jump to 14
        assertGet( sequence, 14, new long[]{0} );
    }

    @Test
    public void shouldKeepItsCoolWhenMultipleThreadsAreHammeringIt() throws Exception
    {
        // An interesting note is that during tests the call to sequence#offer made no difference
        // in performance, so there seems to be no visible penalty in using ArrayQueueOutOfOrderSequence.

        // GIVEN a sequence with intentionally low starting queue size
        final AtomicLong numberSource = new AtomicLong();
        final OutOfOrderSequence sequence = new ArrayQueueOutOfOrderSequence( numberSource.get(), 5, new long[1] );
        final AtomicBoolean end = new AtomicBoolean();
        // and a bunch of threads that will start offering numbers at the same time
        final CountDownLatch startSignal = new CountDownLatch( 1 );
        Thread[] threads = new Thread[1];
        for ( int i = 0; i < threads.length; i++ )
        {
            threads[i] = new Thread()
            {
                @Override
                public void run()
                {
                    await( startSignal );
                    while ( !end.get() )
                    {
                        long number = numberSource.incrementAndGet();
                        offer( sequence, number, new long[]{number+2} );
                    }
                }
            };
        }

        // WHEN
        for ( Thread thread : threads )
        {
            thread.start();
        }
        startSignal.countDown();
        while ( numberSource.get() < 10_000_000 )
        {
            sleep( 1 );
            yield();
        }
        end.set( true );
        for ( Thread thread : threads )
        {
            thread.join();
        }

        // THEN
        long lastNumber = numberSource.get();
        assertGet( sequence, lastNumber, new long[]{lastNumber + 2} );
    }

    private boolean offer( OutOfOrderSequence sequence, long number, long[] meta )
    {
        assertFalse( sequence.seen( number, meta ) );
        boolean result = sequence.offer( number, meta );
        assertTrue( sequence.seen( number, meta ) );
        return result;
    }

    private void assertGet( OutOfOrderSequence sequence, long number, long[] meta )
    {
        long[] data = sequence.get();
        long[] expected = new long[meta.length + 1];
        expected[0] = number;
        System.arraycopy( meta, 0, expected, 1, meta.length );
        assertArrayEquals( expected, data );
    }

    private void await( CountDownLatch latch )
    {
        try
        {
            latch.await();
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }
}
