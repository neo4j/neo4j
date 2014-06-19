/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.store;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import static java.lang.Thread.sleep;
import static java.lang.Thread.yield;

import static org.junit.Assert.assertEquals;

public class ArrayQueueOutOfOrderSequenceTest
{
    @Test
    public void shouldExposeGapFreeSequenceSingleThreaded() throws Exception
    {
        // GIVEN
        OutOfOrderSequence sequence = new ArrayQueueOutOfOrderSequence( 0L, 10 );

        // WHEN/THEN
        sequence.offer( 1 );
        assertEquals( 1L, sequence.get() );
        sequence.offer( 2 );
        assertEquals( 2L, sequence.get() );
        sequence.offer( 4 );
        assertEquals( 2L, sequence.get() );
        sequence.offer( 3 );
        assertEquals( 4L, sequence.get() );
        sequence.offer( 5 );
        assertEquals( 5L, sequence.get() );

        // AND WHEN/THEN
        sequence.offer( 10 );
        sequence.offer( 11 );
        sequence.offer( 8 );
        sequence.offer( 9 );
        sequence.offer( 7 );
        assertEquals( 5L, sequence.get() );
        sequence.offer( 6 );
        assertEquals( 11L, sequence.get() );
    }

    @Test
    public void shouldExtendArrayIfNeedBe() throws Exception
    {
        // GIVEN
        OutOfOrderSequence sequence = new ArrayQueueOutOfOrderSequence( 0L, 5 );
        sequence.offer( 3L );
        sequence.offer( 2L );
        sequence.offer( 5L );
        sequence.offer( 4L );

        // WHEN offering a number that should result in extending the array
        sequence.offer( 6L );
        // and WHEN offering the missing number to fill the gap
        sequence.offer( 1L );
        long high = sequence.get();

        // THEN the high number should be visible
        assertEquals( 6L, high );
    }

    @Test
    public void shouldDealWithThisScenario() throws Exception
    {
        // GIVEN
        OutOfOrderSequence sequence = new ArrayQueueOutOfOrderSequence( 0, 5 );
        sequence.offer( 1 );
        sequence.offer( 3 );
        sequence.offer( 4 );
        sequence.offer( 2 );
        sequence.offer( 6 );
        sequence.offer( 5 );
        // leave out 7
        sequence.offer( 8 );
        sequence.offer( 9 );
        sequence.offer( 10 );
        sequence.offer( 11 );
        // putting 12 should need extending the backing queue array
        sequence.offer( 12 );
        sequence.offer( 13 );
        sequence.offer( 14 );

        // WHEN finally offering nr 7
        offer( sequence, 7 );

        // THEN the number should jump to 14
        assertEquals( 14, sequence.get() );
    }

    private void offer( OutOfOrderSequence sequence, long... numbers )
    {
        for ( long number : numbers )
        {
            sequence.offer( number );
        }
    }

    @Test
    public void shouldKeepItsCoolWhenMultipleThreadsAreHammeringIt() throws Exception
    {
        // An interesting note is that during tests the call to sequence#offer made no difference
        // in performance, so there seems to be no visible penalty in using ArrayQueueOutOfOrderSequence.

        // GIVEN a sequence with intentionally low starting queue size
        final AtomicLong numberSource = new AtomicLong();
        final OutOfOrderSequence sequence = new ArrayQueueOutOfOrderSequence( numberSource.get(), 5 );
        final AtomicBoolean end = new AtomicBoolean();
        // and a bunch of threads that will start offering numbers at the same time
        final CountDownLatch startSignal = new CountDownLatch( 1 );
        Thread[] threads = new Thread[40];
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
                        sequence.offer( number );
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
        assertEquals( numberSource.get(), sequence.get() );
    }

    protected void await( CountDownLatch latch )
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
