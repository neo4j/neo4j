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
package org.neo4j.kernel.impl.transaction;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.junit.Assert.assertTrue;

import static org.neo4j.test.DoubleLatch.awaitLatch;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.kernel.impl.util.IdOrderingQueue;
import org.neo4j.kernel.impl.util.SynchronizedArrayIdOrderingQueue;

public class SynchronizedArrayIdOrderingQueueStressTest
{
    @Test
    public void shouldWithstandHighStressAndStillKeepOrder() throws Exception
    {
        // GIVEN an ordering queue w/ low initial size as to also exercise resize under stress
        VerifyingIdOrderingQueue queue = new VerifyingIdOrderingQueue(
                new SynchronizedArrayIdOrderingQueue( 5 ) );
        Committer[] committers = new Committer[20];
        CountDownLatch readySignal = new CountDownLatch( committers.length );
        AtomicLong endTime = new AtomicLong();
        CountDownLatch startSignal = new CountDownLatch( 1 );
        PrimitiveLongIterator idSource = neverEndingIdStream();
        for ( int i = 0; i < committers.length; i++ )
        {
            committers[i] = new Committer( queue, idSource, endTime, readySignal, startSignal );
        }

        // WHEN GO!
        readySignal.await();
        endTime.set( currentTimeMillis() + SECONDS.toMillis( 3 ) );
        startSignal.countDown();
        for ( Committer committer : committers )
        {
            committer.awaitFinish();
        }

        // THEN there should have been at least a few ids processed. The order of those
        // are verified as they go, by the VerifyingIdOrderingQueue
        assertTrue( "Would have wanted at least a few ids to be processed, but only saw " +
                queue.getNumberOfOrderlyRemovedIds(), queue.getNumberOfOrderlyRemovedIds() > 50 );
    }

    private static class VerifyingIdOrderingQueue implements IdOrderingQueue
    {
        private final IdOrderingQueue delegate;
        private final AtomicInteger removedCount = new AtomicInteger();
        private volatile long previousId = -1;

        public VerifyingIdOrderingQueue( IdOrderingQueue delegate )
        {
            this.delegate = delegate;
        }

        @Override
        public void removeChecked( long expectedValue )
        {
            if ( expectedValue < previousId )
            {   // Just to bypass the string creation every check
                assertTrue( "Expected to remove head " + expectedValue +
                        ", which should have been greater than previously seen id " + previousId, expectedValue > previousId );
            }
            previousId = expectedValue;
            delegate.removeChecked( expectedValue );
            removedCount.incrementAndGet();
        }

        @Override
        public void offer( long value )
        {
            delegate.offer( value );
        }

        @Override
        public boolean isEmpty()
        {
            return delegate.isEmpty();
        }

        @Override
        public void waitFor( long value ) throws InterruptedException
        {
            delegate.waitFor( value );
        }

        public int getNumberOfOrderlyRemovedIds()
        {
            return removedCount.get();
        }
    }

    private PrimitiveLongIterator neverEndingIdStream()
    {
        return new PrimitiveLongIterator()
        {
            private final Stride stride = new Stride();
            private long next;

            @Override
            public boolean hasNext()
            {
                return true;
            }

            @Override
            public long next()
            {
                try
                {
                    return next;
                }
                finally
                {
                    next += stride.next();
                }
            }
        };
    }

    private static class Committer extends Thread
    {
        private final Random random = new Random();
        private final IdOrderingQueue queue;
        private final AtomicLong endTime;
        private final CountDownLatch startSignal;
        private final PrimitiveLongIterator idSource;
        private final CountDownLatch readySignal;
        private volatile Exception exception;

        public Committer( IdOrderingQueue queue, PrimitiveLongIterator idSource,
                AtomicLong endTime, CountDownLatch readySignal, CountDownLatch startSignal )
        {
            this.queue = queue;
            this.idSource = idSource;
            this.endTime = endTime;
            this.readySignal = readySignal;
            this.startSignal = startSignal;
            start();
        }

        public void awaitFinish() throws Exception
        {
            join();
            if ( exception != null )
            {
                throw exception;
            }
        }

        @Override
        public void run()
        {
            try
            {
                readySignal.countDown();
                awaitLatch( startSignal );
                while ( currentTimeMillis() < endTime.get() )
                {
                    long id;

                    // Ids must be offered in order
                    synchronized ( queue )
                    {
                        id = idSource.next();
                        queue.offer( id );
                    }

                    queue.waitFor( id );
                    for ( int i = 0, max = random.nextInt( 10_000 ); i < max; i++ )
                    {
                        // Jit - please don't take this loop away. Look busy... check queue for empty, or something!
                        queue.isEmpty();
                    }
                    queue.removeChecked( id );
                }
            }
            catch ( Exception e )
            {
                this.exception = e;
            }
        }
    }

    /**
     * Strides predictably: 1, 2, 3, ..., MAX, 1, 2, 3, ... a.s.o
     */
    private static class Stride
    {
        private int stride;
        private final int max = 5;

        public int next()
        {
            return (stride++%max) + 1;
        }
    }
}
