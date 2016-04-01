/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.io.pagecache.impl.muninn;

import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.test.RepeatRule;

public class SequenceLockStressIT
{
    private static final ExecutorService executor = Executors.newCachedThreadPool(new DaemonThreadFactory());

    @AfterClass
    public static void shutDownExecutor()
    {
        executor.shutdown();
    }

    @Rule
    public RepeatRule repeatRule = new RepeatRule();

    private SequenceLock lock = new SequenceLock();

    @RepeatRule.Repeat( times = 20 )
    @Test
    public void stressTest() throws Exception
    {
        int[][] data = new int[10][10];
        AtomicBoolean stop = new AtomicBoolean();
        AtomicInteger writerId = new AtomicInteger();

        abstract class Worker implements Runnable
        {
            @Override
            public void run()
            {
                try
                {
                    doWork();
                }
                finally
                {
                    stop.set( true );
                }
            }

            protected abstract void doWork();
        }

        Worker reader = new Worker()
        {
            @Override
            protected void doWork()
            {
                while ( !stop.get() )
                {
                    ThreadLocalRandom rng = ThreadLocalRandom.current();
                    int[] record = data[rng.nextInt( data.length )];

                    long stamp = lock.tryOptimisticReadLock();
                    int value = record[0];
                    boolean consistent = true;
                    for ( int i : record )
                    {
                        consistent &= i == value;
                    }
                    if ( lock.validateReadLock( stamp ) && !consistent )
                    {
                        throw new AssertionError( "inconsistent read" );
                    }
                }
            }
        };

        Worker writer = new Worker()
        {
            private volatile long unused;
            @Override
            protected void doWork()
            {
                int id = writerId.getAndIncrement();
                int counter = 1;
                ThreadLocalRandom rng = ThreadLocalRandom.current();
                int smallSpin = rng.nextInt( 5, 50 );
                int bigSpin = rng.nextInt( 100, 1000 );

                while ( !stop.get() )
                {
                    if ( lock.tryWriteLock() )
                    {
                        int[] record = data[id];
                        for ( int i = 0; i < record.length; i++ )
                        {
                            record[i] = counter;
                            for ( int j = 0; j < smallSpin; j++ )
                            {
                                unused = rng.nextLong();
                            }
                        }
                        lock.unlockWrite();
                    }

                    for ( int j = 0; j < bigSpin; j++ )
                    {
                        unused = rng.nextLong();
                    }
                }
            }
        };

        Worker exclusive = new Worker()
        {
            private volatile long unused;
            @Override
            protected void doWork()
            {
                ThreadLocalRandom rng = ThreadLocalRandom.current();
                int spin = rng.nextInt( 20, 2000 );
                while ( !stop.get() )
                {
                    while ( !lock.tryExclusiveLock() ) {}
                    long sumA = 0;
                    long sumB = 0;
                    for ( int[] ints : data )
                    {
                        for ( int i : ints )
                        {
                            sumA += i;
                        }
                    }
                    for ( int i = 0; i < spin; i++ )
                    {
                        unused = rng.nextLong();
                    }
                    for ( int[] record : data )
                    {
                        for ( int value : record  )
                        {
                            sumB += value;
                        }
                        Arrays.fill(record, 0);
                    }
                    lock.unlockExclusive();
                    if ( sumA != sumB )
                    {
                        throw new AssertionError( "Inconsistent exclusive lock. 'Sum A' = " + sumA + ", 'Sum B' = " + sumB );
                    }
                }
            }
        };

        List<Future<?>> readers = new ArrayList<>();
        List<Future<?>> writers = new ArrayList<>();
        Future<?> exclusiveFuture = executor.submit( exclusive );
        for ( int i = 0; i < 20; i++ )
        {
            readers.add( executor.submit( reader ) );
        }
        for ( int i = 0; i < data.length; i++ )
        {
            writers.add( executor.submit( writer ) );
        }

        long deadline = System.currentTimeMillis() + 1000;
        while ( !stop.get() && System.currentTimeMillis() < deadline )
        {
            Thread.sleep( 20 );
        }
        stop.set( true );

        exclusiveFuture.get();
        for ( Future<?> future : writers )
        {
            future.get();
        }
        for ( Future<?> future : readers )
        {
            future.get();
        }
    }

    @Test
    public void thoroughlyEnsureAtomicityOfUnlockExclusiveAndTakeWriteLock() throws Exception
    {
        SequenceLockTest test = new SequenceLockTest();
        for ( int i = 0; i < 30000; i++ )
        {
            test.unlockExclusiveAndTakeWriteLockMustBeAtomic();
            test.lock = new SequenceLock();
        }

    }
}
