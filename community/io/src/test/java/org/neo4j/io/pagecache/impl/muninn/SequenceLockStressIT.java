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
package org.neo4j.io.pagecache.impl.muninn;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.memory.GlobalMemoryTracker;
import org.neo4j.test.rule.RepeatRule;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

public class SequenceLockStressIT
{
    private static ExecutorService executor;
    private static long lockAddr;

    @BeforeClass
    public static void initialise()
    {
        lockAddr = UnsafeUtil.allocateMemory( Long.BYTES );
        executor = Executors.newCachedThreadPool( new DaemonThreadFactory() );
    }

    @AfterClass
    public static void cleanup()
    {
        executor.shutdown();
        UnsafeUtil.free( lockAddr, Long.BYTES, GlobalMemoryTracker.INSTANCE );
    }

    @Rule
    public RepeatRule repeatRule = new RepeatRule();

    @Before
    public void allocateLock()
    {
        UnsafeUtil.putLong( lockAddr, 0 );
    }

    @RepeatRule.Repeat( times = 2 )
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

                    long stamp = OffHeapPageLock.tryOptimisticReadLock( lockAddr );
                    int value = record[0];
                    boolean consistent = true;
                    for ( int i : record )
                    {
                        consistent &= i == value;
                    }
                    if ( OffHeapPageLock.validateReadLock( lockAddr, stamp ) && !consistent )
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
                    if ( OffHeapPageLock.tryWriteLock( lockAddr ) )
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
                        OffHeapPageLock.unlockWrite( lockAddr );
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
                    while ( !OffHeapPageLock.tryExclusiveLock( lockAddr ) )
                    {
                    }
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
                        for ( int value : record )
                        {
                            sumB += value;
                        }
                        Arrays.fill( record, 0 );
                    }
                    OffHeapPageLock.unlockExclusive( lockAddr );
                    if ( sumA != sumB )
                    {
                        throw new AssertionError(
                                "Inconsistent exclusive lock. 'Sum A' = " + sumA + ", 'Sum B' = " + sumB );
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
        for ( int i = 0; i < 30000; i++ )
        {
            unlockExclusiveAndTakeWriteLockMustBeAtomic();
            OffHeapPageLock.unlockWrite( lockAddr );
        }
    }

    public void unlockExclusiveAndTakeWriteLockMustBeAtomic() throws Exception
    {
        int threads = Runtime.getRuntime().availableProcessors() - 1;
        CountDownLatch start = new CountDownLatch( threads );
        AtomicBoolean stop = new AtomicBoolean();
        OffHeapPageLock.tryExclusiveLock( lockAddr );
        Runnable runnable = () ->
        {
            while ( !stop.get() )
            {
                if ( OffHeapPageLock.tryExclusiveLock( lockAddr ) )
                {
                    OffHeapPageLock.unlockExclusive( lockAddr );
                    throw new RuntimeException( "I should not have gotten that lock" );
                }
                start.countDown();
            }
        };

        List<Future<?>> futures = new ArrayList<>();
        for ( int i = 0; i < threads; i++ )
        {
            futures.add( executor.submit( runnable ) );
        }

        start.await();
        OffHeapPageLock.unlockExclusiveAndTakeWriteLock( lockAddr );
        stop.set( true );
        for ( Future<?> future : futures )
        {
            future.get(); // Assert that this does not throw
        }
    }
}
