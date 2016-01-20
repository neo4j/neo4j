/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.enterprise.lock;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.impl.enterprise.lock.forseti.ForsetiLockManager;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;

// TODO We should move our benchmarks, like this one, to a dedicated repository or maven module.
public class LocksBench
{
    // Contention: 8 random locks
    //
    //  2 threads: 1 346 574/s
    //  4 threads: 1 043 534/s
    //  8 threads:   917 365/s
    // 16 threads:   703 876/s

    // Uncontended
    //
    //  1 threads: 1 633 319/s
    //  2 threads: 1 558 542/s
    //  4 threads: 1 260 884/s
    //  8 threads: 1 205 954/s
    // 16 threads: 1 202 442/s

    public static void main(String ... args) throws InterruptedException
    {
        final int   numLocks = 8,
                  iterations = 1_000_000,
                numResources = 1024;

        System.out.println(" == WARMUP == ");
        for ( int i = 0; i < 4; i++ )
        {
            test( 1, 8, 100_000, numResources, manager() );
        }

        System.out.println(" === "+ manager().getClass().getSimpleName() +" === ");
        System.out.println(" (" + numLocks + " locks/tx across " + numResources + " lockable resources)");
        test( 1, numLocks, iterations, numResources, manager() );
        test( 2, numLocks, iterations, numResources, manager() );
        test( 4, numLocks, iterations, numResources, manager() );
    }

    private static Locks manager()
    {
        return new ForsetiLockManager( ResourceTypes.NODE );
//        return new CommunityLockManger();
    }

    private static void test( int numThreads, final int numLocks, final int iterations, final int numResources, final Locks lockManager )
            throws InterruptedException
    {
        AtomicLong deadlocks = new AtomicLong();
        ExecutorService executor = Executors.newFixedThreadPool( numThreads );
        long start = System.currentTimeMillis();
        for(int i=numThreads; i --> 0 ;)
        {
            executor.execute( new Runnable()
            {
                private final Locks.Client client = lockManager.newClient();

                @Override
                public void run()
                {
                    ThreadLocalRandom rand = ThreadLocalRandom.current();

                    for(int i=iterations;i --> 0;)
                    {
                        // Acquire
                        try
                        {
                            for(int currentLock = 0; currentLock<numLocks; currentLock++)
                            {
                                client.acquireExclusive( ResourceTypes.NODE, rand.nextInt( numResources ) );
                            }
                        }
                        catch(DeadlockDetectedException e)
                        {
                            deadlocks.incrementAndGet();
                        }

                        // Release
                        client.releaseAll();
                    }
                }
            } );
        }

        executor.shutdown();
        executor.awaitTermination( 1, TimeUnit.HOURS );
        long delta = System.currentTimeMillis() - start;
        double locksPerSecond = ((double)numLocks * numThreads * iterations) / (delta/1000.0);

        System.out.println("Concurrency: " + numThreads);
        System.out.println("Locks per second: " + ((long)locksPerSecond));
        System.out.println("Deadlocks: " + deadlocks.get());
    }
}
