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
package org.neo4j.kernel.impl.locking.performance;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;

import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.impl.locking.community.LockManagerImpl;
import org.neo4j.kernel.impl.locking.community.RagManager;

/**
 * Performance numbers show total number of locks per second, the number of locks listed in the stats denote how many
 * locks are taken per "transaction".
 */
// TODO We should move our benchmarks, like this one, to a dedicated repository or maven module.
public class PerformanceTestLegacyLocks
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
        final int numThreads = 1,
                    numLocks = 8,
                  iterations = 1000000,
                numResources = 1024;
        final Object[] resources = newResources( numResources );

        final LockManagerImpl lockManager = new LockManagerImpl( new RagManager() );

        ExecutorService executor = Executors.newFixedThreadPool( numThreads );
        long start = System.currentTimeMillis();
        for(int i=numThreads; i --> 0 ;)
        {
            executor.execute( new Runnable()
            {
                private final Transaction tx = new NoOpTransaction();
                final Object[] localResources = newResources( numResources );

                @Override
                public void run()
                {
                    Object[] acquired = new Object[numLocks];
                    ThreadLocalRandom rand = ThreadLocalRandom.current();

                    for(int i=iterations;i --> 0;)
                    {
                        // Acquire
                        int currentLock = 0;
                        try
                        {
                            for(; currentLock<numLocks; currentLock++)
                            {
                                Object resource = localResources[rand.nextInt( numResources )];
                                lockManager.getWriteLock( resource, tx );
                                acquired[currentLock] = resource;
                            }
                        }
                        catch(DeadlockDetectedException e)
                        {
                            // ignore
                        }

                        // Release
                        for(; currentLock --> 0; )
                        {
                            lockManager.releaseWriteLock( acquired[currentLock], tx );
                        }
                    }
                }
            } );
        }

        executor.shutdown();
        executor.awaitTermination( 1, TimeUnit.HOURS );
        long delta = System.currentTimeMillis() - start;
        double locksPerSecond = ((double)numLocks * numThreads * iterations) / (delta/1000.0);

        System.out.println("Locks per second: " + locksPerSecond);
        System.out.println("Deadlocks: " + lockManager.getDetectedDeadlockCount());
    }

    private static Object[] newResources( int numResources )
    {
        final Object[] resources = new Object[numResources];
        for(int i=numResources;i-->0; ) { resources[i] = new Object(); }
        return resources;
    }

    private static class NoOpTransaction implements Transaction
    {
        @Override
        public void commit() throws HeuristicMixedException, HeuristicRollbackException, RollbackException, SecurityException, SystemException
        {
        }

        @Override
        public boolean delistResource( XAResource xaRes, int flag ) throws IllegalStateException,
                SystemException
        {
            return false;
        }

        @Override
        public boolean enlistResource( XAResource xaRes ) throws IllegalStateException,
                RollbackException, SystemException
        {
            return false;
        }

        @Override
        public int getStatus() throws SystemException
        {
            return 0;
        }

        @Override
        public void registerSynchronization( Synchronization synch ) throws IllegalStateException, RollbackException, SystemException
        {
        }

        @Override
        public void rollback() throws IllegalStateException, SystemException
        {
        }

        @Override
        public void setRollbackOnly() throws IllegalStateException, SystemException
        {
        }
    }

}
