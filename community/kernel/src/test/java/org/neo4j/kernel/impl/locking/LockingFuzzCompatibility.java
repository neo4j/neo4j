/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.locking;

import org.junit.Ignore;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.neo4j.kernel.DeadlockDetectedException;

import static java.lang.System.currentTimeMillis;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.fail;

@Ignore("Not a test, part of a compatibility suite.")
public class LockingFuzzCompatibility extends LockingCompatibilityTestSuite.Compatibility
{
    private static final long seed = currentTimeMillis();
    public static final Random baseRandom = new Random( seed );

    public LockingFuzzCompatibility( LockingCompatibilityTestSuite suite )
    {
        super( suite );
    }

    @Test
    public void testStressMultipleThreads() throws Exception
    {
        /*
        This test starts a bunch of threads, and randomly takes read or write locks on random resources.
        No thread should wait more than five seconds for a lock - if it does, we consider it a failure.
        Successful outcomes are when threads either finish with all their lock taking and releasing, or
        are terminated with a DeadlockDetectedException.
         */

        // Given
        for ( int i = 0; i < StressThread.resources.length; i++ )
        {
            StressThread.resources[i] = i;
        }
        StressThread stressThreads[] = new StressThread[20];
        CountDownLatch startSignal = new CountDownLatch( 1 );
        for ( int i = 0; i < stressThreads.length; i++ )
        {
            int numberOfIterations = 500;
            int depthCount = 10;
            float readWriteRatio = 0.80f;
            stressThreads[i] = new StressThread( "T" + i, numberOfIterations, depthCount, readWriteRatio,
                    locks.newClient(),
                    startSignal );
        }
        for ( Thread thread : stressThreads )
        {
            thread.start();
        }

        // When
        startSignal.countDown();
        while ( anyAliveAndAllWell( stressThreads ) )
        {
            throwErrorsIfAny( stressThreads );
            sleepALittle();
        }

        // Then
        assertThat( "Deadlocks were more frequent than can be tolerated. Random seed was: " + seed,
                deadlockFrequency( stressThreads ), lessThan( 0.2 ));
    }

    private double deadlockFrequency( StressThread[] threads )
    {
        double lockAttempts = 0, deadlocks = 0;
        for ( StressThread thread : threads )
        {
            lockAttempts += thread.lockAttempts;
            deadlocks    += thread.deadlocks;
        }

        return deadlocks / lockAttempts;
    }

    public static class StressThread extends Thread
    {
        private static long[] resources = new long[500];

        static
        {
            for ( int i = 0; i < resources.length; i++ )
            {
                resources[i] = i;
            }
        }

        private final Random rand = new Random( baseRandom.nextLong() );
        private final CountDownLatch startSignal;
        private final String name;
        private final int numberOfIterations;
        private final int depthCount;
        private final float readWriteRatio;
        private final Locks.Client lm;

        private volatile Exception error;

        public volatile Long startedWaiting = null;

        public int deadlocks;
        public int lockAttempts;

        StressThread( String name, int numberOfIterations, int depthCount,
            float readWriteRatio, Locks.Client lm, CountDownLatch startSignal )
        {
            super();
            this.name = name;
            this.numberOfIterations = numberOfIterations;
            this.depthCount = depthCount;
            this.readWriteRatio = readWriteRatio;
            this.lm = lm;
            this.startSignal = startSignal;
        }

        @Override
        public void run()
        {
            try
            {
                startSignal.await();
                for ( int i = 0; i < numberOfIterations; i++ )
                {
                    try
                    {
                        int depth = depthCount;
                        do
                        {
                            float f = rand.nextFloat();
                            int n = rand.nextInt( resources.length );
                            lockAttempts++;
                            if ( f < readWriteRatio )
                            {
                                startedWaiting = currentTimeMillis();
                                lm.acquireShared( ResourceTypes.NODE, resources[n] );
                                startedWaiting = null;
                            }
                            else
                            {
                                startedWaiting = currentTimeMillis();
                                lm.acquireExclusive( ResourceTypes.NODE, resources[n] );
                                startedWaiting = null;
                            }
                        }
                        while ( --depth > 0 );
                    }
                    catch ( DeadlockDetectedException e )
                    {
                        deadlocks++;
                    }
                    finally
                    {
                        lm.releaseAll();
                    }
                }
            }
            catch ( Exception e )
            {
                error = e;
            }

        }

        @Override
        public String toString()
        {
            return this.name;
        }
    }

    private String diagnostics( StressThread culprit, StressThread[] stressThreads, long waited )
    {
        StringBuilder builder = new StringBuilder();
        for ( StressThread stressThread : stressThreads )
        {
            if ( stressThread.isAlive() )
            {
                if ( stressThread == culprit )
                {
                    builder.append( "This is the thread that waited too long. It waited: " ).append( waited ).append(
                            " milliseconds" );
                }
                for ( StackTraceElement element : stressThread.getStackTrace() )
                {
                    builder.append( element.toString() ).append( "\n" );
                }
            }
            builder.append( "\n" );
        }
        return builder.toString();
    }

    private void throwErrorsIfAny( StressThread[] stressThreads ) throws Exception
    {
        for ( StressThread stressThread : stressThreads )
        {
            if ( stressThread.error != null )
            {
                throw stressThread.error;
            }
        }
    }

    private void sleepALittle()
    {
        try
        {
            Thread.sleep( 1000 );
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
        }
    }

    private boolean anyAliveAndAllWell( StressThread[] stressThreads )
    {
        for ( StressThread stressThread : stressThreads )
        {
            if ( stressThread.isAlive() )
            {
                Long startedWaiting = stressThread.startedWaiting;
                if ( startedWaiting != null )
                {
                    long waitingTime = currentTimeMillis() - startedWaiting;
                    if ( waitingTime > 5000 )
                    {
                        fail( "One of the threads waited far too long. Diagnostics: \n" +
                                diagnostics( stressThread, stressThreads, waitingTime) );
                    }
                }
                return true;
            }
        }
        return false;
    }
}
