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
package org.neo4j.kernel.impl.locking;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.lock.LockTracer;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.lock.ResourceTypes.NODE;

/**
 * This is the test suite that tested the original (from 2007) lock manager.
 * It has been ported to test {@link org.neo4j.kernel.impl.locking.Locks}
 * to ensure implementors of that API don't fall in any of the traps this test suite sets for them.
 */
abstract class RWLockCompatibility extends LockCompatibilityTestSupport
{
    RWLockCompatibility( LockingCompatibilityTestSuite suite )
    {
        super( suite );
    }

    @Test
    void testSingleThread()
    {
        assertThrows( Exception.class, () -> clientA.releaseExclusive( NODE, 1L ), "Invalid release should throw exception" );
        assertThrows( Exception.class, () -> clientA.releaseShared( NODE, 1L ), "Invalid release should throw exception" );

        clientA.acquireShared( LockTracer.NONE, NODE, 1L );
        assertThrows( Exception.class, () -> clientA.releaseExclusive( NODE, 1L ), "Invalid release should throw exception" );
        clientA.releaseShared( NODE, 1L );

        clientA.acquireExclusive( LockTracer.NONE, NODE, 1L );
        assertThrows( Exception.class, () -> clientA.releaseShared( NODE, 1L ), "Invalid release should throw exception" );
        clientA.releaseExclusive( NODE, 1L );

        clientA.acquireShared( LockTracer.NONE, NODE, 1L );
        clientA.acquireExclusive( LockTracer.NONE, NODE, 1L );
        clientA.releaseExclusive( NODE, 1L );
        clientA.releaseShared( NODE, 1L );

        clientA.acquireExclusive( LockTracer.NONE, NODE, 1L );
        clientA.acquireShared( LockTracer.NONE, NODE, 1L );
        clientA.releaseShared( NODE, 1L );
        clientA.releaseExclusive( NODE, 1L );

        for ( int i = 0; i < 10; i++ )
        {
            if ( (i % 2) == 0 )
            {
                clientA.acquireExclusive( LockTracer.NONE, NODE, 1L );
            }
            else
            {
                clientA.acquireShared( LockTracer.NONE, NODE, 1L );
            }
        }
        for ( int i = 9; i >= 0; i-- )
        {
            if ( (i % 2) == 0 )
            {
                clientA.releaseExclusive( NODE, 1L );
            }
            else
            {
                clientA.releaseShared( NODE, 1L );
            }
        }
    }

    @Test
    void testMultipleThreads() throws Exception
    {
        LockWorker t1 = new LockWorker( "T1", locks );
        LockWorker t2 = new LockWorker( "T2", locks );
        LockWorker t3 = new LockWorker( "T3", locks );
        LockWorker t4 = new LockWorker( "T4", locks );
        long r1 = 1L;
        try
        {
            t1.getReadLock( r1, true );
            t2.getReadLock( r1, true );
            t3.getReadLock( r1, true );
            Future<Void> t4Wait = t4.getWriteLock( r1, false );
            t3.releaseReadLock( r1 );
            t2.releaseReadLock( r1 );
            assertFalse( t4Wait.isDone() );
            t1.releaseReadLock( r1 );
            // now we can wait for write lock since it can be acquired
            // get write lock
            t4.awaitFuture( t4Wait );
            t4.getReadLock( r1, true );
            t4.getReadLock( r1, true );
            // put readlock in queue
            Future<Void> t1Wait = t1.getReadLock( r1, false );
            t4.getReadLock( r1, true );
            t4.releaseReadLock( r1 );
            t4.getWriteLock( r1, true );
            t4.releaseWriteLock( r1 );
            assertFalse( t1Wait.isDone() );
            t4.releaseWriteLock( r1 );
            // get read lock
            t1.awaitFuture( t1Wait );
            t4.releaseReadLock( r1 );
            // t4 now has 1 readlock and t1 one readlock
            // let t1 drop readlock and t4 get write lock
            t4Wait = t4.getWriteLock( r1, false );
            t1.releaseReadLock( r1 );
            t4.awaitFuture( t4Wait );

            t4.releaseReadLock( r1 );
            t4.releaseWriteLock( r1 );

            t4.getWriteLock( r1, true );
            t1Wait = t1.getReadLock( r1, false );
            Future<Void> t2Wait = t2.getReadLock( r1, false );
            Future<Void> t3Wait = t3.getReadLock( r1, false );
            t4.getReadLock( r1, true );
            t4.releaseWriteLock( r1 );
            t1.awaitFuture( t1Wait );
            t2.awaitFuture( t2Wait );
            t3.awaitFuture( t3Wait );

            t1Wait = t1.getWriteLock( r1, false );
            t2.releaseReadLock( r1 );
            t4.releaseReadLock( r1 );
            t3.releaseReadLock( r1 );

            t1.awaitFuture( t1Wait );
            t1.releaseWriteLock( r1 );
            t2.getReadLock( r1, true );
            t1.releaseReadLock( r1 );
            t2.getWriteLock( r1, true );
            t2.releaseWriteLock( r1 );
            t2.releaseReadLock( r1 );
        }
        catch ( Exception e )
        {
            LockWorkFailureDump dumper = new LockWorkFailureDump( testDir.file( getClass().getSimpleName() ) );
            File file = dumper.dumpState( locks, t1, t2, t3, t4 );
            throw new RuntimeException( "Failed, forensics information dumped to " + file.getAbsolutePath(), e );
        }
        finally
        {
            t1.close();
            t2.close();
            t3.close();
            t4.close();
        }
    }

    public class StressThread extends Thread
    {
        private final Random rand = new Random( currentTimeMillis() );
        private final Object READ = new Object();
        private final Object WRITE = new Object();

        private final String name;
        private final int numberOfIterations;
        private final int depthCount;
        private final float readWriteRatio;
        private final CountDownLatch startSignal;
        private final Locks.Client client;
        private final long nodeId;
        private Exception error;

        StressThread( String name, int numberOfIterations, int depthCount,
            float readWriteRatio, long nodeId, CountDownLatch startSignal )
        {
            super();
            this.nodeId = nodeId;
            this.client = locks.newClient();
            this.name = name;
            this.numberOfIterations = numberOfIterations;
            this.depthCount = depthCount;
            this.readWriteRatio = readWriteRatio;
            this.startSignal = startSignal;
        }

        @Override
        public void run()
        {
            try
            {
                startSignal.await();
                java.util.Stack<Object> lockStack = new java.util.Stack<>();
                for ( int i = 0; i < numberOfIterations; i++ )
                {
                    try
                    {
                        int depth = depthCount;
                        do
                        {
                            float f = rand.nextFloat();
                            if ( f < readWriteRatio )
                            {
                                client.acquireShared( LockTracer.NONE, NODE, nodeId );
                                lockStack.push( READ );
                            }
                            else
                            {
                                client.acquireExclusive( LockTracer.NONE, NODE, nodeId );
                                lockStack.push( WRITE );
                            }
                        }
                        while ( --depth > 0 );

                        while ( !lockStack.isEmpty() )
                        {
                            if ( lockStack.pop() == READ )
                            {
                                client.releaseShared( NODE, nodeId );
                            }
                            else
                            {
                                client.releaseExclusive( NODE, nodeId );
                            }
                        }
                    }
                    catch ( DeadlockDetectedException ignored )
                    {
                    }
                    finally
                    {
                        while ( !lockStack.isEmpty() )
                        {
                            if ( lockStack.pop() == READ )
                            {
                                client.releaseShared( NODE, nodeId );
                            }
                            else
                            {
                                client.releaseExclusive( NODE, nodeId );
                            }
                        }
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

    @Test
    void testStressMultipleThreads() throws Exception
    {
        long r1 = 1L;
        int numThreads = 25;
        StressThread[] stressThreads = new StressThread[numThreads];
        CountDownLatch startSignal = new CountDownLatch( 1 );
        for ( int i = 0; i < numThreads; i++ )
        {
            stressThreads[i] = new StressThread( "Thread" + i, 75, 9, 0.50f, r1, startSignal );
        }
        for ( int i = 0; i < numThreads; i++ )
        {
            stressThreads[i].start();
        }
        startSignal.countDown();

        long end = currentTimeMillis() + MINUTES.toMillis( 5 );
        boolean anyAlive;
        while ( (anyAlive = anyAliveAndAllWell( stressThreads )) && currentTimeMillis() < end )
        {
            sleepALittle();
        }

        for ( StressThread stressThread : stressThreads )
        {
            if ( stressThread.error != null )
            {
                throw stressThread.error;
            }
            else if ( stressThread.isAlive() )
            {
                for ( StackTraceElement stackTraceElement : stressThread.getStackTrace() )
                {
                    System.out.println( stackTraceElement );
                }
            }
        }
        if ( anyAlive )
        {
            throw new RuntimeException( "Expected all threads to complete." );
        }

    }

    private void sleepALittle()
    {
        try
        {
            Thread.sleep( 100 );
        }
        catch ( InterruptedException ignore )
        {
        }
    }

    private boolean anyAliveAndAllWell( StressThread[] stressThreads )
    {
        for ( StressThread stressThread : stressThreads )
        {
            if ( stressThread.error != null )
            {
                return false;
            }
            if ( stressThread.isAlive() )
            {
                return true;
            }
        }
        return false;
    }
}
