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
package org.neo4j.kernel.impl.locking;

import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import org.neo4j.kernel.DeadlockDetectedException;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.locking.ResourceTypes.NODE;

/**
 * This is the test suite that tested the original (from 2007) lock manager. It has been ported to test {@link org.neo4j.kernel.impl.locking.Locks}
 * to ensure implementors of that API don't fall in any of the traps this test suite sets for them.
 */
@Ignore("Not a test. This is a compatibility suite, run from LockingCompatibilityTestSuite.")
public class RWLockCompatibility extends LockingCompatibilityTestSuite.Compatibility
{
    public RWLockCompatibility( LockingCompatibilityTestSuite suite )
    {
        super( suite );
    }

    @Test
    public void testSingleThread() throws Exception
    {
        try
        {
            clientA.releaseExclusive( NODE, 1l );
            fail( "Invalid release should throw exception" );
        }
        catch ( Exception e )
        {
            // good
        }
        try
        {
            clientA.releaseShared( NODE, 1l );
            fail( "Invalid release should throw exception" );
        }
        catch ( Exception e )
        {
            // good
        }

        clientA.acquireShared( NODE, 1l );
        try
        {
            clientA.releaseExclusive( NODE, 1l );
            fail( "Invalid release should throw exception" );
        }
        catch ( Exception e )
        {
            // good
        }

        clientA.releaseShared( NODE, 1l );
        clientA.acquireExclusive( NODE, 1l );
        try
        {
            clientA.releaseShared( NODE, 1l );
            fail( "Invalid release should throw exception" );
        }
        catch ( Exception e )
        {
            // good
        }
        clientA.releaseExclusive( NODE, 1l );

        clientA.acquireShared( NODE, 1l );
        clientA.acquireExclusive( NODE, 1l );
        clientA.releaseExclusive( NODE, 1l );
        clientA.releaseShared( NODE, 1l );

        clientA.acquireExclusive( NODE, 1l );
        clientA.acquireShared( NODE, 1l );
        clientA.releaseShared( NODE, 1l );
        clientA.releaseExclusive( NODE, 1l );

        for ( int i = 0; i < 10; i++ )
        {
            if ( (i % 2) == 0 )
            {
                clientA.acquireExclusive( NODE, 1l );
            }
            else
            {
                clientA.acquireShared( NODE, 1l );
            }
        }
        for ( int i = 9; i >= 0; i-- )
        {
            if ( (i % 2) == 0 )
            {
                clientA.releaseExclusive( NODE, 1l );
            }
            else
            {
                clientA.releaseShared( NODE, 1l );
            }
        }
    }

    @Test
    public void testMultipleThreads() throws Exception
    {
        LockWorker t1 = new LockWorker( "T1", locks );
        LockWorker t2 = new LockWorker( "T2", locks );
        LockWorker t3 = new LockWorker( "T3", locks );
        LockWorker t4 = new LockWorker( "T4", locks );
        long r1 = 1l;
        try
        {
            t1.getReadLock( r1, true );
            t2.getReadLock( r1, true );
            t3.getReadLock( r1, true );
            Future<Void> t4Wait = t4.getWriteLock( r1, false );
            t3.releaseReadLock( r1 );
            t2.releaseReadLock( r1 );
            assertTrue( !t4Wait.isDone() );
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
            assertTrue( !t1Wait.isDone() );
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
            LockWorkFailureDump dumper = new LockWorkFailureDump( testDir.directory( getClass().getSimpleName() ) );
            File file = dumper.dumpState( locks, t1, t2, t3, t4 );
            throw new RuntimeException( "Failed, forensics information dumped to " + file.getAbsolutePath(), e );
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
                java.util.Stack<Object> lockStack = new java.util.Stack<Object>();
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
                                client.acquireShared( NODE, nodeId );
                                lockStack.push( READ );
                            }
                            else
                            {
                                client.acquireExclusive( NODE, nodeId );
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
                    catch ( DeadlockDetectedException e )
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
    public void testStressMultipleThreads() throws Exception
    {
        long r1 = 1l;
        StressThread stressThreads[] = new StressThread[100];
        CountDownLatch startSignal = new CountDownLatch( 1 );
        for ( int i = 0; i < 100; i++ )
        {
            stressThreads[i] = new StressThread( "Thread" + i, 100, 9, 0.50f, r1, startSignal );
        }
        for ( int i = 0; i < 100; i++ )
        {
            stressThreads[i].start();
        }
        startSignal.countDown();

        long end = currentTimeMillis() + SECONDS.toMillis( 2000 );
        boolean anyAlive;
        while ( (anyAlive = anyAliveAndAllWell( stressThreads )) && currentTimeMillis() < end )
        {
            sleepALittle();
        }

        for ( StressThread stressThread : stressThreads )
        {
            if ( stressThread.error != null )
                throw stressThread.error;
            else if ( stressThread.isAlive() )
            {
                for ( StackTraceElement stackTraceElement : stressThread.getStackTrace() )
                {
                    System.out.println(stackTraceElement);
                }
            }
        }
        if(anyAlive)
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
        catch ( InterruptedException e )
        {
            Thread.interrupted();
        }
    }

    private boolean anyAliveAndAllWell( StressThread[] stressThreads )
    {
        for ( StressThread stressThread : stressThreads )
        {
            if ( stressThread.error != null )
                return false;
            if ( stressThread.isAlive() )
                return true;
        }
        return false;
    }
}
