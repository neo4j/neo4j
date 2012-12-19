/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.transaction.LockWorker.newResourceObject;

import java.io.File;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.impl.transaction.LockWorker.ResourceObject;

public class TestDeadlockDetection
{
    @Test
    public void testDeadlockDetection() throws Exception
    {
        ResourceObject r1 = newResourceObject( "R1" );
        ResourceObject r2 = newResourceObject( "R2" );
        ResourceObject r3 = newResourceObject( "R3" );
        ResourceObject r4 = newResourceObject( "R4" );
        
        PlaceboTm tm = new PlaceboTm( null, null );
        LockManager lm = new LockManagerImpl( new RagManager( tm ) );
        tm.setLockManager( lm );
        
        LockWorker t1 = new LockWorker( "T1", lm );
        LockWorker t2 = new LockWorker( "T2", lm );
        LockWorker t3 = new LockWorker( "T3", lm );
        LockWorker t4 = new LockWorker( "T4", lm );

        try
        {
            t1.getReadLock( r1, true );
            t1.getReadLock( r4, true );
            t2.getReadLock( r2, true );
            t2.getReadLock( r3, true );
            t3.getReadLock( r3, true );
            t3.getWriteLock( r1, false ); // t3-r1-t1                           // T3
            t2.getWriteLock( r4, false ); // t2-r4-t1
            t1.getWriteLock( r2, true );
            assertTrue( t1.isLastGetLockDeadLock() ); // t1-r2-t2-r4-t1
            // resolve and try one more time
            t1.releaseReadLock( r4 ); // will give r4 to t2
            t1.getWriteLock( r2, false );
            // t1-r2-t2
            t2.releaseReadLock( r2 ); // will give r2 to t1
            t1.getWriteLock( r4, false ); // t1-r4-t2                           // T1
            // dead lock
            t2.getWriteLock( r2, true );                                        // T2
            assertTrue( t2.isLastGetLockDeadLock() );
            // t2-r2-t3-r1-t1-r4-t2 or t2-r2-t1-r4-t2
            t2.releaseWriteLock( r4 ); // give r4 to t1
            t1.releaseWriteLock( r4 );
            t2.getReadLock( r4, true );
            t1.releaseWriteLock( r2 );
            t1.getReadLock( r2, true );
            t1.releaseReadLock( r1 ); // give r1 to t3
            t3.getReadLock( r2, true );
            t3.releaseWriteLock( r1 );
            t1.getReadLock( r1, true ); // give r1->t1
            t1.getWriteLock( r4, false );
            t3.getWriteLock( r1, false );
            t4.getReadLock( r2, true );
            // deadlock
            t2.getWriteLock( r2, true );
            assertTrue( t2.isLastGetLockDeadLock() );
            // t2-r2-t3-r1-t1-r4-t2
            // resolve
            t2.releaseReadLock( r4 );
            t1.releaseWriteLock( r4 );
            t1.releaseReadLock( r1 );
            t2.getReadLock( r4, true ); // give r1 to t3
            t3.releaseWriteLock( r1 );
            t1.getReadLock( r1, true ); // give r1 to t1
            t1.getWriteLock( r4, false );
            t3.releaseReadLock( r2 );
            t3.getWriteLock( r1, false );
            // cleanup
            t2.releaseReadLock( r4 ); // give r4 to t1
            t1.releaseWriteLock( r4 );
            t1.releaseReadLock( r1 ); // give r1 to t3
            t3.releaseWriteLock( r1 );
            t1.releaseReadLock( r2 );
            t4.releaseReadLock( r2 );
            t2.releaseReadLock( r3 );
            t3.releaseReadLock( r3 );
            // -- special case...
            t1.getReadLock( r1, true );
            t2.getReadLock( r1, true );
            t1.getWriteLock( r1, false ); // t1->r1-t1&t2
            t2.getWriteLock( r1, true );
            assertTrue( t2.isLastGetLockDeadLock() );
            // t2->r1->t1->r1->t2
            t2.releaseReadLock( r1 );
            t1.releaseReadLock( r1 );
            t1.releaseWriteLock( r1 );
        }
        catch ( Exception e )
        {
            File file = new LockWorkFailureDump( getClass() ).dumpState( lm, new LockWorker[] { t1, t2, t3, t4 } );
            throw new RuntimeException( "Failed, forensics information dumped to " + file.getAbsolutePath(), e );
        }
    }

    public static class StressThread extends Thread
    {
        private static final Object READ = new Object();
        private static final Object WRITE = new Object();
        private static ResourceObject resources[] = new ResourceObject[10];
        private Random rand = new Random( currentTimeMillis() );
        static
        {
            for ( int i = 0; i < resources.length; i++ )
                resources[i] = new ResourceObject( "RX" + i );
        }
        private final CountDownLatch startSignal;
        private final String name;
        private final int numberOfIterations;
        private final int depthCount;
        private final float readWriteRatio;
        private final LockManager lm;
        private volatile Exception error;

        StressThread( String name, int numberOfIterations, int depthCount,
            float readWriteRatio, LockManager lm, CountDownLatch startSignal )
        {
            super();
            this.name = name;
            this.numberOfIterations = numberOfIterations;
            this.depthCount = depthCount;
            this.readWriteRatio = readWriteRatio;
            this.lm = lm;
            this.startSignal = startSignal;
        }

        public void run()
        {
            try
            {
                startSignal.await();
                java.util.Stack<Object> lockStack = new java.util.Stack<Object>();
                java.util.Stack<ResourceObject> resourceStack = new java.util.Stack<ResourceObject>();
                for ( int i = 0; i < numberOfIterations; i++ )
                {
                    try
                    {
                        int depth = depthCount;
                        do
                        {
                            float f = rand.nextFloat();
                            int n = rand.nextInt( resources.length );
                            if ( f < readWriteRatio )
                            {
                                lm.getReadLock( resources[n] );
                                lockStack.push( READ );
                            }
                            else
                            {
                                lm.getWriteLock( resources[n] );
                                lockStack.push( WRITE );
                            }
                            resourceStack.push( resources[n] );
                        }
                        while ( --depth > 0 );
                        /*
                         * try { sleep( rand.nextInt( 100 ) ); } catch (
                         * InterruptedException e ) {}
                         */
                        while ( !lockStack.isEmpty() )
                        {
                            if ( lockStack.pop() == READ )
                            {
                                lm.releaseReadLock( resourceStack.pop(), null );
                            }
                            else
                            {
                                lm.releaseWriteLock( resourceStack.pop() , null);
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
                                lm.releaseReadLock( resourceStack.pop(), null );
                            }
                            else
                            {
                                lm.releaseWriteLock( resourceStack.pop(), null);
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

        public String toString()
        {
            return this.name;
        }
    }

    @Test
    public void testStressMultipleThreads() throws Exception
    {
        for ( int i = 0; i < StressThread.resources.length; i++ )
        {
            StressThread.resources[i] = new ResourceObject( "RX" + i );
        }
        StressThread stressThreads[] = new StressThread[50];
        PlaceboTm tm = new PlaceboTm( null, null );
        LockManager lm = new LockManagerImpl( new RagManager( tm ) );
        tm.setLockManager( lm );
        CountDownLatch startSignal = new CountDownLatch( 1 );
        for ( int i = 0; i < stressThreads.length; i++ )
        {
            stressThreads[i] = new StressThread( "T" + i, 100, 10, 0.80f, lm, startSignal );
        }
        for ( Thread thread : stressThreads )
        {
            thread.start();
        }
        startSignal.countDown();
        
        long end = currentTimeMillis() + SECONDS.toMillis( 10 );
        boolean anyAlive = true;
        while ( (anyAlive = anyAliveAndAllWell( stressThreads )) && currentTimeMillis() < end )
        {
            sleepALittle();
        }
        
        assertFalse( anyAlive );
        for ( StressThread stressThread : stressThreads )
            if ( stressThread.error != null )
                throw stressThread.error;
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
