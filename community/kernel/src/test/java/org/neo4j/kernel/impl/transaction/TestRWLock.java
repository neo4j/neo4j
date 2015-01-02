/**
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
package org.neo4j.kernel.impl.transaction;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.impl.transaction.LockWorker.newResourceObject;

import java.io.File;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import javax.transaction.Transaction;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.impl.transaction.LockWorker.ResourceObject;

public class TestRWLock
{
    private LockManagerImpl lm;
    
    @Before
    public void before() throws Exception
    {
        lm = new LockManagerImpl( new RagManager() );
    }

    @Test
    public void testSingleThread() throws Exception
    {
        Transaction tx = mock( Transaction.class );
        try
        {
            lm.getReadLock( null, tx );
            fail( "Null parameter should throw exception" );
        }
        catch ( Exception e )
        {
            // good
        }
        try
        {
            lm.getWriteLock( null, tx );
            fail( "Null parameter should throw exception" );
        }
        catch ( Exception e )
        {
            // good
        }
        try
        {
            lm.releaseReadLock( null, tx );
            fail( "Null parameter should throw exception" );
        }
        catch ( Exception e )
        {
            // good
        }
        try
        {
            lm.releaseWriteLock( null, tx );
            fail( "Null parameter should throw exception" );
        }
        catch ( Exception e )
        {
            // good
        }

        Object entity = new Object();
        try
        {
            lm.releaseWriteLock( entity, tx );
            fail( "Invalid release should throw exception" );
        }
        catch ( Exception e )
        {
            // good
        }
        try
        {
            lm.releaseReadLock( entity, tx );
            fail( "Invalid release should throw exception" );
        }
        catch ( Exception e )
        {
            // good
        }

        lm.getReadLock( entity, tx );
        try
        {
            lm.releaseWriteLock( entity, tx );
            fail( "Invalid release should throw exception" );
        }
        catch ( Exception e )
        {
            // good
        }
        lm.releaseReadLock( entity, tx );
        lm.getWriteLock( entity, tx );
        try
        {
            lm.releaseReadLock( entity, tx );
            fail( "Invalid release should throw exception" );
        }
        catch ( Exception e )
        {
            // good
        }
        lm.releaseWriteLock( entity, tx );

        lm.getReadLock( entity, tx );
        lm.getWriteLock( entity, tx );
        lm.releaseWriteLock( entity, tx );
        lm.releaseReadLock( entity, tx );

        lm.getWriteLock( entity, tx );
        lm.getReadLock( entity, tx );
        lm.releaseReadLock( entity, tx );
        lm.releaseWriteLock( entity, tx );

        for ( int i = 0; i < 10; i++ )
        {
            if ( (i % 2) == 0 )
            {
                lm.getWriteLock( entity, tx );
            }
            else
            {
                lm.getReadLock( entity, tx );
            }
        }
        for ( int i = 9; i >= 0; i-- )
        {
            if ( (i % 2) == 0 )
            {
                lm.releaseWriteLock( entity , tx );
            }
            else
            {
                lm.releaseReadLock( entity, tx );
            }
        }
    }

    @Test
    public void testMultipleThreads()
    {
        LockWorker t1 = new LockWorker( "T1", lm );
        LockWorker t2 = new LockWorker( "T2", lm );
        LockWorker t3 = new LockWorker( "T3", lm );
        LockWorker t4 = new LockWorker( "T4", lm );
        ResourceObject r1 = newResourceObject( "R1" );
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
            File file = new LockWorkFailureDump( getClass() ).dumpState( lm, new LockWorker[] { t1, t2, t3, t4 } );
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
        private final Object resource;
        private final CountDownLatch startSignal;
        private final Transaction tx = mock( Transaction.class );
        private Exception error;

        StressThread( String name, int numberOfIterations, int depthCount,
            float readWriteRatio, Object resource, CountDownLatch startSignal )
        {
            super();
            this.name = name;
            this.numberOfIterations = numberOfIterations;
            this.depthCount = depthCount;
            this.readWriteRatio = readWriteRatio;
            this.resource = resource;
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
                                lm.getReadLock( resource, tx );
                                lockStack.push( READ );
                            }
                            else
                            {
                                lm.getWriteLock( resource, tx );
                                lockStack.push( WRITE );
                            }
                        }
                        while ( --depth > 0 );

                        while ( !lockStack.isEmpty() )
                        {
                            if ( lockStack.pop() == READ )
                            {
                                lm.releaseReadLock( resource, tx );
                            }
                            else
                            {
                                lm.releaseWriteLock( resource , tx );
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
                                lm.releaseReadLock( resource, tx );
                            }
                            else
                            {
                                lm.releaseWriteLock( resource , tx );
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
        ResourceObject r1 = new ResourceObject( "R1" );
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

        long end = currentTimeMillis() + SECONDS.toMillis( 20 );
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
