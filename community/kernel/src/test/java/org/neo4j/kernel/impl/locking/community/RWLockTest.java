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
package org.neo4j.kernel.impl.locking.community;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.DeadlockDetectedException;

import static java.lang.Thread.sleep;
import static java.time.Duration.ofMillis;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.impl.locking.LockTracer.NONE;
import static org.neo4j.kernel.impl.locking.ResourceTypes.NODE;
import static org.neo4j.time.Clocks.systemClock;

public class RWLockTest
{
    private static final long TEST_TIMEOUT_MILLIS = 10_000;

    private static ExecutorService executor;

    @BeforeAll
    public static void initExecutor()
    {
        executor = newCachedThreadPool();
    }

    @AfterAll
    public static void stopExecutor() throws InterruptedException
    {
        executor.shutdown();
        executor.awaitTermination( 2, SECONDS );
    }

    @Test
    public void assertWriteLockDoesNotLeakMemory()
    {
        final RagManager ragManager = new RagManager();
        final LockResource resource = new LockResource( NODE, 0 );
        final RWLock lock = createRWLock( ragManager, resource );
        final Transaction tx1 = mock( Transaction.class );

        lock.mark();
        lock.acquireWriteLock( NONE, tx1 );
        lock.mark();

        assertEquals( 1, lock.getTxLockElementCount() );
        lock.releaseWriteLock( tx1 );
        assertEquals( 0, lock.getTxLockElementCount() );
    }

    @Test
    public void assertReadLockDoesNotLeakMemory()
    {
        final RagManager ragManager = new RagManager();
        final LockResource resource = new LockResource( NODE, 0 );
        final RWLock lock = createRWLock( ragManager, resource );
        final Transaction tx1 = mock( Transaction.class );

        lock.mark();
        lock.acquireReadLock( NONE, tx1 );
        lock.mark();

        assertEquals( 1, lock.getTxLockElementCount() );
        lock.releaseReadLock( tx1 );
        assertEquals( 0, lock.getTxLockElementCount() );
    }

    /*
     * In case if writer thread can't grab write lock now, it should be added to
     * into a waiting list, wait till resource will be free and grab it.
     */
    @Test
    public void testWaitingWriterLock() throws InterruptedException
    {
        assertTimeout( ofMillis( TEST_TIMEOUT_MILLIS ), () -> {
            RagManager ragManager = new RagManager();
            LockResource resource = new LockResource( NODE, 1L );
            final RWLock lock = createRWLock( ragManager, resource );
            final LockTransaction lockTransaction = new LockTransaction();
            final LockTransaction anotherTransaction = new LockTransaction();

            lock.mark();
            lock.acquireReadLock( NONE, lockTransaction );
            lock.mark();
            lock.acquireReadLock( NONE, anotherTransaction );

            final CountDownLatch writerCompletedLatch = new CountDownLatch( 1 );

            Runnable writer = createWriter( lock, lockTransaction, writerCompletedLatch );

            // start writer that will be placed in a wait list
            executor.execute( writer );

            // wait till writer will be added into a list of waiters
            waitWaitingThreads( lock, 1 );

            assertEquals( 0, lock.getWriteCount(), "No writers for now." );
            assertEquals( 2, lock.getReadCount() );

            // releasing read locks that will allow writer to grab the lock
            lock.releaseReadLock( lockTransaction );
            lock.releaseReadLock( anotherTransaction );

            // wait till writer will have write lock
            writerCompletedLatch.await();

            assertEquals( 1, lock.getWriteCount() );
            assertEquals( 0, lock.getReadCount() );

            // now releasing write lock
            lock.releaseWriteLock( lockTransaction );

            assertEquals( 0, lock.getWriteCount(), "Lock should not have any writers left." );
            assertEquals( 0, lock.getWaitingThreadsCount(), "No waiting threads left." );
            assertEquals( 0, lock.getTxLockElementCount(), "No lock elements left." );
        } );
    }

    @Test
    public void testWaitingReaderLock() throws InterruptedException
    {
        assertTimeout( ofMillis( TEST_TIMEOUT_MILLIS ), () -> {
            RagManager ragManager = new RagManager();
            LockResource resource = new LockResource( NODE, 1L );
            final RWLock lock = createRWLock( ragManager, resource );
            final LockTransaction transaction = new LockTransaction();
            final LockTransaction readerTransaction = new LockTransaction();

            final CountDownLatch readerCompletedLatch = new CountDownLatch( 1 );

            lock.mark();
            lock.acquireWriteLock( NONE, transaction );

            Runnable reader = createReader( lock, readerTransaction, readerCompletedLatch );

            // start reader that should wait for writer to release write lock
            executor.execute( reader );

            waitWaitingThreads( lock, 1 );

            assertEquals( 1, lock.getWriteCount() );
            assertEquals( 0, lock.getReadCount(), "No readers for now" );

            lock.releaseWriteLock( transaction );

            // wait till reader finish lock grab
            readerCompletedLatch.await();

            assertEquals( 0, lock.getWriteCount() );
            assertEquals( 1, lock.getReadCount() );

            lock.releaseReadLock( readerTransaction );

            assertEquals( 0, lock.getReadCount(), "Lock should not have any readers left." );
            assertEquals( 0, lock.getWaitingThreadsCount(), "No waiting threads left." );
            assertEquals( 0, lock.getTxLockElementCount(), "No lock elements left." );
        } );
    }

    @Test
    public void testThreadRemovedFromWaitingListOnDeadlock() throws InterruptedException
    {
        assertTimeout( ofMillis( TEST_TIMEOUT_MILLIS ), () -> {
            RagManager ragManager = mock( RagManager.class );
            LockResource resource = new LockResource( NODE, 1L );
            final RWLock lock = createRWLock( ragManager, resource );
            final LockTransaction lockTransaction = new LockTransaction();
            final LockTransaction anotherTransaction = new LockTransaction();

            final CountDownLatch exceptionLatch = new CountDownLatch( 1 );
            final CountDownLatch completionLatch = new CountDownLatch( 1 );

            doNothing().doAnswer( invocation -> {
                exceptionLatch.countDown();
                throw new DeadlockDetectedException( "Deadlock" );
            } ).when( ragManager ).checkWaitOn( lock, lockTransaction );

            lock.mark();
            lock.mark();
            lock.acquireReadLock( NONE, lockTransaction );
            lock.acquireReadLock( NONE, anotherTransaction );

            // writer will be added to a waiting list
            // then spurious wake up will be simulated
            // and deadlock will be detected
            Runnable writer = () -> {
                try
                {
                    lock.mark();
                    lock.acquireWriteLock( NONE, lockTransaction );
                }
                catch ( DeadlockDetectedException ignored )
                {
                    // ignored
                }
                completionLatch.countDown();
            };
            executor.execute( writer );

            waitWaitingThreads( lock, 1 );

            // sending notify for all threads till our writer will not cause deadlock exception
            do
            {
                //noinspection SynchronizationOnLocalVariableOrMethodParameter
                synchronized ( lock )
                {
                    lock.notifyAll();
                }
            }
            while ( exceptionLatch.getCount() == 1 );

            // waiting for writer to finish
            completionLatch.await();

            assertEquals( 0, lock.getWaitingThreadsCount(),
                    "In case of deadlock caused by spurious wake up " + "thread should be removed from waiting list" );
        } );
    }

    @Test
    public void testLockCounters() throws InterruptedException
    {
        RagManager ragManager = new RagManager();
        LockResource resource = new LockResource( NODE, 1L );
        final RWLock lock = createRWLock( ragManager, resource );
        LockTransaction lockTransaction = new LockTransaction();
        LockTransaction anotherTransaction = new LockTransaction();
        final LockTransaction writeTransaction = new LockTransaction();

        final CountDownLatch writerCompletedLatch = new CountDownLatch( 1 );

        lock.mark();
        lock.acquireReadLock( NONE, lockTransaction );
        lock.mark();
        lock.acquireReadLock( NONE, anotherTransaction );

        assertEquals( 2, lock.getReadCount() );
        assertEquals( 0, lock.getWriteCount() );
        assertEquals( 2, lock.getTxLockElementCount() );

        Runnable writer = createWriter( lock, writeTransaction, writerCompletedLatch );

        executor.submit( writer );

        waitWaitingThreads( lock, 1 );

        // check that all reader, writes, threads counters are correct
        assertEquals( 2, lock.getReadCount() );
        assertEquals( 0, lock.getWriteCount() );
        assertEquals( 3, lock.getTxLockElementCount() );
        assertEquals( 1, lock.getWaitingThreadsCount() );

        lock.releaseReadLock( lockTransaction );
        lock.releaseReadLock( anotherTransaction );
        writerCompletedLatch.await();

        // test readers and waiting thread gone
        assertEquals( 0, lock.getReadCount() );
        assertEquals( 1, lock.getWriteCount() );
        assertEquals( 1, lock.getTxLockElementCount() );
        assertEquals( 0, lock.getWaitingThreadsCount() );

        lock.releaseWriteLock( writeTransaction );

        // check lock is clean in the end
        assertEquals( 0, lock.getTxLockElementCount() );
        assertEquals( 0, lock.getWaitingThreadsCount() );
        assertEquals( 0, lock.getReadCount() );
        assertEquals( 0, lock.getWriteCount() );
    }

    @Test
    public void testDeadlockDetection() throws InterruptedException
    {
        assertTimeout( ofMillis( TEST_TIMEOUT_MILLIS ), () -> {
            RagManager ragManager = new RagManager();
            LockResource node1 = new LockResource( NODE, 1L );
            LockResource node2 = new LockResource( NODE, 2L );
            LockResource node3 = new LockResource( NODE, 3L );

            final RWLock lockNode1 = createRWLock( ragManager, node1 );
            final RWLock lockNode2 = createRWLock( ragManager, node2 );
            final RWLock lockNode3 = createRWLock( ragManager, node3 );

            final LockTransaction client1Transaction = new LockTransaction();
            final LockTransaction client2Transaction = new LockTransaction();
            final LockTransaction client3Transaction = new LockTransaction();

            final CountDownLatch deadLockDetector = new CountDownLatch( 1 );

            lockNode1.mark();
            lockNode1.acquireWriteLock( NONE, client1Transaction );
            lockNode2.mark();
            lockNode2.acquireWriteLock( NONE, client2Transaction );
            lockNode3.mark();
            lockNode3.acquireWriteLock( NONE, client3Transaction );

            Runnable readerLockNode2 = createReaderForDeadlock( lockNode3, client1Transaction, deadLockDetector );
            Runnable readerLockNode3 = createReaderForDeadlock( lockNode1, client2Transaction, deadLockDetector );
            Runnable readerLockNode1 = createReaderForDeadlock( lockNode2, client3Transaction, deadLockDetector );
            executor.execute( readerLockNode2 );
            executor.execute( readerLockNode3 );
            executor.execute( readerLockNode1 );

            // Deadlock should occur
            assertTrue( deadLockDetector.await( TEST_TIMEOUT_MILLIS, MILLISECONDS ),
                    "Deadlock was detected as expected." );

            lockNode3.releaseWriteLock( client3Transaction );
            lockNode2.releaseWriteLock( client2Transaction );
            lockNode1.releaseWriteLock( client1Transaction );
        } );
    }

    @Test
    public void testLockRequestsTermination()
    {
        assertTimeout( ofMillis( TEST_TIMEOUT_MILLIS ), () -> {
            //  given

            RagManager ragManager = new RagManager();
            LockResource node1 = new LockResource( NODE, 1L );
            final RWLock lock = createRWLock( ragManager, node1 );
            final LockTransaction mainTransaction = new LockTransaction();

            final LockTransaction writerTransaction = new LockTransaction();
            final CountDownLatch writerCompletedLatch = new CountDownLatch( 1 );
            Runnable conflictingWriter = createFailedWriter( lock, writerTransaction, writerCompletedLatch );

            final LockTransaction readerTransaction = new LockTransaction();
            final CountDownLatch readerCompletedLatch = new CountDownLatch( 1 );
            Runnable reader = createFailedReader( lock, readerTransaction, readerCompletedLatch );

            // when
            lock.mark();
            assertTrue( lock.acquireWriteLock( NONE, mainTransaction ) );
            executor.submit( reader );
            executor.submit( conflictingWriter );

            // wait waiters to come
            waitWaitingThreads( lock, 2 );
            assertEquals( 3, lock.getTxLockElementCount() );

            // when
            lock.terminateLockRequestsForLockTransaction( readerTransaction );
            lock.terminateLockRequestsForLockTransaction( writerTransaction );

            readerCompletedLatch.await();
            writerCompletedLatch.await();

            // expect only main write lock counters and elements present
            // all the rest should be cleaned up
            assertEquals( 0, lock.getWaitingThreadsCount() );
            assertEquals( 0, lock.getReadCount() );
            assertEquals( 1, lock.getWriteCount() );
            assertEquals( 1, lock.getTxLockElementCount() );
        } );
    }

    private Runnable createReader( final RWLock lock, final LockTransaction transaction, final CountDownLatch latch )
    {
        return () -> {
            lock.mark();
            lock.acquireReadLock( NONE, transaction );
            latch.countDown();
        };
    }

    private Runnable createFailedReader( final RWLock lock, final LockTransaction transaction, final CountDownLatch latch )
    {
        return () -> {
            lock.mark();
            assertFalse( lock.acquireReadLock( NONE, transaction ) );
            latch.countDown();
        };
    }

    private Runnable createWriter( final RWLock lock, final LockTransaction transaction, final CountDownLatch latch )
    {
        return () -> {
            lock.mark();
            lock.acquireWriteLock( NONE, transaction );
            latch.countDown();
        };
    }

    private Runnable createFailedWriter( final RWLock lock, final LockTransaction transaction, final CountDownLatch latch )
    {
        return () -> {
            lock.mark();
            assertFalse( lock.acquireWriteLock( NONE, transaction ) );
            latch.countDown();
        };
    }

    private Runnable createReaderForDeadlock( final RWLock node, final LockTransaction transaction, final CountDownLatch latch )
    {
        return () -> {
            try
            {
                node.mark();
                node.acquireReadLock( NONE, transaction );
            }
            catch ( DeadlockDetectedException e )
            {
                latch.countDown();
            }
        };
    }

    private RWLock createRWLock( RagManager ragManager, LockResource resource )
    {
        return new RWLock( resource, ragManager, systemClock(), 0 );
    }

    private void waitWaitingThreads( RWLock lock, int expectedThreads ) throws InterruptedException
    {
        while ( lock.getWaitingThreadsCount() != expectedThreads )
        {
            sleep( 20 );
        }
    }
}
