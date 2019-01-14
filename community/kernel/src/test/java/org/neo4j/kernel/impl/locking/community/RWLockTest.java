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
package org.neo4j.kernel.impl.locking.community;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.time.Clocks;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class RWLockTest
{
    private static final long TEST_TIMEOUT_MILLIS = 10_000;

    private static ExecutorService executor;

    @BeforeClass
    public static void initExecutor()
    {
        executor = Executors.newCachedThreadPool();
    }

    @AfterClass
    public static void stopExecutor() throws InterruptedException
    {
        executor.shutdown();
        executor.awaitTermination( 2, TimeUnit.SECONDS );
    }

    @Test
    public void assertWriteLockDoesNotLeakMemory()
    {
        final RagManager ragManager = new RagManager();
        final LockResource resource = new LockResource( ResourceTypes.NODE, 0 );
        final RWLock lock = createRWLock( ragManager, resource );
        final Transaction tx1 = mock( Transaction.class );

        lock.mark();
        lock.acquireWriteLock( LockTracer.NONE, tx1 );
        lock.mark();

        assertEquals( 1, lock.getTxLockElementCount() );
        lock.releaseWriteLock( tx1 );
        assertEquals( 0, lock.getTxLockElementCount() );
    }

    @Test
    public void assertReadLockDoesNotLeakMemory()
    {
        final RagManager ragManager = new RagManager();
        final LockResource resource = new LockResource( ResourceTypes.NODE, 0 );
        final RWLock lock = createRWLock( ragManager, resource );
        final Transaction tx1 = mock( Transaction.class );

        lock.mark();
        lock.acquireReadLock( LockTracer.NONE, tx1 );
        lock.mark();

        assertEquals( 1, lock.getTxLockElementCount() );
        lock.releaseReadLock( tx1 );
        assertEquals( 0, lock.getTxLockElementCount() );
    }

    /*
     * In case if writer thread can't grab write lock now, it should be added to
     * into a waiting list, wait till resource will be free and grab it.
     */
    @Test( timeout = TEST_TIMEOUT_MILLIS )
    public void testWaitingWriterLock() throws InterruptedException
    {
        RagManager ragManager = new RagManager();
        LockResource resource = new LockResource( ResourceTypes.NODE, 1L );
        final RWLock lock = createRWLock( ragManager, resource );
        final LockTransaction lockTransaction = new LockTransaction();
        final LockTransaction anotherTransaction = new LockTransaction();

        lock.mark();
        lock.acquireReadLock( LockTracer.NONE, lockTransaction );
        lock.mark();
        lock.acquireReadLock( LockTracer.NONE, anotherTransaction );

        final CountDownLatch writerCompletedLatch = new CountDownLatch( 1 );

        Runnable writer = createWriter( lock, lockTransaction, writerCompletedLatch );

        // start writer that will be placed in a wait list
        executor.execute( writer );

        // wait till writer will be added into a list of waiters
        waitWaitingThreads( lock, 1 );

        assertEquals( "No writers for now.", 0, lock.getWriteCount() );
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

        assertEquals( "Lock should not have any writers left.", 0, lock.getWriteCount() );
        assertEquals( "No waiting threads left.", 0, lock.getWaitingThreadsCount() );
        assertEquals( "No lock elements left.", 0, lock.getTxLockElementCount() );
    }

    @Test( timeout = TEST_TIMEOUT_MILLIS )
    public void testWaitingReaderLock() throws InterruptedException
    {
        RagManager ragManager = new RagManager();
        LockResource resource = new LockResource( ResourceTypes.NODE, 1L );
        final RWLock lock = createRWLock( ragManager, resource );
        final LockTransaction transaction = new LockTransaction();
        final LockTransaction readerTransaction = new LockTransaction();

        final CountDownLatch readerCompletedLatch = new CountDownLatch( 1 );

        lock.mark();
        lock.acquireWriteLock( LockTracer.NONE, transaction );

        Runnable reader = createReader( lock, readerTransaction, readerCompletedLatch );

        // start reader that should wait for writer to release write lock
        executor.execute( reader );

        waitWaitingThreads( lock, 1 );

        assertEquals( 1, lock.getWriteCount() );
        assertEquals( "No readers for now", 0, lock.getReadCount() );

        lock.releaseWriteLock( transaction );

        // wait till reader finish lock grab
        readerCompletedLatch.await();

        assertEquals( 0, lock.getWriteCount() );
        assertEquals( 1, lock.getReadCount() );

        lock.releaseReadLock( readerTransaction );

        assertEquals( "Lock should not have any readers left.", 0, lock.getReadCount() );
        assertEquals( "No waiting threads left.", 0, lock.getWaitingThreadsCount() );
        assertEquals( "No lock elements left.", 0, lock.getTxLockElementCount() );
    }

    @Test( timeout = TEST_TIMEOUT_MILLIS )
    public void testThreadRemovedFromWaitingListOnDeadlock() throws InterruptedException
    {
        RagManager ragManager = Mockito.mock( RagManager.class );
        LockResource resource = new LockResource( ResourceTypes.NODE, 1L );
        final RWLock lock = createRWLock( ragManager, resource );
        final LockTransaction lockTransaction = new LockTransaction();
        final LockTransaction anotherTransaction = new LockTransaction();

        final CountDownLatch exceptionLatch = new CountDownLatch( 1 );
        final CountDownLatch completionLatch = new CountDownLatch( 1 );

        Mockito.doNothing().doAnswer( invocation ->
        {
            exceptionLatch.countDown();
            throw new DeadlockDetectedException( "Deadlock" );
        } ).when( ragManager ).checkWaitOn( lock, lockTransaction );

        lock.mark();
        lock.mark();
        lock.acquireReadLock( LockTracer.NONE, lockTransaction );
        lock.acquireReadLock( LockTracer.NONE, anotherTransaction );

        // writer will be added to a waiting list
        // then spurious wake up will be simulated
        // and deadlock will be detected
        Runnable writer = () ->
        {
            try
            {
                lock.mark();
                lock.acquireWriteLock( LockTracer.NONE, lockTransaction );
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

        assertEquals( "In case of deadlock caused by spurious wake up " +
                      "thread should be removed from waiting list", 0, lock.getWaitingThreadsCount() );
    }

    @Test
    public void testLockCounters() throws InterruptedException
    {
        RagManager ragManager = new RagManager();
        LockResource resource = new LockResource( ResourceTypes.NODE, 1L );
        final RWLock lock = createRWLock( ragManager, resource );
        LockTransaction lockTransaction = new LockTransaction();
        LockTransaction anotherTransaction = new LockTransaction();
        final LockTransaction writeTransaction = new LockTransaction();

        final CountDownLatch writerCompletedLatch = new CountDownLatch( 1 );

        lock.mark();
        lock.acquireReadLock( LockTracer.NONE, lockTransaction );
        lock.mark();
        lock.acquireReadLock( LockTracer.NONE, anotherTransaction );

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

    @Test( timeout = TEST_TIMEOUT_MILLIS )
    public void testDeadlockDetection() throws InterruptedException
    {
        RagManager ragManager = new RagManager();
        LockResource node1 = new LockResource( ResourceTypes.NODE, 1L );
        LockResource node2 = new LockResource( ResourceTypes.NODE, 2L );
        LockResource node3 = new LockResource( ResourceTypes.NODE, 3L );

        final RWLock lockNode1 = createRWLock( ragManager, node1 );
        final RWLock lockNode2 = createRWLock( ragManager, node2 );
        final RWLock lockNode3 = createRWLock( ragManager, node3 );

        final LockTransaction client1Transaction = new LockTransaction();
        final LockTransaction client2Transaction = new LockTransaction();
        final LockTransaction client3Transaction = new LockTransaction();

        final CountDownLatch deadLockDetector = new CountDownLatch( 1 );

        lockNode1.mark();
        lockNode1.acquireWriteLock( LockTracer.NONE, client1Transaction );
        lockNode2.mark();
        lockNode2.acquireWriteLock( LockTracer.NONE, client2Transaction );
        lockNode3.mark();
        lockNode3.acquireWriteLock( LockTracer.NONE, client3Transaction );

        Runnable readerLockNode2 = createReaderForDeadlock( lockNode3, client1Transaction, deadLockDetector );
        Runnable readerLockNode3 = createReaderForDeadlock( lockNode1, client2Transaction, deadLockDetector );
        Runnable readerLockNode1 = createReaderForDeadlock( lockNode2, client3Transaction, deadLockDetector );
        executor.execute( readerLockNode2 );
        executor.execute( readerLockNode3 );
        executor.execute( readerLockNode1 );

        // Deadlock should occur
        Assert.assertTrue( "Deadlock was detected as expected.",
                deadLockDetector.await( TEST_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS ) );

        lockNode3.releaseWriteLock( client3Transaction );
        lockNode2.releaseWriteLock( client2Transaction );
        lockNode1.releaseWriteLock( client1Transaction );
    }

    @Test( timeout = TEST_TIMEOUT_MILLIS )
    public void testLockRequestsTermination() throws InterruptedException
    {
        // given
        RagManager ragManager = new RagManager();
        LockResource node1 = new LockResource( ResourceTypes.NODE, 1L );
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
        assertTrue( lock.acquireWriteLock( LockTracer.NONE, mainTransaction ) );
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

    }

    private Runnable createReader( final RWLock lock, final LockTransaction transaction,
                                   final CountDownLatch latch )
    {
        return () ->
        {
            lock.mark();
            lock.acquireReadLock( LockTracer.NONE, transaction );
            latch.countDown();
        };
    }

    private Runnable createFailedReader( final RWLock lock, final LockTransaction transaction,
                                         final CountDownLatch latch )
    {
        return () ->
        {
            lock.mark();
            Assert.assertFalse( lock.acquireReadLock( LockTracer.NONE, transaction ) );
            latch.countDown();
        };
    }

    private Runnable createWriter( final RWLock lock, final LockTransaction transaction,
                                   final CountDownLatch latch )
    {
        return () ->
        {
            lock.mark();
            lock.acquireWriteLock( LockTracer.NONE, transaction );
            latch.countDown();
        };
    }

    private Runnable createFailedWriter( final RWLock lock, final LockTransaction transaction,
                                         final CountDownLatch latch )
    {
        return () ->
        {
            lock.mark();
            Assert.assertFalse( lock.acquireWriteLock( LockTracer.NONE, transaction ) );
            latch.countDown();
        };
    }

    private Runnable createReaderForDeadlock( final RWLock node, final LockTransaction transaction,
                                              final CountDownLatch latch )
    {
        return () ->
        {
            try
            {
                node.mark();
                node.acquireReadLock( LockTracer.NONE, transaction );
            }
            catch ( DeadlockDetectedException e )
            {
                latch.countDown();
            }
        };
    }

    private RWLock createRWLock( RagManager ragManager, LockResource resource )
    {
        return new RWLock( resource, ragManager, Clocks.systemClock(), 0 );
    }

    private void waitWaitingThreads( RWLock lock, int expectedThreads ) throws InterruptedException
    {
        while ( lock.getWaitingThreadsCount() != expectedThreads )
        {
            Thread.sleep( 20 );
        }
    }
}
