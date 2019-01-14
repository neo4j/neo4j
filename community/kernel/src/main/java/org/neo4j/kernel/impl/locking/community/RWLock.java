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

import java.time.Clock;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Map;

import org.neo4j.helpers.MathUtil;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.impl.locking.LockAcquisitionTimeoutException;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.locking.LockType;
import org.neo4j.kernel.impl.locking.LockWaitEvent;
import org.neo4j.logging.Logger;

import static java.lang.Thread.currentThread;
import static java.lang.Thread.interrupted;
import static org.neo4j.kernel.impl.locking.LockType.READ;
import static org.neo4j.kernel.impl.locking.LockType.WRITE;

/**
 * A read/write lock is a lock that will allow many transactions to acquire read
 * locks as long as there is no transaction holding the write lock.
 * <p/>
 * When a transaction has write lock no other tx is allowed to acquire read or
 * write lock on that resource but the tx holding the write lock. If one tx has
 * acquired write lock and another tx needs a lock on the same resource that tx
 * must wait. When the lock is released the other tx is notified and wakes up so
 * it can acquire the lock.
 * <p/>
 * Waiting for locks may lead to a deadlock. Consider the following scenario. Tx
 * T1 acquires write lock on resource R1. T2 acquires write lock on R2. Now T1
 * tries to acquire read lock on R2 but has to wait since R2 is locked by T2. If
 * T2 now tries to acquire a lock on R1 it also has to wait because R1 is locked
 * by T1. T2 cannot wait on R1 because that would lead to a deadlock where T1
 * and T2 waits forever.
 * <p/>
 * Avoiding deadlocks can be done by keeping a resource allocation graph. This
 * class works together with the {@link RagManager} to make sure no deadlocks
 * occur.
 * <p/>
 * Waiting transactions are put into a queue and when some tx releases the lock
 * the queue is checked for waiting txs. This implementation tries to avoid lock
 * starvation and increase performance since only waiting txs that can acquire
 * the lock are notified.
 */
class RWLock
{
    private final LockResource resource; // the resource this RWLock locks
    private final LinkedList<LockRequest> waitingThreadList = new LinkedList<>();
    private final Map<Object, TxLockElement> txLockElementMap = new HashMap<>();
    private final RagManager ragManager;
    private final Clock clock;
    private final long lockAcquisitionTimeoutMillis;

    // access to these is guarded by synchronized blocks
    private int totalReadCount;
    private int totalWriteCount;
    private int marked; // synch helper in LockManager

    RWLock( LockResource resource, RagManager ragManager, Clock clock, long lockAcquisitionTimeoutMillis )
    {
        this.resource = resource;
        this.ragManager = ragManager;
        this.clock = clock;
        this.lockAcquisitionTimeoutMillis = lockAcquisitionTimeoutMillis;
    }

    // keeps track of a transactions read and write lock count on this RWLock
    private static class TxLockElement
    {
        private final Object tx;

        // access to these is guarded by synchronized blocks
        private int readCount;
        private int writeCount;
        // represent number of active request that where current TxLockElement participate in
        // as soon as hasNoRequests return true - txLockElement can be cleaned up
        private int requests;
        // flag indicate that current TxLockElement is terminated because owning client closed
        private boolean terminated;

        TxLockElement( Object tx )
        {
            this.tx = tx;
        }

        void incrementRequests()
        {
            requests = Math.incrementExact( requests );
        }

        void decrementRequests()
        {
            requests = MathUtil.decrementExactNotPastZero( requests );
        }

        boolean hasNoRequests()
        {
            return requests == 0;
        }

        boolean isFree()
        {
            return readCount == 0 && writeCount == 0;
        }

        public boolean isTerminated()
        {
            return terminated;
        }

        public void setTerminated( boolean terminated )
        {
            this.terminated = terminated;
        }
    }

    // keeps track of what type of lock a thread is waiting for
    private static class LockRequest
    {
        private final TxLockElement element;
        private final LockType lockType;
        private final Thread waitingThread;
        private final long since = System.currentTimeMillis();

        LockRequest( TxLockElement element, LockType lockType, Thread thread )
        {
            this.element = element;
            this.lockType = lockType;
            this.waitingThread = thread;
        }
    }

    public Object resource()
    {
        return resource;
    }

    synchronized void mark()
    {
        marked = Math.incrementExact( marked );
    }

    /** synchronized by all caller methods */
    private void unmark()
    {
        marked = MathUtil.decrementExactNotPastZero( marked );
    }

    synchronized boolean isMarked()
    {
        return marked > 0;
    }

    /**
     * Tries to acquire read lock for a given transaction. If
     * <CODE>this.writeCount</CODE> is greater than the currents tx's write
     * count the transaction has to wait and the {@link RagManager#checkWaitOn}
     * method is invoked for deadlock detection.
     * <p/>
     * If the lock can be acquired the lock count is updated on <CODE>this</CODE>
     * and the transaction lock element (tle).
     * Waiting for a lock can also be terminated. In that case waiting thread will be interrupted and corresponding
     * {@link org.neo4j.kernel.impl.locking.community.RWLock.TxLockElement} will be marked as terminated.
     * In that case lock will not be acquired and false will be return as result of acquisition
     *
     * @return true is lock was acquired, false otherwise
     * @throws DeadlockDetectedException if a deadlock is detected
     */
    synchronized boolean acquireReadLock( LockTracer tracer, Object tx ) throws DeadlockDetectedException
    {
        TxLockElement tle = getOrCreateLockElement( tx );

        LockRequest lockRequest = null;
        LockWaitEvent waitEvent = null;
        // used to track do we need to add lock request to a waiting queue or we still have it there
        boolean addLockRequest = true;
        try
        {
            tle.incrementRequests();
            Thread currentThread = currentThread();

            long lockAcquisitionTimeBoundary = clock.millis() + lockAcquisitionTimeoutMillis;
            while ( !tle.isTerminated() && (totalWriteCount > tle.writeCount) )
            {
                assertNotExpired( lockAcquisitionTimeBoundary );
                ragManager.checkWaitOn( this, tx );

                if ( addLockRequest )
                {
                    lockRequest = new LockRequest( tle, READ, currentThread );
                    waitingThreadList.addFirst( lockRequest );
                }

                if ( waitEvent == null )
                {
                    waitEvent = tracer.waitForLock( false, resource.type(), resource.resourceId() );
                }
                addLockRequest = waitUninterruptedly( lockAcquisitionTimeBoundary );
                ragManager.stopWaitOn( this, tx );
            }

            if ( !tle.isTerminated() )
            {
                registerReadLockAcquired( tx, tle );
                return true;
            }
            else
            {
                // in case if lock element was interrupted and it was never register before
                // we need to clean it from lock element map
                // if it was register before it will be cleaned up during standard lock release call
                if ( tle.requests == 1 && tle.isFree() )
                {
                    txLockElementMap.remove( tx );
                }
                return false;
            }
        }
        finally
        {
            if ( waitEvent != null )
            {
                waitEvent.close();
            }
            cleanupWaitingListRequests( lockRequest, tle, addLockRequest );
            // for cases when spurious wake up was the reason why we waked up, but also there
            // was an interruption as described at 17.2 just clearing interruption flag
            interrupted();
            // if deadlocked, remove marking so lock is removed when empty
            tle.decrementRequests();
            unmark();
        }
    }

    synchronized boolean tryAcquireReadLock( Object tx )
    {
        TxLockElement tle = getOrCreateLockElement( tx );

        try
        {
            if ( tle.isTerminated() || (totalWriteCount > tle.writeCount) )
            {
                return false;
            }

            registerReadLockAcquired( tx, tle );
            return true;
        }
        finally
        {
            // if deadlocked, remove marking so lock is removed when empty
            unmark();
        }
    }

    /**
     * Releases the read lock held by the provided transaction. If it is null then
     * an attempt to acquire the current transaction will be made. This is to
     * make safe calling the method from the context of an
     * <code>afterCompletion()</code> hook where the tx is locally stored and
     * not necessarily available through the tm. If there are waiting
     * transactions in the queue they will be interrupted if they can acquire
     * the lock.
     */
    synchronized void releaseReadLock( Object tx ) throws LockNotFoundException
    {
        TxLockElement tle = getLockElement( tx );

        if ( tle.readCount == 0 )
        {
            throw new LockNotFoundException( "" + tx + " don't have readLock" );
        }

        totalReadCount = MathUtil.decrementExactNotPastZero( totalReadCount );
        tle.readCount = MathUtil.decrementExactNotPastZero( tle.readCount );
        if ( tle.isFree() )
        {
            ragManager.lockReleased( this, tx );
            if ( tle.hasNoRequests() )
            {
                txLockElementMap.remove( tx );
            }
        }
        if ( !waitingThreadList.isEmpty() )
        {
            LockRequest lockRequest = waitingThreadList.getLast();

            if ( lockRequest.lockType == LockType.WRITE )
            {
                // this one is tricky...
                // if readCount > 0 lockRequest either have to find a waiting read lock
                // in the queue or a waiting write lock that has all read
                // locks, if none of these are found it means that there
                // is a (are) thread(s) that will release read lock(s) in the
                // near future...
                if ( totalReadCount == lockRequest.element.readCount )
                {
                    // found a write lock with all read locks
                    waitingThreadList.removeLast();
                    lockRequest.waitingThread.interrupt();
                }
                else
                {
                    ListIterator<LockRequest> listItr = waitingThreadList.listIterator(
                            waitingThreadList.lastIndexOf( lockRequest ) );
                    // hm am I doing the first all over again?
                    // think I am if cursor is at lastIndex + 0.5 oh well...
                    while ( listItr.hasPrevious() )
                    {
                        lockRequest = listItr.previous();
                        if ( lockRequest.lockType == LockType.WRITE && totalReadCount == lockRequest.element.readCount )
                        {
                            // found a write lock with all read locks
                            listItr.remove();
                            lockRequest.waitingThread.interrupt();
                            break;
                        }
                        else if ( lockRequest.lockType == LockType.READ )
                        {
                            // found a read lock, let it do the job...
                            listItr.remove();
                            lockRequest.waitingThread.interrupt();
                        }
                    }
                }
            }
            else
            {
                // some thread may have the write lock and released a read lock
                // if writeCount is down to zero lockRequest can interrupt the waiting
                // read lock
                if ( totalWriteCount == 0 )
                {
                    waitingThreadList.removeLast();
                    lockRequest.waitingThread.interrupt();
                }
            }
        }
    }

    /**
     * Tries to acquire write lock for a given transaction. If
     * <CODE>this.writeCount</CODE> is greater than the currents tx's write
     * count or the read count is greater than the currents tx's read count the
     * transaction has to wait and the {@link RagManager#checkWaitOn} method is
     * invoked for deadlock detection.
     * <p/>
     * If the lock can be acquires the lock count is updated on <CODE>this</CODE>
     * and the transaction lock element (tle).
     * Waiting for a lock can also be terminated. In that case waiting thread will be interrupted and corresponding
     * {@link org.neo4j.kernel.impl.locking.community.RWLock.TxLockElement} will be marked as terminated.
     * In that case lock will not be acquired and false will be return as result of acquisition
     *
     * @return true is lock was acquired, false otherwise
     * @throws DeadlockDetectedException if a deadlock is detected
     */
    synchronized boolean acquireWriteLock( LockTracer tracer, Object tx ) throws DeadlockDetectedException
    {
        TxLockElement tle = getOrCreateLockElement( tx );

        LockRequest lockRequest = null;
        LockWaitEvent waitEvent = null;
        // used to track do we need to add lock request to a waiting queue or we still have it there
        boolean addLockRequest = true;
        try
        {
            tle.incrementRequests();
            Thread currentThread = currentThread();

            long lockAcquisitionTimeBoundary = clock.millis() + lockAcquisitionTimeoutMillis;
            while ( !tle.isTerminated() && (totalWriteCount > tle.writeCount || totalReadCount > tle.readCount) )
            {
                assertNotExpired( lockAcquisitionTimeBoundary );
                ragManager.checkWaitOn( this, tx );

                if ( addLockRequest )
                {
                    lockRequest = new LockRequest( tle, WRITE, currentThread );
                    waitingThreadList.addFirst( lockRequest );
                }

                if ( waitEvent == null )
                {
                    waitEvent = tracer.waitForLock( true, resource.type(), resource.resourceId() );
                }
                addLockRequest = waitUninterruptedly( lockAcquisitionTimeBoundary );
                ragManager.stopWaitOn( this, tx );
            }

            if ( !tle.isTerminated() )
            {
                registerWriteLockAcquired( tx, tle );
                return true;
            }
            else
            {
                // in case if lock element was interrupted and it was never register before
                // we need to clean it from lock element map
                // if it was register before it will be cleaned up during standard lock release call
                if ( tle.requests == 1 && tle.isFree() )
                {
                    txLockElementMap.remove( tx );
                }
                return false;
            }
        }
        finally
        {
            if ( waitEvent != null )
            {
                waitEvent.close();
            }
            cleanupWaitingListRequests( lockRequest, tle, addLockRequest );
            // for cases when spurious wake up was the reason why we waked up, but also there
            // was an interruption as described at 17.2 just clearing interruption flag
            interrupted();
            // if deadlocked, remove marking so lock is removed when empty
            tle.decrementRequests();
            unmark();
        }
    }

    private boolean waitUninterruptedly( long lockAcquisitionTimeBoundary )
    {
        boolean addLockRequest;
        try
        {
            if ( lockAcquisitionTimeoutMillis > 0 )
            {
                assertNotExpired( lockAcquisitionTimeBoundary );
                wait( Math.abs( lockAcquisitionTimeBoundary - clock.millis() ) );
            }
            else
            {
                wait();
            }
            addLockRequest = false;
        }
        catch ( InterruptedException e )
        {
            interrupted();
            addLockRequest = true;
        }
        return addLockRequest;
    }

    // in case of spurious wake up, deadlock during spurious wake up, termination
    // when we already have request in a queue we need to clean it up
    private void cleanupWaitingListRequests( LockRequest lockRequest, TxLockElement lockElement,
                                             boolean addLockRequest )
    {
        if ( lockRequest != null && (lockElement.isTerminated() || !addLockRequest) )
        {
            waitingThreadList.remove( lockRequest );
        }
    }

    synchronized boolean tryAcquireWriteLock( Object tx )
    {
        TxLockElement tle = getOrCreateLockElement( tx );

        try
        {
            if ( tle.isTerminated() || (totalWriteCount > tle.writeCount) || (totalReadCount > tle.readCount) )
            {
                return false;
            }

            registerWriteLockAcquired( tx, tle );
            return true;
        }
        finally
        {
            // if deadlocked, remove marking so lock is removed when empty
            unmark();
        }
    }

    /**
     * Releases the write lock held by the provided tx. If it is null then an
     * attempt to acquire the current transaction from the transaction manager
     * will be made. This is to make safe calling this method as an
     * <code>afterCompletion()</code> hook where the transaction context is not
     * necessarily available. If write count is zero and there are waiting
     * transactions in the queue they will be interrupted if they can acquire
     * the lock.
     */
    synchronized void releaseWriteLock( Object tx ) throws LockNotFoundException
    {
        TxLockElement tle = getLockElement( tx );

        if ( tle.writeCount == 0 )
        {
            throw new LockNotFoundException( "" + tx + " don't have writeLock" );
        }

        totalWriteCount = MathUtil.decrementExactNotPastZero( totalWriteCount );
        tle.writeCount = MathUtil.decrementExactNotPastZero( tle.writeCount );
        if ( tle.isFree() )
        {
            ragManager.lockReleased( this, tx );
            if ( tle.hasNoRequests() )
            {
                txLockElementMap.remove( tx );
            }
        }

        // the threads in the waitingList cannot be currentThread
        // so we only have to wake other elements if writeCount is down to zero
        // (that is: If writeCount > 0 a waiting thread in the queue cannot be
        // the thread that holds the write locks because then it would never
        // have been put into wait mode)
        if ( totalWriteCount == 0 && waitingThreadList.size() > 0 )
        {
            // wake elements in queue until a write lock is found or queue is
            // empty
            do
            {
                LockRequest lockRequest = waitingThreadList.removeLast();
                lockRequest.waitingThread.interrupt();
                if ( lockRequest.lockType == LockType.WRITE )
                {
                    break;
                }
            }
            while ( !waitingThreadList.isEmpty() );
        }
    }

    synchronized int getWriteCount()
    {
        return totalWriteCount;
    }

    synchronized int getReadCount()
    {
        return totalReadCount;
    }

    synchronized int getWaitingThreadsCount()
    {
        return waitingThreadList.size();
    }

    public synchronized boolean logTo( Logger logger )
    {
        logger.log( "Total lock count: readCount=" + totalReadCount
                    + " writeCount=" + totalWriteCount + " for " + resource );

        logger.log( "Waiting list:" );
        Iterator<LockRequest> wElements = waitingThreadList.iterator();
        while ( wElements.hasNext() )
        {
            LockRequest lockRequest = wElements.next();
            logger.log( "[" + lockRequest.waitingThread + "("
                        + lockRequest.element.readCount + "r," + lockRequest.element.writeCount + "w),"
                        + lockRequest.lockType + "]" );
            if ( wElements.hasNext() )
            {
                logger.log( "," );
            }
            else
            {
                logger.log( "" );
            }
        }

        logger.log( "Locking transactions:" );
        for ( TxLockElement tle : txLockElementMap.values() )
        {
            logger.log( "" + tle.tx + "(" + tle.readCount + "r,"
                        + tle.writeCount + "w)" );
        }
        return true;
    }

    public synchronized String describe()
    {
        StringBuilder sb = new StringBuilder( this.toString() );
        sb.append( " Total lock count: readCount=" ).append( totalReadCount ).append( " writeCount=" )
          .append( totalWriteCount ).append( " for " ).append( resource ).append( "\n" )
          .append( "Waiting list:" + "\n" );
        Iterator<LockRequest> wElements = waitingThreadList.iterator();
        while ( wElements.hasNext() )
        {
            LockRequest lockRequest = wElements.next();
            sb.append( "[" ).append( lockRequest.waitingThread ).append( "(" ).append( lockRequest.element.readCount )
              .append( "r," ).append( lockRequest.element.writeCount ).append( "w)," ).append( lockRequest.lockType )
              .append( "]\n" );
            if ( wElements.hasNext() )
            {
                sb.append( "," );
            }
        }

        sb.append( "Locking transactions:\n" );
        for ( TxLockElement tle : txLockElementMap.values() )
        {
            sb.append( "" ).append( tle.tx ).append( "(" ).append( tle.readCount ).append( "r," )
              .append( tle.writeCount ).append( "w)\n" );
        }
        return sb.toString();
    }

    public synchronized long maxWaitTime()
    {
        long max = 0L;
        for ( LockRequest thread : waitingThreadList )
        {
            if ( thread.since < max )
            {
                max = thread.since;
            }
        }
        return System.currentTimeMillis() - max;
    }

    // for specified transaction object mark all lock elements as terminated
    // and interrupt all waiters
    synchronized void terminateLockRequestsForLockTransaction( Object lockTransaction )
    {
        TxLockElement lockElement = txLockElementMap.get( lockTransaction );
        if ( lockElement != null && !lockElement.isTerminated() )
        {
            lockElement.setTerminated( true );
            for ( LockRequest lockRequest : waitingThreadList )
            {
                if ( lockRequest.element.tx.equals( lockTransaction ) )
                {
                    lockRequest.waitingThread.interrupt();
                }
            }
        }
    }

    @Override
    public String toString()
    {
        return "RWLock[" + resource + ", hash=" + hashCode() + "]";
    }

    private void registerReadLockAcquired( Object tx, TxLockElement tle )
    {
        registerLockAcquired( tx, tle );
        totalReadCount = Math.incrementExact( totalReadCount );
        tle.readCount = Math.incrementExact( tle.readCount );
    }

    private void registerWriteLockAcquired( Object tx, TxLockElement tle )
    {
        registerLockAcquired( tx, tle );
        totalWriteCount = Math.incrementExact( totalWriteCount );
        tle.writeCount = Math.incrementExact( tle.writeCount );
    }

    private void registerLockAcquired( Object tx, TxLockElement tle )
    {
        if ( tle.isFree() )
        {
            ragManager.lockAcquired( this, tx );
        }
    }

    private TxLockElement getLockElement( Object tx )
    {
        TxLockElement tle = txLockElementMap.get( tx );
        if ( tle == null )
        {
            throw new LockNotFoundException( "No transaction lock element found for " + tx );
        }
        return tle;
    }

    private void assertTransaction( Object tx )
    {
        if ( tx == null )
        {
            throw new IllegalArgumentException();
        }
    }

    private TxLockElement getOrCreateLockElement( Object tx )
    {
        assertTransaction( tx );
        TxLockElement tle = txLockElementMap.get( tx );
        if ( tle == null )
        {
            txLockElementMap.put( tx, tle = new TxLockElement( tx ) );
        }
        return tle;
    }

    private void assertNotExpired( long timeBoundary )
    {
        if ( lockAcquisitionTimeoutMillis > 0 )
        {
            if ( timeBoundary < clock.millis() )
            {
                throw new LockAcquisitionTimeoutException( resource.type(), resource.resourceId(),
                        lockAcquisitionTimeoutMillis );
            }
        }
    }

    synchronized Object getTxLockElementCount()
    {
        return txLockElementMap.size();
    }
}
