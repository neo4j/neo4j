/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.impl.transaction;

import java.util.Iterator;
import java.util.LinkedList;
import javax.transaction.Synchronization;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;
import org.neo4j.impl.util.ArrayMap;

/**
 * A read/write lock is a lock that will allow many transactions to acquire read
 * locks as long as there is no transaction holding the write lock.
 * <p>
 * When a transaction has write lock no other tx is allowed to acquire read or
 * write lock on that resource but the tx holding the write lock. If one tx has
 * acquired write lock and another tx needs a lock on the same resource that tx
 * must wait. When the lock is released the other tx is notified and wakes up so
 * it can acquire the lock.
 * <p>
 * Waiting for locks may lead to a deadlock. Consider the following scenario. Tx
 * T1 acquires write lock on resource R1. T2 acquires write lock on R2. Now T1
 * tries to acquire read lock on R2 but has to wait since R2 is locked by T2. If
 * T2 now tries to acquire a lock on R1 it also has to wait because R1 is locked
 * by T1. T2 cannot wait on R1 because that would lead to a deadlock where T1
 * and T2 waits forever.
 * <p>
 * Avoiding deadlocks can be done by keeping a resource allocation graph. This
 * class works together with the {@link RagManager} to make sure no deadlocks
 * occur.
 * <p>
 * Waiting transactions are put into a queue and when some tx releases the lock
 * the queue is checked for waiting txs. This implementation tries to avoid lock
 * starvation and increase performance since only waiting txs that can acquire
 * the lock are notified.
 */
class RWLock
{
    private int writeCount = 0; // total writeCount
    private int readCount = 0; // total readCount
    private int marked = 0; // synch helper in LockManager

    private final Object resource; // the resource for this RWLock

    private final LinkedList<WaitElement> waitingThreadList = 
        new LinkedList<WaitElement>();

    private final ArrayMap<Transaction,TxLockElement> txLockElementMap = 
        new ArrayMap<Transaction,TxLockElement>( 5, false, true );

    private final RagManager ragManager;

    RWLock( Object resource, RagManager ragManager )
    {
        this.resource = resource;
        this.ragManager = ragManager;
    }

    // keeps track of a transactions read and write lock count on this RWLock
    private static class TxLockElement
    {
        final Transaction tx;
        int readCount = 0;
        int writeCount = 0;

        TxLockElement( Transaction tx )
        {
            this.tx = tx;
        }
    }

    // keeps track of what type of lock a thread is waiting for
    private static class WaitElement
    {
        final TxLockElement element;
        final LockType lockType;
        final Thread waitingThread;

        WaitElement( TxLockElement element, LockType lockType, Thread thread )
        {
            this.element = element;
            this.lockType = lockType;
            this.waitingThread = thread;
        }
    }

    synchronized void mark()
    {
        this.marked++;
    }

    synchronized boolean isMarked()
    {
        return marked > 0;
    }

    /**
     * Tries to acquire read lock for current transaction. If 
     * <CODE>this.writeCount</CODE> is greater than the currents tx's write 
     * count the transaction has to wait and the {@link RagManager#checkWaitOn} 
     * method is invoked for deadlock detection.
     * <p>
     * If the lock can be acquires the lock count is updated on <CODE>this</CODE>
     * and the transaction lock element (tle).
     * 
     * @throws DeadlockDetectedException
     *             if a deadlock is detected
     */
    synchronized void acquireReadLock() throws DeadlockDetectedException
    {
        Transaction tx = ragManager.getCurrentTransaction();
        if ( tx == null )
        {
            tx = new PlaceboTransaction();
        }
        TxLockElement tle = txLockElementMap.get( tx );
        if ( tle == null )
        {
            tle = new TxLockElement( tx );
        }

        try
        {
            while ( writeCount > tle.writeCount )
            {
                ragManager.checkWaitOn( this, tx );
                waitingThreadList.addFirst( new WaitElement( tle,
                    LockType.READ, Thread.currentThread() ) );
                try
                {
                    wait();
                }
                catch ( InterruptedException e )
                {
                    Thread.interrupted();
                }
                ragManager.stopWaitOn( this, tx );
            }

            if ( tle.readCount == 0 && tle.writeCount == 0 )
            {
                ragManager.lockAcquired( this, tx );
            }
            readCount++;
            tle.readCount++;
            // TODO: this put could be optimized?
            txLockElementMap.put( tx, tle );
        }
        finally
        {
            // if deadlocked, remove marking so lock is removed when empty
            marked--;
        }
    }

    /**
     * Releases the read lock held by current transaction. If there are waiting
     * transactions in the queue they will be interrupted if they can acquire
     * the lock.
     */
    synchronized void releaseReadLock() throws LockNotFoundException
    {
        Transaction tx = ragManager.getCurrentTransaction();
        if ( tx == null )
        {
            tx = new PlaceboTransaction();
        }
        TxLockElement tle = txLockElementMap.get( tx );
        if ( tle == null )
        {
            throw new LockNotFoundException(
                "No transaction lock element found for " + tx );
        }

        if ( tle.readCount == 0 )
        {
            throw new LockNotFoundException( "" + tx + " don't have readLock" );
        }

        readCount--;
        tle.readCount--;
        if ( tle.readCount == 0 && tle.writeCount == 0 )
        {
            if ( !this.isMarked() )
            {
                txLockElementMap.remove( tx );
            }
            ragManager.lockReleased( this, tx );
        }
        if ( waitingThreadList.size() > 0 )
        {
            WaitElement we = waitingThreadList.getLast();

            if ( we.lockType == LockType.WRITE )
            {
                // this one is tricky...
                // if readCount > 0 we either have to find a waiting read lock
                // in the queue or a waiting write lock that has all read
                // locks, if none of these are found it means that there
                // is a (are) thread(s) that will release read lock(s) in the
                // near future...
                if ( readCount == we.element.readCount )
                {
                    // found a write lock with all read locks
                    waitingThreadList.removeLast();
                    we.waitingThread.interrupt();
                }
                else
                {
                    java.util.ListIterator<WaitElement> listItr = 
                        waitingThreadList.listIterator( 
                            waitingThreadList.lastIndexOf( we ) );
                    // hm am I doing the first all over again?
                    // think I am if cursor is at lastIndex + 0.5 oh well...
                    while ( listItr.hasPrevious() )
                    {
                        we = listItr.previous();
                        if ( we.lockType == LockType.WRITE
                            && readCount == we.element.readCount )
                        {
                            // found a write lock with all read locks
                            listItr.remove();
                            we.waitingThread.interrupt();
                            // ----
                            break;
                        }
                        else if ( we.lockType == LockType.READ )
                        {
                            // found a read lock, let it do the job...
                            listItr.remove();
                            we.waitingThread.interrupt();
                        }
                    }
                }
            }
            else
            {
                // some thread may have the write lock and released a read lock
                // if writeCount is down to zero we can interrupt the waiting
                // readlock
                if ( writeCount == 0 )
                {
                    waitingThreadList.removeLast();
                    we.waitingThread.interrupt();
                }
            }
        }
    }

    /**
     * Tries to acquire write lock for current transaction. If 
     * <CODE>this.writeCount</CODE> is greater than the currents tx's write 
     * count or the read count is greater than the currents tx's read count the 
     * transaction has to wait and the {@link RagManager#checkWaitOn} method is 
     * invoked for deadlock detection.
     * <p>
     * If the lock can be acquires the lock count is updated on <CODE>this</CODE>
     * and the transaction lock element (tle).
     * 
     * @throws DeadlockDetectedException
     *             if a deadlock is detected
     */
    synchronized void acquireWriteLock() throws DeadlockDetectedException
    {
        Transaction tx = ragManager.getCurrentTransaction();
        if ( tx == null )
        {
            tx = new PlaceboTransaction();
        }
        TxLockElement tle = txLockElementMap.get( tx );
        if ( tle == null )
        {
            tle = new TxLockElement( tx );
        }

        try
        {
            while ( writeCount > tle.writeCount || readCount > tle.readCount )
            {
                ragManager.checkWaitOn( this, tx );
                waitingThreadList.addFirst( new WaitElement( tle,
                    LockType.WRITE, Thread.currentThread() ) );
                try
                {
                    wait();
                }
                catch ( InterruptedException e )
                {
                    Thread.interrupted();
                }
                ragManager.stopWaitOn( this, tx );
            }

            if ( tle.readCount == 0 && tle.writeCount == 0 )
            {
                ragManager.lockAcquired( this, tx );
            }
            writeCount++;
            tle.writeCount++;
            // TODO optimize this put?
            txLockElementMap.put( tx, tle );
        }
        finally
        {
            // if deadlocked, remove marking so lock is removed when empty
            marked--;
        }
    }

    /**
     * Releases the write lock held by current tx. If write count is zero and
     * there are waiting transactions in the queue they will be interrupted if
     * they can acquire the lock.
     */
    synchronized void releaseWriteLock() throws LockNotFoundException
    {
        Transaction tx = ragManager.getCurrentTransaction();
        if ( tx == null )
        {
            tx = new PlaceboTransaction();
        }
        TxLockElement tle = txLockElementMap.get( tx );
        if ( tle == null )
        {
            throw new LockNotFoundException(
                "No transaction lock element found for " + tx );
        }

        if ( tle.writeCount == 0 )
        {
            throw new LockNotFoundException( "" + tx + " don't have writeLock" );
        }

        writeCount--;
        tle.writeCount--;
        if ( tle.readCount == 0 && tle.writeCount == 0 )
        {
            if ( !this.isMarked() )
            {
                txLockElementMap.remove( tx );
            }
            ragManager.lockReleased( this, tx );
        }

        // the threads in the waitingList cannot be currentThread
        // so we only have to wake other elements if writeCount is down to zero
        // (that is: If writeCount > 0 a waiting thread in the queue cannot be
        // the thread that holds the write locks because then it would never
        // have been put into wait mode)
        if ( writeCount == 0 && waitingThreadList.size() > 0 )
        {
            // wake elements in queue until a write lock is found or queue is
            // empty
            do
            {
                WaitElement we = waitingThreadList.removeLast();
                we.waitingThread.interrupt();
                if ( we.lockType == LockType.WRITE )
                {
                    break;
                }
            }
            while ( waitingThreadList.size() > 0 );
        }
    }

    int getWriteCount()
    {
        return writeCount;
    }

    int getReadCount()
    {
        return readCount;
    }

    synchronized int getWaitingThreadsCount()
    {
        return waitingThreadList.size();
    }

    synchronized void dumpStack()
    {
        System.out.println( "Total lock count: readCount=" + readCount
            + " writeCount=" + writeCount + " for " + resource );

        System.out.println( "Waiting list:" );
        Iterator<WaitElement> wElements = waitingThreadList.iterator();
        while ( wElements.hasNext() )
        {
            WaitElement we = wElements.next();
            System.out.print( "[" + we.waitingThread + "("
                + we.element.readCount + "r," + we.element.writeCount + "w),"
                + we.lockType + "]" );
            if ( wElements.hasNext() )
            {
                System.out.print( "," );
            }
            else
            {
                System.out.println();
            }
        }

        System.out.println( "Locking transactions:" );
        Iterator<TxLockElement> lElements = txLockElementMap.values()
            .iterator();
        while ( lElements.hasNext() )
        {
            TxLockElement tle = lElements.next();
            System.out.println( "" + tle.tx + "(" + tle.readCount + "r,"
                + tle.writeCount + "w)" );
        }
    }

    public String toString()
    {
        return "RWLock[" + resource + "]";
    }

    private static class PlaceboTransaction implements Transaction
    {
        private final Thread currentThread;

        PlaceboTransaction()
        {
            this.currentThread = Thread.currentThread();
        }

        public boolean equals( Object o )
        {
            if ( !(o instanceof PlaceboTransaction) )
            {
                return false;
            }
            return this.currentThread
                .equals( ((PlaceboTransaction) o).currentThread );
        }

        public int hashCode()
        {
            return currentThread.hashCode();
        }

        public void commit()
        {
            throw new UnsupportedOperationException();
        }

        public boolean delistResource( XAResource arg0, int arg1 )
        {
            throw new UnsupportedOperationException();
        }

        public boolean enlistResource( XAResource arg0 )
        {
            throw new UnsupportedOperationException();
        }

        public int getStatus()
        {
            throw new UnsupportedOperationException();
        }

        public void registerSynchronization( Synchronization arg0 )
        {
            throw new UnsupportedOperationException();
        }

        public void rollback()
        {
            throw new UnsupportedOperationException();
        }

        public void setRollbackOnly()
        {
        }

        public String toString()
        {
            return "Placebo tx for thread " + currentThread;
        }
    }
}