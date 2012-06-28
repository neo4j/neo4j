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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.transaction.Transaction;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.info.LockInfo;
import org.neo4j.kernel.info.LockingTransaction;
import org.neo4j.kernel.info.WaitingThread;

/**
 * The LockManager can lock resources for reading or writing. By doing this one
 * may achieve different transaction isolation levels. A resource can for now be
 * any object (but null).
 * <p>
 * When acquiring a lock you have to release it. Failure to do so will result in
 * the resource being blocked to all other transactions. Put all locks in a try -
 * finally block.
 * <p>
 * Multiple locks on the same resource held by the same transaction requires the
 * transaction to invoke the release lock method multiple times. If a tx has
 * invoked <CODE>getReadLock</CODE> on the same resource x times in a row it
 * must invoke <CODE>releaseReadLock</CODE> x times to release all the locks.
 * <p>
 * LockManager just maps locks to resources and they do all the hard work
 * together with a resource allocation graph.
 */
public class LockManager
{
    private final Map<Object,RWLock> resourceLockMap =
        new HashMap<Object,RWLock>();

    private final RagManager ragManager;

    public LockManager( RagManager ragManager)
    {
        this.ragManager = ragManager;
    }

    public long getDetectedDeadlockCount()
    {
        return ragManager.getDeadlockCount();
    }

    /**
     * Calls {{@link #getReadLock(Object, Transaction)} with parameters
     * that will make the call try to get the read lock for the transaction
     * associated with the current thread.
     * 
     * @param resource
     * @throws DeadlockDetectedException
     * @throws IllegalResourceException
     */
    public void getReadLock( Object resource)
            throws DeadlockDetectedException, IllegalResourceException
    {
        getReadLock( resource, null );
    }
    
    /**
     * Tries to acquire read lock on <CODE>resource</CODE> for a given
     * transaction. If read lock can't be acquired the transaction will wait for
     * the transaction until it can acquire it. If waiting leads to dead lock a
     * {@link DeadlockDetectedException} will be thrown.
     *
     * @param resource
     *            The resource
     * @throws DeadlockDetectedException
     *             If a deadlock is detected
     * @throws IllegalResourceException
     */
    public void getReadLock( Object resource, Transaction tx )
        throws DeadlockDetectedException, IllegalResourceException
    {
        if ( resource == null )
        {
            throw new IllegalResourceException( "Null parameter" );
        }

        RWLock lock = null;
        synchronized ( resourceLockMap )
        {
            lock = resourceLockMap.get( resource );
            if ( lock == null )
            {
                lock = new RWLock( resource, ragManager );
                resourceLockMap.put( resource, lock );
            }
            lock.mark();
        }
        lock.acquireReadLock(tx);
    }

    /**
     * Calls {{@link #getWriteLock(Object, Transaction)} with parameters
     * that will make the call try to get the write lock for the transaction
     * associated with the current thread.
     * 
     * @param resource
     * @throws DeadlockDetectedException
     * @throws IllegalResourceException
     */
    public void getWriteLock( Object resource)
            throws DeadlockDetectedException, IllegalResourceException
    {
        getWriteLock( resource, null );
    }
    
    /**
     * Tries to acquire write lock on <CODE>resource</CODE> for a given
     * transaction. If write lock can't be acquired the transaction will wait
     * for the lock until it can acquire it. If waiting leads to dead lock a
     * {@link DeadlockDetectedException} will be thrown.
     *
     * @param resource
     *            The resource
     * @throws DeadlockDetectedException
     *             If a deadlock is detected
     * @throws IllegalResourceException
     */
    public void getWriteLock( Object resource, Transaction tx )
        throws DeadlockDetectedException, IllegalResourceException
    {
        if ( resource == null )
        {
            throw new IllegalResourceException( "Null parameter" );
        }

        RWLock lock = null;
        synchronized ( resourceLockMap )
        {
            lock = resourceLockMap.get( resource );
            if ( lock == null )
            {
                lock = new RWLock( resource, ragManager );
                resourceLockMap.put( resource, lock );
            }
            lock.mark();
        }
        lock.acquireWriteLock(tx);
    }

    /**
     * Releases a read lock held by the current transaction on <CODE>resource</CODE>.
     * If current transaction don't have read lock a
     * {@link LockNotFoundException} will be thrown.
     *
     * @param resource
     *            The resource
     * @throws IllegalResourceException
     * @throws LockNotFoundException
     */
    public void releaseReadLock( Object resource, Transaction tx )
        throws LockNotFoundException, IllegalResourceException
    {
        if ( resource == null )
        {
            throw new IllegalResourceException( "Null parameter" );
        }

        RWLock lock = null;
        synchronized ( resourceLockMap )
        {
            lock = resourceLockMap.get( resource );
            if ( lock == null )
            {
                throw new LockNotFoundException( "Lock not found for: "
                    + resource );
            }
            if ( !lock.isMarked() && lock.getReadCount() == 1 &&
                lock.getWriteCount() == 0 &&
                lock.getWaitingThreadsCount() == 0 )
            {
                resourceLockMap.remove( resource );
            }
            lock.releaseReadLock(tx);
        }
    }

    /**
     * Releases a write lock held by the current transaction on <CODE>resource</CODE>.
     * If current transaction don't have read lock a
     * {@link LockNotFoundException} will be thrown.
     *
     * @param resource
     *            The resource
     * @throws IllegalResourceException
     * @throws LockNotFoundException
     */
    public void releaseWriteLock( Object resource, Transaction tx )
        throws LockNotFoundException, IllegalResourceException
    {
        if ( resource == null )
        {
            throw new IllegalResourceException( "Null parameter" );
        }

        RWLock lock = null;
        synchronized ( resourceLockMap )
        {
            lock = resourceLockMap.get( resource );
            if ( lock == null )
            {
                throw new LockNotFoundException( "Lock not found for: "
                    + resource );
            }
            if ( !lock.isMarked() && lock.getReadCount() == 0 &&
                lock.getWriteCount() == 1 &&
                lock.getWaitingThreadsCount() == 0 )
            {
                resourceLockMap.remove( resource );
            }
            lock.releaseWriteLock(tx);
        }

    }

    /**
     * Utility method for debugging. Dumps info to console of txs having locks
     * on resources.
     *
     * @param resource
     */
    public void dumpLocksOnResource( Object resource )
    {
        RWLock lock = null;
        synchronized ( resourceLockMap )
        {
            if ( !resourceLockMap.containsKey( resource ) )
            {
                System.out.println( "No locks on " + resource );
                return;
            }
            lock = resourceLockMap.get( resource );
        }
        lock.dumpStack();
    }

    public List<LockInfo> getAllLocks()
    {
        return eachLock( new ListAppendingVisitor() ).result;
    }

    public List<LockInfo> getAwaitedLocks( long minWaitTime )
    {
        return eachAwaitedLock( new ListAppendingVisitor(), minWaitTime ).result;
    }

    /**
     * Visit all locks.
     * 
     * The supplied visitor may not block.
     * 
     * @param visitor visitor for visiting each lock.
     */
    private <V extends Visitor<LockInfo>> V eachLock( V visitor )
    {
        synchronized ( resourceLockMap )
        {
            for ( RWLock lock : resourceLockMap.values() )
            {
                if ( visitor.visit( lock.info() ) ) break;
            }
        }
        return visitor;
    }
    
    /**
     * Visit all locks that some thread has been waiting for at least the
     * supplied number of milliseconds.
     * 
     * The supplied visitor may not block.
     * 
     * @param visitor visitor for visiting each lock that has had a thread
     *            waiting at least the specified time.
     * @param minWaitTime the number of milliseconds a thread should have waited
     *            on a lock for it to be visited.
     */
    private <V extends Visitor<LockInfo>> V eachAwaitedLock( V visitor, long minWaitTime )
    {
        long waitStart = System.currentTimeMillis() - minWaitTime;
        synchronized ( resourceLockMap )
        {
            for ( RWLock lock : resourceLockMap.values() )
            {
                if ( lock.acceptVisitorIfWaitedSinceBefore( visitor, waitStart ) ) break;
            }
        }
        return visitor;
    }

    /**
     * Utility method for debugging. Dumps the resource allocation graph to
     * console.
     */
    public void dumpRagStack()
    {
        ragManager.dumpStack();
    }

    /**
     * Utility method for debugging. Dumps info about each lock to console.
     */
    public void dumpAllLocks()
    {
        DumpVisitor dump = new DumpVisitor();
        eachLock( dump );
        dump.done();
    }

    private static class ListAppendingVisitor implements Visitor<LockInfo>
    {
        private final List<LockInfo> result = new ArrayList<LockInfo>();

        @Override
        public boolean visit( LockInfo element )
        {
            result.add( element );
            return false;
        }
    }
    
    private static class DumpVisitor implements Visitor<LockInfo>
    {
        int emptyLockCount = 0;

        @Override
        public boolean visit( LockInfo lock )
        {
            if ( lock.getWriteCount() > 0 || lock.getReadCount() > 0 )
            {
                dumpStack( lock );
            }
            else
            {
                if ( lock.getWaitingThreadsCount() > 0 )
                {
                    dumpStack( lock );
                }
                emptyLockCount++;
            }
            return false;
        }

        private void dumpStack( LockInfo lock )
        {
            System.out.println( "Total lock count: readCount=" + lock.getReadCount() + " writeCount="
                                + lock.getWriteCount() + " for "
                                + lock.getResourceType().toString( lock.getResourceId() ) );
            System.out.println( "Waiting list:" );
            StringBuilder waitlist = new StringBuilder();
            String sep = "";
            for ( WaitingThread we : lock.getWaitingThreads() )
            {
                waitlist.append( sep ).append( "[tid=" ).append( we.getThreadId() ).append( "(" ).append(
                        we.getReadCount() ).append( "r," ).append( we.getWriteCount() ).append( "w )," ).append(
                        we.isWaitingOnWriteLock() ? "Write" : "Read" ).append( "Lock]" );
                sep = ", ";
            }
            System.out.println( waitlist );
            for ( LockingTransaction tle : lock.getLockingTransactions() )
            {
                System.out.println( "" + tle.getTransaction() + "(" + tle.getReadCount() + "r," + tle.getWriteCount()
                                    + "w)" );
            }
        }

        void done()
        {
            if ( emptyLockCount > 0 )
            {
                System.out.println( "There are " + emptyLockCount + " empty locks" );
            }
            else
            {
                System.out.println( "There are no empty locks" );
            }
        }
    }
}