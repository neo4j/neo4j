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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.transaction.Transaction;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.info.LockInfo;
import org.neo4j.kernel.info.LockingTransaction;
import org.neo4j.kernel.info.WaitingThread;
import org.neo4j.kernel.logging.Logging;

public class LockManagerImpl implements LockManager
{
    private final Map<Object,RWLock> resourceLockMap = new HashMap<>();
    private final RagManager ragManager;

    public LockManagerImpl( RagManager ragManager )
    {
        this.ragManager = ragManager;
    }

    @Override
    public long getDetectedDeadlockCount()
    {
        return ragManager.getDeadlockCount();
    }

    @Override
    public void getReadLock( Object resource, Transaction tx )
        throws DeadlockDetectedException, IllegalResourceException
    {
        getRWLockForAcquiring( resource, tx ).acquireReadLock( tx );
    }

    @Override
    public boolean tryReadLock( Object resource, Transaction tx )
        throws IllegalResourceException
    {
        return getRWLockForAcquiring( resource, tx ).tryAcquireReadLock( tx );
    }

    @Override
    public void getWriteLock( Object resource, Transaction tx )
        throws DeadlockDetectedException, IllegalResourceException
    {
        getRWLockForAcquiring( resource, tx ).acquireWriteLock( tx );
    }

    @Override
    public boolean tryWriteLock( Object resource, Transaction tx )
        throws IllegalResourceException
    {
        return getRWLockForAcquiring( resource, tx ).tryAcquireWriteLock( tx );
    }
    
    @Override
    public void releaseReadLock( Object resource, Transaction tx )
        throws LockNotFoundException, IllegalResourceException
    {
        getRWLockForReleasing( resource, tx, 1, 0 ).releaseReadLock( tx );
    }

    @Override
    public void releaseWriteLock( Object resource, Transaction tx )
        throws LockNotFoundException, IllegalResourceException
    {
        getRWLockForReleasing( resource, tx, 0, 1 ).releaseWriteLock( tx );
    }

    @Override
    public void dumpLocksOnResource( Object resource, Logging logging )
    {
        StringLogger logger = logging.getMessagesLog( LockManager.class );
        RWLock lock;
        synchronized ( resourceLockMap )
        {
            if ( !resourceLockMap.containsKey( resource ) )
            {
                logger.info( "No locks on " + resource );
                return;
            }
            lock = resourceLockMap.get( resource );
        }
        logger.logLongMessage( "Dump locks on resource " + resource, lock );
    }

    @Override
    public List<LockInfo> getAllLocks()
    {
        return eachLock( new ListAppendingVisitor() ).result;
    }

    @Override
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
    private <V extends Visitor<LockInfo, RuntimeException>> V eachLock( V visitor )
    {
        synchronized ( resourceLockMap )
        {
            for ( RWLock lock : resourceLockMap.values() )
            {
                if ( visitor.visit( lock.info() ) )
                {
                    break;
                }
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
    private <V extends Visitor<LockInfo, RuntimeException>> V eachAwaitedLock( V visitor, long minWaitTime )
    {
        long waitStart = System.currentTimeMillis() - minWaitTime;
        synchronized ( resourceLockMap )
        {
            for ( RWLock lock : resourceLockMap.values() )
            {
                if ( lock.acceptVisitorIfWaitedSinceBefore( visitor, waitStart ) )
                {
                    break;
                }
            }
        }
        return visitor;
    }

    @Override
    public void dumpRagStack( Logging logging )
    {
        logging.getMessagesLog( getClass() ).logLongMessage( "RAG stack", ragManager );
    }

    @Override
    public void dumpAllLocks( Logging logging )
    {
        DumpVisitor dump = new DumpVisitor( logging );
        eachLock( dump );
        dump.done();
    }
    
    private void assertValidArguments( Object resource, Transaction tx )
    {
        if ( resource == null || tx == null )
        {
            throw new IllegalResourceException( "Null parameter" );
        }
    }
    
    private RWLock getRWLockForAcquiring( Object resource, Transaction tx )
    {
        assertValidArguments( resource, tx );
        synchronized ( resourceLockMap )
        {
            RWLock lock = resourceLockMap.get( resource );
            if ( lock == null )
            {
                lock = new RWLock( resource, ragManager );
                resourceLockMap.put( resource, lock );
            }
            lock.mark();
            return lock;
        }
    }
    
    private RWLock getRWLockForReleasing( Object resource, Transaction tx, int readCountPrerequisite,
            int writeCountPrerequisite )
    {
        assertValidArguments( resource, tx );
        synchronized ( resourceLockMap )
        {
            RWLock lock = resourceLockMap.get( resource );
            if ( lock == null )
            {
                throw new LockNotFoundException( "Lock not found for: "
                    + resource + " tx:" + tx );
            }
            if ( !lock.isMarked() && lock.getReadCount() == readCountPrerequisite &&
                lock.getWriteCount() == writeCountPrerequisite &&
                lock.getWaitingThreadsCount() == 0 )
            {
                resourceLockMap.remove( resource );
            }
            return lock;
        }
    }

    private static class ListAppendingVisitor implements Visitor<LockInfo, RuntimeException>
    {
        private final List<LockInfo> result = new ArrayList<>();

        @Override
        public boolean visit( LockInfo element )
        {
            result.add( element );
            return false;
        }
    }
    
    private static class DumpVisitor implements Visitor<LockInfo, RuntimeException>
    {
        private final StringLogger logger;
        
        DumpVisitor( Logging logging )
        {
            logger = logging.getMessagesLog( LockManager.class );
        }
        
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
            logger.info( "Total lock count: readCount=" + lock.getReadCount() + " writeCount="
                                + lock.getWriteCount() + " for "
                                + lock.getResourceType().toString( lock.getResourceId() ) );
            logger.info( "Waiting list:" );
            StringBuilder waitlist = new StringBuilder();
            String sep = "";
            for ( WaitingThread we : lock.getWaitingThreads() )
            {
                waitlist.append( sep ).append( "[tid=" ).append( we.getThreadId() ).append( "(" ).append(
                        we.getReadCount() ).append( "r," ).append( we.getWriteCount() ).append( "w )," ).append(
                        we.isWaitingOnWriteLock() ? "Write" : "Read" ).append( "Lock]" );
                sep = ", ";
            }
            logger.info( waitlist.toString() );
            for ( LockingTransaction tle : lock.getLockingTransactions() )
            {
                logger.info( "" + tle.getTransaction() + "(" + tle.getReadCount() + "r," + tle.getWriteCount()
                                    + "w)" );
            }
        }

        void done()
        {
            if ( emptyLockCount > 0 )
            {
                logger.info( "There are " + emptyLockCount + " empty locks" );
            }
            else
            {
                logger.info( "There are no empty locks" );
            }
        }
    }
}
