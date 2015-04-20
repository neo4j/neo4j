/*
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
package org.neo4j.kernel.impl.locking.community;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.kernel.impl.transaction.IllegalResourceException;
import org.neo4j.kernel.impl.util.StringLogger;
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
    public void getReadLock( Object resource, Object tx )
        throws DeadlockDetectedException, IllegalResourceException
    {
        getRWLockForAcquiring( resource, tx ).acquireReadLock( tx );
    }

    @Override
    public boolean tryReadLock( Object resource, Object tx )
        throws IllegalResourceException
    {
        return getRWLockForAcquiring( resource, tx ).tryAcquireReadLock( tx );
    }

    @Override
    public void getWriteLock( Object resource, Object tx )
        throws DeadlockDetectedException, IllegalResourceException
    {
        getRWLockForAcquiring( resource, tx ).acquireWriteLock( tx );
    }

    @Override
    public boolean tryWriteLock( Object resource, Object tx )
        throws IllegalResourceException
    {
        return getRWLockForAcquiring( resource, tx ).tryAcquireWriteLock( tx );
    }

    @Override
    public void releaseReadLock( Object resource, Object tx )
        throws LockNotFoundException, IllegalResourceException
    {
        getRWLockForReleasing( resource, tx, 1, 0 ).releaseReadLock( tx );
    }

    @Override
    public void releaseWriteLock( Object resource, Object tx )
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

    /**
     * Visit all locks.
     *
     * The supplied visitor may not block.
     *
     * @param visitor visitor for visiting each lock.
     */
    public void accept( Visitor<RWLock, RuntimeException> visitor )
    {
        synchronized ( resourceLockMap )
        {
            for ( RWLock lock : resourceLockMap.values() )
            {
                if ( visitor.visit( lock ) )
                {
                    break;
                }
            }
        }
    }

    private void assertValidArguments( Object resource, Object tx )
    {
        if ( resource == null || tx == null )
        {
            throw new IllegalResourceException( "Null parameter: resource = " + resource + ", tx = " + tx );
        }
    }

    private RWLock getRWLockForAcquiring( Object resource, Object tx )
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

    private RWLock getRWLockForReleasing( Object resource, Object tx, int readCountPrerequisite,
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
}
