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
import org.neo4j.kernel.impl.transaction.IllegalResourceException;
import org.neo4j.logging.Logger;

public class LockManagerImpl
{
    private final Map<Object,RWLock> resourceLockMap = new HashMap<>();
    private final RagManager ragManager;

    public LockManagerImpl( RagManager ragManager )
    {
        this.ragManager = ragManager;
    }

    public long getDetectedDeadlockCount()
    {
        return ragManager.getDeadlockCount();
    }

    public boolean getReadLock( Object resource, Object tx )
        throws DeadlockDetectedException, IllegalResourceException
    {
        return unusedResourceGuard( resource, tx, getRWLockForAcquiring( resource, tx ).acquireReadLock( tx ) );
    }

    public boolean tryReadLock( Object resource, Object tx )
        throws IllegalResourceException
    {
        return unusedResourceGuard( resource, tx, getRWLockForAcquiring( resource, tx ).tryAcquireReadLock( tx ) );
    }

    public boolean getWriteLock( Object resource, Object tx )
        throws DeadlockDetectedException, IllegalResourceException
    {
        return unusedResourceGuard(resource, tx, getRWLockForAcquiring( resource, tx ).acquireWriteLock( tx ) );
    }

    public boolean tryWriteLock( Object resource, Object tx )
        throws IllegalResourceException
    {
        return unusedResourceGuard( resource, tx, getRWLockForAcquiring( resource, tx ).tryAcquireWriteLock( tx ) );
    }

    public void releaseReadLock( Object resource, Object tx )
        throws LockNotFoundException, IllegalResourceException
    {
        getRWLockForReleasing( resource, tx, 1, 0, true ).releaseReadLock( tx );
    }

    public void releaseWriteLock( Object resource, Object tx )
        throws LockNotFoundException, IllegalResourceException
    {
        getRWLockForReleasing( resource, tx, 0, 1, true ).releaseWriteLock( tx );
    }

    public void dumpLocksOnResource( final Object resource, Logger logger )
    {
        final RWLock lock;
        synchronized ( resourceLockMap )
        {
            if ( !resourceLockMap.containsKey( resource ) )
            {
                logger.log( "No locks on " + resource );
                return;
            }
            lock = resourceLockMap.get( resource );
        }
        logger.bulk( bulkLogger -> {
            bulkLogger.log( "Dump locks on resource %s", resource );
            lock.logTo( bulkLogger );
        } );
    }

    /**
     * Check if lock was obtained and in case if not will try to clear optimistically allocated lock from global
     * resource map
     *
     * @return {@code lockObtained }
     **/
    private boolean unusedResourceGuard(Object resource, Object tx, boolean lockObtained) {
        if (!lockObtained)
        {
            // if lock was not acquired cleaning up optimistically allocated value
            // for case when it was only used by current call, if it was used by somebody else
            // lock will be released during release call
            getRWLockForReleasing( resource, tx, 0, 0, false );
        }
        return lockObtained;
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
                lock = createLock( resource );
                resourceLockMap.put( resource, lock );
            }
            lock.mark();
            return lock;
        }
    }

    // visible for testing
    protected RWLock createLock( Object resource )
    {
        return new RWLock( resource, ragManager );
    }

    private RWLock getRWLockForReleasing( Object resource, Object tx, int readCountPrerequisite,
            int writeCountPrerequisite, boolean strict )
    {
        assertValidArguments( resource, tx );
        synchronized ( resourceLockMap )
        {
            RWLock lock = resourceLockMap.get( resource );
            if (lock == null )
            {
                if (!strict)
                {
                    return null;
                }
                throw new LockNotFoundException( "Lock not found for: "
                    + resource + " tx:" + tx );
            }
            // we need to get info from a couple of synchronized methods
            // to make it info consistent we need to synchronized lock to make sure it will not change between
            // various calls
            synchronized ( lock )
            {
                if ( !lock.isMarked() && lock.getReadCount() == readCountPrerequisite &&
                     lock.getWriteCount() == writeCountPrerequisite &&
                     lock.getWaitingThreadsCount() == 0 )
                {
                    resourceLockMap.remove( resource );
                }
            }
            return lock;
        }
    }
}
