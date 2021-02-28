/*
 * Copyright (c) "Neo4j"
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

import java.util.HashMap;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.helpers.collection.Visitor;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.impl.transaction.IllegalResourceException;
import org.neo4j.lock.LockTracer;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.util.VisibleForTesting;

public class LockManagerImpl
{
    private final Map<Object,RWLock> resourceLockMap = new HashMap<>();
    private final RagManager ragManager;
    private final SystemNanoClock clock;

    /**
     * Time within which any particular lock should be acquired.
     * @see GraphDatabaseSettings#lock_acquisition_timeout
     */
    private final long lockAcquisitionTimeoutNano;

    LockManagerImpl( RagManager ragManager, Config config, SystemNanoClock clock )
    {
        this.ragManager = ragManager;
        this.clock = clock;
        this.lockAcquisitionTimeoutNano = config.get( GraphDatabaseSettings.lock_acquisition_timeout ).toNanos();
    }

    boolean getReadLock( LockTracer tracer, LockResource resource, LockTransaction tx, MemoryTracker memoryTracker )
            throws DeadlockDetectedException
    {
        return unusedResourceGuard( resource, tx, getRWLockForAcquiring( resource, tx, memoryTracker ).acquireReadLock( tracer, tx, memoryTracker ) );
    }

    boolean tryReadLock( LockResource resource, LockTransaction tx, MemoryTracker memoryTracker )
    {
        return unusedResourceGuard( resource, tx, getRWLockForAcquiring( resource, tx, memoryTracker ).tryAcquireReadLock( tx, memoryTracker ) );
    }

    boolean getWriteLock( LockTracer tracer, LockResource resource, LockTransaction tx, MemoryTracker memoryTracker )
            throws DeadlockDetectedException
    {
        return unusedResourceGuard( resource, tx, getRWLockForAcquiring( resource, tx, memoryTracker ).acquireWriteLock( tracer, tx, memoryTracker ) );
    }

    boolean tryWriteLock( LockResource resource, LockTransaction tx, MemoryTracker memoryTracker )
    {
        return unusedResourceGuard( resource, tx, getRWLockForAcquiring( resource, tx, memoryTracker ).tryAcquireWriteLock( tx, memoryTracker ) );
    }

    void releaseReadLock( Object resource, LockTransaction tx, MemoryTracker memoryTracker )
    {
        getRWLockForReleasing( resource, tx, 1, 0, true ).releaseReadLock( tx, memoryTracker );
    }

    void releaseWriteLock( Object resource, LockTransaction tx, MemoryTracker memoryTracker )
    {
        getRWLockForReleasing( resource, tx, 0, 1, true ).releaseWriteLock( tx, memoryTracker );
    }

    /**
     * Check if lock was obtained and in case if not will try to clear optimistically allocated lock from global
     * resource map
     *
     * @return {@code lockObtained }
     **/
    private boolean unusedResourceGuard( Object resource, LockTransaction tx, boolean lockObtained )
    {
        if ( !lockObtained )
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
     * <p/>
     * The supplied visitor may not block.
     *
     * @param visitor visitor for visiting each lock.
     */
    public void accept( Visitor<RWLock,RuntimeException> visitor )
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

    private static void assertValidArguments( Object resource, Object tx )
    {
        if ( resource == null || tx == null )
        {
            throw new IllegalResourceException( "Null parameter: resource = " + resource + ", tx = " + tx );
        }
    }

    private RWLock getRWLockForAcquiring( LockResource resource, Object tx, MemoryTracker memoryTracker )
    {
        assertValidArguments( resource, tx );
        synchronized ( resourceLockMap )
        {
            RWLock lock = resourceLockMap.computeIfAbsent( resource, k -> createLock( resource, memoryTracker ) );
            lock.mark();
            return lock;
        }
    }

    @VisibleForTesting
    protected RWLock createLock( LockResource resource, MemoryTracker memoryTracker )
    {
        memoryTracker.allocateHeap( RWLock.SHALLOW_SIZE + HeapEstimator.HASH_MAP_NODE_SHALLOW_SIZE );
        return new RWLock( resource, ragManager, clock, lockAcquisitionTimeoutNano );
    }

    private RWLock getRWLockForReleasing( Object resource, Object tx, int readCountPrerequisite,
                                          int writeCountPrerequisite, boolean strict )
    {
        assertValidArguments( resource, tx );
        synchronized ( resourceLockMap )
        {
            RWLock lock = resourceLockMap.get( resource );
            if ( lock == null )
            {
                if ( !strict )
                {
                    return null;
                }
                throw new LockNotFoundException( "Lock not found for: "
                                                 + resource + " tx:" + tx );
            }
            // we need to get info from a couple of synchronized methods
            // to make it info consistent we need to synchronized lock to make sure it will not change between
            // various calls
            //noinspection SynchronizationOnLocalVariableOrMethodParameter
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
