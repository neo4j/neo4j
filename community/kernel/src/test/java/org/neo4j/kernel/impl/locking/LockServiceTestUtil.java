/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.locking;

import java.util.concurrent.locks.LockSupport;

public class LockServiceTestUtil
{
    /**
     * Find a thread that is currently blocked waiting to grab a lock with the LockService,
     * on the given lockedNodeId. Or throws an AssertionError if it didn't such a thread within
     * maxWaitTimeMillis.
     *
     * Note that only a single thread is found, even though in principle, more than one could
     * be blocked on the same lock.
     *
     * @param lockedNodeId
     * @param maxWaitTimeMillis
     * @return
     */
    public static Thread spinFindThreadBlockedByNodeLock( long lockedNodeId, long maxWaitTimeMillis )
    {
        long deadline = System.currentTimeMillis() + maxWaitTimeMillis;
        AbstractLockService.LockedNode targetLock = new AbstractLockService.LockedNode( lockedNodeId );

        Thread thread = null;
        ThreadGroup group = Thread.currentThread().getThreadGroup();
        while ( group.getParent() != null )
        {
            group = group.getParent();
        }

        Thread[] threads = new Thread[100];
        int snapshotCount = 0;
        while ( thread == null && System.currentTimeMillis() < deadline )
        {
            if ( snapshotCount == threads.length )
            {
                threads = new Thread[threads.length * 2];
            }
            snapshotCount = group.enumerate( threads );

            for ( int i = 0; i < snapshotCount; i++ )
            {
                Thread candidate = threads[i];
                Object blocker = LockSupport.getBlocker( candidate );
                if ( blocker instanceof AbstractLockService.LockedNode )
                {
                    AbstractLockService.LockedNode lockedNode = (AbstractLockService.LockedNode) blocker;
                    if ( lockedNode.equals( targetLock ) )
                    {
                        thread = candidate;
                        break;
                    }
                }
            }
        }

        if (thread == null )
        {
            throw new AssertionError( "Found no thread blocked on " + targetLock );
        }

        return thread;
    }
}
