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
package org.neo4j.kernel.impl.locking;

import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.Future;

import org.neo4j.storageengine.api.lock.ResourceType;

import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.impl.locking.ResourceTypes.NODE;

@Ignore( "Not a test. This is a compatibility suite, run from LockingCompatibilityTestSuite." )
public class LockReentrancyCompatibility extends LockingCompatibilityTestSuite.Compatibility
{
    public LockReentrancyCompatibility( LockingCompatibilityTestSuite suite )
    {
        super( suite );
    }

    @Test
    public void shouldAcquireExclusiveIfClientIsOnlyOneHoldingShared()
    {
        // When
        clientA.acquireShared( LockTracer.NONE, NODE, 1L );
        clientA.acquireExclusive( LockTracer.NONE, NODE, 1L );

        // Then shared locks should wait
        Future<Object> clientBLock = acquireExclusive( clientB, LockTracer.NONE, NODE, 1L ).callAndAssertWaiting();

        // And when
        clientA.releaseExclusive( NODE, 1L );

        // Then other thread should still wait
        assertWaiting( clientB, clientBLock );

        // But when
        clientA.releaseShared( NODE, 1L );

        // Then
        assertNotWaiting( clientB, clientBLock );
    }

    @Test
    public void shouldRetainExclusiveLockAfterReleasingSharedLock()
    {
        // When
        clientA.acquireShared( LockTracer.NONE, NODE, 1L );
        clientA.acquireExclusive( LockTracer.NONE, NODE, 1L );

        // Then shared locks should wait
        Future<Object> clientBLock = acquireShared( clientB, LockTracer.NONE, NODE, 1L ).callAndAssertWaiting();

        // And when
        clientA.releaseShared( NODE, 1L );

        // Then other thread should still wait
        assertWaiting( clientB, clientBLock );

        // But when
        clientA.releaseExclusive( NODE, 1L );

        // Then
        assertNotWaiting( clientB, clientBLock );
    }

    @Test
    public void shouldRetainSharedLockWhenAcquiredAfterExclusiveLock()
    {
        // When
        clientA.acquireExclusive( LockTracer.NONE, NODE, 1L );
        clientA.acquireShared( LockTracer.NONE, NODE, 1L );

        // Then this should wait
        Future<Object> clientBLock = acquireExclusive( clientB, LockTracer.NONE, NODE, 1L ).callAndAssertWaiting();

        // And when
        clientA.releaseExclusive( NODE, 1L );

        // Then other thread should still wait
        assertWaiting( clientB, clientBLock );

        // But when
        clientA.releaseShared( NODE, 1L );

        // Then
        assertNotWaiting( clientB, clientBLock );
    }

    @Test
    public void sharedLocksShouldStack()
    {
        // When
        clientA.acquireShared( LockTracer.NONE, NODE, 1L );
        clientA.acquireShared( LockTracer.NONE, NODE, 1L );
        clientA.acquireShared( LockTracer.NONE, NODE, 1L );

        // Then exclusive locks should wait
        Future<Object> clientBLock = acquireExclusive( clientB, LockTracer.NONE, NODE, 1L ).callAndAssertWaiting();

        // And when
        clientA.releaseShared( NODE, 1L );
        clientA.releaseShared( NODE, 1L );

        // Then other thread should still wait
        assertWaiting( clientB, clientBLock );

        // But when
        clientA.releaseShared( NODE, 1L );

        // Then
        assertNotWaiting( clientB, clientBLock );
    }

    @Test
    public void exclusiveLocksShouldBeReentrantAndBlockOtherExclusiveLocks()
    {
        // When
        clientA.acquireExclusive( LockTracer.NONE, NODE, 1L );
        clientA.acquireExclusive( LockTracer.NONE, NODE, 1L );
        clientA.acquireExclusive( LockTracer.NONE, NODE, 1L );

        // Then exclusive locks should wait
        Future<Object> clientBLock = acquireExclusive( clientB, LockTracer.NONE, NODE, 1L ).callAndAssertWaiting();

        // And when
        clientA.releaseExclusive( NODE, 1L );
        clientA.releaseExclusive( NODE, 1L );

        // Then other thread should still wait
        assertWaiting( clientB, clientBLock );

        // But when
        clientA.releaseExclusive( NODE, 1L );

        // Then
        assertNotWaiting( clientB, clientBLock );
    }

    @Test
    public void exclusiveLocksShouldBeReentrantAndBlockOtherSharedLocks()
    {
        // When
        clientA.acquireExclusive( LockTracer.NONE, NODE, 1L );
        clientA.acquireShared( LockTracer.NONE, NODE, 1L );
        clientA.tryExclusiveLock( NODE, 1L );

        // Then exclusive locks should wait
        Future<Object> clientBLock = acquireShared( clientB, LockTracer.NONE, NODE, 1L ).callAndAssertWaiting();

        // And when
        clientA.releaseExclusive( NODE, 1L );
        clientA.releaseShared( NODE, 1L );

        // Then other thread should still wait
        assertWaiting( clientB, clientBLock );

        // But when
        clientA.releaseExclusive( NODE, 1L );

        // Then
        assertNotWaiting( clientB, clientBLock );
    }

    @Test
    public void sharedLocksShouldNotReplaceExclusiveLocks()
    {
        // When
        clientA.acquireExclusive( LockTracer.NONE, NODE, 1L );
        clientA.acquireShared( LockTracer.NONE, NODE, 1L );

        // Then shared locks should wait
        Future<Object> clientBLock = acquireShared( clientB, LockTracer.NONE, NODE, 1L ).callAndAssertWaiting();

        // And when
        clientA.releaseShared( NODE, 1L );

        // Then other thread should still wait
        assertWaiting( clientB, clientBLock );

        // But when
        clientA.releaseExclusive( NODE, 1L );

        // Then
        assertNotWaiting( clientB, clientBLock );
    }

    @Test
    public void shouldUpgradeAndDowngradeSameSharedLock()
    {
        // when
        clientA.acquireShared( LockTracer.NONE, NODE, 1L );
        clientB.acquireShared( LockTracer.NONE, NODE, 1L );

        LockIdentityExplorer sharedLockExplorer = new LockIdentityExplorer( NODE, 1L );
        locks.accept( sharedLockExplorer );

        // then xclusive should wait for shared from other client to be released
        Future<Object> exclusiveLockFuture = acquireExclusive( clientB, LockTracer.NONE, NODE, 1L ).callAndAssertWaiting();

        // and when
        clientA.releaseShared( NODE, 1L );

        // exclusive lock should be received
        assertNotWaiting( clientB, exclusiveLockFuture );

        // and when releasing exclusive
        clientB.releaseExclusive( NODE, 1L );

        // we still should have same read lock
        LockIdentityExplorer releasedLockExplorer = new LockIdentityExplorer( NODE, 1L );
        locks.accept( releasedLockExplorer );

        // we still hold same lock as before
        assertEquals( sharedLockExplorer.getLockIdentityHashCode(),
                releasedLockExplorer.getLockIdentityHashCode() );
    }

    private static class LockIdentityExplorer implements Locks.Visitor
    {
        private final ResourceType resourceType;
        private final long resourceId;
        private long lockIdentityHashCode;

        LockIdentityExplorer( ResourceType resourceType, long resourceId )
        {
            this.resourceType = resourceType;
            this.resourceId = resourceId;
        }

        @Override
        public void visit( ResourceType resourceType, long resourceId, String description,
                long estimatedWaitTime,
                long lockIdentityHashCode )
        {
            if ( this.resourceType.equals( resourceType ) && this.resourceId == resourceId )
            {
                this.lockIdentityHashCode = lockIdentityHashCode;
            }
        }

        public long getLockIdentityHashCode()
        {
            return lockIdentityHashCode;
        }
    }
}
