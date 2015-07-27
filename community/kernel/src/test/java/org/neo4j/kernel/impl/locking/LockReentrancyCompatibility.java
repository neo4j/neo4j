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
package org.neo4j.kernel.impl.locking;

import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.impl.locking.ResourceTypes.NODE;

@Ignore("Not a test. This is a compatibility suite, run from LockingCompatibilityTestSuite.")
public class LockReentrancyCompatibility extends LockingCompatibilityTestSuite.Compatibility
{
    public LockReentrancyCompatibility( LockingCompatibilityTestSuite suite )
    {
        super( suite );
    }

    @Test
    public void shouldAcquireExclusiveIfClientIsOnlyOneHoldingShared() throws Exception
    {
        // When
        clientA.acquireShared( NODE, 1l );
        clientA.acquireExclusive( NODE, 1l );

        // Then shared locks should wait
        Future<Object> clientBLock = acquireExclusive( clientB, NODE, 1l ).callAndAssertWaiting();

        // And when
        clientA.releaseExclusive( NODE, 1l );

        // Then other thread should still wait
        assertWaiting( clientB, clientBLock );

        // But when
        clientA.releaseShared( NODE, 1l );

        // Then
        assertNotWaiting( clientB, clientBLock );
    }

    @Test
    public void shouldRetainExclusiveLockAfterReleasingSharedLock() throws Exception
    {
        // When
        clientA.acquireShared( NODE, 1l );
        clientA.acquireExclusive( NODE, 1l );

        // Then shared locks should wait
        Future<Object> clientBLock = acquireShared( clientB, NODE, 1l ).callAndAssertWaiting();

        // And when
        clientA.releaseShared( NODE, 1l );

        // Then other thread should still wait
        assertWaiting( clientB, clientBLock );

        // But when
        clientA.releaseExclusive( NODE, 1l );

        // Then
        assertNotWaiting( clientB, clientBLock );
    }

    @Test
    public void shouldRetainSharedLockWhenAcquiredAfterExclusiveLock() throws Exception
    {
        // When
        clientA.acquireExclusive( NODE, 1l );
        clientA.acquireShared( NODE, 1l );

        // Then this should wait
        Future<Object> clientBLock = acquireExclusive( clientB, NODE, 1l ).callAndAssertWaiting();

        // And when
        clientA.releaseExclusive( NODE, 1l );

        // Then other thread should still wait
        assertWaiting( clientB, clientBLock );

        // But when
        clientA.releaseShared( NODE, 1l );

        // Then
        assertNotWaiting( clientB, clientBLock );
    }

    @Test
    public void sharedLocksShouldStack() throws Exception
    {
        // When
        clientA.acquireShared( NODE, 1l );
        clientA.acquireShared( NODE, 1l );
        clientA.acquireShared( NODE, 1l );

        // Then exclusive locks should wait
        Future<Object> clientBLock = acquireExclusive( clientB, NODE, 1l ).callAndAssertWaiting();

        // And when
        clientA.releaseShared( NODE, 1l );
        clientA.releaseShared( NODE, 1l );

        // Then other thread should still wait
        assertWaiting( clientB, clientBLock );

        // But when
        clientA.releaseShared( NODE, 1l );

        // Then
        assertNotWaiting( clientB, clientBLock );
    }

    @Test
    public void exclusiveLocksShouldBeReentrantAndBlockOtherExclusiveLocks() throws Exception
    {
        // When
        clientA.acquireExclusive( NODE, 1l );
        clientA.acquireExclusive( NODE, 1l );
        clientA.acquireExclusive( NODE, 1l );

        // Then exclusive locks should wait
        Future<Object> clientBLock = acquireExclusive( clientB, NODE, 1l ).callAndAssertWaiting();

        // And when
        clientA.releaseExclusive( NODE, 1l );
        clientA.releaseExclusive( NODE, 1l );

        // Then other thread should still wait
        assertWaiting( clientB, clientBLock );

        // But when
        clientA.releaseExclusive( NODE, 1l );

        // Then
        assertNotWaiting( clientB, clientBLock );
    }

    @Test
    public void exclusiveLocksShouldBeReentrantAndBlockOtherSharedLocks() throws Exception
    {
        // When
        clientA.acquireExclusive( NODE, 1l );
        clientA.acquireShared( NODE, 1l );
        clientA.tryExclusiveLock( NODE, 1l );

        // Then exclusive locks should wait
        Future<Object> clientBLock = acquireShared( clientB, NODE, 1l ).callAndAssertWaiting();

        // And when
        clientA.releaseExclusive( NODE, 1l );
        clientA.releaseShared( NODE, 1l );

        // Then other thread should still wait
        assertWaiting( clientB, clientBLock );

        // But when
        clientA.releaseExclusive( NODE, 1l );

        // Then
        assertNotWaiting( clientB, clientBLock );
    }

    @Test
    public void sharedLocksShouldNotReplaceExclusiveLocks() throws Exception
    {
        // When
        clientA.acquireExclusive( NODE, 1l );
        clientA.acquireShared( NODE, 1l );

        // Then shared locks should wait
        Future<Object> clientBLock = acquireShared( clientB, NODE, 1l ).callAndAssertWaiting();

        // And when
        clientA.releaseShared( NODE, 1l );

        // Then other thread should still wait
        assertWaiting( clientB, clientBLock );

        // But when
        clientA.releaseExclusive( NODE, 1l );

        // Then
        assertNotWaiting( clientB, clientBLock );
    }

    @Test
    public void shouldUpgradeAndDowngradeSameSharedLock() throws InterruptedException
    {
        // when
        clientA.acquireShared( NODE, 1L );
        clientB.acquireShared( NODE, 1L );


        LockIdentityExplorer sharedLockExplorer = new LockIdentityExplorer( NODE, 1L );
        locks.accept( sharedLockExplorer );

        // then xclusive should wait for shared from other client to be released
        Future<Object> exclusiveLockFuture = acquireExclusive( clientB, NODE, 1L ).callAndAssertWaiting();

        // and when
        clientA.releaseAll();

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

        private Locks.ResourceType resourceType;
        private long resourceId;
        private long lockIdentityHashCode;

        public LockIdentityExplorer( Locks.ResourceType resourceType, long resourceId )
        {
            this.resourceType = resourceType;
            this.resourceId = resourceId;
        }

        @Override
        public void visit( Locks.ResourceType resourceType, long resourceId, String description,
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
