/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
        clientA.acquireShared( NODE, 1L );
        clientA.acquireExclusive( NODE, 1L );

        // Then shared locks should wait
        Future<Object> clientBLock = acquireExclusive( clientB, NODE, 1L ).callAndAssertWaiting();

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
    public void shouldRetainExclusiveLockAfterReleasingSharedLock() throws Exception
    {
        // When
        clientA.acquireShared( NODE, 1L );
        clientA.acquireExclusive( NODE, 1L );

        // Then shared locks should wait
        Future<Object> clientBLock = acquireShared( clientB, NODE, 1L ).callAndAssertWaiting();

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
    public void shouldRetainSharedLockWhenAcquiredAfterExclusiveLock() throws Exception
    {
        // When
        clientA.acquireExclusive( NODE, 1L );
        clientA.acquireShared( NODE, 1L );

        // Then this should wait
        Future<Object> clientBLock = acquireExclusive( clientB, NODE, 1L ).callAndAssertWaiting();

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
    public void sharedLocksShouldStack() throws Exception
    {
        // When
        clientA.acquireShared( NODE, 1L );
        clientA.acquireShared( NODE, 1L );
        clientA.acquireShared( NODE, 1L );

        // Then exclusive locks should wait
        Future<Object> clientBLock = acquireExclusive( clientB, NODE, 1L ).callAndAssertWaiting();

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
    public void exclusiveLocksShouldBeReentrantAndBlockOtherExclusiveLocks() throws Exception
    {
        // When
        clientA.acquireExclusive( NODE, 1L );
        clientA.acquireExclusive( NODE, 1L );
        clientA.acquireExclusive( NODE, 1L );

        // Then exclusive locks should wait
        Future<Object> clientBLock = acquireExclusive( clientB, NODE, 1L ).callAndAssertWaiting();

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
    public void exclusiveLocksShouldBeReentrantAndBlockOtherSharedLocks() throws Exception
    {
        // When
        clientA.acquireExclusive( NODE, 1L );
        clientA.acquireShared( NODE, 1L );
        clientA.tryExclusiveLock( NODE, 1L );

        // Then exclusive locks should wait
        Future<Object> clientBLock = acquireShared( clientB, NODE, 1L ).callAndAssertWaiting();

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
    public void sharedLocksShouldNotReplaceExclusiveLocks() throws Exception
    {
        // When
        clientA.acquireExclusive( NODE, 1L );
        clientA.acquireShared( NODE, 1L );

        // Then shared locks should wait
        Future<Object> clientBLock = acquireShared( clientB, NODE, 1L ).callAndAssertWaiting();

        // And when
        clientA.releaseShared( NODE, 1L );

        // Then other thread should still wait
        assertWaiting( clientB, clientBLock );

        // But when
        clientA.releaseExclusive( NODE, 1L );

        // Then
        assertNotWaiting( clientB, clientBLock );
    }
}
