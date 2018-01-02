/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertFalse;
import static org.neo4j.kernel.impl.locking.ResourceTypes.NODE;

/**
 * Tests simple acquiring and releasing of single locks.
 * For testing "stacking" locks on the same client, see {@link LockReentrancyCompatibility}. */
@Ignore("Not a test. This is a compatibility suite, run from LockingCompatibilityTestSuite.")
public class AcquireAndReleaseLocksCompatibility extends LockingCompatibilityTestSuite.Compatibility
{
    public AcquireAndReleaseLocksCompatibility( LockingCompatibilityTestSuite suite )
    {
        super( suite );
    }

    @Test
    public void exclusiveShouldWaitForExclusive() throws Exception
    {
        // When
        clientA.acquireExclusive( NODE, 1L );

        // Then
        Future<Object> clientBLock = acquireExclusive( clientB, NODE, 1L ).callAndAssertWaiting();

        // And when
        clientA.releaseExclusive( NODE, 1L );

        // Then this should not block
        assertNotWaiting( clientB, clientBLock );
    }

    @Test
    public void exclusiveShouldWaitForShared() throws Exception
    {
        // When
        clientA.acquireShared( NODE, 1L );

        // Then other shared locks are allowed
        clientC.acquireShared( NODE, 1L );

        // But exclusive locks should wait
        Future<Object> clientBLock = acquireExclusive( clientB, NODE, 1L ).callAndAssertWaiting();

        // And when
        clientA.releaseShared( NODE, 1L );
        clientC.releaseShared( NODE, 1L );

        // Then this should not block
        assertNotWaiting( clientB, clientBLock );
    }

    @Test
    public void sharedShouldWaitForExclusive() throws Exception
    {
        // When
        clientA.acquireExclusive( NODE, 1L );

        // Then shared locks should wait
        Future<Object> clientBLock = acquireShared( clientB, NODE, 1L ).callAndAssertWaiting();

        // And when
        clientA.releaseExclusive( NODE, 1L );

        // Then this should not block
        assertNotWaiting( clientB, clientBLock );
    }

    @Test
    public void shouldTrySharedLock() throws Exception
    {
        // Given I've grabbed a share lock
        assertTrue( clientA.trySharedLock( NODE, 1L ) );

        // Then other clients can't have exclusive locks
        assertFalse( clientB.tryExclusiveLock( NODE, 1L ) );

        // But they are allowed share locks
        assertTrue( clientB.trySharedLock( NODE, 1L ) );
    }

    @Test
    public void shouldTryExclusiveLock() throws Exception
    {
        // Given I've grabbed an exclusive lock
        assertTrue( clientA.tryExclusiveLock( NODE, 1L ) );

        // Then other clients can't have exclusive locks
        assertFalse( clientB.tryExclusiveLock( NODE, 1L ) );

        // Nor can they have share locks
        assertFalse( clientB.trySharedLock( NODE, 1L ) );
    }

    @Test
    public void shouldTryUpgradeSharedToExclusive() throws Exception
    {
        // Given I've grabbed an exclusive lock
        assertTrue( clientA.trySharedLock( NODE, 1L ) );

        // Then I can upgrade it to exclusive
        assertTrue( clientA.tryExclusiveLock( NODE, 1L ) );

        // And other clients are denied it
        assertFalse( clientB.trySharedLock( NODE, 1L ) );
    }

    @Test
    public void shouldUpgradeExclusiveOnTry() throws Exception
    {
        // Given I've grabbed a shared lock
        clientA.acquireShared( NODE, 1L );

        // When
        assertTrue( clientA.tryExclusiveLock( NODE, 1L ) );

        // Then I should be able to release it
        clientA.releaseExclusive( NODE, 1L );
    }
}
