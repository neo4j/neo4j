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

import org.junit.jupiter.api.Test;

import org.neo4j.kernel.impl.locking.Locks.Client;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceTypes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.lock.ResourceTypes.NODE;

abstract class CloseCompatibility extends LockCompatibilityTestSupport
{
    CloseCompatibility( LockingCompatibilityTestSuite suite )
    {
        super( suite );
    }

    @Test
    void shouldNotBeAbleToHandOutClientsIfClosed()
    {
        // GIVEN a lock manager and working clients
        try ( Client client = locks.newClient() )
        {
            client.acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 0 );
        }

        // WHEN
        locks.close();

        // THEN
        assertThrows( IllegalStateException.class, locks::newClient );
    }

    @Test
    void closeShouldWaitAllOperationToFinish()
    {
        // given
        clientA.acquireShared( LockTracer.NONE, NODE, 1L );
        clientA.acquireShared( LockTracer.NONE, NODE, 3L );
        clientB.acquireShared( LockTracer.NONE, NODE, 1L );
        acquireShared( clientC, LockTracer.NONE, NODE, 2L );
        acquireExclusive( clientB, LockTracer.NONE, NODE, 1L ).callAndAssertWaiting();
        acquireExclusive( clientC, LockTracer.NONE, NODE, 1L ).callAndAssertWaiting();

        // when
        clientB.close();
        clientC.close();
        clientA.close();

        // all locks should be closed at this point regardless of
        // reader/writer waiter in any threads
        // those should be gracefully finish and client should be closed

        LockCountVisitor lockCountVisitor = new LockCountVisitor();
        locks.accept( lockCountVisitor );
        assertEquals( 0, lockCountVisitor.getLockCount() );

    }

    @Test
    void shouldNotBeAbleToAcquireSharedLockFromClosedClient()
    {
        clientA.close();
        assertThrows( LockClientStoppedException.class, () -> clientA.acquireShared( LockTracer.NONE, NODE, 1L ) );
    }

    @Test
    void shouldNotBeAbleToAcquireExclusiveLockFromClosedClient()
    {
        clientA.close();
        assertThrows( LockClientStoppedException.class, () -> clientA.acquireExclusive( LockTracer.NONE, NODE, 1L ) );
    }

    @Test
    void shouldNotBeAbleToTryAcquireSharedLockFromClosedClient()
    {
        clientA.close();
        assertThrows( LockClientStoppedException.class, () -> clientA.trySharedLock( NODE, 1L ) );
    }

    @Test
    void shouldNotBeAbleToTryAcquireExclusiveLockFromClosedClient()
    {
        clientA.close();
        assertThrows( LockClientStoppedException.class, () -> clientA.tryExclusiveLock( NODE, 1L ) );
    }

    @Test
    void releaseTryLocksOnClose()
    {
        assertTrue( clientA.trySharedLock( ResourceTypes.NODE, 1L ) );
        assertTrue( clientB.tryExclusiveLock( ResourceTypes.NODE, 2L ) );

        clientA.close();
        clientB.close();

        LockCountVisitor lockCountVisitor = new LockCountVisitor();
        locks.accept( lockCountVisitor );
        assertEquals( 0, lockCountVisitor.getLockCount() );
    }
}
