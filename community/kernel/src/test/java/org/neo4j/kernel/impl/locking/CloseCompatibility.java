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

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import org.neo4j.kernel.impl.locking.Locks.Client;

import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.locking.ResourceTypes.NODE;

@Ignore( "Not a test. This is a compatibility suite, run from LockingCompatibilityTestSuite." )
public class CloseCompatibility extends LockingCompatibilityTestSuite.Compatibility
{
    public CloseCompatibility( LockingCompatibilityTestSuite suite )
    {
        super( suite );
    }

    @Test
    public void shouldNotBeAbleToHandOutClientsIfShutDown() throws Throwable
    {
        // GIVEN a lock manager and working clients
        try ( Client client = locks.newClient() )
        {
            client.acquireExclusive( ResourceTypes.NODE, 0 );
        }

        // WHEN
        locks.stop();
        locks.shutdown();

        // THEN
        try
        {
            locks.newClient();
            fail( "Should fail" );
        }
        catch ( IllegalStateException e )
        {
            // Good
        }
    }

    @Test
    public void closeShouldWaitAllOperationToFinish()
    {
        // given
        clientA.acquireShared( NODE, 1L );
        clientA.acquireShared( NODE, 3L );
        clientB.acquireShared( NODE, 1L );
        acquireShared( clientC, NODE, 2L );
        acquireExclusive( clientB, NODE, 1L ).callAndAssertWaiting();
        acquireExclusive( clientC, NODE, 1L ).callAndAssertWaiting();

        // when
        clientB.close();
        clientC.close();
        clientA.close();

        // all locks should be closed at this point regardless of
        // reader/writer waiter in any threads
        // those should be gracefully finish and client should be closed

        LockCountVisitor lockCountVisitor = new LockCountVisitor();
        locks.accept( lockCountVisitor );
        Assert.assertEquals( 0, lockCountVisitor.getLockCount() );

    }

    @Test( expected = LockClientStoppedException.class )
    public void shouldNotBeAbleToAcquireSharedLockFromClosedClient()
    {
        clientA.close();
        clientA.acquireShared( NODE, 1l );
    }

    @Test( expected = LockClientStoppedException.class )
    public void shouldNotBeAbleToAcquireExclusiveLockFromClosedClient()
    {
        clientA.close();
        clientA.acquireExclusive( NODE, 1l );
    }

    @Test( expected = LockClientStoppedException.class )
    public void shouldNotBeAbleToTryAcquireSharedLockFromClosedClient()
    {
        clientA.close();
        clientA.trySharedLock( NODE, 1L );
    }

    @Test( expected = LockClientStoppedException.class )
    public void shouldNotBeAbleToTryAcquireExclusiveLockFromClosedClient()
    {
        clientA.close();
        clientA.tryExclusiveLock( NODE, 1L );
    }
}
