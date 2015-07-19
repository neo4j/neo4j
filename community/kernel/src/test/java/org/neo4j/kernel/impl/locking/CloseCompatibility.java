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

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import static org.neo4j.kernel.impl.locking.ResourceTypes.NODE;

@Ignore("Not a test. This is a compatibility suite, run from LockingCompatibilityTestSuite.")
public class CloseCompatibility extends LockingCompatibilityTestSuite.Compatibility
{
    public CloseCompatibility( LockingCompatibilityTestSuite suite )
    {
        super( suite );
    }

    @Test
    @Ignore("Will be merged and enabled when both lock managers will support it")
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

        final int[] counters = new int[1];
        locks.accept( new Locks.Visitor()
        {
            @Override
            public void visit( Locks.ResourceType resourceType, long resourceId, String description,
                    long estimatedWaitTime )
            {
                counters[0] ++;
            }
        } );
        Assert.assertEquals( 0, counters[0] );
    }

    @Test(expected = LockClientAlreadyClosedException.class)
    @Ignore("Will be enabled when both clients support it.")
    public void shouldNotBeAbleToAcquireSharedLockFromClosedClient()
    {
        clientA.close();
        clientA.acquireShared( NODE, 1l);
    }

    @Test(expected = LockClientAlreadyClosedException.class)
    @Ignore("Will be enabled when both clients support it.")
    public void shouldNotBeAbleToAcquireExclusiveLockFromClosedClient()
    {
        clientA.close();
        clientA.acquireExclusive( NODE, 1l);
    }

    @Test
    @Ignore("Will be enabled when both clients support it.")
    public void shouldNotBeAbleToAcquireLocksUsingTryFromClosedClient()
    {
        clientA.close();
        Assert.assertFalse( clientA.trySharedLock( NODE, 1l ) );
        Assert.assertFalse( clientA.tryExclusiveLock( NODE, 1l ) );
    }
}