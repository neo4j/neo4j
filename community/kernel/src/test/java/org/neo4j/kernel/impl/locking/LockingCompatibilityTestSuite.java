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

import org.junit.jupiter.api.Nested;

import java.time.Clock;

import org.neo4j.configuration.Config;
import org.neo4j.test.extension.actors.Actor;

/** Base for locking tests. */
public abstract class LockingCompatibilityTestSuite
{
    protected abstract Locks createLockManager( Config config, Clock clock );

    /**
     * Implementing this requires intricate knowledge of implementation of the particular locks client.
     * This is the most efficient way of telling whether or not a thread awaits a lock acquisition or not
     * so the price we pay for the potential fragility introduced here we gain in much snappier testing
     * when testing deadlocks and lock acquisitions.
     *
     * @param actor {@link Actor} that we wish to confirm is waiting for a lock acquisition.
     * @return {@code true} if the wait details marks a wait on a lock acquisition, otherwise {@code false}
     * so that a new thread wait/block will be registered and this method called again.
     */
    protected abstract boolean isAwaitingLockAcquisition( Actor actor ) throws Exception;

    @Nested
    class AcquireAndReleaseLocks extends AcquireAndReleaseLocksCompatibility
    {
        AcquireAndReleaseLocks()
        {
            super( LockingCompatibilityTestSuite.this );
        }
    }

    @Nested
    class Deadlock extends DeadlockCompatibility
    {
        Deadlock()
        {
            super( LockingCompatibilityTestSuite.this );
        }
    }

    @Nested
    class LockReentrancy extends LockReentrancyCompatibility
    {
        LockReentrancy()
        {
            super( LockingCompatibilityTestSuite.this );
        }
    }

    @Nested
    class RWLock extends RWLockCompatibility
    {
        RWLock()
        {
            super( LockingCompatibilityTestSuite.this );
        }
    }

    @Nested
    class Stop extends StopCompatibility
    {
        Stop()
        {
            super( LockingCompatibilityTestSuite.this );
        }
    }

    @Nested
    class Close extends CloseCompatibility
    {
        Close()
        {
            super( LockingCompatibilityTestSuite.this );
        }
    }

    @Nested
    class AcquisitionTimeout extends AcquisitionTimeoutCompatibility
    {
        AcquisitionTimeout()
        {
            super( LockingCompatibilityTestSuite.this );
        }
    }

    @Nested
    class Tracer extends TracerCompatibility
    {
        Tracer()
        {
            super( LockingCompatibilityTestSuite.this );
        }
    }

    @Nested
    class ActiveLocks extends ActiveLocksListingCompatibility
    {
        ActiveLocks()
        {
            super( LockingCompatibilityTestSuite.this );
        }
    }
}
