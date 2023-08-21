/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.locking.forseti;

import org.junit.jupiter.api.Nested;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.lock.LockType;
import org.neo4j.lock.ResourceType;
import org.neo4j.test.extension.actors.Actor;
import org.neo4j.time.SystemNanoClock;

/** Base for locking tests. */
public class LockingCompatibilityTest {
    protected LockManager createLockManager(Config config, SystemNanoClock clock) {
        return new ForsetiLockManager(config, clock, ResourceType.values());
    }

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
    protected boolean isAwaitingLockAcquisition(Actor actor) throws Exception {
        actor.untilWaitingIn(ForsetiClient.class.getDeclaredMethod(
                "waitFor", ForsetiLockManager.Lock.class, ResourceType.class, long.class, LockType.class, int.class));
        return true;
    }

    @Nested
    class AcquireAndReleaseLocks extends AcquireAndReleaseLocksCompatibility {
        AcquireAndReleaseLocks() {
            super(LockingCompatibilityTest.this);
        }
    }

    @Nested
    class Deadlock extends DeadlockCompatibility {
        Deadlock() {
            super(LockingCompatibilityTest.this);
        }
    }

    @Nested
    class LockReentrancy extends LockReentrancyCompatibility {
        LockReentrancy() {
            super(LockingCompatibilityTest.this);
        }
    }

    @Nested
    class RWLock extends RWLockCompatibility {
        RWLock() {
            super(LockingCompatibilityTest.this);
        }
    }

    @Nested
    class Stop extends StopCompatibility {
        Stop() {
            super(LockingCompatibilityTest.this);
        }
    }

    @Nested
    class Close extends CloseCompatibility {
        Close() {
            super(LockingCompatibilityTest.this);
        }
    }

    @Nested
    class AcquisitionTimeout extends AcquisitionTimeoutCompatibility {
        AcquisitionTimeout() {
            super(LockingCompatibilityTest.this);
        }
    }

    @Nested
    class Tracer extends TracerCompatibility {
        Tracer() {
            super(LockingCompatibilityTest.this);
        }
    }

    @Nested
    class ActiveLocks extends ActiveLocksListingCompatibility {
        ActiveLocks() {
            super(LockingCompatibilityTest.this);
        }
    }
}
