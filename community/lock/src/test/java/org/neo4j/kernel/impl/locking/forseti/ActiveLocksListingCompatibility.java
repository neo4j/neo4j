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

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.lock.ResourceType.LABEL;
import static org.neo4j.lock.ResourceType.NODE;
import static org.neo4j.lock.ResourceType.RELATIONSHIP;

import org.apache.commons.lang3.mutable.MutableInt;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.impl.api.LeaseService;
import org.neo4j.lock.ActiveLock;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.LockType;
import org.neo4j.memory.EmptyMemoryTracker;

abstract class ActiveLocksListingCompatibility extends LockCompatibilityTestSupport {
    ActiveLocksListingCompatibility(LockingCompatibilityTest suite) {
        super(suite);
    }

    @Test
    void activeLockShouldContainUserTransactionFromClient() {
        clientA.initialize(LeaseService.NO_LEASES.newClient(), 15, EmptyMemoryTracker.INSTANCE, Config.defaults());
        clientA.acquireExclusive(LockTracer.NONE, NODE, 1);

        assertEquals(1, clientA.activeLockCount());
        var lock = clientA.activeLocks().iterator().next();
        assertEquals(15, lock.transactionId());
    }

    @Test
    void visitedExclusiveLockPreserveOwningTransaction() {
        int userTransactionId = 15;
        clientA.initialize(
                LeaseService.NO_LEASES.newClient(), userTransactionId, EmptyMemoryTracker.INSTANCE, Config.defaults());
        clientA.acquireExclusive(LockTracer.NONE, NODE, 1);

        MutableInt observedLocks = new MutableInt();
        locks.accept(
                (lockType,
                        resourceType,
                        transactionId,
                        resourceId,
                        description,
                        estimatedWaitTime,
                        lockIdentityHashCode) -> {
                    assertEquals(userTransactionId, transactionId);
                    assertSame(NODE, resourceType);
                    observedLocks.increment();
                });
        assertEquals(1, observedLocks.intValue());
    }

    @Test
    void visitedSharedLockPreserveOwningTransaction() {
        int userTransactionId = 15;
        clientA.initialize(
                LeaseService.NO_LEASES.newClient(), userTransactionId, EmptyMemoryTracker.INSTANCE, Config.defaults());
        clientA.acquireShared(LockTracer.NONE, NODE, 1);

        MutableInt observedLocks = new MutableInt();
        locks.accept(
                (lockType,
                        resourceType,
                        transactionId,
                        resourceId,
                        description,
                        estimatedWaitTime,
                        lockIdentityHashCode) -> {
                    assertEquals(userTransactionId, transactionId);
                    assertSame(NODE, resourceType);
                    observedLocks.increment();
                });
        assertEquals(1, observedLocks.intValue());
    }

    @Test
    void visitedSharedLockLockOwningByMultipleClients() {
        int userTransactionIdA = 15;
        int userTransactionIdB = 16;

        clientA.initialize(
                LeaseService.NO_LEASES.newClient(), userTransactionIdA, EmptyMemoryTracker.INSTANCE, Config.defaults());
        clientA.acquireShared(LockTracer.NONE, NODE, 1);

        clientB.initialize(
                LeaseService.NO_LEASES.newClient(), userTransactionIdB, EmptyMemoryTracker.INSTANCE, Config.defaults());
        clientB.acquireShared(LockTracer.NONE, NODE, 1);

        MutableInt observedLocks = new MutableInt();
        var observedTransactions = LongSets.mutable.empty();
        locks.accept(
                (lockType,
                        resourceType,
                        transactionId,
                        resourceId,
                        description,
                        estimatedWaitTime,
                        lockIdentityHashCode) -> {
                    observedTransactions.add(transactionId);
                    assertSame(NODE, resourceType);
                    observedLocks.increment();
                });

        assertEquals(2, observedLocks.intValue());
        assertTrue(
                observedTransactions.containsAll(userTransactionIdA, userTransactionIdB),
                "Observer set: " + observedTransactions);
    }

    @Test
    void shouldListLocksHeldByTheCurrentClient() {
        // given
        clientA.initialize(LeaseService.NO_LEASES.newClient(), 1, EmptyMemoryTracker.INSTANCE, Config.defaults());
        clientA.acquireExclusive(LockTracer.NONE, NODE, 1, 2, 3);
        clientA.acquireShared(LockTracer.NONE, NODE, 3, 4, 5);

        // when
        var locks = clientA.activeLocks();

        // then
        assertEquals(
                asList(
                        new ActiveLock(NODE, LockType.EXCLUSIVE, 1, 1),
                        new ActiveLock(NODE, LockType.EXCLUSIVE, 1, 2),
                        new ActiveLock(NODE, LockType.EXCLUSIVE, 1, 3),
                        new ActiveLock(NODE, LockType.SHARED, 1, 4),
                        new ActiveLock(NODE, LockType.SHARED, 1, 5)),
                locks);
    }

    @Test
    void shouldCountNumberOfActiveLocks() {
        // given
        clientA.initialize(LeaseService.NO_LEASES.newClient(), 1, EmptyMemoryTracker.INSTANCE, Config.defaults());
        clientA.acquireShared(LockTracer.NONE, LABEL, 0);
        clientA.acquireShared(LockTracer.NONE, RELATIONSHIP, 17);
        clientA.acquireShared(LockTracer.NONE, NODE, 12);

        // when
        long count = clientA.activeLockCount();

        // then
        assertEquals(3, count);
    }
}
