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
import static org.assertj.core.api.Assertions.assertThat;
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

        assertThat(clientA.activeLockCount()).isEqualTo(1);
        var lock = clientA.activeLocks(EmptyMemoryTracker.INSTANCE).iterator().next();
        assertThat(lock.transactionId()).isEqualTo(15);
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
                    assertThat(transactionId).isEqualTo(userTransactionId);
                    assertThat(resourceType).isSameAs(NODE);
                    observedLocks.increment();
                });
        assertThat(observedLocks.intValue()).isEqualTo(1);
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
                    assertThat(transactionId).isEqualTo(userTransactionId);
                    assertThat(resourceType).isSameAs(NODE);
                    observedLocks.increment();
                });
        assertThat(observedLocks.intValue()).isEqualTo(1);
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
                    assertThat(resourceType).isSameAs(NODE);
                    observedLocks.increment();
                });

        assertThat(observedLocks.intValue()).isEqualTo(2);
        assertThat(observedTransactions.containsAll(userTransactionIdA, userTransactionIdB))
                .as("Observer set: " + observedTransactions)
                .isTrue();
    }

    @Test
    void shouldListLocksHeldByTheCurrentClient() {
        // given
        clientA.initialize(LeaseService.NO_LEASES.newClient(), 1, EmptyMemoryTracker.INSTANCE, Config.defaults());
        clientA.acquireExclusive(LockTracer.NONE, NODE, 1, 2, 3);
        clientA.acquireShared(LockTracer.NONE, NODE, 3, 4, 5);

        // when
        var locks = clientA.activeLocks(EmptyMemoryTracker.INSTANCE);

        // then
        assertThat(locks)
                .isEqualTo(asList(
                        new ActiveLock(NODE, LockType.EXCLUSIVE, 1, 1),
                        new ActiveLock(NODE, LockType.EXCLUSIVE, 1, 2),
                        new ActiveLock(NODE, LockType.EXCLUSIVE, 1, 3),
                        new ActiveLock(NODE, LockType.SHARED, 1, 4),
                        new ActiveLock(NODE, LockType.SHARED, 1, 5)));
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
        assertThat(count).isEqualTo(3);
    }
}
