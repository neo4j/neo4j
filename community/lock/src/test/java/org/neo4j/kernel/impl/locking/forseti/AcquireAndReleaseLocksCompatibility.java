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

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.lock.ResourceType.NODE;

import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.impl.locking.LockCountVisitor;
import org.neo4j.lock.LockTracer;

/**
 * Tests simple acquiring and releasing of single locks.
 * For testing "stacking" locks on the same client, see {@link LockReentrancyCompatibility}.
 **/
abstract class AcquireAndReleaseLocksCompatibility extends LockCompatibilityTestSupport {
    AcquireAndReleaseLocksCompatibility(LockingCompatibilityTest suite) {
        super(suite);
    }

    @Test
    void exclusiveShouldWaitForExclusive() {
        // When
        clientA.acquireExclusive(LockTracer.NONE, NODE, 1L);

        // Then
        Future<Void> clientBLock =
                acquireExclusive(clientB, LockTracer.NONE, NODE, 1L).callAndAssertWaiting();

        // And when
        clientA.releaseExclusive(NODE, 1L);

        // Then this should not block
        assertNotWaiting(clientBLock);
    }

    @Test
    void exclusiveShouldWaitForShared() {
        // When
        clientA.acquireShared(LockTracer.NONE, NODE, 1L);

        // Then other shared locks are allowed
        clientC.acquireShared(LockTracer.NONE, NODE, 1L);

        // But exclusive locks should wait
        Future<Void> clientBLock =
                acquireExclusive(clientB, LockTracer.NONE, NODE, 1L).callAndAssertWaiting();

        // And when
        clientA.releaseShared(NODE, 1L);
        clientC.releaseShared(NODE, 1L);

        // Then this should not block
        assertNotWaiting(clientBLock);
    }

    @Test
    void sharedShouldWaitForExclusive() {
        // When
        clientA.acquireExclusive(LockTracer.NONE, NODE, 1L);

        // Then shared locks should wait
        Future<Void> clientBLock =
                acquireShared(clientB, LockTracer.NONE, NODE, 1L).callAndAssertWaiting();

        // And when
        clientA.releaseExclusive(NODE, 1L);

        // Then this should not block
        assertNotWaiting(clientBLock);
    }

    @Test
    void shouldTrySharedLock() {
        // Given I've grabbed a share lock
        assertThat(clientA.trySharedLock(NODE, 1L)).isTrue();

        // Then other clients can't have exclusive locks
        assertThat(clientB.tryExclusiveLock(NODE, 1L)).isFalse();

        // But they are allowed share locks
        assertThat(clientB.trySharedLock(NODE, 1L)).isTrue();
    }

    @Test
    void shouldTryExclusiveLock() {
        // Given I've grabbed an exclusive lock
        assertThat(clientA.tryExclusiveLock(NODE, 1L)).isTrue();

        // Then other clients can't have exclusive locks
        assertThat(clientB.tryExclusiveLock(NODE, 1L)).isFalse();

        // Nor can they have share locks
        assertThat(clientB.trySharedLock(NODE, 1L)).isFalse();
    }

    @Test
    void shouldTryUpgradeSharedToExclusive() {
        // Given I've grabbed an exclusive lock
        assertThat(clientA.trySharedLock(NODE, 1L)).isTrue();

        // Then I can upgrade it to exclusive
        assertThat(clientA.tryExclusiveLock(NODE, 1L)).isTrue();

        // And other clients are denied it
        assertThat(clientB.trySharedLock(NODE, 1L)).isFalse();
    }

    @Test
    void shouldUpgradeExclusiveOnTry() {
        // Given I've grabbed a shared lock
        clientA.acquireShared(LockTracer.NONE, NODE, 1L);

        // When
        assertThat(clientA.tryExclusiveLock(NODE, 1L)).isTrue();

        // Then I should be able to release it
        clientA.releaseExclusive(NODE, 1L);
    }

    @Test
    void shouldAcquireMultipleSharedLocks() {
        clientA.acquireShared(LockTracer.NONE, NODE, 10, 100, 1000);

        assertThat(clientB.tryExclusiveLock(NODE, 10)).isFalse();
        assertThat(clientB.tryExclusiveLock(NODE, 100)).isFalse();
        assertThat(clientB.tryExclusiveLock(NODE, 1000)).isFalse();

        assertThat(lockCount()).isEqualTo(3);
    }

    @Test
    void shouldAcquireMultipleExclusiveLocks() {
        clientA.acquireExclusive(LockTracer.NONE, NODE, 10, 100, 1000);

        assertThat(clientB.trySharedLock(NODE, 10)).isFalse();
        assertThat(clientB.trySharedLock(NODE, 100)).isFalse();
        assertThat(clientB.trySharedLock(NODE, 1000)).isFalse();

        assertThat(lockCount()).isEqualTo(3);
    }

    @Test
    void shouldAcquireMultipleAlreadyAcquiredSharedLocks() {
        clientA.acquireShared(LockTracer.NONE, NODE, 10, 100, 1000);
        clientA.acquireShared(LockTracer.NONE, NODE, 100, 1000, 10000);

        assertThat(clientB.tryExclusiveLock(NODE, 10)).isFalse();
        assertThat(clientB.tryExclusiveLock(NODE, 100)).isFalse();
        assertThat(clientB.tryExclusiveLock(NODE, 1000)).isFalse();
        assertThat(clientB.tryExclusiveLock(NODE, 10000)).isFalse();

        assertThat(lockCount()).isEqualTo(4);
    }

    @Test
    void shouldAcquireMultipleAlreadyAcquiredExclusiveLocks() {
        clientA.acquireExclusive(LockTracer.NONE, NODE, 10, 100, 1000);
        clientA.acquireExclusive(LockTracer.NONE, NODE, 100, 1000, 10000);

        assertThat(clientB.trySharedLock(NODE, 10)).isFalse();
        assertThat(clientB.trySharedLock(NODE, 100)).isFalse();
        assertThat(clientB.trySharedLock(NODE, 1000)).isFalse();
        assertThat(clientB.trySharedLock(NODE, 10000)).isFalse();

        assertThat(lockCount()).isEqualTo(4);
    }

    @Test
    void shouldAcquireMultipleSharedLocksWhileHavingSomeExclusiveLocks() {
        clientA.acquireExclusive(LockTracer.NONE, NODE, 10, 100, 1000);
        clientA.acquireShared(LockTracer.NONE, NODE, 100, 1000, 10000);

        assertThat(clientB.trySharedLock(NODE, 10)).isFalse();
        assertThat(clientB.trySharedLock(NODE, 100)).isFalse();
        assertThat(clientB.trySharedLock(NODE, 1000)).isFalse();
        assertThat(clientB.tryExclusiveLock(NODE, 10000)).isFalse();

        assertThat(lockCount()).isEqualTo(4);
    }

    @Test
    void shouldReleaseSharedLocksAcquiredInABatch() {
        clientA.acquireShared(LockTracer.NONE, NODE, 1, 10, 100);
        assertThat(lockCount()).isEqualTo(3);

        clientA.releaseShared(NODE, 1);
        assertThat(lockCount()).isEqualTo(2);

        clientA.releaseShared(NODE, 10);
        assertThat(lockCount()).isEqualTo(1);

        clientA.releaseShared(NODE, 100);
        assertThat(lockCount()).isEqualTo(0);
    }

    @Test
    void shouldReleaseExclusiveLocksAcquiredInABatch() {
        clientA.acquireExclusive(LockTracer.NONE, NODE, 1, 10, 100);
        assertThat(lockCount()).isEqualTo(3);

        clientA.releaseExclusive(NODE, 1);
        assertThat(lockCount()).isEqualTo(2);

        clientA.releaseExclusive(NODE, 10);
        assertThat(lockCount()).isEqualTo(1);

        clientA.releaseExclusive(NODE, 100);
        assertThat(lockCount()).isEqualTo(0);
    }

    @Test
    void releaseMultipleSharedLocks() {
        clientA.acquireShared(LockTracer.NONE, NODE, 10, 100, 1000);
        assertThat(lockCount()).isEqualTo(3);

        clientA.releaseShared(NODE, 100, 1000);
        assertThat(lockCount()).isEqualTo(1);

        assertThat(clientB.tryExclusiveLock(NODE, 10)).isFalse();
        assertThat(clientB.tryExclusiveLock(NODE, 100)).isTrue();
        assertThat(clientB.tryExclusiveLock(NODE, 1000)).isTrue();
    }

    @Test
    void releaseMultipleExclusiveLocks() {
        clientA.acquireExclusive(LockTracer.NONE, NODE, 10, 100, 1000);

        assertThat(clientB.trySharedLock(NODE, 10)).isFalse();
        assertThat(clientB.trySharedLock(NODE, 100)).isFalse();
        assertThat(clientB.trySharedLock(NODE, 1000)).isFalse();
        assertThat(lockCount()).isEqualTo(3);

        clientA.releaseExclusive(NODE, 10, 100);
        assertThat(lockCount()).isEqualTo(1);

        assertThat(clientB.trySharedLock(NODE, 10)).isTrue();
        assertThat(clientB.trySharedLock(NODE, 100)).isTrue();
        assertThat(clientB.trySharedLock(NODE, 1000)).isFalse();
    }

    @Test
    void releaseMultipleAlreadyAcquiredSharedLocks() {
        clientA.acquireShared(LockTracer.NONE, NODE, 10, 100, 1000);
        clientA.acquireShared(LockTracer.NONE, NODE, 100, 1000, 10000);

        clientA.releaseShared(NODE, 100, 1000);
        assertThat(lockCount()).isEqualTo(4);

        assertThat(clientB.tryExclusiveLock(NODE, 100)).isFalse();
        assertThat(clientB.tryExclusiveLock(NODE, 1000)).isFalse();

        clientA.releaseShared(NODE, 100, 1000);
        assertThat(lockCount()).isEqualTo(2);
    }

    @Test
    void releaseMultipleAlreadyAcquiredExclusiveLocks() {
        clientA.acquireExclusive(LockTracer.NONE, NODE, 10, 100, 1000);
        clientA.acquireExclusive(LockTracer.NONE, NODE, 100, 1000, 10000);

        clientA.releaseExclusive(NODE, 100, 1000);
        assertThat(lockCount()).isEqualTo(4);

        assertThat(clientB.trySharedLock(NODE, 10)).isFalse();
        assertThat(clientB.trySharedLock(NODE, 100)).isFalse();
        assertThat(clientB.trySharedLock(NODE, 1000)).isFalse();
        assertThat(clientB.trySharedLock(NODE, 10000)).isFalse();

        clientA.releaseExclusive(NODE, 100, 1000);

        assertThat(lockCount()).isEqualTo(2);
    }

    @Test
    void releaseSharedLocksAcquiredSeparately() {
        clientA.acquireShared(LockTracer.NONE, NODE, 1);
        clientA.acquireShared(LockTracer.NONE, NODE, 2);
        clientA.acquireShared(LockTracer.NONE, NODE, 3);
        assertThat(lockCount()).isEqualTo(3);

        assertThat(clientB.tryExclusiveLock(NODE, 1)).isFalse();
        assertThat(clientB.tryExclusiveLock(NODE, 2)).isFalse();
        assertThat(clientB.tryExclusiveLock(NODE, 3)).isFalse();

        clientA.releaseShared(NODE, 1, 2, 3);

        assertThat(lockCount()).isEqualTo(0);
        assertThat(clientB.tryExclusiveLock(NODE, 1)).isTrue();
        assertThat(clientB.tryExclusiveLock(NODE, 2)).isTrue();
        assertThat(clientB.tryExclusiveLock(NODE, 3)).isTrue();
    }

    @Test
    void releaseExclusiveLocksAcquiredSeparately() {
        clientA.acquireExclusive(LockTracer.NONE, NODE, 1);
        clientA.acquireExclusive(LockTracer.NONE, NODE, 2);
        clientA.acquireExclusive(LockTracer.NONE, NODE, 3);
        assertThat(lockCount()).isEqualTo(3);

        assertThat(clientB.trySharedLock(NODE, 1)).isFalse();
        assertThat(clientB.trySharedLock(NODE, 2)).isFalse();
        assertThat(clientB.trySharedLock(NODE, 3)).isFalse();

        clientA.releaseExclusive(NODE, 1, 2, 3);

        assertThat(lockCount()).isEqualTo(0);
        assertThat(clientB.trySharedLock(NODE, 1)).isTrue();
        assertThat(clientB.trySharedLock(NODE, 2)).isTrue();
        assertThat(clientB.trySharedLock(NODE, 3)).isTrue();
    }

    @Test
    void releaseMultipleSharedLocksWhileHavingSomeExclusiveLocks() {
        clientA.acquireExclusive(LockTracer.NONE, NODE, 10, 100, 1000);
        clientA.acquireShared(LockTracer.NONE, NODE, 100, 1000, 10000);

        assertThat(clientB.trySharedLock(NODE, 10)).isFalse();
        assertThat(clientB.trySharedLock(NODE, 100)).isFalse();
        assertThat(clientB.trySharedLock(NODE, 1000)).isFalse();
        assertThat(clientB.tryExclusiveLock(NODE, 10000)).isFalse();
        assertThat(lockCount()).isEqualTo(4);

        clientA.releaseShared(NODE, 100, 1000);

        assertThat(clientB.trySharedLock(NODE, 10)).isFalse();
        assertThat(clientB.trySharedLock(NODE, 100)).isFalse();
        assertThat(clientB.trySharedLock(NODE, 1000)).isFalse();
        assertThat(clientB.tryExclusiveLock(NODE, 10000)).isFalse();

        assertThat(lockCount()).isEqualTo(4);
    }

    @Test
    void releaseMultipleExclusiveLocksWhileHavingSomeSharedLocks() {
        clientA.acquireShared(LockTracer.NONE, NODE, 100, 1000, 10000);
        clientA.acquireExclusive(LockTracer.NONE, NODE, 10, 100, 1000);

        assertThat(clientB.trySharedLock(NODE, 10)).isFalse();
        assertThat(clientB.trySharedLock(NODE, 100)).isFalse();
        assertThat(clientB.trySharedLock(NODE, 1000)).isFalse();
        assertThat(clientB.tryExclusiveLock(NODE, 10000)).isFalse();
        assertThat(lockCount()).isEqualTo(4);

        clientA.releaseExclusive(NODE, 100, 1000);

        assertThat(clientB.trySharedLock(NODE, 10)).isFalse();
        assertThat(clientB.tryExclusiveLock(NODE, 100)).isFalse();
        assertThat(clientB.tryExclusiveLock(NODE, 1000)).isFalse();
        assertThat(clientB.tryExclusiveLock(NODE, 10000)).isFalse();

        assertThat(lockCount()).isEqualTo(4);
    }

    private int lockCount() {
        LockCountVisitor lockVisitor = new LockCountVisitor();
        locks.accept(lockVisitor);
        return lockVisitor.getLockCount();
    }
}
