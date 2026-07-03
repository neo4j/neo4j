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
package org.neo4j.lock;

import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.locks.LockSupport.getBlocker;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.lock.LockType.EXCLUSIVE;
import static org.neo4j.lock.LockType.SHARED;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.Race;

class ReentrantLockServiceTest {
    private final ReentrantLockService locks = new ReentrantLockService();

    @Test
    void shouldAllowReEntrance() {
        try (var lock = locks.acquireNodeLock(11, EXCLUSIVE);
                var lock2 = locks.acquireNodeLock(11, EXCLUSIVE);
                var lock3 = locks.acquireNodeLock(11, EXCLUSIVE)) {
            assertLock(lock, 11, 3, 0);
        }
    }

    @Test
    @Timeout(60)
    void shouldBlockOnLockedLock() {
        // given
        try (var executor = Executors.newSingleThreadExecutor()) {
            var threadHolder = new AtomicReference<Thread>();
            try (var lock = locks.acquireNodeLock(17, EXCLUSIVE)) {
                executor.execute(() -> {
                    threadHolder.set(currentThread());
                    locks.acquireNodeLock(17, EXCLUSIVE);
                });

                while (true) {
                    if (threadHolder.get() != null) {
                        var blocker = getBlocker(threadHolder.get());
                        if (blocker != null) {
                            return;
                        }
                    }
                    parkNanos(MILLISECONDS.toNanos(10));
                }
            }
        }
    }

    @Test
    void shouldNotLeaveResidualLockStateAfterAllLocksHaveBeenReleased() {
        // when
        locks.acquireNodeLock(42, EXCLUSIVE).release();

        // then
        assertThat(locks.lockCount()).isEqualTo(0);
    }

    @Test
    void shouldPresentLockStateInStringRepresentationOfLock() {
        // given
        Lock first;
        Lock second;

        // when
        try (Lock lock = first = locks.acquireNodeLock(666, EXCLUSIVE)) {
            // then
            assertLock(lock, 666, 1, 0);

            // when
            try (Lock inner = second = locks.acquireNodeLock(666, EXCLUSIVE)) {
                assertLock(lock, 666, 2, 0);
                assertThat(inner).hasToString(lock.toString());
            }

            // then
            assertLock(lock, 666, 1, 0);
            assertLock(second, 666, 0, 0);
        }

        // then
        assertLock(first, 666, 0, 0);
        assertLock(second, 666, 0, 0);
    }

    @Test
    void shouldAcquireSharedLocks() throws Exception {
        // given
        long nodeId = 10;
        try (Lock lock = locks.acquireNodeLock(nodeId, SHARED)) {
            assertLock(lock, nodeId, 0, 1);
            // when
            try (OtherThreadExecutor t2 = new OtherThreadExecutor("T2")) {
                t2.execute(() -> {
                    try (Lock t2Lock = locks.acquireNodeLock(nodeId, SHARED)) {
                        // then
                        assertLock(t2Lock, nodeId, 0, 2);
                        assertLock(lock, nodeId, 0, 2);
                    }
                    return null;
                });
                assertLock(lock, nodeId, 0, 1);
            }
        }
    }

    @Test
    void shouldPruneDeadLockInstances() {
        // given
        var race = new Race();
        int numThreads = 4;

        // when
        race.addContestants(numThreads, () -> {
            var rng = ThreadLocalRandom.current();
            for (int i = 0; i < 100_000; i++) {
                try (var lock = locks.acquireNodeLock(rng.nextLong(1_000), EXCLUSIVE)) {
                    // then
                    assertThat(locks.lockCount()).isLessThanOrEqualTo(numThreads * 2);
                }
            }
        });
        race.goUnchecked();
    }

    @Test
    void shouldWorkWithClientAbstraction() throws Exception {
        // given
        try (var t2 = new OtherThreadExecutor("T2");
                var client1 = locks.newClient()) {
            client1.acquireNodeLock(123, EXCLUSIVE);
            var lockFuture = t2.executeDontWait(() -> {
                try (var client2 = locks.newClient()) {
                    client2.acquireNodeLock(123, EXCLUSIVE);
                }
                return null;
            });
            t2.waitUntilWaiting();
            client1.close();
            lockFuture.get();
        }
    }

    @Test
    void shouldWorkWithClientAbstractionAndCustomLocks() throws Exception {
        // given
        try (var t2 = new OtherThreadExecutor("T2");
                var client1 = locks.newClient()) {
            for (int i = 0; i < 10; i++) {
                client1.acquireCustomLock(9, 10, SHARED);
            }
            var lockFuture = t2.executeDontWait(() -> {
                try (var client2 = locks.newClient()) {
                    client2.acquireCustomLock(9, 10, EXCLUSIVE);
                }
                return null;
            });
            t2.waitUntilWaiting();
            client1.close();
            lockFuture.get();
        }
    }

    @Test
    void shouldAllowHighAcquisitionCountForLockClients() {
        // given
        try (var client = locks.newClient()) {
            // when/then (not throwing an error from ReentrantReadWriteLock)
            for (int i = 0; i < 100_000; i++) {
                client.acquireCustomLock(23, 45, SHARED);
            }
        }
    }

    private void assertLock(Lock lock, long id, int writeLockCount, int readLockCount) {
        String lockToString = lock.toString();
        assertThat(lockToString).contains("[id=" + id + "]");
        if (writeLockCount == 0 && readLockCount == 0) {
            assertThat(lockToString).contains("RELEASED");
        } else {
            assertThat(lockToString)
                    .contains("Write locks = " + writeLockCount)
                    .contains("Read locks = " + readLockCount);
        }
    }
}
