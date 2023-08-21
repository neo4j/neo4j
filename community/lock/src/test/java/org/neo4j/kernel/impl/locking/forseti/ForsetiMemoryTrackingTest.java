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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.lock.ResourceType.NODE;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.impl.api.LeaseService;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceType;
import org.neo4j.memory.GlobalMemoryGroupTracker;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryGroup;
import org.neo4j.memory.MemoryLimitExceededException;
import org.neo4j.memory.MemoryPool;
import org.neo4j.memory.MemoryPools;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.Race;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.time.Clocks;

@ExtendWith(RandomExtension.class)
class ForsetiMemoryTrackingTest {

    @Inject
    private RandomSupport random;

    private static final AtomicLong TRANSACTION_ID = new AtomicLong();
    private static final int ONE_LOCK_SIZE_ESTIMATE = 56;
    private GlobalMemoryGroupTracker memoryPool;
    private MemoryTracker memoryTracker;
    private ForsetiLockManager forsetiLockManager;

    @BeforeEach
    void setUp() {
        memoryPool = new MemoryPools().pool(MemoryGroup.TRANSACTION, 0L, null);
        memoryTracker = new LocalMemoryTracker(memoryPool);
        forsetiLockManager = new ForsetiLockManager(Config.defaults(), Clocks.nanoClock(), ResourceType.values());
    }

    @AfterEach
    void tearDown() {
        assertThat(memoryTracker.estimatedHeapMemory()).isEqualTo(0);
        memoryTracker.close();
        assertThat(memoryPool.getPoolMemoryTracker().estimatedHeapMemory()).isEqualTo(0);
    }

    @Test
    void trackMemoryOnSharedLockAcquire() {
        try (LockManager.Client client = getClient()) {
            assertThat(memoryTracker.estimatedHeapMemory()).isEqualTo(0);
            client.acquireShared(LockTracer.NONE, NODE, 1);
            var oneLockAllocatedMemory = memoryTracker.estimatedHeapMemory();
            assertThat(oneLockAllocatedMemory).isGreaterThan(0);

            client.acquireShared(LockTracer.NONE, NODE, 2);
            var twoLocksAllocatedMemory = memoryTracker.estimatedHeapMemory();
            assertThat(twoLocksAllocatedMemory)
                    .isGreaterThan(0)
                    .isEqualTo(oneLockAllocatedMemory + ONE_LOCK_SIZE_ESTIMATE);
        }
    }

    @Test
    void trackMemoryOnExclusiveLockAcquire() {
        try (LockManager.Client client = getClient()) {
            assertThat(memoryTracker.estimatedHeapMemory()).isEqualTo(0);
            client.acquireExclusive(LockTracer.NONE, NODE, 1);
            var oneLockAllocatedMemory = memoryTracker.estimatedHeapMemory();
            assertThat(oneLockAllocatedMemory).isGreaterThan(0);

            client.acquireExclusive(LockTracer.NONE, NODE, 2);
            var twoLocksAllocatedMemory = memoryTracker.estimatedHeapMemory();
            assertThat(twoLocksAllocatedMemory)
                    .isGreaterThan(0)
                    .isEqualTo(oneLockAllocatedMemory + ONE_LOCK_SIZE_ESTIMATE);
        }
    }

    @Test
    void sharedLockReAcquireDoesNotAllocateMemory() {
        try (LockManager.Client client = getClient()) {
            assertThat(memoryTracker.estimatedHeapMemory()).isEqualTo(0);
            client.acquireShared(LockTracer.NONE, NODE, 1);
            var oneLockAllocatedMemory = memoryTracker.estimatedHeapMemory();
            assertThat(oneLockAllocatedMemory).isGreaterThan(0);

            client.acquireShared(LockTracer.NONE, NODE, 1);
            var twoLocksAllocatedMemory = memoryTracker.estimatedHeapMemory();
            assertEquals(oneLockAllocatedMemory, twoLocksAllocatedMemory);
        }
    }

    @Test
    void exclusiveLockReAcquireDoesNotAllocateMemory() {
        try (LockManager.Client client = getClient()) {
            assertThat(memoryTracker.estimatedHeapMemory()).isEqualTo(0);
            client.acquireExclusive(LockTracer.NONE, NODE, 1);
            var oneLockAllocatedMemory = memoryTracker.estimatedHeapMemory();
            assertThat(oneLockAllocatedMemory).isGreaterThan(0);

            client.acquireExclusive(LockTracer.NONE, NODE, 1);
            var twoLocksAllocatedMemory = memoryTracker.estimatedHeapMemory();
            assertEquals(oneLockAllocatedMemory, twoLocksAllocatedMemory);
        }
    }

    @Test
    void exclusiveLockOverSharedDoesNotAllocateMemory() {
        try (LockManager.Client client = getClient()) {
            assertThat(memoryTracker.estimatedHeapMemory()).isEqualTo(0);
            client.acquireShared(LockTracer.NONE, NODE, 1);
            var sharedAllocatedMemory = memoryTracker.estimatedHeapMemory();
            assertThat(sharedAllocatedMemory).isGreaterThan(0);

            client.acquireExclusive(LockTracer.NONE, NODE, 1);
            var twoLocksAllocatedMemory = memoryTracker.estimatedHeapMemory();
            assertEquals(sharedAllocatedMemory, twoLocksAllocatedMemory);
        }
    }

    @Test
    void sharedLockOverExclusiveAllocateMemory() {
        try (LockManager.Client client = getClient()) {
            assertThat(memoryTracker.estimatedHeapMemory()).isEqualTo(0);
            client.acquireExclusive(LockTracer.NONE, NODE, 1);
            var exclusiveAllocatedMemory = memoryTracker.estimatedHeapMemory();
            assertThat(exclusiveAllocatedMemory).isGreaterThan(0);

            client.acquireShared(LockTracer.NONE, NODE, 1);
            var twoLocksAllocatedMemory = memoryTracker.estimatedHeapMemory();
            assertThat(twoLocksAllocatedMemory).isGreaterThan(exclusiveAllocatedMemory);

            client.acquireShared(LockTracer.NONE, NODE, 1);
            var threeLocksAllocatedMemory = memoryTracker.estimatedHeapMemory();

            assertThat(threeLocksAllocatedMemory).isEqualTo(twoLocksAllocatedMemory);
        }
    }

    @Test
    void releaseMemoryOfSharedLock() {
        try (LockManager.Client client = getClient()) {
            assertThat(memoryTracker.estimatedHeapMemory()).isEqualTo(0);
            client.acquireShared(LockTracer.NONE, NODE, 1);
            var sharedAllocatedMemory = memoryTracker.estimatedHeapMemory();
            assertThat(sharedAllocatedMemory).isGreaterThan(0);

            client.releaseShared(NODE, 1);
            var noLocksClientMemory = memoryTracker.estimatedHeapMemory();
            assertThat(noLocksClientMemory).isGreaterThan(0).isEqualTo(sharedAllocatedMemory - ONE_LOCK_SIZE_ESTIMATE);
        }
    }

    @Test
    void releaseMemoryOfExclusiveLock() {
        try (LockManager.Client client = getClient()) {
            assertThat(memoryTracker.estimatedHeapMemory()).isEqualTo(0);
            client.acquireExclusive(LockTracer.NONE, NODE, 1);

            // we take shared lock here as well to create internal maps and report them into tracker before release call
            client.acquireShared(LockTracer.NONE, NODE, 1);
            client.releaseShared(NODE, 1);

            var exclusiveAllocatedMemory = memoryTracker.estimatedHeapMemory();
            assertThat(exclusiveAllocatedMemory).isGreaterThan(0);

            client.releaseExclusive(NODE, 1);
            var noLocksClientMemory = memoryTracker.estimatedHeapMemory();
            assertThat(noLocksClientMemory)
                    .isGreaterThan(0)
                    .isEqualTo(exclusiveAllocatedMemory - ONE_LOCK_SIZE_ESTIMATE);
        }
    }

    @Test
    void releaseExclusiveLockWhyHoldingSharedDoNotReleaseAnyMemory() {
        try (LockManager.Client client = getClient()) {
            assertThat(memoryTracker.estimatedHeapMemory()).isEqualTo(0);
            client.acquireExclusive(LockTracer.NONE, NODE, 1);
            client.acquireShared(LockTracer.NONE, NODE, 1);

            var locksMemory = memoryTracker.estimatedHeapMemory();
            assertThat(locksMemory).isGreaterThan(0);

            client.releaseExclusive(NODE, 1);
            var noExclusiveLockMemory = memoryTracker.estimatedHeapMemory();
            assertThat(noExclusiveLockMemory).isGreaterThan(0).isEqualTo(locksMemory);
        }
    }

    @Test
    void releaseLocksReleasingMemory() {
        try (LockManager.Client client = getClient()) {
            assertThat(memoryTracker.estimatedHeapMemory()).isEqualTo(0);
            client.acquireExclusive(LockTracer.NONE, NODE, 1);
            client.acquireShared(LockTracer.NONE, NODE, 1);
            client.releaseExclusive(NODE, 1);
            client.releaseShared(NODE, 1);

            var noLocksMemory = memoryTracker.estimatedHeapMemory();
            assertThat(noLocksMemory).isGreaterThan(0);

            int lockNumber = 10;
            for (int i = 0; i < lockNumber; i++) {
                client.acquireExclusive(LockTracer.NONE, NODE, i);
            }
            long exclusiveLocksMemory = memoryTracker.estimatedHeapMemory();
            assertThat(exclusiveLocksMemory).isEqualTo(noLocksMemory + lockNumber * ONE_LOCK_SIZE_ESTIMATE);

            for (int i = 0; i < lockNumber; i++) {
                client.acquireShared(LockTracer.NONE, NODE, i);
            }
            long sharedLocksMemory = memoryTracker.estimatedHeapMemory();
            assertThat(sharedLocksMemory).isEqualTo(exclusiveLocksMemory);

            for (int i = 0; i < lockNumber; i++) {
                client.releaseShared(NODE, i);
                client.releaseExclusive(NODE, i);
            }

            assertThat(memoryTracker.estimatedHeapMemory()).isEqualTo(noLocksMemory);
        }
    }

    @Test
    void trackMemoryOnLocksAcquire() {
        try (LockManager.Client client = getClient()) {
            client.acquireShared(LockTracer.NONE, NODE, 1);
            client.acquireExclusive(LockTracer.NONE, NODE, 2);
            assertThat(memoryTracker.estimatedHeapMemory()).isGreaterThan(0);
        }
    }

    @Test
    void releaseMemoryOnUnlock() {
        try (LockManager.Client client = getClient()) {
            client.acquireShared(LockTracer.NONE, NODE, 1);
            client.releaseShared(NODE, 1);
            client.acquireExclusive(LockTracer.NONE, NODE, 2);
            long lockedSize = memoryTracker.estimatedHeapMemory();
            assertThat(lockedSize).isGreaterThan(0);
            client.releaseExclusive(NODE, 2);
            assertThat(memoryTracker.estimatedHeapMemory()).isLessThan(lockedSize);
        }
    }

    @Test
    void upgradingLockShouldNotLeakMemory() {
        try (LockManager.Client client = getClient()) {
            client.acquireShared(LockTracer.NONE, NODE, 1);
            client.acquireShared(LockTracer.NONE, NODE, 1);
            client.acquireExclusive(LockTracer.NONE, NODE, 1); // Should be upgraded
            client.acquireExclusive(LockTracer.NONE, NODE, 1);
            client.releaseExclusive(NODE, 1);
            client.releaseExclusive(NODE, 1);
            client.releaseShared(NODE, 1);
            client.releaseShared(NODE, 1);
        }
    }

    @Test
    void closeShouldReleaseAllMemory() {
        try (LockManager.Client client = getClient()) {
            client.acquireShared(LockTracer.NONE, NODE, 1);
            client.acquireShared(LockTracer.NONE, NODE, 1);
            client.acquireExclusive(LockTracer.NONE, NODE, 1); // Should be upgraded
            client.acquireExclusive(LockTracer.NONE, NODE, 1);
        }
    }

    @Test
    void concurrentMemoryShouldEndUpZero() throws Throwable {
        Race race = new Race();
        int numThreads = 4;
        LocalMemoryTracker[] trackers = new LocalMemoryTracker[numThreads];
        for (int i = 0; i < numThreads; i++) {
            trackers[i] = new LocalMemoryTracker(memoryPool);
            LockManager.Client client = forsetiLockManager.newClient();
            client.initialize(LeaseService.NoLeaseClient.INSTANCE, i, trackers[i], Config.defaults());
            race.addContestant(new SimulatedTransaction(client));
        }
        race.go();
        for (int i = 0; i < numThreads; i++) {
            try (LocalMemoryTracker tracker = trackers[i]) {
                assertThat(tracker.estimatedHeapMemory())
                        .describedAs("Tracker " + tracker)
                        .isGreaterThanOrEqualTo(0);
            }
        }
    }

    private static class SimulatedTransaction implements Runnable {
        private final Deque<LockEvent> heldLocks = new ArrayDeque<>();
        private final LockManager.Client client;

        SimulatedTransaction(LockManager.Client client) {
            this.client = client;
        }

        @Override
        public void run() {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            try {
                for (int i = 0; i < 100; i++) {
                    if (heldLocks.isEmpty() || random.nextFloat() > 0.33) {
                        // Acquire new lock
                        int nodeId = random.nextInt(10);
                        if (random.nextBoolean()) {
                            // Exclusive
                            if (random.nextBoolean()) {
                                client.acquireExclusive(LockTracer.NONE, NODE, nodeId);
                                heldLocks.push(new LockEvent(true, nodeId));
                            } else {
                                if (client.tryExclusiveLock(NODE, nodeId)) {
                                    heldLocks.push(new LockEvent(true, nodeId));
                                }
                            }
                        } else {
                            // Shared
                            if (random.nextBoolean()) {
                                client.acquireShared(LockTracer.NONE, NODE, nodeId);
                                heldLocks.push(new LockEvent(false, nodeId));
                            } else {
                                if (client.trySharedLock(NODE, nodeId)) {
                                    heldLocks.push(new LockEvent(false, nodeId));
                                }
                            }
                        }
                    } else {
                        // Release old lock
                        LockEvent pop = heldLocks.pop();
                        if (pop.isExclusive) {
                            client.releaseExclusive(NODE, pop.nodeId);
                        } else {
                            client.releaseShared(NODE, pop.nodeId);
                        }
                    }
                }
            } catch (DeadlockDetectedException ignore) {
            } finally {
                client.close(); // Should release all of the locks, end resolve deadlock
            }
        }

        private static class LockEvent {
            final boolean isExclusive;
            final long nodeId;

            LockEvent(boolean isExclusive, long nodeId) {
                this.isExclusive = isExclusive;
                this.nodeId = nodeId;
            }
        }
    }

    @Test
    void unsuccessfulTryExclusiveShouldNotLeakMemory() {
        try (var client1 = getClient();
                var tracker = new LocalMemoryTracker(memoryPool);
                var client2 = forsetiLockManager.newClient()) {

            client1.acquireExclusive(LockTracer.NONE, NODE, 10);

            client2.initialize(
                    LeaseService.NoLeaseClient.INSTANCE, TRANSACTION_ID.getAndIncrement(), tracker, Config.defaults());

            client2.acquireExclusive(LockTracer.NONE, NODE, 999); // trigger inner map creation

            var heapBefore = tracker.estimatedHeapMemory();
            assertThat(client2.tryExclusiveLock(NODE, 10)).isFalse();
            assertThat(tracker.estimatedHeapMemory()).isEqualTo(heapBefore);
        }
    }

    @Test
    void unsuccessfulTrySharedShouldNotLeakMemory() {
        try (var client1 = getClient();
                var tracker = new LocalMemoryTracker(memoryPool);
                var client2 = forsetiLockManager.newClient()) {

            client1.acquireExclusive(LockTracer.NONE, NODE, 10);

            client2.initialize(
                    LeaseService.NoLeaseClient.INSTANCE, TRANSACTION_ID.getAndIncrement(), tracker, Config.defaults());

            client2.acquireShared(LockTracer.NONE, NODE, 99); // trigger inner map creation

            var heapBefore = tracker.estimatedHeapMemory();

            assertThat(client2.trySharedLock(NODE, 10)).isFalse();
            assertThat(tracker.estimatedHeapMemory()).isEqualTo(heapBefore);
        }
    }

    @Test
    void shouldReleaseMemoryInCaseOfDeadlock() {
        assertThatThrownBy(() -> {
                    try (var client1 = getClient();
                            var client2 = getClient()) {
                        client1.acquireExclusive(LockTracer.NONE, NODE, 10);
                        client1.prepareForCommit();

                        client2.acquireExclusive(LockTracer.NONE, NODE, 10);
                    }
                })
                .isInstanceOf(DeadlockDetectedException.class);
    }

    @RepeatedTest(20)
    void shouldReleaseLocksAndMemoryWhenMemoryLimited() throws Throwable {
        // find max used heap first
        long maxUsedHeap;
        try (var tracker = new HighWaterMarkTracker(memoryPool, Long.MAX_VALUE, 1024, "forsetiClientLimitTest")) {
            lockSomeNodes(tracker, forsetiLockManager);
            maxUsedHeap = tracker.maxUsedHeap;
        }
        assertThat(maxUsedHeap).isGreaterThan(0L);

        // limit heap
        try (var tracker =
                new LocalMemoryTracker(memoryPool, random.nextLong(maxUsedHeap), 1024, "forsetiClientLimitTest")) {
            lockSomeNodes(tracker, forsetiLockManager);
        }
    }

    private void lockSomeNodes(MemoryTracker tracker, LockManager lockManager) {
        var nodesPerBatch = 30;
        var message = new AtomicReference<>("No MemoryLimitExceededException observed");
        try (var client = lockManager.newClient()) {
            client.initialize(LeaseService.NoLeaseClient.INSTANCE, 1, tracker, Config.defaults());

            long nodeId = 0;
            long extraSharedStart = nodeId;
            // take some shared locks first so exclusive one will trigger upgrade and downgrade
            var doubleBatch = nodesPerBatch * 2;
            for (long i = extraSharedStart; i < extraSharedStart + doubleBatch; i += 3) {
                client.acquireShared(LockTracer.NONE, NODE, i);
            }

            for (int i = 0; i < nodesPerBatch; i++) {
                client.acquireExclusive(LockTracer.NONE, NODE, nodeId++);
            }
            for (int i = 0; i < nodesPerBatch; i++) {
                client.tryExclusiveLock(NODE, nodeId++);
            }

            for (long i = extraSharedStart; i < extraSharedStart + doubleBatch; i += 3) {
                client.releaseShared(NODE, i);
            }

            long extraExclusive = nodeId;
            // take some exlusive locks first so shared one does not trigger more allocations
            for (long i = extraExclusive; i < extraExclusive + doubleBatch; i += 3) {
                client.acquireExclusive(LockTracer.NONE, NODE, i);
            }
            for (int i = 0; i < nodesPerBatch; i++) {
                client.acquireShared(LockTracer.NONE, NODE, nodeId++);
            }
            for (int i = 0; i < nodesPerBatch; i++) {
                client.trySharedLock(NODE, nodeId++);
            }
            for (long i = extraExclusive; i < extraExclusive + doubleBatch; i += 3) {
                client.releaseExclusive(NODE, i);
            }

        } catch (MemoryLimitExceededException mlee) {
            message.set("Observed exception: " + Exceptions.stringify(mlee));
        }

        verifyNoLocks(lockManager, message);

        assertThat(tracker.estimatedHeapMemory()).as(message.get()).isZero();
    }

    private void verifyNoLocks(LockManager lockManager, AtomicReference<String> message) {
        lockManager.accept(
                (lockType,
                        resourceType,
                        transactionId,
                        resourceId,
                        description,
                        estimatedWaitTime,
                        lockIdentityHashCode) -> {
                    fail(
                            "Leaked global lock after client is closed for resource id %d transaction id %d. %s",
                            resourceId, transactionId, message);
                });
    }

    @RepeatedTest(20)
    void shouldBeAbleToTrackMemoryCorrectlyWhenTakingExclusiveLockOnSharedLockedObject()
            throws ExecutionException, InterruptedException {
        long maxUsedHeap;
        try (var tracker = new HighWaterMarkTracker(memoryPool, Long.MAX_VALUE, 1024, "forsetiClientLimitTest")) {
            raceSharedAndExclusiveLock(tracker, forsetiLockManager);
            maxUsedHeap = tracker.maxUsedHeap;
        }
        assertThat(maxUsedHeap).isGreaterThan(0L);

        // limit heap
        try (var tracker =
                new LocalMemoryTracker(memoryPool, random.nextLong(maxUsedHeap), 1024, "forsetiClientLimitTest")) {
            raceSharedAndExclusiveLock(tracker, forsetiLockManager);
        }
    }

    private void raceSharedAndExclusiveLock(LocalMemoryTracker memoryTracker1, LockManager lockManager)
            throws InterruptedException, ExecutionException {
        try (var memoryTracker2 = new LocalMemoryTracker(); ) {
            var message = new AtomicReference<>("No MemoryLimitExceededException observed");

            try (var executor1 = new OtherThreadExecutor("test1");
                    var executor2 = new OtherThreadExecutor("test2")) {
                var acquireSharedLockLatch = new CountDownLatch(1);
                var releaseSharedLockLatch = new CountDownLatch(1);
                // executor2 takes shared lock
                var future2 = executor2.executeDontWait(() -> {
                    try (var otherClient = lockManager.newClient()) {
                        otherClient.initialize(
                                LeaseService.NoLeaseClient.INSTANCE, 2, memoryTracker2, Config.defaults());
                        otherClient.acquireShared(LockTracer.NONE, NODE, 1);
                        acquireSharedLockLatch.countDown();
                        releaseSharedLockLatch.await();
                    }
                    return null;
                });

                acquireSharedLockLatch.await();
                // executor1 tries to grab exlusive lock, but as there is shared one, it needs to take shared too and
                // then upgrade
                var future1 = executor1.executeDontWait(() -> {
                    try (var client = lockManager.newClient()) {
                        client.initialize(LeaseService.NoLeaseClient.INSTANCE, 1, memoryTracker1, Config.defaults());
                        client.acquireExclusive(LockTracer.NONE, ResourceType.NODE, 1);
                    } catch (MemoryLimitExceededException mlee) {
                        message.set("Observed exception: " + Exceptions.stringify(mlee));
                    }
                    return null;
                });

                // let retry loop loop
                Thread.sleep(100);
                // release shared lock
                releaseSharedLockLatch.countDown();
                future2.get();
                future1.get();
            }

            verifyNoLocks(lockManager, message);

            assertThat(memoryTracker1.estimatedHeapMemory()).as(message.get()).isZero();
            assertThat(memoryTracker2.estimatedHeapMemory()).isZero();
        }
    }

    private static class HighWaterMarkTracker extends LocalMemoryTracker {
        private long maxUsedHeap = 0;

        public HighWaterMarkTracker(
                MemoryPool memoryPool, long localBytesLimit, long grabSize, String limitSettingName) {
            super(memoryPool, localBytesLimit, grabSize, limitSettingName);
        }

        @Override
        public void allocateHeap(long bytes) {
            super.allocateHeap(bytes);
            var heapMemroy = estimatedHeapMemory();
            this.maxUsedHeap = Math.max(maxUsedHeap, heapMemroy);
        }
    }

    private LockManager.Client getClient() {
        LockManager.Client client = forsetiLockManager.newClient();
        client.initialize(
                LeaseService.NoLeaseClient.INSTANCE,
                TRANSACTION_ID.getAndIncrement(),
                memoryTracker,
                Config.defaults());
        return client;
    }
}
