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

import static java.lang.Integer.max;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.impl.locking.LockManager.Client;
import static org.neo4j.test.Race.throwing;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.ThrowingConsumer;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.function.ThrowingAction;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.impl.api.LeaseService.NoLeaseClient;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceType;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.Race;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.time.Clocks;
import org.neo4j.util.concurrent.BinaryLatch;

@ExtendWith(RandomExtension.class)
class ForsetiFalseDeadlockTest {
    private static final int TEST_RUNS = 10;
    private static final ExecutorService executor = Executors.newCachedThreadPool(r -> {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        return thread;
    });

    @Inject
    RandomSupport random;

    @AfterAll
    static void tearDown() {
        executor.shutdown();
    }

    @TestFactory
    Stream<DynamicTest> testMildlyForFalseDeadlocks() {
        ThrowingConsumer<Fixture> fixtureConsumer = fixture -> loopRunTest(fixture, TEST_RUNS);
        return DynamicTest.stream(fixtures(), Fixture::toString, fixtureConsumer);
    }

    @Test
    void shouldManageToTakeSortedLocksWithoutFalseDeadlocks() throws Throwable {
        Config config = Config.defaults(GraphDatabaseInternalSettings.lock_manager_verbose_deadlocks, true);
        ForsetiLockManager manager = new ForsetiLockManager(config, Clocks.nanoClock(), ResourceType.values());
        AtomicInteger txCount = new AtomicInteger();
        AtomicInteger numDeadlocks = new AtomicInteger();
        Race race = new Race().withEndCondition(() -> txCount.get() > 10000);

        race.addContestants(max(Runtime.getRuntime().availableProcessors(), 2), throwing(() -> {
            try (Client client = manager.newClient()) {
                int txId = txCount.incrementAndGet();
                client.initialize(NoLeaseClient.INSTANCE, txId, EmptyMemoryTracker.INSTANCE, config);
                long prevRel = random.nextInt(10);
                long nextRel = random.nextInt(10);

                // Lock two "relationships" always in order -> impossible to get a loop -> no deadlocks
                long min = Math.min(prevRel, nextRel);
                long max = Math.max(prevRel, nextRel);
                client.acquireExclusive(LockTracer.NONE, ResourceType.RELATIONSHIP, min);
                if (prevRel != nextRel) {
                    client.acquireExclusive(LockTracer.NONE, ResourceType.RELATIONSHIP, max);
                }
                Thread.sleep(1);
            } catch (DeadlockDetectedException e) {
                numDeadlocks.incrementAndGet();
            }
        }));
        race.go(3, TimeUnit.MINUTES);
        assertThat(numDeadlocks.get())
                .isLessThanOrEqualTo(
                        3); // These deadlocks are extremely rare but can still happen so we can't have a strict assert.
    }

    private static Iterator<Fixture> fixtures() {
        List<Fixture> fixtures = new ArrayList<>();

        // During development I also had iteration counts 1 and 2 here, but they never found anything, so for actually
        // running this test, I leave only iteration count 100 enabled.
        int iteration = 100;
        LockManagerFactory[] lockManagerFactories = LockManagerFactory.values();
        LockType[] lockTypes = LockType.values();
        for (LockManagerFactory lockManagerFactory : lockManagerFactories) {
            for (LockType lockTypeAX : lockTypes) {
                for (LockType lockTypeAY : lockTypes) {
                    for (LockType lockTypeBX : lockTypes) {
                        for (LockType lockTypeBY : lockTypes) {
                            fixtures.add(new Fixture(
                                    iteration, lockManagerFactory, lockTypeAX, lockTypeAY, lockTypeBX, lockTypeBY));
                        }
                    }
                }
            }
        }
        return fixtures.iterator();
    }

    private static void loopRunTest(Fixture fixture, int testRuns) {
        List<Throwable> exceptionList = new ArrayList<>();
        loopRun(fixture, testRuns, exceptionList);

        if (!exceptionList.isEmpty()) {
            // We saw exceptions. Run it 99 more times, and then verify that our false deadlock rate is less than 2%.
            int additionalRuns = testRuns * 99;
            loopRun(fixture, additionalRuns, exceptionList);
            double totalRuns = additionalRuns + testRuns;
            double failures = exceptionList.size();
            double failureRate = failures / totalRuns;
            if (failureRate > 0.02) {
                // We have more than 2% failures. Report it!
                AssertionError error =
                        new AssertionError("False deadlock failure rate of " + failureRate + " is greater than 2%");
                for (Throwable th : exceptionList) {
                    error.addSuppressed(th);
                }
                throw error;
            }
        }
    }

    private static void loopRun(Fixture fixture, int testRuns, List<Throwable> exceptionList) {
        for (int i = 0; i < testRuns; i++) {
            try {
                runTest(fixture);
            } catch (Throwable th) {
                th.addSuppressed(new Exception("Failed at iteration " + i));
                exceptionList.add(th);
            }
        }
    }

    private static void runTest(Fixture fixture) throws InterruptedException, java.util.concurrent.ExecutionException {
        int iterations = fixture.iterations();
        ResourceType resourceType = fixture.createResourceType();
        LockManager manager = fixture.createLockManager(resourceType);
        try (Client a = manager.newClient();
                Client b = manager.newClient()) {
            a.initialize(NoLeaseClient.INSTANCE, 1, EmptyMemoryTracker.INSTANCE, Config.defaults());
            b.initialize(NoLeaseClient.INSTANCE, 2, EmptyMemoryTracker.INSTANCE, Config.defaults());

            BinaryLatch startLatch = new BinaryLatch();
            BlockedCallable callA =
                    new BlockedCallable(startLatch, () -> workloadA(fixture, a, resourceType, iterations));
            BlockedCallable callB =
                    new BlockedCallable(startLatch, () -> workloadB(fixture, b, resourceType, iterations));

            Future<Void> futureA = executor.submit(callA);
            Future<Void> futureB = executor.submit(callB);

            callA.awaitBlocked();
            callB.awaitBlocked();

            startLatch.release();

            futureA.get();
            futureB.get();
        } finally {
            manager.close();
        }
    }

    private static void workloadA(Fixture fixture, Client a, ResourceType resourceType, int iterations) {
        for (int i = 0; i < iterations; i++) {
            fixture.acquireAX(a, resourceType);
            fixture.acquireAY(a, resourceType);
            fixture.releaseAY(a, resourceType);
            fixture.releaseAX(a, resourceType);
        }
    }

    private static void workloadB(Fixture fixture, Client b, ResourceType resourceType, int iterations) {
        for (int i = 0; i < iterations; i++) {
            fixture.acquireBX(b, resourceType);
            fixture.releaseBX(b, resourceType);
            fixture.acquireBY(b, resourceType);
            fixture.releaseBY(b, resourceType);
        }
    }

    private static class BlockedCallable implements Callable<Void> {
        private final BinaryLatch startLatch;
        private final ThrowingAction<Exception> delegate;
        private volatile Thread runner;

        BlockedCallable(BinaryLatch startLatch, ThrowingAction<Exception> delegate) {
            this.startLatch = startLatch;
            this.delegate = delegate;
        }

        @Override
        public Void call() throws Exception {
            runner = Thread.currentThread();
            startLatch.await();
            delegate.apply();
            return null;
        }

        void awaitBlocked() {
            Thread t;
            do {
                t = runner;
            } while (t == null || t.getState() != Thread.State.WAITING);
        }
    }

    private static class Fixture {
        private final int iterations;
        private final LockManagerFactory lockManagerFactory;
        private final LockType lockTypeAX;
        private final LockType lockTypeAY;
        private final LockType lockTypeBX;
        private final LockType lockTypeBY;

        Fixture(
                int iterations,
                LockManagerFactory lockManagerFactory,
                LockType lockTypeAX,
                LockType lockTypeAY,
                LockType lockTypeBX,
                LockType lockTypeBY) {
            this.iterations = iterations;
            this.lockManagerFactory = lockManagerFactory;
            this.lockTypeAX = lockTypeAX;
            this.lockTypeAY = lockTypeAY;
            this.lockTypeBX = lockTypeBX;
            this.lockTypeBY = lockTypeBY;
        }

        int iterations() {
            return iterations;
        }

        LockManager createLockManager(ResourceType resourceType) {
            return lockManagerFactory.create(resourceType);
        }

        ResourceType createResourceType() {
            return ResourceType.NODE;
        }

        void acquireAX(Client client, ResourceType resourceType) {
            lockTypeAX.acquire(client, resourceType, 1);
        }

        void releaseAX(Client client, ResourceType resourceType) {
            lockTypeAX.release(client, resourceType, 1);
        }

        void acquireAY(Client client, ResourceType resourceType) {
            lockTypeAY.acquire(client, resourceType, 2);
        }

        void releaseAY(Client client, ResourceType resourceType) {
            lockTypeAY.release(client, resourceType, 2);
        }

        void acquireBX(Client client, ResourceType resourceType) {
            lockTypeBX.acquire(client, resourceType, 1);
        }

        void releaseBX(Client client, ResourceType resourceType) {
            lockTypeBX.release(client, resourceType, 1);
        }

        void acquireBY(Client client, ResourceType resourceType) {
            lockTypeBY.acquire(client, resourceType, 2);
        }

        void releaseBY(Client client, ResourceType resourceType) {
            lockTypeBY.release(client, resourceType, 2);
        }

        @Override
        public String toString() {
            return "iterations=" + iterations + ", lockManager="
                    + lockManagerFactory + ", lockTypeAX="
                    + lockTypeAX + ", lockTypeAY="
                    + lockTypeAY + ", lockTypeBX="
                    + lockTypeBX + ", lockTypeBY="
                    + lockTypeBY;
        }
    }

    public enum LockType {
        EXCLUSIVE {
            @Override
            public void acquire(Client client, ResourceType resourceType, int resource) {
                client.acquireExclusive(LockTracer.NONE, resourceType, resource);
            }

            @Override
            public void release(Client client, ResourceType resourceType, int resource) {
                client.releaseExclusive(resourceType, resource);
            }
        },
        SHARED {
            @Override
            public void acquire(Client client, ResourceType resourceType, int resource) {
                client.acquireShared(LockTracer.NONE, resourceType, resource);
            }

            @Override
            public void release(Client client, ResourceType resourceType, int resource) {
                client.releaseShared(resourceType, resource);
            }
        };

        public abstract void acquire(Client client, ResourceType resourceType, int resource);

        public abstract void release(Client client, ResourceType resourceType, int resource);
    }

    public enum LockManagerFactory {
        FORSETI {
            @Override
            public LockManager create(ResourceType resourceType) {
                return new ForsetiLockManager(Config.defaults(), Clocks.nanoClock(), resourceType);
            }
        };

        public abstract LockManager create(ResourceType resourceType);
    }
}
