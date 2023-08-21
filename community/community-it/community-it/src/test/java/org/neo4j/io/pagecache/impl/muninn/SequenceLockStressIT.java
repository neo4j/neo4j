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
package org.neo4j.io.pagecache.impl.muninn;

import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.test.scheduler.DaemonThreadFactory;
import org.neo4j.util.concurrent.Futures;

class SequenceLockStressIT {
    private static ExecutorService executor;
    private static long lockAddr;

    @BeforeAll
    static void initialise() {
        lockAddr = UnsafeUtil.allocateMemory(Long.BYTES, INSTANCE);
        executor = Executors.newCachedThreadPool(new DaemonThreadFactory());
    }

    @AfterAll
    static void cleanup() {
        executor.shutdown();
        UnsafeUtil.free(lockAddr, Long.BYTES, INSTANCE);
    }

    @BeforeEach
    void allocateLock() {
        UnsafeUtil.putLong(lockAddr, 0);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void stressTest(boolean multiVersioned) throws Exception {
        for (int i = 0; i < 5; i++) {
            runTest(multiVersioned);
        }
    }

    private void runTest(boolean multiVersioned) throws InterruptedException, ExecutionException {
        int[][] data = new int[10][10];
        AtomicBoolean stop = new AtomicBoolean();
        AtomicInteger writerId = new AtomicInteger();

        abstract class Worker implements Runnable {
            @Override
            public void run() {
                try {
                    doWork();
                } finally {
                    stop.set(true);
                }
            }

            protected abstract void doWork();
        }

        Worker reader = new Worker() {
            @Override
            protected void doWork() {
                while (!stop.get()) {
                    ThreadLocalRandom rng = ThreadLocalRandom.current();
                    int[] record = data[rng.nextInt(data.length)];

                    long stamp = OffHeapPageLock.tryOptimisticReadLock(lockAddr);
                    int value = record[0];
                    boolean consistent = true;
                    for (int i : record) {
                        consistent &= i == value;
                    }
                    if (OffHeapPageLock.validateReadLock(lockAddr, stamp) && !consistent) {
                        throw new AssertionError("inconsistent read");
                    }
                }
            }
        };

        Worker writer = new Worker() {
            private volatile long unused;

            @Override
            protected void doWork() {
                int id = writerId.getAndIncrement();
                int counter = 1;
                ThreadLocalRandom rng = ThreadLocalRandom.current();
                int smallSpin = rng.nextInt(5, 50);
                int bigSpin = rng.nextInt(100, 1000);

                while (!stop.get()) {
                    if (OffHeapPageLock.tryWriteLock(lockAddr, multiVersioned)) {
                        int[] record = data[id];
                        for (int i = 0; i < record.length; i++) {
                            record[i] = counter;
                            for (int j = 0; j < smallSpin; j++) {
                                unused = rng.nextLong();
                            }
                        }
                        OffHeapPageLock.unlockWrite(lockAddr);
                    }

                    for (int j = 0; j < bigSpin; j++) {
                        unused = rng.nextLong();
                    }
                }
            }
        };

        Worker exclusive = new Worker() {
            private volatile long unused;

            @Override
            protected void doWork() {
                ThreadLocalRandom rng = ThreadLocalRandom.current();
                int spin = rng.nextInt(20, 2000);
                while (!stop.get()) {
                    while (!OffHeapPageLock.tryExclusiveLock(lockAddr)) {}
                    long sumA = 0;
                    long sumB = 0;
                    for (int[] ints : data) {
                        for (int i : ints) {
                            sumA += i;
                        }
                    }
                    for (int i = 0; i < spin; i++) {
                        unused = rng.nextLong();
                    }
                    for (int[] record : data) {
                        for (int value : record) {
                            sumB += value;
                        }
                        Arrays.fill(record, 0);
                    }
                    OffHeapPageLock.unlockExclusive(lockAddr);
                    if (sumA != sumB) {
                        throw new AssertionError(
                                "Inconsistent exclusive lock. 'Sum A' = " + sumA + ", 'Sum B' = " + sumB);
                    }
                }
            }
        };

        List<Future<?>> readers = new ArrayList<>();
        List<Future<?>> writers = new ArrayList<>();
        Future<?> exclusiveFuture = executor.submit(exclusive);
        for (int i = 0; i < 20; i++) {
            readers.add(executor.submit(reader));
        }
        for (int i = 0; i < data.length; i++) {
            writers.add(executor.submit(writer));
        }

        long deadline = System.currentTimeMillis() + 1000;
        while (!stop.get() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        stop.set(true);

        exclusiveFuture.get();
        Futures.getAll(writers);
        Futures.getAll(readers);
    }

    @Test
    void thoroughlyEnsureAtomicityOfUnlockExclusiveAndTakeWriteLock() throws Exception {
        for (int i = 0; i < 30000; i++) {
            unlockExclusiveAndTakeWriteLockMustBeAtomic();
            OffHeapPageLock.unlockWrite(lockAddr);
        }
    }

    private static void unlockExclusiveAndTakeWriteLockMustBeAtomic() throws Exception {
        int threads = Runtime.getRuntime().availableProcessors() - 1;
        CountDownLatch start = new CountDownLatch(threads);
        AtomicBoolean stop = new AtomicBoolean();
        OffHeapPageLock.tryExclusiveLock(lockAddr);
        Runnable runnable = () -> {
            while (!stop.get()) {
                if (OffHeapPageLock.tryExclusiveLock(lockAddr)) {
                    OffHeapPageLock.unlockExclusive(lockAddr);
                    throw new RuntimeException("I should not have gotten that lock");
                }
                start.countDown();
            }
        };

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(executor.submit(runnable));
        }

        start.await();
        OffHeapPageLock.unlockExclusiveAndTakeWriteLock(lockAddr);
        stop.set(true);
        Futures.getAll(futures);
    }
}
