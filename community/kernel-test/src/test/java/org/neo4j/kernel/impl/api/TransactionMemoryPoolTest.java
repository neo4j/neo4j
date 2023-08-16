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
package org.neo4j.kernel.impl.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.initial_transaction_heap_grab_size;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.initial_transaction_heap_grab_size_per_worker;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.max_transaction_heap_grab_size_per_worker;
import static org.neo4j.configuration.GraphDatabaseSettings.memory_tracking;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.io.ByteUnit;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.ExecutionContextMemoryTracker;
import org.neo4j.memory.GlobalMemoryGroupTracker;
import org.neo4j.memory.MemoryGroup;
import org.neo4j.memory.MemoryPools;
import org.neo4j.util.concurrent.Futures;

class TransactionMemoryPoolTest {

    private TransactionMemoryPool pool;
    private GlobalMemoryGroupTracker groupTracker;

    private final long grabSize = 4L;
    private final long maxGrabSize = 4L;

    @BeforeEach
    void setUp() {
        MemoryPools memoryPools = new MemoryPools();
        groupTracker = new GlobalMemoryGroupTracker(
                memoryPools, MemoryGroup.TRANSACTION, ByteUnit.gibiBytes(1), true, true, "foo");
        var config = Config.defaults(Map.of(
                initial_transaction_heap_grab_size,
                4L,
                initial_transaction_heap_grab_size_per_worker,
                grabSize,
                max_transaction_heap_grab_size_per_worker,
                4L));
        pool = new TransactionMemoryPool(groupTracker, config, () -> true, NullLogProvider.getInstance());
    }

    @Test
    void createEmptyMemoryTrackerWhenConfigured() {
        var config = Config.defaults(memory_tracking, false);
        var customConfiguredPool =
                new TransactionMemoryPool(groupTracker, config, () -> false, NullLogProvider.getInstance());
        assertThat(customConfiguredPool.getExecutionContextPoolMemoryTracker(grabSize, maxGrabSize))
                .isInstanceOf(EmptyMemoryTracker.class);
    }

    @Test
    void createRealMemoryTrackerWhenConfigured() {
        var config = Config.defaults(memory_tracking, true);
        var customConfiguredPool =
                new TransactionMemoryPool(groupTracker, config, () -> false, NullLogProvider.getInstance());
        assertThat(customConfiguredPool.getExecutionContextPoolMemoryTracker(grabSize, maxGrabSize))
                .isInstanceOf(ExecutionContextMemoryTracker.class);
    }

    @Test
    void defaultGetPoolMemoryTrackerMethodNotSupported() {
        var config = Config.defaults(memory_tracking, true);
        var customConfiguredPool =
                new TransactionMemoryPool(groupTracker, config, () -> false, NullLogProvider.getInstance());
        assertThrows(UnsupportedOperationException.class, () -> customConfiguredPool.getPoolMemoryTracker());
    }

    @Test
    void releaseHeapOnPoolReset() {
        var tracker1 = pool.getExecutionContextPoolMemoryTracker(grabSize, maxGrabSize);
        var tracker2 = pool.getExecutionContextPoolMemoryTracker(grabSize, maxGrabSize);
        tracker1.allocateHeap(1);
        tracker1.allocateHeap(2);
        tracker2.allocateHeap(3);
        tracker2.allocateHeap(5);

        pool.reset();

        assertEquals(0, pool.usedHeap());
    }

    @Test
    void exceptionOnReleaseNativeOnPoolReset() {
        var tracker1 = pool.getExecutionContextPoolMemoryTracker(grabSize, maxGrabSize);
        var tracker2 = pool.getExecutionContextPoolMemoryTracker(grabSize, maxGrabSize);
        tracker1.allocateHeap(1);
        tracker1.allocateNative(2);
        tracker2.allocateHeap(3);
        tracker2.allocateNative(5);

        assertThrows(AssertionError.class, () -> pool.reset());
    }

    @Test
    void rootTransactionTrackerHeapReleaseOnReset() {
        var transactionTracker = pool.getTransactionTracker();

        transactionTracker.allocateHeap(1);
        transactionTracker.allocateHeap(3);

        assertEquals(4, pool.usedHeap());

        pool.reset();
        assertEquals(0, pool.usedHeap());
    }

    @Test
    void rootTransactionTrackerNativeMemoryThrowOnReset() {
        var transactionTracker = pool.getTransactionTracker();

        transactionTracker.allocateNative(1);
        transactionTracker.allocateNative(3);

        assertEquals(4, pool.usedNative());

        assertThrows(AssertionError.class, () -> pool.reset());
    }

    @Test
    void reportHeapMemoryUsage() {
        var tracker = pool.getExecutionContextPoolMemoryTracker(grabSize, maxGrabSize);
        tracker.allocateHeap(1);
        tracker.allocateHeap(2);
        tracker.allocateHeap(3);
        tracker.allocateHeap(5);

        assertEquals(13, pool.usedHeap());
        assertEquals(0, pool.usedNative());
    }

    @Test
    void reportNativeMemoryUsage() {
        var tracker = pool.getExecutionContextPoolMemoryTracker(grabSize, maxGrabSize);
        tracker.allocateNative(10);
        tracker.allocateNative(2);
        tracker.allocateNative(3);

        assertEquals(0, pool.usedHeap());
        assertEquals(15, pool.usedNative());
    }

    @Test
    void reportHeapMemoryUsageFromSeveralClients() {
        var tracker1 = pool.getExecutionContextPoolMemoryTracker(grabSize, maxGrabSize);
        var tracker2 = pool.getExecutionContextPoolMemoryTracker(grabSize, maxGrabSize);
        var tracker3 = pool.getExecutionContextPoolMemoryTracker(grabSize, maxGrabSize);
        for (int i = 0; i < 10; i++) {
            tracker1.allocateHeap(i);
            tracker2.allocateHeap(i);
            tracker3.allocateHeap(i);
        }

        assertEquals(141, pool.usedHeap());
        assertEquals(0, pool.usedNative());
    }

    @Test
    void reportHeapMemoryUsageFromSeveralClientsWithNegatives() {
        long grabSize = 100;
        var tracker1 = pool.getExecutionContextPoolMemoryTracker(grabSize, grabSize);
        var tracker2 = pool.getExecutionContextPoolMemoryTracker(grabSize, grabSize);

        tracker1.allocateHeap(50);
        tracker2.releaseHeap(50);
        tracker1.allocateHeap(150);
        tracker2.releaseHeap(150);
        tracker1.releaseHeap(100);
        tracker2.allocateHeap(100);

        assertEquals(250, pool.usedHeap());
        assertEquals(0, pool.usedNative());

        tracker1.reset();
        assertEquals(100, pool.usedHeap());
        tracker2.reset();
        assertEquals(0, pool.usedHeap());
    }

    @Test
    void reportNativeMemoryUsageFromSeveralClients() {
        var tracker1 = pool.getExecutionContextPoolMemoryTracker(grabSize, maxGrabSize);
        var tracker2 = pool.getExecutionContextPoolMemoryTracker(grabSize, maxGrabSize);
        var tracker3 = pool.getExecutionContextPoolMemoryTracker(grabSize, maxGrabSize);
        for (int i = 0; i < 15; i++) {
            tracker1.allocateNative(i);
            tracker2.allocateNative(i);
            tracker3.allocateNative(i);
        }

        assertEquals(0, pool.usedHeap());
        assertEquals(315, pool.usedNative());
    }

    @Test
    void reportHeapFromMultipleThreads() throws ExecutionException {
        int numberOfWorkers = 20;
        var executor = Executors.newFixedThreadPool(numberOfWorkers);
        try {
            var futures = new ArrayList<Future<?>>(numberOfWorkers);
            for (int i = 0; i < 20; i++) {
                futures.add(executor.submit(() -> {
                    var memoryTracker = pool.getExecutionContextPoolMemoryTracker(grabSize, maxGrabSize);
                    for (int allocation = 0; allocation < 10; allocation++) {
                        memoryTracker.allocateHeap(allocation);
                    }
                }));
            }
            Futures.getAll(futures);

            assertEquals(940, pool.usedHeap());
            assertEquals(0, pool.usedNative());
        } finally {
            executor.shutdown();
        }
    }

    @Test
    void reportNativeFromMultipleThreads() throws ExecutionException {
        int numberOfWorkers = 20;
        var executor = Executors.newFixedThreadPool(numberOfWorkers);
        try {
            var futures = new ArrayList<Future<?>>(numberOfWorkers);
            for (int i = 0; i < numberOfWorkers; i++) {
                futures.add(executor.submit(() -> {
                    var memoryTracker = pool.getExecutionContextPoolMemoryTracker(grabSize, maxGrabSize);
                    for (int allocation = 0; allocation < 10; allocation++) {
                        memoryTracker.allocateNative(allocation);
                    }
                }));
            }
            Futures.getAll(futures);

            assertEquals(0, pool.usedHeap());
            assertEquals(900, pool.usedNative());
        } finally {
            executor.shutdown();
        }
    }
}
