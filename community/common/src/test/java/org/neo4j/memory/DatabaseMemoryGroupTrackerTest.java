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
package org.neo4j.memory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class DatabaseMemoryGroupTrackerTest {
    private final MemoryPools memoryPools = new MemoryPools();
    private final GlobalMemoryGroupTracker globalPool = memoryPools.pool(MemoryGroup.TRANSACTION, 100, null);

    @AfterEach
    void tearDown() {
        assertEquals(0, globalPool.totalUsed());
        globalPool.close();
    }

    private static Stream<Arguments> arguments() {
        return Stream.of(
                Arguments.of(new AllocationFacade(
                        "heap",
                        MemoryPool::usedHeap,
                        MemoryPool::reserveHeap,
                        MemoryPool::releaseHeap,
                        pool -> pool.getPoolMemoryTracker().estimatedHeapMemory())),
                Arguments.of(new AllocationFacade(
                        "native",
                        MemoryPool::usedNative,
                        MemoryPool::reserveNative,
                        MemoryPool::releaseNative,
                        pool -> pool.getPoolMemoryTracker().usedNativeMemory())));
    }

    @ParameterizedTest
    @MethodSource("arguments")
    void allocateOnParent(AllocationFacade methods) {
        ScopedMemoryPool subPool = globalPool.newDatabasePool("pool1", 10, null);
        methods.reserve(subPool, 2);
        assertEquals(2, methods.used(subPool));
        assertEquals(2, methods.used(globalPool));

        methods.release(subPool, 2);
        subPool.close();
    }

    @ParameterizedTest
    @MethodSource("arguments")
    void ownPoolFromTracking(AllocationFacade methods) {
        methods.reserve(globalPool, 2);
        ScopedMemoryPool subPool = globalPool.newDatabasePool("pool1", 10, null);
        methods.reserve(subPool, 2);
        assertEquals(2, methods.used(subPool));
        assertEquals(4, methods.used(globalPool));

        methods.release(subPool, 2);
        subPool.close();
        methods.release(globalPool, 2);
    }

    @ParameterizedTest
    @MethodSource("arguments")
    void trackMemoryWithDefaultTracker(AllocationFacade methods) {
        ScopedMemoryPool subPool = globalPool.newDatabasePool("pool1", 12, null);
        methods.reserve(subPool, 3);

        assertEquals(3, methods.used(subPool));
        assertEquals(3, methods.trackedMemory(subPool));

        methods.release(subPool, 3);
        assertEquals(0, methods.trackedMemory(subPool));
        subPool.close();
    }

    @Test
    void trackedHeapFromPoolAndTrackerMatch() {
        ScopedMemoryPool subPool = globalPool.newDatabasePool("pool1", 120, null);

        var memoryTracker = subPool.getPoolMemoryTracker();

        memoryTracker.allocateHeap(12);

        assertEquals(12, subPool.usedHeap());
        assertEquals(12, memoryTracker.estimatedHeapMemory());

        subPool.close();
    }

    @Test
    void trackedNativeFromPoolAndTrackerMatch() {
        ScopedMemoryPool subPool = globalPool.newDatabasePool("pool1", 120, null);

        var memoryTracker = subPool.getPoolMemoryTracker();

        memoryTracker.allocateNative(13);

        assertEquals(13, subPool.usedNative());
        assertEquals(13, memoryTracker.usedNativeMemory());

        subPool.close();
    }

    @ParameterizedTest
    @MethodSource("arguments")
    void respectLocalLimit(AllocationFacade methods) {
        ScopedMemoryPool subPool = globalPool.newDatabasePool("pool1", 10, null);
        assertThrows(MemoryLimitExceededException.class, () -> methods.reserve(subPool, 11));
        subPool.close();
    }

    @ParameterizedTest
    @MethodSource("arguments")
    void respectParentLimit(AllocationFacade methods) {
        ScopedMemoryPool subPool = globalPool.newDatabasePool("pool1", 102, null);
        assertThrows(MemoryLimitExceededException.class, () -> methods.reserve(subPool, 101));
        subPool.close();
    }

    private static final class AllocationFacade {
        final String name;
        final Function<ScopedMemoryPool, Long> used;
        final BiConsumer<ScopedMemoryPool, Long> reserve;
        final BiConsumer<ScopedMemoryPool, Long> release;
        private final ToLongFunction<ScopedMemoryPool> trackedMemory;

        private AllocationFacade(
                String name,
                Function<ScopedMemoryPool, Long> used,
                BiConsumer<ScopedMemoryPool, Long> reserve,
                BiConsumer<ScopedMemoryPool, Long> release,
                ToLongFunction<ScopedMemoryPool> trackedMemory) {
            this.name = name;
            this.used = used;
            this.reserve = reserve;
            this.release = release;
            this.trackedMemory = trackedMemory;
        }

        long used(ScopedMemoryPool pool) {
            return used.apply(pool);
        }

        void reserve(ScopedMemoryPool pool, long bytes) {
            reserve.accept(pool, bytes);
        }

        long trackedMemory(ScopedMemoryPool pool) {
            return trackedMemory.applyAsLong(pool);
        }

        void release(ScopedMemoryPool pool, long bytes) {
            release.accept(pool, bytes);
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
