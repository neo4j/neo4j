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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.memory.ExecutionContextMemoryTracker.NO_LIMIT;
import static org.neo4j.memory.HighWaterMarkMemoryPool.NO_TRACKING;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class ExecutionContextMemoryTrackerTest {
    @Test
    void trackDirectMemoryAllocations() {
        ExecutionContextMemoryTracker memoryTracker = new ExecutionContextMemoryTracker();
        memoryTracker.allocateNative(10);
        memoryTracker.allocateNative(20);
        memoryTracker.allocateNative(40);
        assertEquals(70, memoryTracker.usedNativeMemory());
    }

    @Test
    void trackDirectMemoryDeallocations() {
        ExecutionContextMemoryTracker memoryTracker = new ExecutionContextMemoryTracker();
        memoryTracker.allocateNative(100);
        assertEquals(100, memoryTracker.usedNativeMemory());

        memoryTracker.releaseNative(20);
        assertEquals(80, memoryTracker.usedNativeMemory());

        memoryTracker.releaseNative(40);
        assertEquals(40, memoryTracker.usedNativeMemory());
    }

    @Test
    void trackHeapMemoryAllocations() {
        ExecutionContextMemoryTracker memoryTracker = new ExecutionContextMemoryTracker();
        memoryTracker.allocateHeap(10);
        memoryTracker.allocateHeap(20);
        memoryTracker.allocateHeap(40);
        assertEquals(70, memoryTracker.estimatedHeapMemory());
    }

    @Test
    void trackHeapMemoryDeallocations() {
        ExecutionContextMemoryTracker memoryTracker = new ExecutionContextMemoryTracker();
        memoryTracker.allocateHeap(100);
        assertEquals(100, memoryTracker.estimatedHeapMemory());

        memoryTracker.releaseHeap(20);
        assertEquals(80, memoryTracker.estimatedHeapMemory());

        memoryTracker.releaseHeap(40);
        assertEquals(40, memoryTracker.estimatedHeapMemory());
    }

    @Test
    void throwsOnLimitHeap() {
        var memoryTracker = new ExecutionContextMemoryTracker(NO_TRACKING, 10, 0, 0, "settingName");
        assertThatThrownBy(() -> memoryTracker.allocateHeap(100))
                .isInstanceOf(MemoryLimitExceededException.class)
                .hasMessageContaining("settingName");
        assertThat(memoryTracker.estimatedHeapMemory()).isEqualTo(0);
    }

    @Test
    void throwsOnLimitNative() {
        var memoryTracker = new ExecutionContextMemoryTracker(NO_TRACKING, 10, 0, 0, "settingName");
        assertThatThrownBy(() -> memoryTracker.allocateNative(100))
                .isInstanceOf(MemoryLimitExceededException.class)
                .hasMessageContaining("settingName");
        assertThat(memoryTracker.usedNativeMemory()).isEqualTo(0);
    }

    @Test
    void throwsOnPoolLimitHeap() {
        var pool = new HighWaterMarkMemoryPool(new MemoryPoolImpl(5, true, "poolSetting"));
        var tracker = new ExecutionContextMemoryTracker(pool, 10, 0, 0, "localSetting");

        assertThatThrownBy(() -> tracker.allocateHeap(10))
                .isInstanceOf(MemoryLimitExceededException.class)
                .hasMessageContaining("poolSetting");

        assertThat(tracker.estimatedHeapMemory()).isEqualTo(0);
    }

    @Test
    void throwsOnPoolLimitNative() {
        var pool = new HighWaterMarkMemoryPool(new MemoryPoolImpl(5, true, "poolSetting"));
        var tracker = new ExecutionContextMemoryTracker(pool, 10, 0, 0, "localSetting");

        assertThatThrownBy(() -> tracker.allocateNative(10))
                .isInstanceOf(MemoryLimitExceededException.class)
                .hasMessageContaining("poolSetting");

        assertThat(tracker.usedNativeMemory()).isEqualTo(0);
    }

    @Test
    void shouldIncreaseGrabSizeOnLargerAllocations() {
        // Given
        long grabSize = 20;
        long maxGrabSize = 100;
        HighWaterMarkMemoryPool pool = mock(HighWaterMarkMemoryPool.class);
        ExecutionContextMemoryTracker memoryTracker =
                new ExecutionContextMemoryTracker(pool, NO_LIMIT, grabSize, maxGrabSize, "settingName");

        // When
        int nAllocations = 100;
        long allocationSize = 10L;
        for (int i = 0; i < nAllocations; i++) {
            memoryTracker.allocateHeap(allocationSize);
        }

        // Then
        // NOTE: The first time is always the given grab size, the second time an increase of the grab size will apply,
        //       but it is only effective _after_ the second grab.
        verify(pool, times(2)).reserveHeap(20);
        verify(pool, times(1)).reserveHeap(32);
        verify(pool, times(1)).reserveHeap(64);
        verify(pool, times(9)).reserveHeap(100);
        verifyNoMoreInteractions(pool);

        long expectedTotalAllocationSize = nAllocations * allocationSize;
        assertEquals(expectedTotalAllocationSize, memoryTracker.estimatedHeapMemory());

        memoryTracker.reset();
        long expectedLocalHeapPool = (2 * 20 + 32 + 64 + 9 * 100) - expectedTotalAllocationSize;
        verify(pool, times(1)).releaseHeap(expectedLocalHeapPool);
    }

    @Test
    void shouldIncreaseGrabSizeOnLargerAllocations2() {
        // Given
        long grabSize = 20;
        long maxGrabSize = 100;
        HighWaterMarkMemoryPool pool = mock(HighWaterMarkMemoryPool.class);
        ExecutionContextMemoryTracker memoryTracker =
                new ExecutionContextMemoryTracker(pool, NO_LIMIT, grabSize, maxGrabSize, "settingName");

        // When
        int nAllocations = 50;
        long allocationSize = 20L;
        for (int i = 0; i < nAllocations; i++) {
            memoryTracker.allocateHeap(allocationSize);
        }

        // Then
        // NOTE: The first time is always the given grab size, the second time an increase of the grab size will apply,
        //       but it is only effective _after_ the second grab.
        verify(pool, times(2)).reserveHeap(20);
        verify(pool, times(1)).reserveHeap(32);
        verify(pool, times(1)).reserveHeap(64);
        verify(pool, times(9)).reserveHeap(100);
        verifyNoMoreInteractions(pool);

        long expectedTotalAllocationSize = nAllocations * allocationSize;
        assertEquals(expectedTotalAllocationSize, memoryTracker.estimatedHeapMemory());

        memoryTracker.reset();
        long expectedLocalHeapPool = (2 * 20 + 32 + 64 + 9 * 100) - expectedTotalAllocationSize;
        verify(pool, times(1)).releaseHeap(expectedLocalHeapPool);
    }

    @Test
    void shouldIncreaseGrabSizeOnLargerAllocationsWithInitialPowerOfTwo() {
        // Given
        long grabSize = 16;
        long maxGrabSize = 100;
        HighWaterMarkMemoryPool pool = mock(HighWaterMarkMemoryPool.class);
        ExecutionContextMemoryTracker memoryTracker =
                new ExecutionContextMemoryTracker(pool, NO_LIMIT, grabSize, maxGrabSize, "settingName");

        // When
        int nAllocations = 100;
        long allocationSize = 10L;
        for (int i = 0; i < nAllocations; i++) {
            memoryTracker.allocateHeap(allocationSize);
        }

        // Then
        // NOTE: The first time is always the given grab size, the second time an increase of the grab size will apply,
        //       but it is only effective _after_ the second grab.
        verify(pool, times(2)).reserveHeap(16);
        verify(pool, times(1)).reserveHeap(32);
        verify(pool, times(1)).reserveHeap(64);
        verify(pool, times(9)).reserveHeap(100);
        verifyNoMoreInteractions(pool);

        long expectedTotalAllocationSize = nAllocations * allocationSize;
        assertEquals(expectedTotalAllocationSize, memoryTracker.estimatedHeapMemory());

        memoryTracker.allocateHeap(1); // Just to make the final release different from the release calls above
        memoryTracker.reset();

        long expectedLocalHeapPool = (2 * 16 + 32 + 64 + 9 * 100) - expectedTotalAllocationSize - 1;
        verify(pool, times(1)).releaseHeap(expectedLocalHeapPool);
    }

    @Test
    void shouldNotIncreaseGrabSizeOnSmallAllocations() {
        // Given
        long grabSize = 20;
        long maxGrabSize = 100;
        HighWaterMarkMemoryPool pool = mock(HighWaterMarkMemoryPool.class);
        ExecutionContextMemoryTracker memoryTracker =
                new ExecutionContextMemoryTracker(pool, NO_LIMIT, grabSize, maxGrabSize, "settingName");

        // When
        int nAllocations = 100;
        long allocationSize = 2L;
        for (int i = 0; i < nAllocations; i++) {
            memoryTracker.allocateHeap(allocationSize);
        }

        // Then
        long expectedTotalAllocationSize = nAllocations * allocationSize;
        int expectedNumberOfGrabs = (int) (expectedTotalAllocationSize / grabSize);
        verify(pool, times(expectedNumberOfGrabs)).reserveHeap(grabSize);
        verifyNoMoreInteractions(pool);

        assertEquals(expectedTotalAllocationSize, memoryTracker.estimatedHeapMemory());

        memoryTracker.reset();
        long expectedLocalHeapPool = expectedNumberOfGrabs * grabSize - expectedTotalAllocationSize;
        verify(pool, never()).releaseHeap(expectedLocalHeapPool);
    }

    @Test
    void shouldIncreasePoolReleaseSizeOnLargerReleases() {
        // Given
        long grabSize = 20;
        long maxGrabSize = 100;
        HighWaterMarkMemoryPool pool = mock(HighWaterMarkMemoryPool.class);
        ExecutionContextMemoryTracker memoryTracker =
                new ExecutionContextMemoryTracker(pool, NO_LIMIT, grabSize, maxGrabSize, "settingName");

        // When
        int nAllocations = 100;
        long allocationSize = 10L;
        for (int i = 0; i < nAllocations; i++) {
            memoryTracker.releaseHeap(allocationSize);
        }

        // Then
        // NOTE: The first time is always the given grab size, the second time an increase of the grab size will apply,
        //       but it is only effective _after_ the second release.
        verify(pool, times(2)).releaseHeap(20);
        verify(pool, times(1)).releaseHeap(32);
        // NOTE: After the first increase to 64 we do not hit the CLIENT_CALLS_PER_POOL_INTERACTION_THRESHOLD
        verify(pool, times(2)).releaseHeap(64);
        verify(pool, times(6)).releaseHeap(100);
        verifyNoMoreInteractions(pool);

        long expectedTotalReleaseSize = nAllocations * allocationSize;
        assertEquals(-expectedTotalReleaseSize, memoryTracker.estimatedHeapMemory());

        memoryTracker.allocateHeap(1); // Just to make the final release different from the release calls above
        memoryTracker.reset();

        long expectedLocalHeapPool = expectedTotalReleaseSize - (2 * 20 + 32 + 2 * 64 + 6 * 100) - 1;
        verify(pool, times(1)).releaseHeap(expectedLocalHeapPool);
    }

    @Test
    void shouldIncreasePoolReleaseSizeOnLargerReleases2() {
        // Given
        long grabSize = 20;
        long maxGrabSize = 100;
        HighWaterMarkMemoryPool pool = mock(HighWaterMarkMemoryPool.class);
        ExecutionContextMemoryTracker memoryTracker =
                new ExecutionContextMemoryTracker(pool, NO_LIMIT, grabSize, maxGrabSize, "settingName");

        // When
        int nAllocations = 50;
        long allocationSize = 20L;
        for (int i = 0; i < nAllocations; i++) {
            memoryTracker.releaseHeap(allocationSize);
        }

        // Then
        // NOTE: The first time is always the given grab size, the second time an increase of the grab size will apply,
        //       but it is only effective _after_ the second release.
        verify(pool, times(2)).releaseHeap(20);
        verify(pool, times(1)).releaseHeap(32);
        verify(pool, times(1)).releaseHeap(64);
        verify(pool, times(7)).releaseHeap(100);
        verifyNoMoreInteractions(pool);

        long expectedTotalReleaseSize = nAllocations * allocationSize;
        assertEquals(-expectedTotalReleaseSize, memoryTracker.estimatedHeapMemory());

        memoryTracker.reset();

        long expectedLocalHeapPool = expectedTotalReleaseSize - (2 * 20 + 32 + 64 + 7 * 100);
        verify(pool, times(1)).releaseHeap(expectedLocalHeapPool);
    }

    @Test
    void shouldIncreasePoolReleaseSizeOnLargerReleasesWithInitialPowerOfTwo() {
        // Given
        long grabSize = 16;
        long maxGrabSize = 100;
        HighWaterMarkMemoryPool pool = mock(HighWaterMarkMemoryPool.class);
        ExecutionContextMemoryTracker memoryTracker =
                new ExecutionContextMemoryTracker(pool, NO_LIMIT, grabSize, maxGrabSize, "settingName");

        // When
        int nAllocations = 100;
        long allocationSize = 10L;
        for (int i = 0; i < nAllocations; i++) {
            memoryTracker.releaseHeap(allocationSize);
        }

        // Then
        // NOTE: The first time is always the given grab size, the second time an increase of the grab size will apply,
        //       but it is only effective _after_ the second release.
        verify(pool, times(2)).releaseHeap(16);
        verify(pool, times(1)).releaseHeap(32);
        // NOTE: After the first increase to 64 we do not hit the CLIENT_CALLS_PER_POOL_INTERACTION_THRESHOLD
        verify(pool, times(2)).releaseHeap(64);
        verify(pool, times(7)).releaseHeap(100);
        verifyNoMoreInteractions(pool);

        long expectedTotalReleaseSize = nAllocations * allocationSize;
        assertEquals(-expectedTotalReleaseSize, memoryTracker.estimatedHeapMemory());

        memoryTracker.reset();

        long expectedLocalHeapPool = expectedTotalReleaseSize - (2 * 16 + 32 + 2 * 64 + 7 * 100);
        verify(pool, times(1)).releaseHeap(expectedLocalHeapPool);
    }

    @Test
    void shouldNotIncreasePoolReleaseSizeOnSmallReleases() {
        // Given
        long grabSize = 20;
        long maxGrabSize = 100;
        HighWaterMarkMemoryPool pool = mock(HighWaterMarkMemoryPool.class);
        ExecutionContextMemoryTracker memoryTracker =
                new ExecutionContextMemoryTracker(pool, NO_LIMIT, grabSize, maxGrabSize, "settingName");

        // When
        int nAllocations = 100;
        long allocationSize = 2L;
        for (int i = 0; i < nAllocations; i++) {
            memoryTracker.releaseHeap(allocationSize);
        }

        // Then
        long expectedTotalReleaseSize = nAllocations * allocationSize;
        int expectedNumberOfPoolReleases = (int) (expectedTotalReleaseSize / grabSize) - 2;
        verify(pool, times(expectedNumberOfPoolReleases)).releaseHeap(grabSize);
        verifyNoMoreInteractions(pool);

        assertEquals(-expectedTotalReleaseSize, memoryTracker.estimatedHeapMemory());

        memoryTracker.allocateHeap(1); // Just to make the final release different from the release calls above
        memoryTracker.reset();

        long expectedLocalHeapPool = expectedTotalReleaseSize - expectedNumberOfPoolReleases * grabSize - 1;
        verify(pool, times(1)).releaseHeap(expectedLocalHeapPool);
    }

    @Test
    void initialCreditShouldPreventGrab() {
        // Given
        long grabSize = 20;
        long maxGrabSize = 100;
        long initialCredit = 20;
        HighWaterMarkMemoryPool pool = mock(HighWaterMarkMemoryPool.class);
        ExecutionContextMemoryTracker memoryTracker =
                new ExecutionContextMemoryTracker(pool, NO_LIMIT, grabSize, maxGrabSize, "settingName");

        // When
        memoryTracker.releaseHeap(initialCredit); // Transfer some initial memory to the tracker up-front

        int nAllocations = 3;
        long allocationSize = 10L;
        for (int i = 0; i < nAllocations; i++) {
            memoryTracker.allocateHeap(allocationSize);
        }

        // Then
        verify(pool, times(1)).reserveHeap(grabSize);
        verifyNoMoreInteractions(pool);
        long expectedTotalAllocationSize = nAllocations * allocationSize - initialCredit;
        assertEquals(expectedTotalAllocationSize, memoryTracker.estimatedHeapMemory());

        memoryTracker.reset();
        long expectedLocalHeapPool = expectedTotalAllocationSize;
        verify(pool, times(1)).releaseHeap(expectedLocalHeapPool);
    }

    @Test
    void shouldHandleBalancedCallsWithLocalHeapPool() {
        // Given
        long grabSize = 20;
        long maxGrabSize = 100;
        long initialCredit = 20;
        HighWaterMarkMemoryPool pool = mock(HighWaterMarkMemoryPool.class);
        ExecutionContextMemoryTracker memoryTracker =
                new ExecutionContextMemoryTracker(pool, NO_LIMIT, grabSize, maxGrabSize, "settingName");

        // When
        memoryTracker.releaseHeap(initialCredit); // Transfer some initial memory to the tracker up-front
        memoryTracker.allocateHeap(20);
        memoryTracker.releaseHeap(20);
        memoryTracker.releaseHeap(20);
        memoryTracker.allocateHeap(20);
        memoryTracker.allocateHeap(20);
        memoryTracker.releaseHeap(20);
        memoryTracker.allocateHeap(20);

        // Then
        verifyNoInteractions(pool);
        assertEquals(0, memoryTracker.estimatedHeapMemory());

        // When
        memoryTracker.reset();
    }

    @Test
    void oneOffLargeAllocationDoesNotImmediatelyAffectGrabSize() {
        // Given
        long grabSize = 20;
        long maxGrabSize = 100;
        HighWaterMarkMemoryPool pool = mock(HighWaterMarkMemoryPool.class);
        ExecutionContextMemoryTracker memoryTracker =
                new ExecutionContextMemoryTracker(pool, NO_LIMIT, grabSize, maxGrabSize, "settingName");

        // When
        memoryTracker.allocateHeap(1000L);
        memoryTracker.allocateHeap(10);
        memoryTracker.allocateHeap(10);
        memoryTracker.allocateHeap(10);

        // Then
        verify(pool, times(1)).reserveHeap(1000L);
        verify(pool, times(1)).reserveHeap(20L);
        verify(pool, times(1)).reserveHeap(32);
        verifyNoMoreInteractions(pool);

        long expectedTotalAllocationSize = 1030L;
        assertEquals(expectedTotalAllocationSize, memoryTracker.estimatedHeapMemory());

        memoryTracker.reset();
        long expectedLocalHeapPool = 1000 + 20 + 32 - expectedTotalAllocationSize;
        verify(pool, times(1)).releaseHeap(expectedLocalHeapPool);
    }

    @Test
    void oneOffLargeReleaseDoesNotImmediatelyAffectGrabSize() {
        // Given
        long grabSize = 20;
        long maxGrabSize = 100;
        HighWaterMarkMemoryPool pool = mock(HighWaterMarkMemoryPool.class);
        ExecutionContextMemoryTracker memoryTracker =
                new ExecutionContextMemoryTracker(pool, NO_LIMIT, grabSize, maxGrabSize, "settingName");

        // When
        memoryTracker.releaseHeap(1000L);
        int nSmallAllocations = 8;
        for (int i = 0; i < nSmallAllocations; i++) {
            memoryTracker.allocateHeap(10L);
        }

        // Then
        verify(pool, times(1)).releaseHeap(1000 - grabSize * 2);
        // NOTE: First 4 small allocations will consume the remaining local pool of grabSize*2
        verify(pool, times(1)).reserveHeap(20L);
        verify(pool, times(1)).reserveHeap(32L);
        verifyNoMoreInteractions(pool);

        long expectedTotalAllocationSize = nSmallAllocations * 10L - 1000L;
        assertEquals(expectedTotalAllocationSize, memoryTracker.estimatedHeapMemory());

        memoryTracker.reset();
        long expectedLocalHeapPool = 20 + 32 - 4 * 10;
        verify(pool, times(1)).releaseHeap(expectedLocalHeapPool);
    }

    @Test
    void usedUpInitialCreditShouldBeReservedOnResetIfThereWereNoGrabs() {
        // Given
        long grabSize = 20;
        long maxGrabSize = 100;
        long initialCredit = 20;
        HighWaterMarkMemoryPool pool = mock(HighWaterMarkMemoryPool.class);
        ExecutionContextMemoryTracker memoryTracker =
                new ExecutionContextMemoryTracker(pool, NO_LIMIT, grabSize, maxGrabSize, "settingName");

        // When
        memoryTracker.releaseHeap(initialCredit); // Transfer some initial memory to the tracker up-front
        memoryTracker.allocateHeap(10);

        // Then
        verify(pool, never()).reserveHeap(grabSize);
        verifyNoMoreInteractions(pool);

        memoryTracker.reset();
    }

    @Test
    void trackHeapHighWaterMark() {
        var pool = new HighWaterMarkMemoryPool(new MemoryPoolImpl(1000, true, "poolSetting"));
        var grabSize = 100;
        ExecutionContextMemoryTracker memoryTracker =
                new ExecutionContextMemoryTracker(pool, NO_LIMIT, grabSize, grabSize, "poolSetting");
        memoryTracker.allocateHeap(10);
        assertEquals(grabSize, memoryTracker.heapHighWaterMark());
        memoryTracker.allocateHeap(89);
        assertEquals(grabSize, memoryTracker.heapHighWaterMark());
        memoryTracker.allocateHeap(10);
        assertEquals(2 * grabSize, memoryTracker.heapHighWaterMark());
    }

    @Test
    void trackHeapHighWaterMarkConcurrent() throws InterruptedException {
        MemoryPoolImpl poolSetting = new MemoryPoolImpl(1000, true, "poolSetting");
        var pool = new HighWaterMarkMemoryPool(poolSetting);
        var grabSize = 100;

        int nThreads = 10;
        ExecutorService service = Executors.newFixedThreadPool(10);

        for (int i = 0; i < nThreads; i++) {
            service.submit(() -> {
                try (var memoryTracker =
                        new ExecutionContextMemoryTracker(pool, NO_LIMIT, grabSize, grabSize, "poolSetting")) {
                    memoryTracker.allocateHeap(11);
                }
            });
        }
        service.shutdown();
        assertTrue(service.awaitTermination(1L, TimeUnit.MINUTES));

        assertThat(pool.heapHighWaterMark()).isLessThanOrEqualTo(nThreads * grabSize);
    }
}
