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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LocalMemoryTrackerWithPoolTest {
    private static final long LOCAL_LIMIT = 10;
    private static final long GRAB_SIZE = 2;

    private MemoryPool memoryPool;
    private LocalMemoryTracker memoryTracker;

    @BeforeEach
    void setUp() {
        memoryPool = new MemoryPoolImpl(0, true, null);
        memoryTracker = new LocalMemoryTracker(memoryPool, LOCAL_LIMIT, GRAB_SIZE, null);
    }

    @AfterEach
    void tearDown() {
        memoryTracker.reset();
        assertReserved(0);
    }

    @Test
    void trackedNativeAllocationReportedInPool() {
        memoryTracker.allocateNative(10);
        try {
            assertEquals(10, memoryTracker.usedNativeMemory());
            assertEquals(0, memoryTracker.estimatedHeapMemory());

            assertEquals(10, memoryPool.usedNative());
            assertEquals(0, memoryPool.usedHeap());
        } finally {
            memoryTracker.releaseNative(10);
        }
    }

    @Test
    void trackedHeapAllocationReportedInPool() {
        memoryTracker.allocateHeap(10);
        assertEquals(10, memoryTracker.estimatedHeapMemory());
        assertEquals(0, memoryTracker.usedNativeMemory());

        assertEquals(10, memoryPool.usedHeap());
        assertEquals(0, memoryPool.usedNative());
    }

    @Test
    void grabSize() {
        memoryTracker.allocateHeap(1);
        assertReserved(GRAB_SIZE);
    }

    @Test
    void respectsLocalLimit() {
        assertThrows(MemoryLimitExceededException.class, () -> memoryTracker.allocateHeap(LOCAL_LIMIT + 1));
    }

    @Test
    void reserveFromParentWhenLocalPoolIsEmpty() {
        memoryTracker.allocateHeap(GRAB_SIZE + 2);
        assertThat(memoryPool.usedHeap()).isGreaterThan(GRAB_SIZE);
    }

    @Test
    void negativeAdjustments() {
        memoryTracker.allocateHeap(1);
        memoryTracker.releaseHeap(1);
        memoryTracker.allocateHeap(1);
        memoryTracker.releaseHeap(1);
        memoryTracker.allocateHeap(1);
        memoryTracker.releaseHeap(1);
        assertReserved(GRAB_SIZE);
    }

    @Test
    void largeAdjustments() {
        memoryTracker.allocateHeap(LOCAL_LIMIT);
        assertThat(memoryPool.usedHeap()).isGreaterThanOrEqualTo(LOCAL_LIMIT);
    }

    @Test
    void zeroAdjustmentsAllowed() {
        memoryTracker.allocateHeap(0);
    }

    @Test
    void releaseToParentIfLocalPoolMuchLargerThanUsed() {
        memoryTracker.allocateHeap(10);
        assertThat(memoryTracker.estimatedHeapMemory()).isEqualTo(10);
        assertThat(memoryPool.usedHeap()).isEqualTo(10);

        memoryTracker.releaseHeap(8);
        assertThat(memoryTracker.estimatedHeapMemory()).isEqualTo(2);
        assertThat(memoryPool.usedHeap()).isLessThan(10);
    }

    private void assertReserved(long i) {
        assertEquals(i, memoryPool.usedHeap());
    }
}
