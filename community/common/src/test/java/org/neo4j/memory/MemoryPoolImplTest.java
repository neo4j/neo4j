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

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class MemoryPoolImplTest {
    @Test
    void unboundedShouldBeUnbounded() {
        MemoryPool memoryPool = new MemoryPoolImpl(0, false, null);
        memoryPool.reserveHeap(Long.MAX_VALUE - 1);
        assertEquals(1, memoryPool.free());
        memoryPool.releaseHeap(Long.MAX_VALUE - 2);
        assertEquals(1, memoryPool.usedHeap());
    }

    @Test
    void trackHeapAndNativeMemory() {
        var memoryPool = new MemoryPoolImpl(1000, true, null);
        memoryPool.reserveHeap(10);

        assertEquals(0, memoryPool.usedNative());
        assertEquals(10, memoryPool.usedHeap());
        assertEquals(10, memoryPool.totalUsed());
        assertEquals(990, memoryPool.free());

        memoryPool.reserveNative(200);

        assertEquals(200, memoryPool.usedNative());
        assertEquals(10, memoryPool.usedHeap());
        assertEquals(210, memoryPool.totalUsed());
        assertEquals(790, memoryPool.free());
    }

    @Test
    void nonStrictPoolAllowAllocationsOverMax() {
        var memoryPool = new MemoryPoolImpl(10, false, null);
        assertDoesNotThrow(() -> memoryPool.reserveHeap(100));
        assertDoesNotThrow(() -> memoryPool.reserveHeap(100));
        assertDoesNotThrow(() -> memoryPool.reserveHeap(100));

        assertEquals(300, memoryPool.totalUsed());
    }

    @Test
    void strictPoolForbidAllocationsOverMax() {
        var memoryPool = new MemoryPoolImpl(100, true, null);
        assertDoesNotThrow(() -> memoryPool.reserveHeap(10));
        assertThrows(MemoryLimitExceededException.class, () -> memoryPool.reserveHeap(100));
        assertDoesNotThrow(() -> memoryPool.reserveHeap(10));

        assertEquals(20, memoryPool.totalUsed());
    }

    @Test
    void freeShouldNotBeNegativeWhenPoolUsingMoreThenALimit() {
        var memoryPool = new MemoryPoolImpl(10, false, null);

        memoryPool.reserveHeap(9);
        assertEquals(1, memoryPool.free());

        memoryPool.reserveHeap(1);
        assertEquals(0, memoryPool.free());

        memoryPool.reserveHeap(10);
        assertEquals(0, memoryPool.free());
        assertEquals(20, memoryPool.usedHeap());
    }

    @Test
    void imposeLimit() {
        final long limit = 10;
        final long halfLimit = limit / 2;
        MemoryPool memoryPool = new MemoryPoolImpl(limit, true, "mySetting");
        assertState(limit, limit, 0, memoryPool);

        memoryPool.reserveHeap(halfLimit);
        assertState(limit, halfLimit, halfLimit, memoryPool);

        memoryPool.reserveHeap(halfLimit);
        assertState(limit, 0, limit, memoryPool);

        MemoryLimitExceededException memoryLimitExceededException =
                assertThrows(MemoryLimitExceededException.class, () -> memoryPool.reserveHeap(1));
        assertThat(memoryLimitExceededException.getMessage())
                .contains("The allocation of an extra 1 B would use more than the limit 10 B. " + "Currently using "
                        + limit + " B");
        assertThat(memoryLimitExceededException.getMessage()).contains("mySetting");

        memoryPool.releaseHeap(halfLimit);
        assertState(limit, halfLimit, halfLimit, memoryPool);

        memoryPool.reserveHeap(1);
        assertState(limit, halfLimit - 1, halfLimit + 1, memoryPool);
    }

    @SuppressWarnings("SameParameterValue")
    private static void assertState(long available, long free, long used, MemoryPool memoryPool) {
        assertEquals(available, memoryPool.totalSize());
        assertEquals(free, memoryPool.free());
        assertEquals(used, memoryPool.usedHeap());
    }
}
