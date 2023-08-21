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
package org.neo4j.internal.batchimport.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import org.junit.jupiter.api.Test;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryPools;

class DynamicIntArrayTest {
    @Test
    void shouldWorkOnSingleChunk() {
        // GIVEN
        int defaultValue = 0;
        IntArray array = NumberArrayFactories.AUTO_WITHOUT_PAGECACHE.newDynamicIntArray(10, defaultValue, INSTANCE);
        array.set(4, 5);

        // WHEN
        assertEquals(5L, array.get(4));
        assertEquals(defaultValue, array.get(12));
        array.set(7, 1324);
        assertEquals(1324L, array.get(7));
    }

    @Test
    void trackHeapMemoryOnArrayAllocations() {
        var memoryTracker = new LocalMemoryTracker(MemoryPools.NO_TRACKING, 300, 0, null);
        var longArray = NumberArrayFactories.HEAP.newDynamicLongArray(10, 1, memoryTracker);

        assertEquals(0, memoryTracker.estimatedHeapMemory());
        assertEquals(0, memoryTracker.usedNativeMemory());

        longArray.set(0, 5);

        assertEquals(96, memoryTracker.estimatedHeapMemory());
        assertEquals(0, memoryTracker.usedNativeMemory());
    }

    @Test
    void trackNativeMemoryOnArrayAllocations() {
        var memoryTracker = new LocalMemoryTracker(MemoryPools.NO_TRACKING, 300, 0, null);
        try (var longArray = NumberArrayFactories.OFF_HEAP.newDynamicLongArray(10, 1, memoryTracker)) {

            assertEquals(0, memoryTracker.estimatedHeapMemory());
            assertEquals(0, memoryTracker.usedNativeMemory());

            longArray.set(0, 5);

            assertEquals(0, memoryTracker.estimatedHeapMemory());
            assertEquals(80, memoryTracker.usedNativeMemory());
        }
    }

    @Test
    void shouldChunksAsNeeded() {
        // GIVEN
        IntArray array = NumberArrayFactories.AUTO_WITHOUT_PAGECACHE.newDynamicIntArray(10, 0, INSTANCE);

        // WHEN
        long index = 243;
        int value = 5485748;
        array.set(index, value);

        // THEN
        assertEquals(value, array.get(index));
    }
}
