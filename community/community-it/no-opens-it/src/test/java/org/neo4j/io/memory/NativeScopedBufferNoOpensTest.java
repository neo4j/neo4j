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
package org.neo4j.io.memory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.memory.MemoryPools.NO_TRACKING;

import java.nio.ByteOrder;
import org.junit.jupiter.api.Test;
import org.neo4j.memory.LocalMemoryTracker;

class NativeScopedBufferNoOpensTest {
    @Test
    void trackBufferScopeMemoryAllocation() {
        var memoryTracker = new LocalMemoryTracker(NO_TRACKING, 400, 0, null);
        try (NativeScopedBuffer bufferScope = new NativeScopedBuffer(100, ByteOrder.LITTLE_ENDIAN, memoryTracker)) {
            assertEquals(0, memoryTracker.estimatedHeapMemory());
            assertEquals(100, memoryTracker.usedNativeMemory());
        }

        assertEquals(0, memoryTracker.estimatedHeapMemory());
        assertEquals(0, memoryTracker.usedNativeMemory());
    }

    @Test
    void closeBufferMultipleTimesIsSafe() {
        var memoryTracker = new LocalMemoryTracker(NO_TRACKING, 400, 0, null);
        NativeScopedBuffer bufferScope = new NativeScopedBuffer(100, ByteOrder.LITTLE_ENDIAN, memoryTracker);
        assertEquals(0, memoryTracker.estimatedHeapMemory());
        assertEquals(100, memoryTracker.usedNativeMemory());

        bufferScope.close();

        assertEquals(0, memoryTracker.estimatedHeapMemory());
        assertEquals(0, memoryTracker.usedNativeMemory());

        bufferScope.close();
        bufferScope.close();
        bufferScope.close();

        assertEquals(0, memoryTracker.estimatedHeapMemory());
        assertEquals(0, memoryTracker.usedNativeMemory());
    }
}
