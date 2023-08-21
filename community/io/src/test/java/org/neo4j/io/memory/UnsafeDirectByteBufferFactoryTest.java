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

import org.junit.jupiter.api.Test;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryTracker;

class UnsafeDirectByteBufferFactoryTest {
    @Test
    void shouldAllocateBuffer() {
        // given
        MemoryTracker tracker = new LocalMemoryTracker();
        try (UnsafeDirectByteBufferAllocator factory = new UnsafeDirectByteBufferAllocator()) {
            // when
            int bufferSize = 128;
            factory.allocate(bufferSize, tracker);

            // then
            assertEquals(bufferSize, tracker.usedNativeMemory());
        }
    }

    @Test
    void shouldFreeOnClose() {
        // given
        MemoryTracker tracker = new LocalMemoryTracker();
        try (UnsafeDirectByteBufferAllocator factory = new UnsafeDirectByteBufferAllocator()) {
            // when
            factory.allocate(256, tracker);
        }

        // then
        assertEquals(0, tracker.usedNativeMemory());
    }

    @Test
    void shouldHandleMultipleClose() {
        // given
        MemoryTracker tracker = new LocalMemoryTracker();
        UnsafeDirectByteBufferAllocator factory = new UnsafeDirectByteBufferAllocator();

        // when
        factory.allocate(256, tracker);
        factory.close();

        // then
        assertEquals(0, tracker.usedNativeMemory());
        factory.close();
        assertEquals(0, tracker.usedNativeMemory());
    }

    @Test
    void shouldNotAllocateAfterClosed() {
        // given
        var localMemoryTracker = new LocalMemoryTracker();
        UnsafeDirectByteBufferAllocator factory = new UnsafeDirectByteBufferAllocator();
        factory.close();

        // when
        try {
            factory.allocate(8, localMemoryTracker);
        } catch (IllegalStateException e) {
            // then good
        }
    }
}
