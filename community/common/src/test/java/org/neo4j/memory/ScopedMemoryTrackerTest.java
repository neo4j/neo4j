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

import org.junit.jupiter.api.Test;

class ScopedMemoryTrackerTest {
    private final MemoryTracker memoryTracker = new LocalMemoryTracker();
    private final DefaultScopedMemoryTracker scopedMemoryTracker = new DefaultScopedMemoryTracker(memoryTracker);

    @Test
    void delegatesToParent() {
        scopedMemoryTracker.allocateNative(10);
        scopedMemoryTracker.releaseNative(2);
        scopedMemoryTracker.allocateHeap(12);
        scopedMemoryTracker.releaseHeap(1);

        assertEquals(8, memoryTracker.usedNativeMemory());
        assertEquals(11, memoryTracker.estimatedHeapMemory());
    }

    @Test
    void dontReleaseParentsResources() {
        memoryTracker.allocateNative(1);
        memoryTracker.allocateHeap(3);

        scopedMemoryTracker.allocateNative(10);
        scopedMemoryTracker.releaseNative(2);
        scopedMemoryTracker.allocateHeap(12);
        scopedMemoryTracker.releaseHeap(1);

        assertEquals(9, memoryTracker.usedNativeMemory());
        assertEquals(8, scopedMemoryTracker.usedNativeMemory());
        assertEquals(14, memoryTracker.estimatedHeapMemory());
        assertEquals(11, scopedMemoryTracker.estimatedHeapMemory());

        scopedMemoryTracker.close();

        assertEquals(1, memoryTracker.usedNativeMemory());
        assertEquals(3, memoryTracker.estimatedHeapMemory());
    }

    @Test
    void closeParentThenCloseChildShouldBeOK() {
        // Given
        // scopedMemoryTracker is the parent in this test
        scopedMemoryTracker.allocateNative(10);
        scopedMemoryTracker.allocateHeap(10);

        DefaultScopedMemoryTracker child = new DefaultScopedMemoryTracker(scopedMemoryTracker);

        child.allocateNative(5);
        child.allocateHeap(5);

        // When
        scopedMemoryTracker.close();

        assertEquals(0, scopedMemoryTracker.usedNativeMemory());
        assertEquals(0, scopedMemoryTracker.estimatedHeapMemory());

        child.close();

        // Then
        assertEquals(0, scopedMemoryTracker.usedNativeMemory());
        assertEquals(0, scopedMemoryTracker.estimatedHeapMemory());
    }

    @Test
    void closeParentThenAllocateReleaseOrResetChildShouldThrow() {
        // scopedMemoryTracker is the parent in this test
        scopedMemoryTracker.allocateNative(10);
        scopedMemoryTracker.allocateHeap(10);

        DefaultScopedMemoryTracker child = new DefaultScopedMemoryTracker(scopedMemoryTracker);

        child.allocateNative(5);
        child.allocateHeap(5);
        scopedMemoryTracker.close();

        assertThrows(IllegalStateException.class, () -> child.allocateHeap(10));
        assertThrows(IllegalStateException.class, () -> child.allocateNative(10));
        assertThrows(IllegalStateException.class, () -> child.releaseHeap(10));
        assertThrows(IllegalStateException.class, () -> child.releaseNative(10));
        assertThrows(IllegalStateException.class, child::reset);
    }
}
