/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.memory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class OuterInnerScopedMemoryTrackerTest {
    private final MemoryTracker memoryTracker = new LocalMemoryTracker();
    private final OuterInnerScopedMemoryTracker scopedMemoryTracker = new OuterInnerScopedMemoryTracker(memoryTracker);

    @Test
    void delegatesToOuterParentByDefault() {
        scopedMemoryTracker.allocateNative(10);
        scopedMemoryTracker.releaseNative(2);
        scopedMemoryTracker.allocateHeap(12);
        scopedMemoryTracker.releaseHeap(1);

        assertEquals(8, memoryTracker.usedNativeMemory());
        assertEquals(11, memoryTracker.estimatedHeapMemory());
    }

    @Test
    void delegatesToInner() {
        scopedMemoryTracker.allocateNative(10);
        scopedMemoryTracker.releaseNative(2);
        scopedMemoryTracker.allocateHeap(12);
        scopedMemoryTracker.releaseHeap(1);

        MemoryTracker inner = new LocalMemoryTracker();
        scopedMemoryTracker.setInnerDelegate(inner);

        scopedMemoryTracker.allocateNative(7);
        scopedMemoryTracker.releaseNative(3);
        scopedMemoryTracker.allocateHeap(10);
        scopedMemoryTracker.releaseHeap(4);

        assertEquals(8, memoryTracker.usedNativeMemory());
        assertEquals(11, memoryTracker.estimatedHeapMemory());

        assertEquals(4, inner.usedNativeMemory());
        assertEquals(6, inner.estimatedHeapMemory());
    }

    @Test
    void dontReleaseParentsResources() {
        MemoryTracker inner = new LocalMemoryTracker();

        memoryTracker.allocateNative(1);
        memoryTracker.allocateHeap(3);

        inner.allocateNative(2);
        inner.allocateHeap(4);

        scopedMemoryTracker.allocateNative(10);
        scopedMemoryTracker.releaseNative(2);
        scopedMemoryTracker.allocateHeap(12);
        scopedMemoryTracker.releaseHeap(1);

        scopedMemoryTracker.setInnerDelegate(inner);

        scopedMemoryTracker.allocateNative(7);
        scopedMemoryTracker.releaseNative(3);
        scopedMemoryTracker.allocateHeap(10);
        scopedMemoryTracker.releaseHeap(4);

        assertEquals(9, memoryTracker.usedNativeMemory());
        assertEquals(6, inner.usedNativeMemory());
        assertEquals(12, scopedMemoryTracker.usedNativeMemory());
        assertEquals(14, memoryTracker.estimatedHeapMemory());
        assertEquals(10, inner.estimatedHeapMemory());
        assertEquals(17, scopedMemoryTracker.estimatedHeapMemory());

        scopedMemoryTracker.close();

        assertEquals(1, memoryTracker.usedNativeMemory());
        assertEquals(3, memoryTracker.estimatedHeapMemory());
        assertEquals(2, inner.usedNativeMemory());
        assertEquals(4, inner.estimatedHeapMemory());
    }

    @Test
    void closeParentThenCloseChildShouldBeOK() {
        // Given
        MemoryTracker inner = new LocalMemoryTracker();

        // scopedMemoryTracker is the parent in this test
        scopedMemoryTracker.allocateNative(10);
        scopedMemoryTracker.allocateHeap(10);
        scopedMemoryTracker.setInnerDelegate(inner);
        inner.allocateNative(7);
        inner.allocateHeap(7);

        ScopedMemoryTracker child = new ScopedMemoryTracker(scopedMemoryTracker);

        child.allocateNative(5);
        child.allocateHeap(5);

        // When
        scopedMemoryTracker.close();

        assertEquals(0, scopedMemoryTracker.usedNativeMemory());
        assertEquals(0, scopedMemoryTracker.estimatedHeapMemory());
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
        MemoryTracker inner = new LocalMemoryTracker();
        scopedMemoryTracker.allocateNative(10);
        scopedMemoryTracker.allocateHeap(10);
        scopedMemoryTracker.setInnerDelegate(inner);
        inner.allocateNative(7);
        inner.allocateHeap(7);

        ScopedMemoryTracker child = new ScopedMemoryTracker(scopedMemoryTracker);

        child.allocateNative(5);
        child.allocateHeap(5);
        scopedMemoryTracker.close();

        assertThrows(IllegalStateException.class, () -> child.allocateHeap(10));
        assertThrows(IllegalStateException.class, () -> child.allocateNative(10));
        assertThrows(IllegalStateException.class, () -> child.releaseHeap(10));
        assertThrows(IllegalStateException.class, () -> child.releaseNative(10));
        assertThrows(IllegalStateException.class, child::reset);
    }

    @Test
    void explicitlyHeapAllocateOnOuter() {
        scopedMemoryTracker.allocateNative(10);
        scopedMemoryTracker.releaseNative(2);
        scopedMemoryTracker.allocateHeap(12);
        scopedMemoryTracker.releaseHeap(1);

        MemoryTracker inner = new LocalMemoryTracker();
        scopedMemoryTracker.setInnerDelegate(inner);

        scopedMemoryTracker.allocateNative(7);
        scopedMemoryTracker.releaseNative(3);
        scopedMemoryTracker.allocateHeap(10);
        scopedMemoryTracker.releaseHeap(4);

        scopedMemoryTracker.allocateHeapOuter(2000);
        scopedMemoryTracker.releaseHeapOuter(1000);

        assertEquals(8, memoryTracker.usedNativeMemory());
        assertEquals(1011, memoryTracker.estimatedHeapMemory());

        assertEquals(4, inner.usedNativeMemory());
        assertEquals(6, inner.estimatedHeapMemory());
    }

    @Test
    void delegatesToInnerUntilCloseInner() {
        scopedMemoryTracker.allocateNative(10);
        scopedMemoryTracker.releaseNative(2);
        scopedMemoryTracker.allocateHeap(12);
        scopedMemoryTracker.releaseHeap(1);

        MemoryTracker inner = new LocalMemoryTracker();
        inner.allocateNative(10000);
        inner.allocateHeap(10000);

        scopedMemoryTracker.setInnerDelegate(inner);

        scopedMemoryTracker.allocateNative(7);
        scopedMemoryTracker.releaseNative(3);
        scopedMemoryTracker.allocateHeap(10);
        scopedMemoryTracker.releaseHeap(4);

        assertEquals(8, memoryTracker.usedNativeMemory());
        assertEquals(11, memoryTracker.estimatedHeapMemory());

        assertEquals(10004, inner.usedNativeMemory());
        assertEquals(10006, inner.estimatedHeapMemory());

        scopedMemoryTracker.closeInner();

        scopedMemoryTracker.allocateNative(2000);
        scopedMemoryTracker.releaseNative(1000);
        scopedMemoryTracker.allocateHeap(5000);
        scopedMemoryTracker.releaseHeap(3000);

        assertEquals(1008, memoryTracker.usedNativeMemory());
        assertEquals(2011, memoryTracker.estimatedHeapMemory());

        assertEquals(10000, inner.usedNativeMemory());
        assertEquals(10000, inner.estimatedHeapMemory());
    }

    @Test
    void delegatesToInnerUntilCloseInnerRepeat() {
        scopedMemoryTracker.allocateNative(10);
        scopedMemoryTracker.releaseNative(2);
        scopedMemoryTracker.allocateHeap(12);
        scopedMemoryTracker.releaseHeap(1);

        MemoryTracker inner = new LocalMemoryTracker();
        inner.allocateNative(10000);
        inner.allocateHeap(10000);

        scopedMemoryTracker.setInnerDelegate(inner);

        scopedMemoryTracker.allocateNative(7);
        scopedMemoryTracker.releaseNative(3);
        scopedMemoryTracker.allocateHeap(10);
        scopedMemoryTracker.releaseHeap(4);

        assertEquals(8, memoryTracker.usedNativeMemory());
        assertEquals(11, memoryTracker.estimatedHeapMemory());

        assertEquals(10004, inner.usedNativeMemory());
        assertEquals(10006, inner.estimatedHeapMemory());

        scopedMemoryTracker.closeInner();

        scopedMemoryTracker.allocateNative(2000);
        scopedMemoryTracker.releaseNative(1000);
        scopedMemoryTracker.allocateHeap(5000);
        scopedMemoryTracker.releaseHeap(3000);

        assertEquals(1008, memoryTracker.usedNativeMemory());
        assertEquals(2011, memoryTracker.estimatedHeapMemory());

        assertEquals(10000, inner.usedNativeMemory());
        assertEquals(10000, inner.estimatedHeapMemory());

        MemoryTracker inner2 = new LocalMemoryTracker();
        inner2.allocateNative(100000);
        inner2.allocateHeap(100000);

        scopedMemoryTracker.setInnerDelegate(inner2);

        scopedMemoryTracker.allocateNative(7);
        scopedMemoryTracker.releaseNative(3);
        scopedMemoryTracker.allocateHeap(10);
        scopedMemoryTracker.releaseHeap(4);

        assertEquals(1008, memoryTracker.usedNativeMemory());
        assertEquals(2011, memoryTracker.estimatedHeapMemory());

        assertEquals(100004, inner2.usedNativeMemory());
        assertEquals(100006, inner2.estimatedHeapMemory());

        scopedMemoryTracker.closeInner();

        scopedMemoryTracker.allocateNative(2000);
        scopedMemoryTracker.releaseNative(1000);
        scopedMemoryTracker.allocateHeap(5000);
        scopedMemoryTracker.releaseHeap(3000);

        assertEquals(2008, memoryTracker.usedNativeMemory());
        assertEquals(4011, memoryTracker.estimatedHeapMemory());

        assertEquals(100000, inner2.usedNativeMemory());
        assertEquals(100000, inner2.estimatedHeapMemory());
    }
}
