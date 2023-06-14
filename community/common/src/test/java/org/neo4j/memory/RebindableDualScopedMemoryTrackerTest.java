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

class RebindableDualScopedMemoryTrackerTest {
    private final MemoryTracker memoryTracker = new LocalMemoryTracker();
    private final RebindableDualScopedMemoryTracker scopedMemoryTracker =
            new RebindableDualScopedMemoryTracker(memoryTracker);

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

        scopedMemoryTracker.releaseNative(4);
        scopedMemoryTracker.releaseHeap(6);

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

        DefaultScopedMemoryTracker child = new DefaultScopedMemoryTracker(scopedMemoryTracker);

        child.allocateNative(5);
        child.releaseNative(5);
        child.allocateHeap(5);
        child.releaseHeap(5);

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

        DefaultScopedMemoryTracker child = new DefaultScopedMemoryTracker(scopedMemoryTracker);

        child.allocateNative(5);
        child.releaseNative(5);
        child.allocateHeap(5);
        child.releaseHeap(5);
        scopedMemoryTracker.close();

        assertThrows(IllegalStateException.class, () -> child.allocateHeap(10));
        assertThrows(IllegalStateException.class, () -> child.allocateNative(10));
        assertThrows(IllegalStateException.class, () -> child.releaseHeap(10));
        assertThrows(IllegalStateException.class, () -> child.releaseNative(10));
        assertThrows(IllegalStateException.class, child::reset);
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

        assertEquals(4, scopedMemoryTracker.unreleasedInnerScopeNative());
        assertEquals(6, scopedMemoryTracker.unreleasedInnerScopeHeap());

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

        final long expectedCarryOverNative = 4;
        final long expectedCarryOverHeap = 6;
        assertEquals(expectedCarryOverNative, scopedMemoryTracker.unreleasedInnerScopeNative());
        assertEquals(expectedCarryOverHeap, scopedMemoryTracker.unreleasedInnerScopeHeap());

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

        scopedMemoryTracker.allocateNative(23);
        scopedMemoryTracker.releaseNative(7);
        scopedMemoryTracker.allocateHeap(11);
        scopedMemoryTracker.releaseHeap(2);

        assertEquals(1008, memoryTracker.usedNativeMemory());
        assertEquals(2011, memoryTracker.estimatedHeapMemory());

        assertEquals(100000 + 23 - 7 + expectedCarryOverNative, inner2.usedNativeMemory());
        assertEquals(100000 + 11 - 2 + expectedCarryOverHeap, inner2.estimatedHeapMemory());

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

    @Test
    void assertNoUnreleasedInnerOnClose() {
        final var outer = new LocalMemoryTracker();
        final var tracker = new RebindableDualScopedMemoryTracker(outer);

        final var inner1 = new LocalMemoryTracker();
        tracker.setInnerDelegate(inner1);
        tracker.allocateNative(3);
        tracker.releaseNative(1);
        tracker.allocateHeap(2);
        tracker.releaseHeap(1);
        tracker.closeInner();

        assertEquals(0, tracker.usedNativeMemory());
        assertEquals(0, tracker.estimatedHeapMemory());
        assertEquals(0, inner1.usedNativeMemory());
        assertEquals(0, inner1.estimatedHeapMemory());
        assertEquals(2, tracker.unreleasedInnerScopeNative());
        assertEquals(1, tracker.unreleasedInnerScopeHeap());

        final var inner2 = new LocalMemoryTracker();
        tracker.setInnerDelegate(inner2);
        tracker.allocateNative(4);
        tracker.releaseNative(3);
        tracker.allocateHeap(10);
        tracker.releaseHeap(5);
        tracker.closeInner();

        assertEquals(0, tracker.usedNativeMemory());
        assertEquals(0, tracker.estimatedHeapMemory());
        assertEquals(0, inner1.usedNativeMemory());
        assertEquals(0, inner1.estimatedHeapMemory());
        assertEquals(0, inner2.usedNativeMemory());
        assertEquals(0, inner2.estimatedHeapMemory());
        assertEquals(2 + 1, tracker.unreleasedInnerScopeNative());
        assertEquals(1 + 5, tracker.unreleasedInnerScopeHeap());

        final var inner3 = new LocalMemoryTracker();
        tracker.setInnerDelegate(inner3);
        tracker.allocateNative(20);
        tracker.releaseNative(10);
        tracker.allocateHeap(34);
        tracker.releaseHeap(30);
        tracker.closeInner();

        assertEquals(0, tracker.usedNativeMemory());
        assertEquals(0, tracker.estimatedHeapMemory());
        assertEquals(0, inner1.usedNativeMemory());
        assertEquals(0, inner1.estimatedHeapMemory());
        assertEquals(0, inner2.usedNativeMemory());
        assertEquals(0, inner2.estimatedHeapMemory());
        assertEquals(0, inner3.usedNativeMemory());
        assertEquals(0, inner3.estimatedHeapMemory());
        assertEquals(2 + 1 + 10, tracker.unreleasedInnerScopeNative());
        assertEquals(1 + 5 + 4, tracker.unreleasedInnerScopeHeap());

        final var error = assertThrows(AssertionError.class, tracker::close);
        assertEquals("Unreleased inner native memory", error.getMessage());
    }

    @Test
    void assertNoUnreleasedInnerOnClose2() {
        final var outer = new LocalMemoryTracker();
        final var tracker = new RebindableDualScopedMemoryTracker(outer);

        final var inner1 = new LocalMemoryTracker();
        tracker.setInnerDelegate(inner1);
        tracker.allocateNative(3);
        tracker.releaseNative(3);
        tracker.allocateHeap(2);
        tracker.releaseHeap(1);
        tracker.closeInner();

        assertEquals(0, tracker.usedNativeMemory());
        assertEquals(0, tracker.estimatedHeapMemory());
        assertEquals(0, inner1.usedNativeMemory());
        assertEquals(0, inner1.estimatedHeapMemory());
        assertEquals(0, tracker.unreleasedInnerScopeNative());
        assertEquals(1, tracker.unreleasedInnerScopeHeap());

        final var error = assertThrows(AssertionError.class, tracker::close);
        assertEquals("Unreleased inner heap memory", error.getMessage());
    }

    @Test
    void assertNoUnreleasedInnerOnClose3() {
        final var outer = new LocalMemoryTracker();
        final var tracker = new RebindableDualScopedMemoryTracker(outer);

        final var inner1 = new LocalMemoryTracker();
        tracker.setInnerDelegate(inner1);
        tracker.allocateNative(3);
        tracker.releaseNative(1);
        tracker.allocateHeap(2);
        tracker.releaseHeap(1);
        tracker.closeInner();

        assertEquals(0, tracker.usedNativeMemory());
        assertEquals(0, tracker.estimatedHeapMemory());
        assertEquals(0, inner1.usedNativeMemory());
        assertEquals(0, inner1.estimatedHeapMemory());
        assertEquals(2, tracker.unreleasedInnerScopeNative());
        assertEquals(1, tracker.unreleasedInnerScopeHeap());

        final var inner2 = new LocalMemoryTracker();
        tracker.setInnerDelegate(inner2);
        tracker.allocateNative(4);
        tracker.releaseNative(3);
        tracker.allocateHeap(10);
        tracker.releaseHeap(5);
        tracker.closeInner();

        assertEquals(0, tracker.usedNativeMemory());
        assertEquals(0, tracker.estimatedHeapMemory());
        assertEquals(0, inner1.usedNativeMemory());
        assertEquals(0, inner1.estimatedHeapMemory());
        assertEquals(0, inner2.usedNativeMemory());
        assertEquals(0, inner2.estimatedHeapMemory());
        assertEquals(2 + 1, tracker.unreleasedInnerScopeNative());
        assertEquals(1 + 5, tracker.unreleasedInnerScopeHeap());

        final var inner3 = new LocalMemoryTracker();
        tracker.setInnerDelegate(inner3);
        tracker.allocateNative(20);
        tracker.releaseNative(20 + 2 + 1);
        tracker.allocateHeap(34);
        tracker.releaseHeap(34 + 1 + 5);
        tracker.closeInner();

        assertEquals(0, tracker.usedNativeMemory());
        assertEquals(0, tracker.estimatedHeapMemory());
        assertEquals(0, inner1.usedNativeMemory());
        assertEquals(0, inner1.estimatedHeapMemory());
        assertEquals(0, inner2.usedNativeMemory());
        assertEquals(0, inner2.estimatedHeapMemory());
        assertEquals(0, inner3.usedNativeMemory());
        assertEquals(0, inner3.estimatedHeapMemory());
        assertEquals(0, tracker.unreleasedInnerScopeNative());
        assertEquals(0, tracker.unreleasedInnerScopeHeap());

        tracker.close();
    }
}
