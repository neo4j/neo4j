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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.internal.batchimport.cache.NumberArrayFactories.HEAP;
import static org.neo4j.internal.batchimport.cache.NumberArrayFactories.OFF_HEAP;
import static org.neo4j.internal.helpers.collection.Iterables.single;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import org.junit.jupiter.api.Test;
import org.neo4j.internal.unsafe.NativeMemoryAllocationRefusedError;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryPools;
import org.neo4j.memory.MemoryTracker;

class NumberArrayFactoryTest {
    private static final long KILO = 1024;

    @Test
    void trackHeapMemoryAllocations() {
        var memoryTracker = new LocalMemoryTracker(MemoryPools.NO_TRACKING, 300, 0, null);
        HEAP.newByteArray(10, new byte[] {0}, memoryTracker);
        assertEquals(32, memoryTracker.estimatedHeapMemory());
        assertEquals(0, memoryTracker.usedNativeMemory());

        memoryTracker = new LocalMemoryTracker(MemoryPools.NO_TRACKING, 300, 0, null);
        HEAP.newLongArray(10, 1, memoryTracker);
        assertEquals(96, memoryTracker.estimatedHeapMemory());
        assertEquals(0, memoryTracker.usedNativeMemory());

        memoryTracker = new LocalMemoryTracker(MemoryPools.NO_TRACKING, 300, 0, null);
        HEAP.newIntArray(10, 1, memoryTracker);
        assertEquals(56, memoryTracker.estimatedHeapMemory());
        assertEquals(0, memoryTracker.usedNativeMemory());
    }

    @Test
    void trackNativeMemoryAllocations() {
        var memoryTracker = new LocalMemoryTracker(MemoryPools.NO_TRACKING, 300, 0, null);
        try (ByteArray byteArray = OFF_HEAP.newByteArray(10, new byte[] {0}, memoryTracker)) {
            assertEquals(0, memoryTracker.estimatedHeapMemory());
            assertEquals(10, memoryTracker.usedNativeMemory());
        }
        assertEquals(0, memoryTracker.usedNativeMemory());
        assertEquals(0, memoryTracker.estimatedHeapMemory());

        try (LongArray longArray = OFF_HEAP.newLongArray(10, 1, memoryTracker)) {
            assertEquals(0, memoryTracker.estimatedHeapMemory());
            assertEquals(80, memoryTracker.usedNativeMemory());
        }
        assertEquals(0, memoryTracker.usedNativeMemory());
        assertEquals(0, memoryTracker.estimatedHeapMemory());

        try (IntArray intArray = OFF_HEAP.newIntArray(10, 1, memoryTracker)) {
            assertEquals(0, memoryTracker.estimatedHeapMemory());
            assertEquals(40, memoryTracker.usedNativeMemory());
        }
        assertEquals(0, memoryTracker.usedNativeMemory());
        assertEquals(0, memoryTracker.estimatedHeapMemory());
    }

    @Test
    void shouldPickFirstAvailableCandidateLongArray() {
        // GIVEN
        NumberArrayFactory factory = new NumberArrayFactories.Auto(NumberArrayFactories.NO_MONITOR, HEAP);

        // WHEN
        LongArray array = factory.newLongArray(KILO, -1, INSTANCE);
        array.set(KILO - 10, 12345);

        // THEN
        assertTrue(array instanceof HeapLongArray);
        assertEquals(12345, array.get(KILO - 10));
    }

    @Test
    void shouldPickFirstAvailableCandidateLongArrayWhenSomeDontHaveEnoughMemory() {
        // GIVEN
        NumberArrayFactory lowMemoryFactory = mock(NumberArrayFactory.class);
        doThrow(OutOfMemoryError.class)
                .when(lowMemoryFactory)
                .newLongArray(anyLong(), anyLong(), anyLong(), any(MemoryTracker.class));
        NumberArrayFactory factory =
                new NumberArrayFactories.Auto(NumberArrayFactories.NO_MONITOR, lowMemoryFactory, HEAP);

        // WHEN
        LongArray array = factory.newLongArray(KILO, -1, INSTANCE);
        array.set(KILO - 10, 12345);

        // THEN
        verify(lowMemoryFactory).newLongArray(KILO, -1, 0, INSTANCE);
        assertTrue(array instanceof HeapLongArray);
        assertEquals(12345, array.get(KILO - 10));
    }

    @Test
    void shouldThrowOomOnNotEnoughMemory() {
        // GIVEN
        FailureMonitor monitor = new FailureMonitor();
        NumberArrayFactory lowMemoryFactory = mock(NumberArrayFactory.class);
        doThrow(OutOfMemoryError.class)
                .when(lowMemoryFactory)
                .newLongArray(anyLong(), anyLong(), anyLong(), any(MemoryTracker.class));
        NumberArrayFactory factory = new NumberArrayFactories.Auto(monitor, lowMemoryFactory);

        // WHEN
        assertThrows(OutOfMemoryError.class, () -> factory.newLongArray(KILO, -1, INSTANCE));
    }

    @Test
    void shouldPickFirstAvailableCandidateIntArray() {
        // GIVEN
        FailureMonitor monitor = new FailureMonitor();
        NumberArrayFactory factory = new NumberArrayFactories.Auto(monitor, HEAP);

        // WHEN
        IntArray array = factory.newIntArray(KILO, -1, INSTANCE);
        array.set(KILO - 10, 12345);

        // THEN
        assertTrue(array instanceof HeapIntArray);
        assertEquals(12345, array.get(KILO - 10));
        assertEquals(HEAP, monitor.successfulFactory);
        assertFalse(monitor.attemptedAllocationFailures.iterator().hasNext());
    }

    @Test
    void shouldPickFirstAvailableCandidateIntArrayWhenSomeThrowOutOfMemoryError() {
        // GIVEN
        NumberArrayFactory lowMemoryFactory = mock(NumberArrayFactory.class);
        doThrow(OutOfMemoryError.class)
                .when(lowMemoryFactory)
                .newIntArray(anyLong(), anyInt(), anyLong(), any(MemoryTracker.class));
        NumberArrayFactory factory =
                new NumberArrayFactories.Auto(NumberArrayFactories.NO_MONITOR, lowMemoryFactory, HEAP);

        // WHEN
        IntArray array = factory.newIntArray(KILO, -1, INSTANCE);
        array.set(KILO - 10, 12345);

        // THEN
        verify(lowMemoryFactory).newIntArray(KILO, -1, 0, INSTANCE);
        assertTrue(array instanceof HeapIntArray);
        assertEquals(12345, array.get(KILO - 10));
    }

    @Test
    void shouldPickFirstAvailableCandidateIntArrayWhenSomeThrowNativeMemoryAllocationRefusedError() {
        // GIVEN
        NumberArrayFactory lowMemoryFactory = mock(NumberArrayFactory.class);
        doThrow(NativeMemoryAllocationRefusedError.class)
                .when(lowMemoryFactory)
                .newIntArray(anyLong(), anyInt(), anyLong(), any(MemoryTracker.class));
        NumberArrayFactory factory =
                new NumberArrayFactories.Auto(NumberArrayFactories.NO_MONITOR, lowMemoryFactory, HEAP);

        // WHEN
        IntArray array = factory.newIntArray(KILO, -1, INSTANCE);
        array.set(KILO - 10, 12345);

        // THEN
        verify(lowMemoryFactory, times(1)).newIntArray(KILO, -1, 0, INSTANCE);
        assertTrue(array instanceof HeapIntArray);
        assertEquals(12345, array.get(KILO - 10));
    }

    @Test
    void shouldCatchArithmeticExceptionsAndTryNext() {
        // GIVEN
        NumberArrayFactory throwingMemoryFactory = mock(NumberArrayFactory.class);
        ArithmeticException failure = new ArithmeticException("This is an artificial failure");
        doThrow(failure)
                .when(throwingMemoryFactory)
                .newByteArray(anyLong(), any(byte[].class), anyLong(), any(MemoryTracker.class));
        FailureMonitor monitor = new FailureMonitor();
        NumberArrayFactory factory = new NumberArrayFactories.Auto(monitor, throwingMemoryFactory, HEAP);
        int itemSize = 4;

        // WHEN
        ByteArray array = factory.newByteArray(KILO, new byte[itemSize], 0, INSTANCE);
        array.setInt(KILO - 10, 0, 12345);

        // THEN
        verify(throwingMemoryFactory).newByteArray(eq(KILO), any(byte[].class), eq(0L), any(MemoryTracker.class));
        assertTrue(array instanceof HeapByteArray);
        assertEquals(12345, array.getInt(KILO - 10, 0));
        assertEquals(KILO * itemSize, monitor.memory);
        assertEquals(HEAP, monitor.successfulFactory);
        assertEquals(
                throwingMemoryFactory,
                single(monitor.attemptedAllocationFailures).getFactory());
        assertThat(single(monitor.attemptedAllocationFailures).getFailure().getMessage())
                .contains(failure.getMessage());
    }

    @Test
    void heapArrayShouldAllowVeryLargeBases() {
        NumberArrayFactory factory = new NumberArrayFactories.Auto(NumberArrayFactories.NO_MONITOR, HEAP);
        verifyVeryLargeBaseSupport(factory);
    }

    @Test
    void offHeapArrayShouldAllowVeryLargeBases() {
        NumberArrayFactory factory = new NumberArrayFactories.Auto(NumberArrayFactories.NO_MONITOR, OFF_HEAP);
        verifyVeryLargeBaseSupport(factory);
    }

    private static void verifyVeryLargeBaseSupport(NumberArrayFactory factory) {
        long base = Integer.MAX_VALUE * 1337L;
        byte[] into = new byte[1];
        into[0] = 1;
        factory.newByteArray(10, new byte[1], base, INSTANCE).get(base + 1, into);
        assertThat(into[0]).isEqualTo((byte) 0);
        assertThat(factory.newIntArray(10, 1, base, INSTANCE).get(base + 1)).isEqualTo(1);
        assertThat(factory.newLongArray(10, 1, base, INSTANCE).get(base + 1)).isEqualTo(1L);
    }

    @Test
    void heapArrayShouldThrowOnTooLargeArraySize() {
        assertThatThrownBy(() -> HEAP.newByteArray(Integer.MAX_VALUE / 2, new byte[10], 0, INSTANCE))
                .isInstanceOf(ArithmeticException.class)
                .hasMessage("integer overflow");
    }

    private static class FailureMonitor implements NumberArrayFactory.Monitor {
        private long memory;
        private NumberArrayFactory successfulFactory;
        private Iterable<NumberArrayFactory.AllocationFailure> attemptedAllocationFailures;

        @Override
        public void allocationSuccessful(
                long memory,
                NumberArrayFactory successfulFactory,
                Iterable<NumberArrayFactory.AllocationFailure> attemptedAllocationFailures) {
            this.memory = memory;
            this.successfulFactory = successfulFactory;
            this.attemptedAllocationFailures = attemptedAllocationFailures;
        }
    }
}
