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
package org.neo4j.bolt.protocol.common.connector.connection;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.neo4j.memory.MemoryPool;

class ConnectionMemoryTrackerTest {

    private MemoryPool pool;
    private InOrder inOrder;

    private ConnectionMemoryTracker memoryTracker;

    @BeforeEach
    void prepare() {
        this.pool = Mockito.mock(MemoryPool.class);
        this.memoryTracker = ConnectionMemoryTracker.createForPool(this.pool);

        this.inOrder = Mockito.inOrder(this.pool);

        this.inOrder.verify(this.pool).reserveHeap(ConnectionMemoryTracker.CHUNK_SIZE);
    }

    @Test
    void shouldReportEstimatedNativeMemory() {
        Assertions.assertThat(this.memoryTracker.usedNativeMemory()).isZero();

        this.memoryTracker.allocateHeap(42);

        Assertions.assertThat(this.memoryTracker.usedNativeMemory()).isZero();

        try {
            this.memoryTracker.allocateNative(42);
        } catch (UnsupportedOperationException ignore) {
        }

        Assertions.assertThat(this.memoryTracker.usedNativeMemory()).isZero();
    }

    @Test
    void shouldReportEstimatedHeapMemory() {
        Assertions.assertThat(this.memoryTracker.estimatedHeapMemory()).isZero();

        this.memoryTracker.allocateHeap(42);

        Assertions.assertThat(this.memoryTracker.estimatedHeapMemory()).isEqualTo(42);

        try {
            this.memoryTracker.allocateNative(84);
        } catch (UnsupportedOperationException ignore) {
        }

        Assertions.assertThat(this.memoryTracker.estimatedHeapMemory()).isEqualTo(42);

        this.memoryTracker.releaseHeap(2);

        Assertions.assertThat(this.memoryTracker.estimatedHeapMemory()).isEqualTo(40);
    }

    @Test
    void shouldNotPermitNativeAllocation() {
        Assertions.assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> this.memoryTracker.allocateNative(42))
                .withMessage("Reporting per-connection native allocation is not supported")
                .withNoCause();
    }

    @Test
    void shouldReleaseNativeMemory() {
        this.memoryTracker.releaseNative(42);

        this.inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldAllocateHeapMemory() {
        this.memoryTracker.allocateHeap(ConnectionMemoryTracker.CHUNK_SIZE - 1);

        // we deliberately requested less than the initially allocated memory, so we do not expect
        // the tracker to request any additional memory from its parent pool
        this.inOrder.verifyNoMoreInteractions();

        this.memoryTracker.allocateHeap(1);

        // we've now exceeded the pre-allocated memory within the tracker but do not expect it to
        // eagerly allocate additional memory as the request is easily satisfied given the current
        // local pool
        this.inOrder.verifyNoMoreInteractions();

        this.memoryTracker.allocateHeap(ConnectionMemoryTracker.CHUNK_SIZE - 1);

        // since the pre-allocated memory is exceeded, we now expect the tracker to request one
        // additional chunk as this easily fits the requested amount
        this.inOrder.verify(this.pool).reserveHeap(ConnectionMemoryTracker.CHUNK_SIZE);

        this.memoryTracker.allocateHeap(ConnectionMemoryTracker.CHUNK_SIZE * 2 + 1);

        // if we now request enough to fit two chunks worth of memory, we expect the tracker to
        // request three chunks to reduce contention on the parent pool
        this.inOrder.verify(this.pool).reserveHeap(ConnectionMemoryTracker.CHUNK_SIZE * 3);

        this.memoryTracker.allocateHeap(ConnectionMemoryTracker.CHUNK_SIZE + 5);

        // the same behavior is applicable if we request a value that is indivisible by the chunk
        // size - CHUNK_SIZE remains, so it is satisfied immediately, another chunk is requested to
        // satisfy the additional 5 bytes
        this.inOrder.verify(this.pool).reserveHeap(ConnectionMemoryTracker.CHUNK_SIZE);
    }

    @Test
    void shouldReleaseMemory() {
        this.memoryTracker.allocateHeap(ConnectionMemoryTracker.CHUNK_SIZE * 4);

        Assertions.assertThat(this.memoryTracker.estimatedHeapMemory())
                .isEqualTo(ConnectionMemoryTracker.CHUNK_SIZE * 4);
        Mockito.verify(this.pool).reserveHeap(ConnectionMemoryTracker.CHUNK_SIZE * 4);

        this.memoryTracker.releaseHeap(ConnectionMemoryTracker.CHUNK_SIZE * 3 + 1);

        Assertions.assertThat(this.memoryTracker.estimatedHeapMemory())
                .isEqualTo(ConnectionMemoryTracker.CHUNK_SIZE - 1);

        // when the resulting local pool is more than twice as large as the new allocation, a
        // quarter of it will be returned back to the parent pool
        Mockito.verify(this.pool).releaseHeap(64);
    }

    @Test
    void shouldFailWithIllegalStateWhenMoreHeapThanAllocatedIsReleased() {
        this.memoryTracker.allocateHeap(42);
        this.memoryTracker.releaseHeap(2);

        Assertions.assertThatExceptionOfType(AssertionError.class)
                .isThrownBy(() -> this.memoryTracker.releaseHeap(41))
                .withMessage("Can't release more than it was allocated. Allocated heap: 40, release request: 41")
                .withNoCause();
    }

    @Test
    void shouldReportHighWatermark() {
        Assertions.assertThat(this.memoryTracker.heapHighWaterMark()).isEqualTo(0);

        this.memoryTracker.allocateHeap(42);

        Assertions.assertThat(this.memoryTracker.heapHighWaterMark()).isEqualTo(42);

        this.memoryTracker.allocateHeap(2);

        Assertions.assertThat(this.memoryTracker.heapHighWaterMark()).isEqualTo(44);

        this.memoryTracker.releaseHeap(2);

        Assertions.assertThat(this.memoryTracker.heapHighWaterMark()).isEqualTo(44);

        this.memoryTracker.allocateHeap(1);

        Assertions.assertThat(this.memoryTracker.heapHighWaterMark()).isEqualTo(44);

        this.memoryTracker.allocateHeap(3);

        Assertions.assertThat(this.memoryTracker.heapHighWaterMark()).isEqualTo(46);
    }

    @Test
    void shouldConstructScopedTrackers() {
        var scopedTracker1 = this.memoryTracker.getScopedMemoryTracker();
        var scopedTracker2 = this.memoryTracker.getScopedMemoryTracker();

        Assertions.assertThat(scopedTracker1).isNotSameAs(scopedTracker2);

        scopedTracker1.allocateHeap(ConnectionMemoryTracker.CHUNK_SIZE / 2);

        Assertions.assertThat(this.memoryTracker.estimatedHeapMemory())
                .isEqualTo(ConnectionMemoryTracker.CHUNK_SIZE / 2);
        Assertions.assertThat(scopedTracker1.estimatedHeapMemory()).isEqualTo(ConnectionMemoryTracker.CHUNK_SIZE / 2);
        Assertions.assertThat(scopedTracker2.estimatedHeapMemory()).isZero();

        this.inOrder.verifyNoMoreInteractions();

        scopedTracker2.allocateHeap(ConnectionMemoryTracker.CHUNK_SIZE / 2);

        Assertions.assertThat(this.memoryTracker.estimatedHeapMemory()).isEqualTo(ConnectionMemoryTracker.CHUNK_SIZE);
        Assertions.assertThat(scopedTracker1.estimatedHeapMemory()).isEqualTo(ConnectionMemoryTracker.CHUNK_SIZE / 2);
        Assertions.assertThat(scopedTracker2.estimatedHeapMemory()).isEqualTo(ConnectionMemoryTracker.CHUNK_SIZE / 2);

        this.inOrder.verifyNoMoreInteractions();

        scopedTracker1.allocateHeap(1);

        Assertions.assertThat(this.memoryTracker.estimatedHeapMemory())
                .isEqualTo(ConnectionMemoryTracker.CHUNK_SIZE + 1);
        Assertions.assertThat(scopedTracker1.estimatedHeapMemory())
                .isEqualTo(ConnectionMemoryTracker.CHUNK_SIZE / 2 + 1);
        Assertions.assertThat(scopedTracker2.estimatedHeapMemory()).isEqualTo(ConnectionMemoryTracker.CHUNK_SIZE / 2);

        this.inOrder.verify(this.pool).reserveHeap(ConnectionMemoryTracker.CHUNK_SIZE);

        scopedTracker1.close();

        Assertions.assertThat(this.memoryTracker.estimatedHeapMemory())
                .isEqualTo(ConnectionMemoryTracker.CHUNK_SIZE / 2);
        Assertions.assertThat(scopedTracker2.estimatedHeapMemory()).isEqualTo(ConnectionMemoryTracker.CHUNK_SIZE / 2);

        Assertions.assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> scopedTracker2.allocateNative(42))
                .withMessage("Reporting per-connection native allocation is not supported")
                .withNoCause();

        scopedTracker2.close();

        Assertions.assertThat(this.memoryTracker.estimatedHeapMemory()).isZero();
    }

    @Test
    void shouldReset() {
        // seed the tracker with some amount of additional heap
        this.memoryTracker.allocateHeap(ConnectionMemoryTracker.CHUNK_SIZE * 2);

        Assertions.assertThat(this.memoryTracker.estimatedHeapMemory())
                .isEqualTo(ConnectionMemoryTracker.CHUNK_SIZE * 2);
        this.inOrder.verify(this.pool).reserveHeap(ConnectionMemoryTracker.CHUNK_SIZE * 2);

        this.memoryTracker.reset();

        // we expect everything but the initial allocation to be returned to the parent pool as a
        // result of this call
        Assertions.assertThat(this.memoryTracker.estimatedHeapMemory()).isZero();
        Assertions.assertThat(this.memoryTracker.heapHighWaterMark()).isZero();
        this.inOrder.verify(this.pool).releaseHeap(ConnectionMemoryTracker.CHUNK_SIZE * 2);
    }

    @Test
    void shouldLeaveInitialAllocationUntouchedOnReset() {
        this.memoryTracker.allocateHeap(4);

        Assertions.assertThat(this.memoryTracker.estimatedHeapMemory()).isEqualTo(4);
        this.inOrder.verifyNoMoreInteractions();

        this.memoryTracker.reset();

        Assertions.assertThat(this.memoryTracker.estimatedHeapMemory()).isZero();
        this.inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldReturnAdditionalAllocationsToThePoolOnReset() {
        this.memoryTracker.allocateHeap(ConnectionMemoryTracker.CHUNK_SIZE * 2);

        Assertions.assertThat(this.memoryTracker.estimatedHeapMemory())
                .isEqualTo(ConnectionMemoryTracker.CHUNK_SIZE * 2);
        this.inOrder.verify(this.pool).reserveHeap(ConnectionMemoryTracker.CHUNK_SIZE * 2);

        this.memoryTracker.reset();

        Assertions.assertThat(this.memoryTracker.estimatedHeapMemory()).isZero();
        this.inOrder.verify(this.pool).releaseHeap(ConnectionMemoryTracker.CHUNK_SIZE * 2);
    }

    @Test
    void shouldReturnFullAllocationOnClose() {
        this.memoryTracker.allocateHeap(ConnectionMemoryTracker.CHUNK_SIZE * 2);

        Assertions.assertThat(this.memoryTracker.estimatedHeapMemory())
                .isEqualTo(ConnectionMemoryTracker.CHUNK_SIZE * 2);
        this.inOrder.verify(this.pool).reserveHeap(ConnectionMemoryTracker.CHUNK_SIZE * 2);

        this.memoryTracker.releaseHeap(ConnectionMemoryTracker.CHUNK_SIZE);
        this.inOrder.verify(this.pool).releaseHeap(32);

        this.memoryTracker.close();

        this.inOrder.verify(this.pool).releaseHeap(ConnectionMemoryTracker.CHUNK_SIZE * 3 - 32);
        this.inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldReturnInitialAllocationOnClose() {
        Assertions.assertThat(this.memoryTracker.estimatedHeapMemory()).isZero();
        this.inOrder.verifyNoMoreInteractions();

        this.memoryTracker.close();

        this.inOrder.verify(this.pool).releaseHeap(ConnectionMemoryTracker.CHUNK_SIZE);
        this.inOrder.verifyNoMoreInteractions();
    }
}
