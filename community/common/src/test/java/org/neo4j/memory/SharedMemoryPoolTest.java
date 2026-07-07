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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.junit.jupiter.api.Test;

class SharedMemoryPoolTest {
    private static final long GLOBAL_BYTES = 1000;

    private final SharedMemoryPool pool = new SharedMemoryPool(MemoryGroup.RAFT, GLOBAL_BYTES, "shared.limit");

    @Test
    void reserveHeapTracksUsageAndReducesFree() {
        // when
        pool.reserveHeap(200);

        // then
        assertThat(pool.usedHeap()).isEqualTo(200);
        assertThat(pool.free()).isEqualTo(GLOBAL_BYTES - 200);
        assertThat(pool.totalSize()).isEqualTo(GLOBAL_BYTES);
    }

    @Test
    void reserveHeapBeyondTotalSizeThrows() {
        // then
        assertThatThrownBy(() -> pool.reserveHeap(GLOBAL_BYTES + 1)).isInstanceOf(MemoryLimitExceededException.class);
    }

    @Test
    void reserveHeapNoThrowBeyondTotalSizeIsRecordedWithoutThrowing() {
        // when
        assertDoesNotThrow(() -> pool.reserveHeapNoThrow(GLOBAL_BYTES + 50));

        // then
        assertThat(pool.usedHeap()).isEqualTo(GLOBAL_BYTES + 50);
    }

    @Test
    void releaseHeapReturnsCapacityToPool() {
        // given
        pool.reserveHeap(200);

        // when
        pool.releaseHeap(150);

        // then
        assertThat(pool.usedHeap()).isEqualTo(50);
        assertThat(pool.free()).isEqualTo(GLOBAL_BYTES - 50);
    }

    @Test
    void reserveNativeTracksUsageAndReducesFree() {
        // when
        pool.reserveNative(200);

        // then
        assertThat(pool.usedNative()).isEqualTo(200);
        assertThat(pool.free()).isEqualTo(GLOBAL_BYTES - 200);
    }

    @Test
    void reserveNativeBeyondTotalSizeThrows() {
        // then
        assertThatThrownBy(() -> pool.reserveNative(GLOBAL_BYTES + 1)).isInstanceOf(MemoryLimitExceededException.class);
    }

    @Test
    void reserveNativeNoThrowBeyondTotalSizeIsRecordedWithoutThrowing() {
        // when
        assertDoesNotThrow(() -> pool.reserveNativeNoThrow(GLOBAL_BYTES + 50));

        // then
        assertThat(pool.usedNative()).isEqualTo(GLOBAL_BYTES + 50);
    }

    @Test
    void releaseNativeReturnsCapacityToPool() {
        // given
        pool.reserveNative(200);

        // when
        pool.releaseNative(150);

        // then
        assertThat(pool.usedNative()).isEqualTo(50);
        assertThat(pool.free()).isEqualTo(GLOBAL_BYTES - 50);
    }

    @Test
    void resizeGrowsTotalSizeByDelta() {
        // when
        pool.resize(500);

        // then
        assertThat(pool.totalSize()).isEqualTo(GLOBAL_BYTES + 500);
        assertThat(pool.free()).isEqualTo(GLOBAL_BYTES + 500);
    }

    @Test
    void resizeShrinkingReducesReservableCapacity() {
        // given
        pool.resize(-500);

        // when
        pool.reserveHeap(GLOBAL_BYTES - 500);

        // then
        assertThatThrownBy(() -> pool.reserveHeap(1)).isInstanceOf(MemoryLimitExceededException.class);
    }

    @Test
    void getPoolMemoryTrackerThrows() {
        assertThatThrownBy(pool::getPoolMemoryTracker).isInstanceOf(UnsupportedOperationException.class);
    }
}
