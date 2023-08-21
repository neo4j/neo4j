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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.memory.MemoryPools.NO_TRACKING;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.Test;

class LocalMemoryTrackerTest {
    @Test
    void trackDirectMemoryAllocations() {
        LocalMemoryTracker memoryTracker = new LocalMemoryTracker();
        memoryTracker.allocateNative(10);
        memoryTracker.allocateNative(20);
        memoryTracker.allocateNative(40);
        assertEquals(70, memoryTracker.usedNativeMemory());
    }

    @Test
    void trackDirectMemoryDeallocations() {
        LocalMemoryTracker memoryTracker = new LocalMemoryTracker();
        memoryTracker.allocateNative(100);
        assertEquals(100, memoryTracker.usedNativeMemory());

        memoryTracker.releaseNative(20);
        assertEquals(80, memoryTracker.usedNativeMemory());

        memoryTracker.releaseNative(40);
        assertEquals(40, memoryTracker.usedNativeMemory());
    }

    @Test
    void trackHeapMemoryAllocations() {
        LocalMemoryTracker memoryTracker = new LocalMemoryTracker();
        memoryTracker.allocateHeap(10);
        memoryTracker.allocateHeap(20);
        memoryTracker.allocateHeap(40);
        assertEquals(70, memoryTracker.estimatedHeapMemory());
    }

    @Test
    void trackHeapMemoryDeallocations() {
        LocalMemoryTracker memoryTracker = new LocalMemoryTracker();
        memoryTracker.allocateHeap(100);
        assertEquals(100, memoryTracker.estimatedHeapMemory());

        memoryTracker.releaseHeap(20);
        assertEquals(80, memoryTracker.estimatedHeapMemory());

        memoryTracker.releaseHeap(40);
        assertEquals(40, memoryTracker.estimatedHeapMemory());
    }

    @Test
    void throwsOnLimitHeap() {
        var memoryTracker = new LocalMemoryTracker(NO_TRACKING, 10, 0, "settingName");
        assertThatThrownBy(() -> memoryTracker.allocateHeap(100))
                .isInstanceOf(MemoryLimitExceededException.class)
                .hasMessageContaining("settingName");
        assertThat(memoryTracker.estimatedHeapMemory()).isEqualTo(0);
    }

    @Test
    void throwsOnLimitNative() {
        var memoryTracker = new LocalMemoryTracker(NO_TRACKING, 10, 0, "settingName");
        assertThatThrownBy(() -> memoryTracker.allocateNative(100))
                .isInstanceOf(MemoryLimitExceededException.class)
                .hasMessageContaining("settingName");
        assertThat(memoryTracker.usedNativeMemory()).isEqualTo(0);
    }

    @Test
    void throwsOnPoolLimitHeap() {
        var pool = new MemoryPoolImpl(5, true, "poolSetting");
        var tracker = new LocalMemoryTracker(pool, 10, 0, "localSetting");

        assertThatThrownBy(() -> tracker.allocateHeap(10))
                .isInstanceOf(MemoryLimitExceededException.class)
                .hasMessageContaining("poolSetting");

        assertThat(tracker.estimatedHeapMemory()).isEqualTo(0);
    }

    @Test
    void throwsOnPoolLimitNative() {
        var pool = new MemoryPoolImpl(5, true, "poolSetting");
        var tracker = new LocalMemoryTracker(pool, 10, 0, "localSetting");

        assertThatThrownBy(() -> tracker.allocateNative(10))
                .isInstanceOf(MemoryLimitExceededException.class)
                .hasMessageContaining("poolSetting");

        assertThat(tracker.usedNativeMemory()).isEqualTo(0);
    }

    @Test
    void reuseAfterReportedDirectMemoryLeak() {
        var tracker = new LocalMemoryTracker(NO_TRACKING, 10, 0, "localSetting");
        tracker.allocateNative(10);
        assertThatThrownBy(tracker::reset)
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("Potential direct memory leak");

        // can be reused after reset
        tracker.allocateNative(10);
        tracker.releaseNative(10);
        tracker.reset();
    }

    @Test
    void shouldTellMonitorOnNativeMemoryLeak() {
        // given
        var leakedMemory = new MutableLong(-1);
        var tracker = new LocalMemoryTracker(
                NO_TRACKING, Long.MAX_VALUE, 100, "setting", () -> true, leakedNativeMemoryBytes -> {
                    assertThat(leakedMemory.longValue()).isEqualTo(-1);
                    leakedMemory.setValue(leakedNativeMemoryBytes);
                });

        // when
        var leakedAmount = 123;
        tracker.allocateNative(leakedAmount);
        tracker.allocateNative(5);
        tracker.releaseNative(5);
        // There's still an assertion on memory leak in reset(), so when testing this an exception will be thrown.
        assertThatThrownBy(tracker::reset).isInstanceOf(AssertionError.class);

        // then
        assertThat(leakedMemory.longValue()).isEqualTo(leakedAmount);
    }
}
