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
package org.neo4j.bolt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.memory.MemoryGroup.OTHER;

import io.netty.buffer.ByteBufAllocatorMetric;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.transport.NettyMemoryPool;
import org.neo4j.io.ByteUnit;
import org.neo4j.memory.MemoryPools;

class NettyMemoryPoolIT {
    private final int requestedSize = (int) ByteUnit.kibiBytes(10);

    private static class SomeNettyMemoryPool extends NettyMemoryPool {
        public SomeNettyMemoryPool(MemoryPools memoryPools, ByteBufAllocatorMetric allocatorMetric) {
            super(memoryPools, allocatorMetric, OTHER);
        }
    }

    @Test
    void reportConsumedHeapMemory() {
        var bufAllocator = createTestAllocator(false);
        var allocatorMetric = bufAllocator.metric();
        assertThat(allocatorMetric.usedDirectMemory()).isZero();
        assertThat(allocatorMetric.usedDirectMemory()).isZero();

        var memoryTracker = new SomeNettyMemoryPool(new MemoryPools(), allocatorMetric);
        var buffer = bufAllocator.buffer(requestedSize);
        try {
            assertThat(buffer.capacity()).isEqualTo(requestedSize);
            assertThat(memoryTracker.usedHeap()).isEqualTo(requestedSize);
            assertThat(memoryTracker.totalUsed()).isEqualTo(requestedSize);
        } finally {
            buffer.release();
        }

        assertThat(memoryTracker.usedHeap()).isZero();
        assertThat(memoryTracker.totalUsed()).isZero();
    }

    @Test
    void reportConsumedDirectMemory() {
        var bufAllocator = createTestAllocator(true);
        var allocatorMetric = bufAllocator.metric();
        assertThat(allocatorMetric.usedDirectMemory()).isZero();
        assertThat(allocatorMetric.usedDirectMemory()).isZero();

        var memoryTracker = new SomeNettyMemoryPool(new MemoryPools(), allocatorMetric);
        var buffer = bufAllocator.buffer(requestedSize);
        try {
            assertThat(buffer.capacity()).isEqualTo(requestedSize);
            assertThat(memoryTracker.usedNative()).isEqualTo(requestedSize);
            assertThat(memoryTracker.totalUsed()).isEqualTo(requestedSize);
        } finally {
            buffer.release();
        }

        assertThat(memoryTracker.usedNative()).isZero();
        assertThat(memoryTracker.totalUsed()).isZero();
    }

    private static PooledByteBufAllocator createTestAllocator(boolean preferDirect) {
        return new PooledByteBufAllocator(preferDirect, 1, 1, (int) ByteUnit.kibiBytes(4), 1, 0, 0, 0, true);
    }
}
