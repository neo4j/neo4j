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
package org.neo4j.buffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.netty.buffer.ByteBuf;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.bufferpool.ByteBufferManger;
import org.neo4j.io.bufferpool.impl.NeoByteBufferPool;
import org.neo4j.memory.MemoryGroup;
import org.neo4j.memory.MemoryPool;
import org.neo4j.memory.MemoryPools;

class HeapMemoryMonitoringTest {
    private final MemoryPools memoryPools = new MemoryPools();
    private final ByteBufferManger byteBufferManger = new NeoByteBufferPool(memoryPools, null);
    private final NettyMemoryManagerWrapper nettyBufferAllocator = new NettyMemoryManagerWrapper(byteBufferManger);
    private MemoryPool memoryPool;

    @BeforeEach
    void beforeEach() {
        memoryPool = memoryPools.getPools().stream()
                .filter(pool -> pool.group() == MemoryGroup.CENTRAL_BYTE_BUFFER_MANAGER)
                .findFirst()
                .get();
    }

    @Test
    void testBasicAllocation() {
        ByteBuf buf = nettyBufferAllocator.heapBuffer(1500, 10_000);

        verifyMemory(1500);
        write(buf, 1000);
        buf.release();
        verifyMemory(0);
    }

    @Test
    void testBufferGrow() {
        ByteBuf buf = nettyBufferAllocator.heapBuffer(1500, 20_000);

        write(buf, 1000);
        verifyMemory(1500);
        write(buf, 1000);
        verifyMemory(ByteUnit.kibiBytes(2));
        write(buf, 1000);
        verifyMemory(ByteUnit.kibiBytes(4));
        write(buf, 10_000);
        verifyMemory(ByteUnit.kibiBytes(16));
        buf.release();
        verifyMemory(ByteUnit.kibiBytes(0));
    }

    @Test
    void testBasicCompositeBufferAllocation() {
        ByteBuf buf = nettyBufferAllocator.compositeHeapBuffer(10);

        write(buf, 1000);
        verifyMemory(ByteUnit.kibiBytes(1));
        buf.release();
        verifyMemory(ByteUnit.kibiBytes(0));
    }

    @Test
    void testCompositeBufferGrow() {
        ByteBuf buf = nettyBufferAllocator.compositeHeapBuffer(10);

        write(buf, 1000);
        verifyMemory(ByteUnit.kibiBytes(1));
        write(buf, 1000);
        verifyMemory(ByteUnit.kibiBytes(2));
        write(buf, 1000);
        verifyMemory(ByteUnit.kibiBytes(4));
        write(buf, 10_000);
        verifyMemory(ByteUnit.kibiBytes(16));
        buf.release();
        verifyMemory(ByteUnit.kibiBytes(0));
    }

    private static void write(ByteBuf buf, int size) {
        for (var i = 0; i < size; i++) {
            buf.writeByte(1);
        }
    }

    private void verifyMemory(long expectedHeap) {
        assertEquals(0, memoryPool.usedNative());
        assertEquals(expectedHeap, memoryPool.usedHeap());
    }
}
